"""Cassandra reader implementations."""

from pyspark.sql.datasource import DataSourceReader

from .partitioning import TokenRangePartition  # noqa: F401 - used in Task 4
from .schema import derive_schema_from_table


def _convert_cassandra_value(value):
    """
    Convert Cassandra-specific types to Python native types.

    Args:
        value: Value from Cassandra row

    Returns:
        Converted value suitable for Spark
    """
    if value is None:
        return None

    # Import here to avoid module-level dependency
    from cassandra.util import (
        Date, Time, Duration, OrderedMap, SortedSet,
        Point, LineString, Polygon, Distance, DateRange
    )
    import uuid
    import ipaddress

    # Convert Cassandra Date to Python date
    if isinstance(value, Date):
        return value.date()

    # Convert Cassandra Time to long (nanoseconds) to match LongType schema
    if isinstance(value, Time):
        return value.nanosecond  # Fixed: use nanosecond not nanoseconds

    # Convert UUID to string
    if isinstance(value, uuid.UUID):
        return str(value)

    # Convert Duration to dict
    if isinstance(value, Duration):
        return {
            "months": value.months,
            "days": value.days,
            "nanoseconds": value.nanoseconds
        }

    # Convert OrderedMap to dict
    if isinstance(value, OrderedMap):
        return dict(value)

    # Convert SortedSet to list
    if isinstance(value, SortedSet):
        return list(value)

    # Convert IP addresses to string
    if isinstance(value, (ipaddress.IPv4Address, ipaddress.IPv6Address)):
        return str(value)

    # Convert geospatial types to string
    if isinstance(value, (Point, LineString, Polygon)):
        return str(value)

    # Convert Distance to float
    if isinstance(value, Distance):
        return float(value)

    # Convert DateRange to string
    if isinstance(value, DateRange):
        return str(value)

    # Pass through all other types (int, str, float, bool, bytes, Decimal, etc.)
    return value


class CassandraReader:
    """Base reader class for Cassandra data sources."""

    def __init__(self, options, schema):
        """
        Initialize reader and load metadata.

        Args:
            options: Configuration options dict
            schema: Optional user-provided schema
        """
        self.options = options
        self.user_schema = schema

        # Validate required options
        self._validate_options()

        # Extract connection options
        # Support comma-separated list of hosts for fault tolerance
        host_str = options["host"]
        self.hosts = [h.strip() for h in host_str.split(",")]
        self.port = int(options.get("port", 9042))
        self.keyspace = options["keyspace"]
        self.table = options["table"]
        self.username = options.get("username")
        self.password = options.get("password")
        self.ssl_enabled = options.get("ssl_enabled", "false").lower() == "true"
        self.ssl_ca_cert = options.get("ssl_ca_cert")

        # Read options
        self.consistency = options.get("consistency", "LOCAL_ONE").upper()
        self.filter = options.get("filter")  # Optional WHERE clause filter
        self.allow_filtering = options.get("allow_filtering", "false").lower() == "true"

        # Load metadata and schema
        self._load_metadata()

    def _validate_options(self):
        """Validate required options are present."""
        required = ["host", "keyspace", "table"]
        missing = [opt for opt in required if opt not in self.options]

        if missing:
            raise ValueError(f"Missing required options: {', '.join(missing)}")

    def _load_metadata(self):
        """Load table metadata and token ranges from Cassandra."""
        # Import cassandra-driver here (not at module level)
        from cassandra.cluster import Cluster
        from cassandra.auth import PlainTextAuthProvider
        import ssl as ssl_module

        # Create cluster connection
        kwargs = {
            "contact_points": self.hosts,
            "port": self.port
        }

        # Add authentication if provided
        if self.username and self.password:
            kwargs["auth_provider"] = PlainTextAuthProvider(
                username=self.username,
                password=self.password
            )

        # Add SSL if enabled
        if self.ssl_enabled:
            ssl_context = ssl_module.create_default_context()
            if self.ssl_ca_cert:
                ssl_context.load_verify_locations(self.ssl_ca_cert)
            kwargs["ssl_context"] = ssl_context

        cluster = Cluster(**kwargs)

        try:
            # Connect to populate metadata (session not needed)
            cluster.connect()

            # Get table metadata
            table_meta = cluster.metadata.keyspaces[self.keyspace].tables[self.table]

            # Store table metadata for later use
            self.table_metadata = table_meta

            # Extract partition key columns
            self.pk_columns = [col.name for col in table_meta.partition_key]

            # Get column types for type conversion
            self.column_types = {col.name: col.cql_type for col in table_meta.columns.values()}

            # Derive or validate schema
            if self.user_schema:
                # User provided schema - validate it matches table
                self._validate_user_schema(table_meta)
                self.schema = self.user_schema
                # Only read columns in user schema (projection)
                self.columns = [field.name for field in self.schema.fields]
            else:
                # Auto-derive schema from Cassandra
                self.schema = derive_schema_from_table(table_meta)
                self.columns = [field.name for field in self.schema.fields]

            # Get token ranges for partitioning
            # Compute ranges from the token ring
            self.token_ranges = self._get_token_ranges(cluster.metadata.token_map)

        finally:
            cluster.shutdown()

    def _get_token_ranges(self, token_map):
        """
        Compute token ranges from the token map.

        Args:
            token_map: Cassandra TokenMap object

        Returns:
            List of TokenRange objects (namedtuples with start/end)
        """
        from collections import namedtuple

        if not token_map or not token_map.ring:
            return []

        TokenRange = namedtuple('TokenRange', ['start', 'end'])
        ranges = []
        ring = sorted(token_map.ring)

        # Create ranges between consecutive tokens
        for i in range(len(ring)):
            start = ring[i]
            end = ring[(i + 1) % len(ring)]  # Wrap around to first token
            ranges.append(TokenRange(start=start, end=end))

        return ranges

    def _validate_user_schema(self, table_meta):
        """Validate user-provided schema matches table structure."""
        cassandra_columns = set(table_meta.columns.keys())
        schema_columns = set(field.name for field in self.user_schema.fields)

        # Check all schema columns exist in Cassandra table
        missing = schema_columns - cassandra_columns
        if missing:
            raise ValueError(
                f"Schema contains columns not in Cassandra table: {', '.join(missing)}. "
                f"Available columns: {', '.join(sorted(cassandra_columns))}"
            )

    def partitions(self):
        """
        Return list of partitions for parallel reading.

        Follows the TokenRangesScan.java pattern which creates different queries
        depending on the token range position. Some ranges may generate multiple
        partitions (e.g., first range creates two partitions).

        Returns:
            List of TokenRangePartition objects, one per query
        """
        if not self.token_ranges:
            # Empty table or no token ranges
            return []

        partitions = []
        sorted_ranges = sorted(self.token_ranges)
        partition_id = 0

        # Get min token for wrap-around handling (first range's start token)
        min_token_obj = sorted_ranges[0].start
        min_token = min_token_obj.value if hasattr(min_token_obj, 'value') else str(min_token_obj)

        for i, token_range in enumerate(sorted_ranges):
            start = token_range.start
            end = token_range.end

            # Extract token values
            start_value = start.value if hasattr(start, 'value') else str(start)
            end_value = end.value if hasattr(end, 'value') else str(end)

            # Following TokenRangesScan.java logic:
            if start_value == end_value:
                # Case 1: Degenerate single-node cluster (entire ring)
                # Query: token >= minToken
                partition = TokenRangePartition(
                    partition_id=partition_id,
                    start_token=min_token,
                    end_token=None,  # Unbounded
                    pk_columns=self.pk_columns,
                    is_wrap_around=True,
                    min_token=min_token
                )
                partitions.append(partition)
                partition_id += 1

            elif i == 0:
                # Case 2: First range - split into TWO partitions
                # Query 1: token <= minToken (wrap-around portion)
                partition1 = TokenRangePartition(
                    partition_id=partition_id,
                    start_token=None,  # Unbounded
                    end_token=min_token,
                    pk_columns=self.pk_columns,
                    is_wrap_around=True,
                    min_token=min_token
                )
                partitions.append(partition1)
                partition_id += 1

                # Query 2: token > start AND token <= end (normal portion)
                partition2 = TokenRangePartition(
                    partition_id=partition_id,
                    start_token=start_value,
                    end_token=end_value,
                    pk_columns=self.pk_columns,
                    is_wrap_around=False,
                    min_token=min_token
                )
                partitions.append(partition2)
                partition_id += 1

            elif end_value == min_token:
                # Case 3: Range ending at minToken
                # Query: token > start (no upper bound)
                partition = TokenRangePartition(
                    partition_id=partition_id,
                    start_token=start_value,
                    end_token=None,  # Unbounded
                    pk_columns=self.pk_columns,
                    is_wrap_around=False,
                    min_token=min_token
                )
                partitions.append(partition)
                partition_id += 1

            else:
                # Case 4: Normal range
                # Query: token > start AND token <= end
                partition = TokenRangePartition(
                    partition_id=partition_id,
                    start_token=start_value,
                    end_token=end_value,
                    pk_columns=self.pk_columns,
                    is_wrap_around=False,
                    min_token=min_token
                )
                partitions.append(partition)
                partition_id += 1

        return partitions

    def read(self, partition):
        """
        Read data from a partition using token range query.

        Args:
            partition: TokenRangePartition to read

        Yields:
            Tuples representing rows
        """
        # Import cassandra-driver on executor (inside the method)
        from cassandra.cluster import Cluster
        from cassandra.auth import PlainTextAuthProvider
        import ssl as ssl_module

        # Create cluster connection
        kwargs = {
            "contact_points": self.hosts,
            "port": self.port
        }

        # Add authentication if provided
        if self.username and self.password:
            kwargs["auth_provider"] = PlainTextAuthProvider(
                username=self.username,
                password=self.password
            )

        # Add SSL if enabled
        if self.ssl_enabled:
            ssl_context = ssl_module.create_default_context()
            if self.ssl_ca_cert:
                ssl_context.load_verify_locations(self.ssl_ca_cert)
            kwargs["ssl_context"] = ssl_context

        cluster = Cluster(**kwargs)

        try:
            session = cluster.connect(self.keyspace)

            # Build SELECT clause with columns
            columns_str = ", ".join(self.columns)

            # Build WHERE clause with token range
            pk_cols_str = ", ".join(partition.pk_columns)

            # Build token range condition based on partition properties
            if partition.start_token is None:
                # Unbounded start: token(pk) <= end_token
                where_clause = f"token({pk_cols_str}) <= {partition.end_token}"
            elif partition.end_token is None:
                # Unbounded end: token(pk) > start_token
                where_clause = f"token({pk_cols_str}) > {partition.start_token}"
            else:
                # Both bounds present: token(pk) > start_token AND token(pk) <= end_token
                where_clause = (
                    f"token({pk_cols_str}) > {partition.start_token} AND "
                    f"token({pk_cols_str}) <= {partition.end_token}"
                )

            # Add optional filter with AND if present
            if self.filter:
                where_clause = f"({where_clause}) AND ({self.filter})"

            # Build full query
            query = f"SELECT {columns_str} FROM {self.table} WHERE {where_clause}"

            # Append ALLOW FILTERING at the end if requested (must be outside WHERE clause)
            if self.allow_filtering:
                query = f"{query} ALLOW FILTERING"

            # Execute query with consistency level
            # Note: Consistency level should be set on the session or SimpleStatement
            # For now, we execute directly (can be enhanced later with SimpleStatement)
            result_set = session.execute(query)

            # Yield rows as tuples matching schema order
            for row in result_set:
                # Convert row to tuple matching schema column order
                # Row is a namedtuple, access attributes by name
                # Convert Cassandra types to Python types
                values = tuple(_convert_cassandra_value(getattr(row, col)) for col in self.columns)
                yield values

        finally:
            # Always close connection
            cluster.shutdown()


class CassandraBatchReader(CassandraReader, DataSourceReader):
    """Batch reader for Cassandra."""
    pass
