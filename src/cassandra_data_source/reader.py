"""Cassandra reader implementations."""

from pyspark.sql.datasource import DataSourceReader

from .partitioning import TokenRangePartition  # noqa: F401 - used in Task 4
from .schema import derive_schema_from_table


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
        self.host = options["host"]
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
            "contact_points": [self.host],
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
            self.token_ranges = list(cluster.metadata.token_ranges())

        finally:
            cluster.shutdown()

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

        Returns one partition per Cassandra token range.
        """
        # This will be implemented in Task 4
        raise NotImplementedError("partitions() will be implemented in Task 4")

    def read(self, partition):
        """
        Read data from a partition.

        Args:
            partition: TokenRangePartition to read

        Yields:
            Tuples representing rows
        """
        # This will be implemented in Task 5
        raise NotImplementedError("read() will be implemented in Task 5")


class CassandraBatchReader(CassandraReader, DataSourceReader):
    """Batch reader for Cassandra."""
    pass
