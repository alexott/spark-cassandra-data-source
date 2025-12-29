"""Cassandra writer implementations."""

from pyspark.sql.datasource import DataSourceWriter, DataSourceStreamWriter


class CassandraWriter:
    """Base writer class with shared write logic."""

    # Valid Cassandra consistency levels
    VALID_CONSISTENCY_LEVELS = {
        "ONE", "TWO", "THREE", "QUORUM", "ALL",
        "LOCAL_QUORUM", "EACH_QUORUM", "LOCAL_ONE"
    }

    def __init__(self, options, schema):
        """Initialize writer and validate configuration."""
        self.options = options
        self.schema = schema

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

        # Write options
        self.concurrency = int(options.get("concurrency", 100))
        self.rows_per_batch = int(options.get("rows_per_batch", 1000))
        self.consistency = options.get("consistency", "LOCAL_QUORUM").upper()
        self.delete_flag_column = options.get("delete_flag_column")
        self.delete_flag_value = options.get("delete_flag_value")

        # Validate consistency level (fail fast)
        if self.consistency not in self.VALID_CONSISTENCY_LEVELS:
            raise ValueError(
                f"Invalid consistency level '{self.consistency}'. "
                f"Valid levels: {', '.join(sorted(self.VALID_CONSISTENCY_LEVELS))}"
            )

        # Validate delete flag options
        if bool(self.delete_flag_column) != bool(self.delete_flag_value):
            raise ValueError(
                "Both delete_flag_column and delete_flag_value must be specified together, or neither"
            )

        # Validate SSL options
        if self.ssl_ca_cert and not self.ssl_enabled:
            raise ValueError(
                "ssl_ca_cert requires ssl_enabled=true"
            )

        # Store metadata (will be populated in write())
        self.pk_columns = None
        self.column_types = None
        self._metadata_loaded = False

    def _validate_options(self):
        """Validate required options are present."""
        required = ["host", "keyspace", "table"]
        missing = [opt for opt in required if opt not in self.options]

        if missing:
            raise ValueError(f"Missing required options: {', '.join(missing)}")

    def _load_metadata_and_validate(self, cluster):
        """Load table metadata and validate schema (called once per partition)."""
        if self._metadata_loaded:
            return

        # Get table metadata
        table_meta = cluster.metadata.keyspaces[self.keyspace].tables[self.table]

        # Extract primary key column names
        self.pk_columns = [col.name for col in table_meta.primary_key]

        # Get all column metadata for type conversion
        self.column_types = {col.name: col.cql_type for col in table_meta.columns.values()}

        # Validate DataFrame schema contains all PK columns
        df_columns = set(field.name for field in self.schema.fields)
        missing_pks = [pk for pk in self.pk_columns if pk not in df_columns]

        if missing_pks:
            raise ValueError(
                f"DataFrame schema missing primary key columns: {', '.join(missing_pks)}. "
                f"Required PK columns: {', '.join(self.pk_columns)}"
            )

        # Validate delete flag column exists if specified
        if self.delete_flag_column and self.delete_flag_column not in df_columns:
            raise ValueError(
                f"delete_flag_column '{self.delete_flag_column}' not found in DataFrame schema. "
                f"Available columns: {', '.join(sorted(df_columns))}"
            )

        # Validate all DataFrame columns (except delete flag) exist in Cassandra
        cassandra_columns = set(self.column_types.keys())
        data_columns = df_columns - {self.delete_flag_column} if self.delete_flag_column else df_columns
        unknown_columns = data_columns - cassandra_columns

        if unknown_columns:
            raise ValueError(
                f"DataFrame contains columns not in Cassandra table: {', '.join(unknown_columns)}. "
                f"Cassandra columns: {', '.join(sorted(cassandra_columns))}"
            )

        self._metadata_loaded = True

    def write(self, iterator):
        """
        Write data to Cassandra with concurrent execution.

        This runs on executors, so import cassandra-driver here.
        Implements incremental batching with flush every rows_per_batch rows.
        """
        # Import cassandra-driver on executor
        from cassandra.cluster import Cluster
        from cassandra.auth import PlainTextAuthProvider
        from cassandra.concurrent import execute_concurrent_with_args
        from cassandra.query import ConsistencyLevel
        from pyspark.sql.datasource import WriterCommitMessage
        import ssl

        # Import type conversion from our module
        from cassandra_data_source.type_conversion import convert_value

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
            ssl_context = ssl.create_default_context()
            if self.ssl_ca_cert:
                ssl_context.load_verify_locations(self.ssl_ca_cert)
            kwargs["ssl_context"] = ssl_context

        cluster = Cluster(**kwargs)
        session = cluster.connect(self.keyspace)

        try:
            # Load metadata and validate (once per partition)
            self._load_metadata_and_validate(cluster)

            # Set consistency level
            consistency_map = {
                "ONE": ConsistencyLevel.ONE,
                "TWO": ConsistencyLevel.TWO,
                "THREE": ConsistencyLevel.THREE,
                "QUORUM": ConsistencyLevel.QUORUM,
                "ALL": ConsistencyLevel.ALL,
                "LOCAL_QUORUM": ConsistencyLevel.LOCAL_QUORUM,
                "EACH_QUORUM": ConsistencyLevel.EACH_QUORUM,
                "LOCAL_ONE": ConsistencyLevel.LOCAL_ONE
            }
            consistency_level = consistency_map[self.consistency]

            # Get all columns except delete flag
            data_columns = [field.name for field in self.schema.fields
                           if field.name != self.delete_flag_column]

            # Prepare INSERT statement
            columns_str = ", ".join(data_columns)
            placeholders = ", ".join(["?"] * len(data_columns))
            insert_cql = f"INSERT INTO {self.table} ({columns_str}) VALUES ({placeholders})"
            prepared_insert = session.prepare(insert_cql)
            prepared_insert.consistency_level = consistency_level

            # Prepare DELETE statement if needed
            prepared_delete = None
            if self.delete_flag_column:
                pk_placeholders = " AND ".join([f"{pk}=?" for pk in self.pk_columns])
                delete_cql = f"DELETE FROM {self.table} WHERE {pk_placeholders}"
                prepared_delete = session.prepare(delete_cql)
                prepared_delete.consistency_level = consistency_level

            # Process rows in batches with incremental flushing
            insert_params = []
            delete_params = []
            row_count = 0

            for row in iterator:
                row_dict = row.asDict() if hasattr(row, 'asDict') else dict(row)

                # Check if this is a delete
                is_delete = False
                if self.delete_flag_column:
                    flag_value = row_dict.get(self.delete_flag_column)
                    # Convert to string for comparison
                    if str(flag_value).lower() == self.delete_flag_value.lower():
                        is_delete = True

                if is_delete:
                    # Extract PK values for delete
                    pk_values = []
                    for pk in self.pk_columns:
                        value = row_dict[pk]
                        if value is None:
                            raise ValueError(f"Primary key column '{pk}' cannot be null for DELETE")
                        cassandra_type = self.column_types.get(pk, "text")
                        converted = convert_value(value, cassandra_type)
                        pk_values.append(converted)
                    delete_params.append(tuple(pk_values))
                else:
                    # Extract all data column values for insert
                    values = []
                    for col in data_columns:
                        value = row_dict.get(col)
                        # Check for null PKs
                        if col in self.pk_columns and value is None:
                            raise ValueError(
                                f"Primary key column '{col}' cannot be null for INSERT (row {row_count})"
                            )
                        cassandra_type = self.column_types.get(col, "text")
                        converted = convert_value(value, cassandra_type)
                        values.append(converted)
                    insert_params.append(tuple(values))

                row_count += 1

                # Flush batch if we hit rows_per_batch limit
                if row_count % self.rows_per_batch == 0:
                    if insert_params:
                        execute_concurrent_with_args(
                            session,
                            prepared_insert,
                            insert_params,
                            concurrency=self.concurrency,
                            raise_on_first_error=True
                        )
                        insert_params = []

                    if delete_params and prepared_delete:
                        execute_concurrent_with_args(
                            session,
                            prepared_delete,
                            delete_params,
                            concurrency=self.concurrency,
                            raise_on_first_error=True
                        )
                        delete_params = []

            # Flush remaining rows
            if insert_params:
                execute_concurrent_with_args(
                    session,
                    prepared_insert,
                    insert_params,
                    concurrency=self.concurrency,
                    raise_on_first_error=True
                )

            if delete_params and prepared_delete:
                execute_concurrent_with_args(
                    session,
                    prepared_delete,
                    delete_params,
                    concurrency=self.concurrency,
                    raise_on_first_error=True
                )

            return WriterCommitMessage()

        finally:
            cluster.shutdown()


class CassandraBatchWriter(CassandraWriter, DataSourceWriter):
    """Batch writer for Cassandra."""
    pass


class CassandraStreamWriter(CassandraWriter, DataSourceStreamWriter):
    """Streaming writer for Cassandra."""

    def commit(self, messages):
        """Handle successful batch completion."""
        pass

    def abort(self, messages):
        """Handle failed batch."""
        pass
