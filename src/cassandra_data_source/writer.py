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
        """Write data to Cassandra (implemented in executor)."""
        # This will be implemented in Task 4
        # For now, this is a placeholder to allow tests to pass
        pass


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
