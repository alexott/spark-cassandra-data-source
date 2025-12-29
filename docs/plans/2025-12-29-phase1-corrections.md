# Phase 1 Implementation Plan - Corrections

**Date:** 2025-12-29
**Purpose:** Corrections to [2025-12-29-phase1-write-operations.md](2025-12-29-phase1-write-operations.md) based on inconsistency review

---

## Critical Corrections Required

### 1. Import Strategy - Use Executor-Side Imports Only

**Problem:** Task 2 imports `Cluster`, `PlainTextAuthProvider`, `ssl` at module top level, but also re-imports in `write()`.

**Solution:** Remove ALL top-level cassandra-driver imports. Import everything inside `write()` method only.

**Corrected code for Task 2 (`src/cassandra_data_source/__init__.py`):**

```python
"""PyCassandra - Python Data Source for Apache Cassandra."""

from pyspark.sql.datasource import DataSource, DataSourceWriter, DataSourceStreamWriter, SimpleCommitMessage
import uuid as uuid_lib  # Only standard library at top level


class CassandraDataSource(DataSource):
    """PySpark Data Source for Apache Cassandra."""

    @classmethod
    def name(cls):
        """Return the data source format name."""
        return "pycassandra"

    def __init__(self, options):
        """Initialize data source with options."""
        self.options = options

    def writer(self, schema, save_mode):
        """Return a batch writer instance."""
        return CassandraBatchWriter(self.options, schema)

    def streamWriter(self, schema, overwrite):
        """Return a streaming writer instance."""
        return CassandraStreamWriter(self.options, schema)


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
        # Import cassandra-driver on executor
        from cassandra.cluster import Cluster
        from cassandra.auth import PlainTextAuthProvider
        from cassandra.concurrent import execute_concurrent_with_args
        from cassandra.query import ConsistencyLevel
        import ssl

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

            # Process rows in batches
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

            return SimpleCommitMessage()

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


def convert_value(value, cassandra_type):
    """Convert a value to match Cassandra column type."""
    if value is None:
        return None

    cass_type = cassandra_type.lower()

    # UUID conversion
    if cass_type in ("uuid", "timeuuid"):
        if isinstance(value, str):
            try:
                return uuid_lib.UUID(value)
            except (ValueError, AttributeError) as e:
                raise ValueError(f"Invalid UUID string '{value}': {e}")
        elif isinstance(value, uuid_lib.UUID):
            return value
        else:
            raise ValueError(f"Cannot convert {type(value)} to UUID")

    # Integer conversions
    if cass_type == "int":
        if isinstance(value, int):
            # Check for overflow (Cassandra int is 32-bit)
            if value < -2**31 or value >= 2**31:
                raise ValueError(f"Integer overflow: {value} exceeds int32 range")
            return value

    if cass_type == "bigint":
        if isinstance(value, int):
            return value

    # String types - pass through
    if cass_type in ("text", "varchar", "ascii"):
        return value

    # For all other types, pass through and let Cassandra driver handle it
    return value


__all__ = ["CassandraDataSource", "convert_value"]
```

---

### 2. Fix Test Patching

**Problem:** Tests patch `cassandra_data_source.Cluster` at module level, but imports now happen inside `write()`.

**Solution:** Patch the imports inside the `write()` method. Tests need to patch the module where the function runs, not where it's imported.

**Corrected test in `tests/test_writer.py`:**

```python
def test_write_basic_insert(basic_options, sample_schema):
    """Test basic write operation with inserts."""
    from cassandra_data_source import CassandraDataSource
    from pyspark.sql import Row
    from unittest.mock import patch, MagicMock

    # Create mock cluster and session
    mock_cluster = MagicMock()
    mock_session = MagicMock()
    mock_cluster.connect.return_value = mock_session

    # Mock table metadata
    table_meta = MagicMock()
    table_meta.primary_key = [MagicMock(name="id")]
    id_col = MagicMock(name="id", cql_type="uuid")
    name_col = MagicMock(name="name", cql_type="text")
    age_col = MagicMock(name="age", cql_type="int")
    score_col = MagicMock(name="score", cql_type="bigint")
    table_meta.columns = {"id": id_col, "name": name_col, "age": age_col, "score": score_col}

    mock_cluster.metadata.keyspaces = {"test_ks": MagicMock(tables={"test_table": table_meta})}

    # Mock prepared statement
    prepared_stmt = MagicMock()
    mock_session.prepare.return_value = prepared_stmt

    # Mock execute_concurrent_with_args to return success
    def mock_execute(*args, **kwargs):
        return [MagicMock(success=True) for _ in range(len(args[2]))]

    # Patch at the point where write() imports them
    with patch("cassandra_data_source.Cluster", return_value=mock_cluster):
        with patch("cassandra_data_source.execute_concurrent_with_args", side_effect=mock_execute):
            ds = CassandraDataSource(basic_options)
            writer = ds.writer(sample_schema, None)

            # Create test data
            rows = [
                Row(id="550e8400-e29b-41d4-a716-446655440000", name="Alice", age=30, score=100),
                Row(id="550e8400-e29b-41d4-a716-446655440001", name="Bob", age=25, score=200)
            ]

            # Execute write
            result = writer.write(iter(rows))

            # Verify calls
            assert mock_session.prepare.called
            assert result is not None
```

---

### 3. Fix Streaming Integration Test

**Problem:** Test uses `spark.createDataFrame(...)` then `writeStream` which is invalid (not a streaming source).

**Solution:** Remove the invalid streaming integration test or replace with a manual test guide.

**Corrected approach in `tests/test_integration.py`:**

```python
# Remove test_write_streaming_integration entirely, or replace with:

@pytest.mark.integration
@pytest.mark.manual
def test_write_streaming_manual():
    """
    Manual test for streaming writes.

    To test streaming:
    1. Set up a streaming source (Kafka, socket, etc.)
    2. Use that source with pycassandra sink
    3. Verify data flows correctly

    This cannot be automated with createDataFrame + writeStream.
    """
    pytest.skip("Manual test - requires actual streaming source")
```

---

### 4. Update Test Fixtures to Match New Approach

**Problem:** `mock_cassandra_cluster` fixture creates temp connection that no longer happens in `__init__`.

**Solution:** Simplify fixtures since validation now happens in `write()`.

**Updated `tests/conftest.py`:**

```python
@pytest.fixture
def mock_table_metadata():
    """Mock table metadata for testing."""
    table_meta = MagicMock()
    table_meta.name = "test_table"

    # Mock primary key columns
    pk_col = MagicMock()
    pk_col.name = "id"
    table_meta.primary_key = [pk_col]

    # Mock column types
    id_col = MagicMock()
    id_col.name = "id"
    id_col.cql_type = "uuid"

    name_col = MagicMock()
    name_col.name = "name"
    name_col.cql_type = "text"

    age_col = MagicMock()
    age_col.name = "age"
    age_col.cql_type = "int"

    score_col = MagicMock()
    score_col.name = "score"
    score_col.cql_type = "bigint"

    table_meta.columns = {
        "id": id_col,
        "name": name_col,
        "age": age_col,
        "score": score_col
    }

    return table_meta
```

---

### 5. Update Option Tests for New Validations

**Add to `tests/test_options.py`:**

```python
def test_invalid_consistency_level():
    """Test that invalid consistency level raises ValueError."""
    from cassandra_data_source import CassandraDataSource
    from pyspark.sql.types import StructType, StructField, StringType

    schema = StructType([StructField("id", StringType())])
    options = {
        "host": "127.0.0.1",
        "keyspace": "test_ks",
        "table": "test_table",
        "consistency": "INVALID_LEVEL"
    }

    ds = CassandraDataSource(options)

    with pytest.raises(ValueError, match="Invalid consistency level"):
        ds.writer(schema, None)


def test_ssl_ca_cert_without_ssl_enabled():
    """Test that ssl_ca_cert without ssl_enabled raises ValueError."""
    from cassandra_data_source import CassandraDataSource
    from pyspark.sql.types import StructType, StructField, StringType

    schema = StructType([StructField("id", StringType())])
    options = {
        "host": "127.0.0.1",
        "keyspace": "test_ks",
        "table": "test_table",
        "ssl_ca_cert": "/path/to/cert.pem"
        # ssl_enabled not set or false
    }

    ds = CassandraDataSource(options)

    with pytest.raises(ValueError, match="ssl_ca_cert requires ssl_enabled"):
        ds.writer(schema, None)


def test_delete_flag_column_without_value():
    """Test that delete_flag_column without value raises ValueError."""
    from cassandra_data_source import CassandraDataSource
    from pyspark.sql.types import StructType, StructField, StringType

    schema = StructType([StructField("id", StringType())])
    options = {
        "host": "127.0.0.1",
        "keyspace": "test_ks",
        "table": "test_table",
        "delete_flag_column": "is_deleted"
        # delete_flag_value not provided
    }

    ds = CassandraDataSource(options)

    with pytest.raises(ValueError, match="must be specified together"):
        ds.writer(schema, None)
```

---

### 6. Update README Examples

**Update README.md to use new options:**

```markdown
### Write with Concurrency Control

```python
df.write.format("pycassandra") \
    .option("host", "127.0.0.1") \
    .option("keyspace", "myks") \
    .option("table", "users") \
    .option("concurrency", "50") \
    .option("rows_per_batch", "500") \
    .save()
```

### Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `concurrency` | 100 | Concurrent request count for execute_concurrent_with_args |
| `rows_per_batch` | 1000 | Number of rows to buffer before flushing |
| `consistency` | LOCAL_QUORUM | Write consistency (ONE, QUORUM, ALL, LOCAL_QUORUM, etc.) |
```

---

## Summary of Changes

1. **Imports**: Executor-side only (inside `write()` method)
2. **Options**: `batch_size` → `concurrency` + `rows_per_batch`
3. **Validation**: Consistency level, SSL, delete flag validated in `__init__`
4. **Metadata**: PK and schema validation deferred to `write()` method
5. **Batching**: Incremental flush every `rows_per_batch` rows
6. **Tests**: Patch targets fixed, streaming test removed/marked manual
7. **Error messages**: More detailed with context

## Implementation Priority

When implementing, follow this order:

1. Task 2 - Fix CassandraWriter class with correct imports and validation
2. Task 1 - Update test fixtures to match new approach
3. Task 3 - Type conversion (no changes needed)
4. Task 4 - Update write execution with incremental batching
5. Task 5 - Stream writer (no changes needed)
6. Task 6 - Fix integration tests
7. Task 7 - Update documentation

All other tasks (1, 3, 5, 7, 8) need minor adjustments to align with these core changes.
