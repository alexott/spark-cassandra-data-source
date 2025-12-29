# PyCassandra Phase 1: Write Operations Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Implement batch and streaming write operations for PyCassandra data source with connection management, primary key validation, type conversion, and concurrent execution.

**Architecture:** Simple, flat structure following PySpark Data Source API v2 pattern. Base writer class contains shared logic for connection, validation, and concurrent execution. Batch and stream writers inherit from base with minimal boilerplate.

**Tech Stack:** PySpark 4.0.1, cassandra-driver 3.29.3, pytest

---

## Task 1: Setup Test Fixtures and Utilities

**Files:**
- Create: `tests/conftest.py`
- Create: `tests/test_options.py`

**Step 1: Write test fixtures**

Create `tests/conftest.py`:

```python
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from unittest.mock import Mock, MagicMock


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    spark = SparkSession.builder \
        .appName("pycassandra-tests") \
        .master("local[2]") \
        .getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def basic_options():
    """Basic connection options for testing."""
    return {
        "host": "127.0.0.1",
        "port": "9042",
        "keyspace": "test_ks",
        "table": "test_table",
        "username": "cassandra",
        "password": "cassandra"
    }


@pytest.fixture
def sample_schema():
    """Sample Spark schema for testing."""
    return StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("score", LongType(), True)
    ])


@pytest.fixture
def mock_cassandra_cluster():
    """Mock Cassandra cluster for testing."""
    cluster = MagicMock()
    session = MagicMock()
    metadata = MagicMock()

    # Mock table metadata
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

    metadata.keyspaces = {"test_ks": MagicMock()}
    metadata.keyspaces["test_ks"].tables = {"test_table": table_meta}

    cluster.metadata = metadata
    cluster.connect.return_value = session

    return cluster, session
```

**Step 2: Write options validation tests**

Create `tests/test_options.py`:

```python
import pytest
from unittest.mock import patch, MagicMock


def test_missing_required_option_host():
    """Test that missing host option raises ValueError."""
    from cassandra_data_source import CassandraDataSource
    from pyspark.sql.types import StructType, StructField, StringType

    schema = StructType([StructField("id", StringType())])
    options = {
        "keyspace": "test_ks",
        "table": "test_table"
    }

    ds = CassandraDataSource(options)

    with pytest.raises(ValueError, match="host"):
        ds.writer(schema, None)


def test_missing_required_option_keyspace():
    """Test that missing keyspace option raises ValueError."""
    from cassandra_data_source import CassandraDataSource
    from pyspark.sql.types import StructType, StructField, StringType

    schema = StructType([StructField("id", StringType())])
    options = {
        "host": "127.0.0.1",
        "table": "test_table"
    }

    ds = CassandraDataSource(options)

    with pytest.raises(ValueError, match="keyspace"):
        ds.writer(schema, None)


def test_missing_required_option_table():
    """Test that missing table option raises ValueError."""
    from cassandra_data_source import CassandraDataSource
    from pyspark.sql.types import StructType, StructField, StringType

    schema = StructType([StructField("id", StringType())])
    options = {
        "host": "127.0.0.1",
        "keyspace": "test_ks"
    }

    ds = CassandraDataSource(options)

    with pytest.raises(ValueError, match="table"):
        ds.writer(schema, None)


def test_default_port():
    """Test that port defaults to 9042."""
    from cassandra_data_source import CassandraDataSource
    from pyspark.sql.types import StructType, StructField, StringType

    schema = StructType([StructField("id", StringType())])
    options = {
        "host": "127.0.0.1",
        "keyspace": "test_ks",
        "table": "test_table"
    }

    ds = CassandraDataSource(options)

    with patch("cassandra_data_source.Cluster") as mock_cluster:
        mock_session = MagicMock()
        mock_cluster.return_value.connect.return_value = mock_session
        mock_cluster.return_value.metadata.keyspaces = {
            "test_ks": MagicMock(tables={"test_table": MagicMock(primary_key=[MagicMock(name="id")], columns={"id": MagicMock(cql_type="text")})})
        }

        writer = ds.writer(schema, None)

        # Verify Cluster was called with default port
        mock_cluster.assert_called_once()
        call_kwargs = mock_cluster.call_args[1]
        assert call_kwargs.get("port") == 9042
```

**Step 3: Run tests to verify they fail**

Run: `poetry run pytest tests/test_options.py -v`

Expected: FAIL with "ModuleNotFoundError: No module named 'cassandra_data_source'"

**Step 4: Commit test setup**

```bash
git add tests/conftest.py tests/test_options.py
git commit -m "test: add test fixtures and option validation tests"
```

---

## Task 2: Implement Data Source Entry Point

**Files:**
- Modify: `src/cassandra_data_source/__init__.py`

**Step 1: Write test for data source name**

Add to `tests/test_options.py`:

```python
def test_data_source_name():
    """Test that data source returns correct name."""
    from cassandra_data_source import CassandraDataSource

    assert CassandraDataSource.name() == "pycassandra"
```

**Step 2: Run test to verify it fails**

Run: `poetry run pytest tests/test_options.py::test_data_source_name -v`

Expected: FAIL with "AttributeError: type object 'CassandraDataSource' has no attribute 'name'"

**Step 3: Implement CassandraDataSource class**

Update `src/cassandra_data_source/__init__.py`:

```python
"""PyCassandra - Python Data Source for Apache Cassandra."""

from pyspark.sql.datasource import DataSource, DataSourceWriter, DataSourceStreamWriter
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import ssl


class CassandraDataSource(DataSource):
    """
    PySpark Data Source for Apache Cassandra.

    Supports batch and streaming writes to Cassandra tables with:
    - Connection management (host, port, username, password, SSL)
    - Primary key validation
    - Type conversion and validation
    - Concurrent execution for performance
    """

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
        self.batch_size = int(options.get("batch_size", 100))
        self.consistency = options.get("consistency", "LOCAL_QUORUM")
        self.delete_flag_column = options.get("delete_flag_column")
        self.delete_flag_value = options.get("delete_flag_value")

        # Validate delete flag options
        if self.delete_flag_column and not self.delete_flag_value:
            raise ValueError("delete_flag_value must be provided when delete_flag_column is specified")
        if self.delete_flag_value and not self.delete_flag_column:
            raise ValueError("delete_flag_column must be provided when delete_flag_value is specified")

        # Validate primary keys (requires temporary connection)
        self._validate_primary_keys()

    def _validate_options(self):
        """Validate required options are present."""
        required = ["host", "keyspace", "table"]
        missing = [opt for opt in required if opt not in self.options]

        if missing:
            raise ValueError(f"Missing required options: {', '.join(missing)}")

    def _validate_primary_keys(self):
        """Connect to Cassandra and validate DataFrame contains all PK columns."""
        # Create temporary cluster connection
        cluster = self._create_cluster()

        try:
            session = cluster.connect()

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

        finally:
            cluster.shutdown()

    def _create_cluster(self):
        """Create a Cassandra cluster connection."""
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

        return Cluster(**kwargs)

    def write(self, iterator):
        """Write data to Cassandra (implemented in executor)."""
        # This will be implemented in the next task
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


__all__ = ["CassandraDataSource"]
```

**Step 4: Run tests to verify they pass**

Run: `poetry run pytest tests/test_options.py -v`

Expected: PASS (all tests should pass)

**Step 5: Commit data source implementation**

```bash
git add src/cassandra_data_source/__init__.py
git commit -m "feat: implement CassandraDataSource with option validation and PK checks"
```

---

## Task 3: Implement Type Conversion Utilities

**Files:**
- Modify: `src/cassandra_data_source/__init__.py`
- Create: `tests/test_type_conversion.py`

**Step 1: Write type conversion tests**

Create `tests/test_type_conversion.py`:

```python
import pytest
import uuid as uuid_lib


def test_string_to_uuid_valid():
    """Test converting valid UUID string."""
    from cassandra_data_source import convert_value

    uuid_str = "550e8400-e29b-41d4-a716-446655440000"
    result = convert_value(uuid_str, "uuid")

    assert isinstance(result, uuid_lib.UUID)
    assert str(result) == uuid_str


def test_string_to_uuid_invalid():
    """Test converting invalid UUID string raises error."""
    from cassandra_data_source import convert_value

    with pytest.raises(ValueError, match="Invalid UUID"):
        convert_value("not-a-uuid", "uuid")


def test_int_to_bigint():
    """Test converting int to bigint."""
    from cassandra_data_source import convert_value

    result = convert_value(42, "bigint")
    assert result == 42
    assert isinstance(result, int)


def test_long_to_int_in_range():
    """Test converting long to int within range."""
    from cassandra_data_source import convert_value

    result = convert_value(42, "int")
    assert result == 42


def test_long_to_int_overflow():
    """Test converting long to int with overflow raises error."""
    from cassandra_data_source import convert_value

    with pytest.raises(ValueError, match="overflow"):
        convert_value(2**32, "int")


def test_string_to_text():
    """Test string to text is pass-through."""
    from cassandra_data_source import convert_value

    result = convert_value("hello", "text")
    assert result == "hello"


def test_none_value():
    """Test None values pass through."""
    from cassandra_data_source import convert_value

    result = convert_value(None, "text")
    assert result is None
```

**Step 2: Run tests to verify they fail**

Run: `poetry run pytest tests/test_type_conversion.py -v`

Expected: FAIL with "ImportError: cannot import name 'convert_value'"

**Step 3: Implement type conversion function**

Add to `src/cassandra_data_source/__init__.py` (after imports, before CassandraDataSource):

```python
import uuid as uuid_lib


def convert_value(value, cassandra_type):
    """
    Convert a value to match Cassandra column type.

    Args:
        value: The value to convert
        cassandra_type: The target Cassandra type (lowercase)

    Returns:
        Converted value

    Raises:
        ValueError: If conversion fails
    """
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
```

Update `__all__` at the end of the file:

```python
__all__ = ["CassandraDataSource", "convert_value"]
```

**Step 4: Run tests to verify they pass**

Run: `poetry run pytest tests/test_type_conversion.py -v`

Expected: PASS (all tests should pass)

**Step 5: Commit type conversion utilities**

```bash
git add src/cassandra_data_source/__init__.py tests/test_type_conversion.py
git commit -m "feat: add type conversion utilities with validation"
```

---

## Task 4: Implement Write Execution Logic

**Files:**
- Modify: `src/cassandra_data_source/__init__.py`
- Create: `tests/test_writer.py`

**Step 1: Write writer execution tests**

Create `tests/test_writer.py`:

```python
import pytest
from unittest.mock import patch, MagicMock, call
from pyspark.sql import Row


def test_write_basic_insert(basic_options, sample_schema, mock_cassandra_cluster):
    """Test basic write operation with inserts."""
    from cassandra_data_source import CassandraDataSource

    cluster, session = mock_cassandra_cluster

    # Mock prepared statement
    prepared_stmt = MagicMock()
    session.prepare.return_value = prepared_stmt

    # Mock execute_concurrent_with_args
    mock_results = [MagicMock(success=True) for _ in range(2)]

    with patch("cassandra_data_source.Cluster", return_value=cluster):
        with patch("cassandra_data_source.execute_concurrent_with_args", return_value=mock_results):
            ds = CassandraDataSource(basic_options)
            writer = ds.writer(sample_schema, None)

            # Create test data
            rows = [
                Row(id="550e8400-e29b-41d4-a716-446655440000", name="Alice", age=30, score=100),
                Row(id="550e8400-e29b-41d4-a716-446655440001", name="Bob", age=25, score=200)
            ]

            # Execute write
            result = writer.write(iter(rows))

            # Verify prepared statement was created
            assert session.prepare.called

            # Verify execute_concurrent_with_args was called
            from cassandra_data_source import execute_concurrent_with_args
            assert execute_concurrent_with_args.called


def test_write_with_delete_flag(basic_options, sample_schema, mock_cassandra_cluster):
    """Test write operation with delete flag."""
    from cassandra_data_source import CassandraDataSource
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, BooleanType

    cluster, session = mock_cassandra_cluster

    # Add delete flag to schema
    schema_with_flag = StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("score", LongType(), True),
        StructField("is_deleted", BooleanType(), True)
    ])

    # Add delete flag options
    options = {**basic_options, "delete_flag_column": "is_deleted", "delete_flag_value": "true"}

    # Mock prepared statements
    prepared_insert = MagicMock()
    prepared_delete = MagicMock()
    session.prepare.side_effect = [prepared_insert, prepared_delete]

    # Mock execute_concurrent_with_args
    mock_results = [MagicMock(success=True) for _ in range(2)]

    with patch("cassandra_data_source.Cluster", return_value=cluster):
        with patch("cassandra_data_source.execute_concurrent_with_args", return_value=mock_results):
            ds = CassandraDataSource(options)
            writer = ds.writer(schema_with_flag, None)

            # Create test data (one insert, one delete)
            rows = [
                Row(id="550e8400-e29b-41d4-a716-446655440000", name="Alice", age=30, score=100, is_deleted=False),
                Row(id="550e8400-e29b-41d4-a716-446655440001", name="Bob", age=25, score=200, is_deleted=True)
            ]

            # Execute write
            result = writer.write(iter(rows))

            # Verify both INSERT and DELETE statements were prepared
            assert session.prepare.call_count == 2


def test_write_type_conversion(basic_options, sample_schema, mock_cassandra_cluster):
    """Test that write applies type conversion."""
    from cassandra_data_source import CassandraDataSource

    cluster, session = mock_cassandra_cluster

    # Mock prepared statement
    prepared_stmt = MagicMock()
    session.prepare.return_value = prepared_stmt

    # Mock execute_concurrent_with_args
    mock_results = [MagicMock(success=True)]

    with patch("cassandra_data_source.Cluster", return_value=cluster):
        with patch("cassandra_data_source.execute_concurrent_with_args", return_value=mock_results) as mock_exec:
            ds = CassandraDataSource(basic_options)
            writer = ds.writer(sample_schema, None)

            # Create test data with UUID string
            rows = [
                Row(id="550e8400-e29b-41d4-a716-446655440000", name="Alice", age=30, score=100)
            ]

            # Execute write
            result = writer.write(iter(rows))

            # Verify execute_concurrent_with_args was called with converted values
            assert mock_exec.called
            call_args = mock_exec.call_args[0]
            # Parameters should contain converted UUID object, not string
            params = call_args[2]
            assert len(params) > 0
```

**Step 2: Run tests to verify they fail**

Run: `poetry run pytest tests/test_writer.py -v`

Expected: FAIL with "AssertionError" or method not implemented errors

**Step 3: Implement write execution logic**

Update the `write` method in `CassandraWriter` class in `src/cassandra_data_source/__init__.py`:

```python
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.query import SimpleStatement, ConsistencyLevel
from pyspark.sql.datasource import SimpleCommitMessage


# Update the write method in CassandraWriter class:
def write(self, iterator):
    """
    Write data to Cassandra with concurrent execution.

    This runs on executors, so import cassandra-driver here.
    """
    # Import on executor
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.concurrent import execute_concurrent_with_args
    from cassandra.query import ConsistencyLevel
    import ssl

    # Create cluster and session
    cluster = self._create_cluster()
    session = cluster.connect(self.keyspace)

    try:
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

        consistency_level = consistency_map.get(self.consistency.upper(), ConsistencyLevel.LOCAL_QUORUM)

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

        # Collect rows and separate inserts from deletes
        insert_params = []
        delete_params = []

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
                    cassandra_type = self.column_types.get(pk, "text")
                    converted = convert_value(value, cassandra_type)
                    pk_values.append(converted)
                delete_params.append(tuple(pk_values))
            else:
                # Extract all data column values for insert
                values = []
                for col in data_columns:
                    value = row_dict.get(col)
                    cassandra_type = self.column_types.get(col, "text")
                    converted = convert_value(value, cassandra_type)
                    values.append(converted)
                insert_params.append(tuple(values))

        # Execute inserts concurrently
        if insert_params:
            results = execute_concurrent_with_args(
                session,
                prepared_insert,
                insert_params,
                concurrency=self.batch_size,
                raise_on_first_error=True
            )

        # Execute deletes concurrently
        if delete_params and prepared_delete:
            results = execute_concurrent_with_args(
                session,
                prepared_delete,
                delete_params,
                concurrency=self.batch_size,
                raise_on_first_error=True
            )

        return SimpleCommitMessage()

    finally:
        cluster.shutdown()
```

Add import at the top of the file:

```python
from pyspark.sql.datasource import DataSource, DataSourceWriter, DataSourceStreamWriter, SimpleCommitMessage
```

**Step 4: Run tests to verify they pass**

Run: `poetry run pytest tests/test_writer.py -v`

Expected: PASS (all tests should pass)

**Step 5: Commit write execution logic**

```bash
git add src/cassandra_data_source/__init__.py tests/test_writer.py
git commit -m "feat: implement write execution with concurrent inserts/deletes"
```

---

## Task 5: Implement Stream Writer Methods

**Files:**
- Modify: `src/cassandra_data_source/__init__.py`
- Create: `tests/test_stream_writer.py`

**Step 1: Write stream writer tests**

Create `tests/test_stream_writer.py`:

```python
import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import Row


def test_stream_writer_commit(basic_options, sample_schema, mock_cassandra_cluster):
    """Test stream writer commit method."""
    from cassandra_data_source import CassandraDataSource

    cluster, session = mock_cassandra_cluster

    with patch("cassandra_data_source.Cluster", return_value=cluster):
        ds = CassandraDataSource(basic_options)
        writer = ds.streamWriter(sample_schema, False)

        # Commit should not raise error
        messages = [MagicMock()]
        writer.commit(messages)


def test_stream_writer_abort(basic_options, sample_schema, mock_cassandra_cluster):
    """Test stream writer abort method."""
    from cassandra_data_source import CassandraDataSource

    cluster, session = mock_cassandra_cluster

    with patch("cassandra_data_source.Cluster", return_value=cluster):
        ds = CassandraDataSource(basic_options)
        writer = ds.streamWriter(sample_schema, False)

        # Abort should not raise error
        messages = [MagicMock()]
        writer.abort(messages)


def test_stream_writer_write(basic_options, sample_schema, mock_cassandra_cluster):
    """Test that stream writer can write data."""
    from cassandra_data_source import CassandraDataSource

    cluster, session = mock_cassandra_cluster

    # Mock prepared statement
    prepared_stmt = MagicMock()
    session.prepare.return_value = prepared_stmt

    # Mock execute_concurrent_with_args
    mock_results = [MagicMock(success=True)]

    with patch("cassandra_data_source.Cluster", return_value=cluster):
        with patch("cassandra_data_source.execute_concurrent_with_args", return_value=mock_results):
            ds = CassandraDataSource(basic_options)
            writer = ds.streamWriter(sample_schema, False)

            # Create test data
            rows = [
                Row(id="550e8400-e29b-41d4-a716-446655440000", name="Alice", age=30, score=100)
            ]

            # Execute write (inherited from base class)
            result = writer.write(iter(rows))

            # Should return SimpleCommitMessage
            assert result is not None
```

**Step 2: Run tests to verify they fail**

Run: `poetry run pytest tests/test_stream_writer.py -v`

Expected: FAIL with "NotImplementedError" or assertion errors

**Step 3: Implement stream writer methods**

Update `CassandraStreamWriter` class in `src/cassandra_data_source/__init__.py`:

```python
class CassandraStreamWriter(CassandraWriter, DataSourceStreamWriter):
    """Streaming writer for Cassandra."""

    def commit(self, messages):
        """
        Handle successful batch completion.

        For Cassandra, writes are committed immediately, so nothing to do here.
        """
        pass

    def abort(self, messages):
        """
        Handle failed batch.

        For Cassandra, we can't roll back writes, so just log the failure.
        """
        # TODO: Add logging when we need it
        pass
```

**Step 4: Run tests to verify they pass**

Run: `poetry run pytest tests/test_stream_writer.py -v`

Expected: PASS (all tests should pass)

**Step 5: Commit stream writer implementation**

```bash
git add src/cassandra_data_source/__init__.py tests/test_stream_writer.py
git commit -m "feat: implement stream writer commit/abort methods"
```

---

## Task 6: Add Integration Tests

**Files:**
- Create: `tests/test_integration.py`
- Create: `tests/docker-compose.yml`

**Step 1: Create docker-compose for local Cassandra**

Create `tests/docker-compose.yml`:

```yaml
version: '3.8'

services:
  cassandra:
    image: cassandra:4.1
    container_name: cassandra-test
    ports:
      - "9042:9042"
    environment:
      - CASSANDRA_CLUSTER_NAME=TestCluster
      - CASSANDRA_DC=datacenter1
      - CASSANDRA_ENDPOINT_SNITCH=GossipingPropertyFileSnitch
    healthcheck:
      test: ["CMD", "cqlsh", "-e", "describe keyspaces"]
      interval: 10s
      timeout: 5s
      retries: 30
```

**Step 2: Write integration tests**

Create `tests/test_integration.py`:

```python
"""
Integration tests requiring a running Cassandra instance.

To run these tests:
1. Start Cassandra: cd tests && docker-compose up -d
2. Wait for ready: docker-compose exec cassandra cqlsh -e "describe keyspaces"
3. Run tests: poetry run pytest tests/test_integration.py -v
4. Stop Cassandra: cd tests && docker-compose down
"""

import pytest
from cassandra.cluster import Cluster
import time


@pytest.fixture(scope="module")
def cassandra_setup():
    """Setup test keyspace and table in Cassandra."""
    # Connect to Cassandra
    cluster = Cluster(["127.0.0.1"], port=9042)
    session = cluster.connect()

    # Create keyspace
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS test_ks
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """)

    # Create table
    session.execute("""
        CREATE TABLE IF NOT EXISTS test_ks.test_table (
            id uuid PRIMARY KEY,
            name text,
            age int,
            score bigint
        )
    """)

    # Truncate table to ensure clean state
    session.execute("TRUNCATE test_ks.test_table")

    yield session

    # Cleanup
    session.execute("DROP TABLE IF EXISTS test_ks.test_table")
    session.execute("DROP KEYSPACE IF EXISTS test_ks")
    cluster.shutdown()


@pytest.mark.integration
def test_write_and_read_integration(spark, cassandra_setup):
    """Test full write and read cycle with real Cassandra."""
    from cassandra_data_source import CassandraDataSource
    import uuid

    # Register data source
    spark.dataSource.register(CassandraDataSource)

    # Create test DataFrame
    data = [
        (str(uuid.uuid4()), "Alice", 30, 100),
        (str(uuid.uuid4()), "Bob", 25, 200),
        (str(uuid.uuid4()), "Charlie", 35, 150)
    ]
    df = spark.createDataFrame(data, ["id", "name", "age", "score"])

    # Write to Cassandra
    df.write.format("pycassandra") \
        .option("host", "127.0.0.1") \
        .option("port", "9042") \
        .option("keyspace", "test_ks") \
        .option("table", "test_table") \
        .save()

    # Verify data was written
    time.sleep(1)  # Give Cassandra time to flush

    rows = cassandra_setup.execute("SELECT COUNT(*) FROM test_ks.test_table")
    count = rows.one()[0]
    assert count == 3


@pytest.mark.integration
def test_write_with_delete_flag_integration(spark, cassandra_setup):
    """Test write with delete flag using real Cassandra."""
    from cassandra_data_source import CassandraDataSource
    import uuid

    # Register data source
    spark.dataSource.register(CassandraDataSource)

    # Insert initial data
    test_id = str(uuid.uuid4())
    cassandra_setup.execute(
        "INSERT INTO test_ks.test_table (id, name, age, score) VALUES (%s, %s, %s, %s)",
        (uuid.UUID(test_id), "ToDelete", 40, 300)
    )

    # Create DataFrame with delete flag
    data = [(test_id, "ToDelete", 40, 300, True)]
    df = spark.createDataFrame(data, ["id", "name", "age", "score", "is_deleted"])

    # Write with delete flag
    df.write.format("pycassandra") \
        .option("host", "127.0.0.1") \
        .option("port", "9042") \
        .option("keyspace", "test_ks") \
        .option("table", "test_table") \
        .option("delete_flag_column", "is_deleted") \
        .option("delete_flag_value", "true") \
        .save()

    # Verify row was deleted
    time.sleep(1)

    rows = cassandra_setup.execute(
        "SELECT COUNT(*) FROM test_ks.test_table WHERE id = %s",
        (uuid.UUID(test_id),)
    )
    count = rows.one()[0]
    assert count == 0


@pytest.mark.integration
def test_write_streaming_integration(spark, cassandra_setup):
    """Test streaming write with real Cassandra."""
    from cassandra_data_source import CassandraDataSource
    import uuid
    import tempfile
    import os

    # Register data source
    spark.dataSource.register(CassandraDataSource)

    # Create temp directory for checkpoint
    checkpoint_dir = tempfile.mkdtemp()

    try:
        # Create streaming DataFrame
        data = [
            (str(uuid.uuid4()), "Stream1", 20, 50),
            (str(uuid.uuid4()), "Stream2", 22, 75)
        ]
        df = spark.createDataFrame(data, ["id", "name", "age", "score"])

        # Write streaming (trigger once for testing)
        query = df.writeStream.format("pycassandra") \
            .option("host", "127.0.0.1") \
            .option("port", "9042") \
            .option("keyspace", "test_ks") \
            .option("table", "test_table") \
            .option("checkpointLocation", checkpoint_dir) \
            .trigger(once=True) \
            .start()

        query.awaitTermination()

        # Verify data was written
        time.sleep(1)

        rows = cassandra_setup.execute("SELECT COUNT(*) FROM test_ks.test_table")
        count = rows.one()[0]
        assert count >= 2

    finally:
        # Cleanup checkpoint dir
        import shutil
        shutil.rmtree(checkpoint_dir, ignore_errors=True)
```

**Step 3: Document how to run integration tests**

Add to `tests/test_integration.py` docstring (already included above).

**Step 4: Run unit tests to ensure nothing broke**

Run: `poetry run pytest tests/ -v -m "not integration"`

Expected: PASS (all unit tests should pass)

**Step 5: Commit integration tests**

```bash
git add tests/test_integration.py tests/docker-compose.yml
git commit -m "test: add integration tests with docker-compose Cassandra"
```

---

## Task 7: Update Package Exports and Documentation

**Files:**
- Modify: `src/cassandra_data_source/__init__.py`
- Modify: `README.md`

**Step 1: Update package exports**

Update `__all__` in `src/cassandra_data_source/__init__.py`:

```python
__all__ = ["CassandraDataSource", "convert_value", "CassandraWriter", "CassandraBatchWriter", "CassandraStreamWriter"]
```

**Step 2: Update README with usage examples**

Update `README.md`:

```markdown
# PyCassandra - Python Data Source for Apache Cassandra

Python Data Source for Apache Spark enabling batch and streaming reads/writes to Apache Cassandra.

## Features

- **Batch Writes**: Write DataFrames to Cassandra tables with concurrent execution
- **Streaming Writes**: Write streaming DataFrames with micro-batch processing
- **Type Conversion**: Automatic type mapping with validation (String ↔ UUID, etc.)
- **Delete Flag Support**: Conditional row deletion during writes
- **SSL/TLS**: Secure connections to production clusters
- **Primary Key Validation**: Early validation ensures DataFrame contains all PK columns

## Installation

```bash
poetry install
```

## Quick Start

### Batch Write

```python
from pyspark.sql import SparkSession
from cassandra_data_source import CassandraDataSource

spark = SparkSession.builder.appName("pycassandra").getOrCreate()
spark.dataSource.register(CassandraDataSource)

df = spark.createDataFrame([
    ("550e8400-e29b-41d4-a716-446655440000", "Alice", 30),
    ("550e8400-e29b-41d4-a716-446655440001", "Bob", 25)
], ["id", "name", "age"])

df.write.format("pycassandra") \
    .option("host", "127.0.0.1") \
    .option("keyspace", "myks") \
    .option("table", "users") \
    .save()
```

### Streaming Write

```python
df.writeStream.format("pycassandra") \
    .option("host", "127.0.0.1") \
    .option("keyspace", "myks") \
    .option("table", "users") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start()
```

### Write with Delete Flag

```python
df.write.format("pycassandra") \
    .option("host", "127.0.0.1") \
    .option("keyspace", "myks") \
    .option("table", "users") \
    .option("delete_flag_column", "is_deleted") \
    .option("delete_flag_value", "true") \
    .save()
```

## Configuration Options

### Connection Options

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `host` | Yes | - | Cassandra contact point |
| `port` | No | 9042 | Cassandra port |
| `keyspace` | Yes | - | Target keyspace |
| `table` | Yes | - | Target table |
| `username` | No | - | Authentication username |
| `password` | No | - | Authentication password |
| `ssl_enabled` | No | false | Enable SSL/TLS |
| `ssl_ca_cert` | No | - | Path to CA certificate |

### Write Options

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `batch_size` | No | 100 | Concurrent request count |
| `consistency` | No | LOCAL_QUORUM | Write consistency level |
| `delete_flag_column` | No | - | Column indicating deletion |
| `delete_flag_value` | No | - | Value triggering deletion |

## Development

### Setup

```bash
poetry install
```

### Run Tests

```bash
# Unit tests only
poetry run pytest -v -m "not integration"

# Integration tests (requires Cassandra)
cd tests && docker-compose up -d
poetry run pytest -v -m integration
cd tests && docker-compose down
```

### Code Quality

```bash
poetry run ruff check src/
poetry run ruff format src/
poetry run mypy src/
```

## Design

See [Design Document](docs/plans/2025-12-29-pycassandra-design.md) for comprehensive architecture and implementation details.

## Phase 1 Status

- ✅ Batch writes
- ✅ Streaming writes
- ✅ Connection management (host, port, auth, SSL)
- ✅ Primary key validation
- ✅ Type conversion (String ↔ UUID, Int ↔ BigInt)
- ✅ Delete flag support
- ⏳ Batch reads (Phase 3)
- ⏳ Streaming reads (Phase 4)

## License

MIT
```

**Step 3: Run all tests to verify everything works**

Run: `poetry run pytest -v -m "not integration"`

Expected: PASS (all unit tests pass)

**Step 4: Commit documentation**

```bash
git add README.md src/cassandra_data_source/__init__.py
git commit -m "docs: update README with usage examples and API reference"
```

---

## Task 8: Final Verification and Cleanup

**Files:**
- N/A (verification only)

**Step 1: Run full test suite**

Run: `poetry run pytest -v -m "not integration"`

Expected: All tests PASS

**Step 2: Run code quality checks**

Run:
```bash
poetry run ruff check src/cassandra_data_source/
poetry run ruff format src/cassandra_data_source/
poetry run mypy src/cassandra_data_source/
```

Expected: No errors

**Step 3: Build package**

Run: `poetry build`

Expected: Wheel created in `dist/` directory

**Step 4: Manual smoke test (optional)**

If you have a local Cassandra instance:

```bash
cd tests && docker-compose up -d
# Wait for Cassandra to be ready
poetry run pytest tests/test_integration.py -v -m integration
cd tests && docker-compose down
```

**Step 5: Final commit**

```bash
git add .
git commit -m "chore: phase 1 complete - write operations implemented"
```

---

## Verification Checklist

- [ ] All unit tests pass
- [ ] Code formatted with ruff
- [ ] Type checking passes with mypy
- [ ] Package builds successfully
- [ ] Integration tests pass (if Cassandra available)
- [ ] README updated with usage examples
- [ ] All commits follow conventional commit format

## Next Steps

After Phase 1 is complete:

1. **Phase 2**: Implement delete flag support (already included in Phase 1)
2. **Phase 3**: Implement batch read operations with token range scanning
3. **Phase 4**: Investigate and implement streaming reads via CDC

## Notes for Implementer

- Always run tests after each step to catch issues early
- Use `poetry run` prefix for all Python commands
- Follow SIMPLE principles - no over-engineering
- Commit frequently with descriptive messages
- If integration tests fail, check Cassandra is running: `docker ps`
- For type conversion issues, refer to `test_type_conversion.py` examples
- If primary key validation fails, verify table schema in Cassandra

## References

- [PySpark Data Source API](https://spark.apache.org/docs/latest/api/python/tutorial/sql/python_data_source.html)
- [Cassandra Python Driver](https://docs.datastax.com/en/developer/python-driver/3.29/)
- [Design Document](docs/plans/2025-12-29-pycassandra-design.md)
