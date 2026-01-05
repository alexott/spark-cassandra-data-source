# PyCassandra Phase 2: Batch Read Operations Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Implement batch read operations for PyCassandra data source with token range scanning, schema derivation, column projection, and optional filtering.

**Architecture:** Implement PySpark DataSourceReader API with token range-based partitioning. Each Cassandra token range becomes one Spark partition. Schema can be auto-derived from Cassandra metadata or provided explicitly by user. All cassandra-driver imports happen inside executor methods.

**Tech Stack:** PySpark 4.0.1, cassandra-driver 3.29.3, pytest

---

## Task 1: Implement Schema Derivation Utilities

**Files:**
- Create: `src/cassandra_data_source/schema.py`
- Create: `tests/test_schema.py`

**Step 1: Write failing tests for schema derivation**

Create `tests/test_schema.py`:

```python
import pytest
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    FloatType, DoubleType, BooleanType, TimestampType, DateType,
    BinaryType, DecimalType
)
from unittest.mock import MagicMock


def test_cassandra_to_spark_type_int():
    """Test int type mapping."""
    from cassandra_data_source.schema import cassandra_to_spark_type

    assert cassandra_to_spark_type("int") == IntegerType()


def test_cassandra_to_spark_type_bigint():
    """Test bigint type mapping."""
    from cassandra_data_source.schema import cassandra_to_spark_type

    assert cassandra_to_spark_type("bigint") == LongType()


def test_cassandra_to_spark_type_uuid():
    """Test UUID maps to StringType."""
    from cassandra_data_source.schema import cassandra_to_spark_type

    assert cassandra_to_spark_type("uuid") == StringType()


def test_cassandra_to_spark_type_text():
    """Test text type mapping."""
    from cassandra_data_source.schema import cassandra_to_spark_type

    assert cassandra_to_spark_type("text") == StringType()


def test_derive_schema_from_table():
    """Test deriving Spark schema from Cassandra table metadata."""
    from cassandra_data_source.schema import derive_schema_from_table

    # Mock table metadata
    table_meta = MagicMock()

    # Mock columns
    id_col = MagicMock()
    id_col.name = "id"
    id_col.cql_type = "uuid"

    name_col = MagicMock()
    name_col.name = "name"
    name_col.cql_type = "text"

    age_col = MagicMock()
    age_col.name = "age"
    age_col.cql_type = "int"

    table_meta.columns = {
        "id": id_col,
        "name": name_col,
        "age": age_col
    }

    schema = derive_schema_from_table(table_meta)

    assert isinstance(schema, StructType)
    assert len(schema.fields) == 3
    assert schema.fields[0].name == "id"
    assert schema.fields[0].dataType == StringType()
    assert schema.fields[1].name == "name"
    assert schema.fields[1].dataType == StringType()
    assert schema.fields[2].name == "age"
    assert schema.fields[2].dataType == IntegerType()


def test_derive_schema_with_column_filter():
    """Test deriving schema with column filtering."""
    from cassandra_data_source.schema import derive_schema_from_table

    # Mock table metadata
    table_meta = MagicMock()

    id_col = MagicMock()
    id_col.name = "id"
    id_col.cql_type = "uuid"

    name_col = MagicMock()
    name_col.name = "name"
    name_col.cql_type = "text"

    age_col = MagicMock()
    age_col.name = "age"
    age_col.cql_type = "int"

    table_meta.columns = {
        "id": id_col,
        "name": name_col,
        "age": age_col
    }

    # Only select id and name
    schema = derive_schema_from_table(table_meta, columns=["id", "name"])

    assert len(schema.fields) == 2
    assert schema.fields[0].name == "id"
    assert schema.fields[1].name == "name"
```

**Step 2: Run tests to verify they fail**

Run: `poetry run pytest tests/test_schema.py -v`

Expected: FAIL with "ModuleNotFoundError: No module named 'cassandra_data_source.schema'"

**Step 3: Implement schema derivation utilities**

Create `src/cassandra_data_source/schema.py`:

```python
"""Schema derivation utilities for Cassandra types."""

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    FloatType, DoubleType, BooleanType, TimestampType, DateType,
    BinaryType, DecimalType
)


def cassandra_to_spark_type(cassandra_type):
    """
    Convert Cassandra type to Spark type.

    Args:
        cassandra_type: Cassandra CQL type string (e.g., "int", "text", "uuid")

    Returns:
        PySpark DataType
    """
    cass_type = cassandra_type.lower()

    # Numeric types
    if cass_type in ("int", "smallint", "tinyint"):
        return IntegerType()
    if cass_type in ("bigint", "counter"):
        return LongType()
    if cass_type == "float":
        return FloatType()
    if cass_type == "double":
        return DoubleType()
    if cass_type == "decimal":
        return DecimalType()

    # Boolean
    if cass_type == "boolean":
        return BooleanType()

    # String types
    if cass_type in ("text", "varchar", "ascii"):
        return StringType()

    # UUID types - map to string (no native UUID in Spark)
    if cass_type in ("uuid", "timeuuid"):
        return StringType()

    # Date/Time types
    if cass_type == "timestamp":
        return TimestampType()
    if cass_type == "date":
        return DateType()

    # Binary
    if cass_type == "blob":
        return BinaryType()

    # INET - map to string
    if cass_type == "inet":
        return StringType()

    # Complex types - map to string (JSON representation)
    if cass_type in ("list", "set", "map", "tuple", "udt"):
        return StringType()

    # Default to string for unknown types
    return StringType()


def derive_schema_from_table(table_metadata, columns=None):
    """
    Derive Spark schema from Cassandra table metadata.

    Args:
        table_metadata: Cassandra table metadata object
        columns: Optional list of column names to include (projection)

    Returns:
        StructType representing the Spark schema
    """
    fields = []

    # Get column names to include
    if columns:
        column_names = columns
    else:
        # Use all columns in order
        column_names = sorted(table_metadata.columns.keys())

    # Create StructField for each column
    for col_name in column_names:
        if col_name not in table_metadata.columns:
            raise ValueError(f"Column '{col_name}' not found in table")

        col_meta = table_metadata.columns[col_name]
        spark_type = cassandra_to_spark_type(col_meta.cql_type)

        # All columns nullable in Spark (Cassandra allows nulls except in PKs)
        fields.append(StructField(col_name, spark_type, nullable=True))

    return StructType(fields)
```

**Step 4: Run tests to verify they pass**

Run: `poetry run pytest tests/test_schema.py -v`

Expected: PASS (all 6 tests)

**Step 5: Update package exports**

Update `src/cassandra_data_source/__init__.py`:

```python
from .data_source import CassandraDataSource
from .schema import cassandra_to_spark_type, derive_schema_from_table
from .type_conversion import convert_value
from .writer import CassandraBatchWriter, CassandraStreamWriter, CassandraWriter

__all__ = [
    "CassandraDataSource",
    "CassandraBatchWriter",
    "CassandraStreamWriter",
    "CassandraWriter",
    "convert_value",
    "cassandra_to_spark_type",
    "derive_schema_from_table",
]
```

**Step 6: Commit**

```bash
git add src/cassandra_data_source/schema.py tests/test_schema.py src/cassandra_data_source/__init__.py
git commit -m "feat: add schema derivation utilities for Cassandra types"
```

---

## Task 2: Implement Token Range Partition Class

**Files:**
- Create: `src/cassandra_data_source/partitioning.py`
- Create: `tests/test_partitioning.py`

**Step 1: Write failing tests for partition class**

Create `tests/test_partitioning.py`:

```python
import pytest


def test_token_range_partition_creation():
    """Test creating a token range partition."""
    from cassandra_data_source.partitioning import TokenRangePartition

    partition = TokenRangePartition(
        partition_id=0,
        start_token="100",
        end_token="200",
        pk_columns=["id"],
        is_wrap_around=False
    )

    assert partition.partition_id == 0
    assert partition.start_token == "100"
    assert partition.end_token == "200"
    assert partition.pk_columns == ["id"]
    assert partition.is_wrap_around is False


def test_token_range_partition_equality():
    """Test partition equality comparison."""
    from cassandra_data_source.partitioning import TokenRangePartition

    p1 = TokenRangePartition(0, "100", "200", ["id"], False)
    p2 = TokenRangePartition(0, "100", "200", ["id"], False)
    p3 = TokenRangePartition(1, "200", "300", ["id"], False)

    assert p1 == p2
    assert p1 != p3
```

**Step 2: Run tests to verify they fail**

Run: `poetry run pytest tests/test_partitioning.py -v`

Expected: FAIL with "ModuleNotFoundError"

**Step 3: Implement TokenRangePartition class**

Create `src/cassandra_data_source/partitioning.py`:

```python
"""Partitioning utilities for Cassandra token ranges."""

from pyspark.sql.datasource import InputPartition


class TokenRangePartition(InputPartition):
    """
    Represents a Cassandra token range partition.

    Each partition corresponds to one Cassandra token range and will be
    read independently by a Spark executor.
    """

    def __init__(self, partition_id, start_token, end_token, pk_columns, is_wrap_around):
        """
        Initialize a token range partition.

        Args:
            partition_id: Unique partition identifier
            start_token: Start token (exclusive)
            end_token: End token (inclusive)
            pk_columns: List of partition key column names
            is_wrap_around: True if this is the wrap-around range (start > end)
        """
        self.partition_id = partition_id
        self.start_token = str(start_token)
        self.end_token = str(end_token)
        self.pk_columns = pk_columns
        self.is_wrap_around = is_wrap_around

    def __eq__(self, other):
        """Check equality based on partition content."""
        if not isinstance(other, TokenRangePartition):
            return False
        return (
            self.partition_id == other.partition_id and
            self.start_token == other.start_token and
            self.end_token == other.end_token and
            self.pk_columns == other.pk_columns and
            self.is_wrap_around == other.is_wrap_around
        )

    def __hash__(self):
        """Return hash for use in sets/dicts."""
        return hash((self.partition_id, self.start_token, self.end_token,
                    tuple(self.pk_columns), self.is_wrap_around))

    def __repr__(self):
        """Return string representation."""
        return (f"TokenRangePartition(id={self.partition_id}, "
                f"start={self.start_token}, end={self.end_token}, "
                f"pk={self.pk_columns}, wrap={self.is_wrap_around})")
```

**Step 4: Run tests to verify they pass**

Run: `poetry run pytest tests/test_partitioning.py -v`

Expected: PASS (all 2 tests)

**Step 5: Update package exports**

Update `src/cassandra_data_source/__init__.py` to add `TokenRangePartition`.

**Step 6: Commit**

```bash
git add src/cassandra_data_source/partitioning.py tests/test_partitioning.py src/cassandra_data_source/__init__.py
git commit -m "feat: add TokenRangePartition class for partitioning"
```

---

## Task 3: Implement CassandraReader Base Class

**Files:**
- Create: `src/cassandra_data_source/reader.py`
- Create: `tests/test_reader.py`

**Step 1: Write failing tests for reader initialization**

Create `tests/test_reader.py`:

```python
import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


def test_reader_init_validates_options():
    """Test reader validates required options."""
    from cassandra_data_source.reader import CassandraReader

    options = {}  # Missing required options
    schema = None

    with pytest.raises(ValueError, match="Missing required options"):
        CassandraReader(options, schema)


def test_reader_init_with_valid_options(mock_table_metadata):
    """Test reader initializes with valid options."""
    from cassandra_data_source.reader import CassandraReader

    options = {
        "host": "127.0.0.1",
        "keyspace": "test_ks",
        "table": "test_table"
    }
    schema = None

    # Mock Cluster to avoid actual connection
    with patch("cassandra_data_source.reader.Cluster") as mock_cluster:
        mock_cluster_instance = MagicMock()
        mock_session = MagicMock()
        mock_cluster_instance.connect.return_value = mock_session
        mock_cluster_instance.metadata.keyspaces = {
            "test_ks": MagicMock(tables={"test_table": mock_table_metadata})
        }
        mock_cluster_instance.metadata.token_ranges.return_value = []
        mock_cluster.return_value = mock_cluster_instance

        reader = CassandraReader(options, schema)

        assert reader.host == "127.0.0.1"
        assert reader.keyspace == "test_ks"
        assert reader.table == "test_table"


def test_reader_derives_schema_if_not_provided(mock_table_metadata):
    """Test reader auto-derives schema from Cassandra."""
    from cassandra_data_source.reader import CassandraReader

    options = {
        "host": "127.0.0.1",
        "keyspace": "test_ks",
        "table": "test_table"
    }
    schema = None  # No schema provided

    with patch("cassandra_data_source.reader.Cluster") as mock_cluster:
        mock_cluster_instance = MagicMock()
        mock_session = MagicMock()
        mock_cluster_instance.connect.return_value = mock_session
        mock_cluster_instance.metadata.keyspaces = {
            "test_ks": MagicMock(tables={"test_table": mock_table_metadata})
        }
        mock_cluster_instance.metadata.token_ranges.return_value = []
        mock_cluster.return_value = mock_cluster_instance

        reader = CassandraReader(options, schema)

        # Should have derived schema
        assert reader.schema is not None
        assert isinstance(reader.schema, StructType)


def test_reader_uses_provided_schema(mock_table_metadata):
    """Test reader uses user-provided schema."""
    from cassandra_data_source.reader import CassandraReader

    options = {
        "host": "127.0.0.1",
        "keyspace": "test_ks",
        "table": "test_table"
    }
    user_schema = StructType([
        StructField("id", StringType()),
        StructField("name", StringType())
    ])

    with patch("cassandra_data_source.reader.Cluster") as mock_cluster:
        mock_cluster_instance = MagicMock()
        mock_session = MagicMock()
        mock_cluster_instance.connect.return_value = mock_session
        mock_cluster_instance.metadata.keyspaces = {
            "test_ks": MagicMock(tables={"test_table": mock_table_metadata})
        }
        mock_cluster_instance.metadata.token_ranges.return_value = []
        mock_cluster.return_value = mock_cluster_instance

        reader = CassandraReader(options, user_schema)

        # Should use provided schema
        assert reader.schema == user_schema
        assert len(reader.schema.fields) == 2
```

**Step 2: Run tests to verify they fail**

Run: `poetry run pytest tests/test_reader.py -v`

Expected: FAIL with "ModuleNotFoundError"

**Step 3: Implement CassandraReader base class**

Create `src/cassandra_data_source/reader.py`:

```python
"""Cassandra reader implementations."""

from pyspark.sql.datasource import DataSourceReader

from .partitioning import TokenRangePartition
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
            session = cluster.connect()

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
        # This will be implemented in next step
        pass

    def read(self, partition):
        """
        Read data from a partition.

        Args:
            partition: TokenRangePartition to read

        Yields:
            Tuples representing rows
        """
        # This will be implemented in next step
        pass


class CassandraBatchReader(CassandraReader, DataSourceReader):
    """Batch reader for Cassandra."""
    pass
```

**Step 4: Run tests to verify they pass**

Run: `poetry run pytest tests/test_reader.py -v`

Expected: PASS (all 4 tests)

**Step 5: Update package exports**

Update `src/cassandra_data_source/__init__.py` to add reader classes.

**Step 6: Commit**

```bash
git add src/cassandra_data_source/reader.py tests/test_reader.py src/cassandra_data_source/__init__.py
git commit -m "feat: add CassandraReader base class with metadata loading"
```

---

## Task 4: Implement Token Range Partitioning Logic

**Files:**
- Modify: `src/cassandra_data_source/reader.py`
- Modify: `tests/test_reader.py`

**Step 1: Write failing tests for partitions() method**

Add to `tests/test_reader.py`:

```python
def test_reader_creates_partitions_from_token_ranges(mock_table_metadata):
    """Test reader creates one partition per token range."""
    from cassandra_data_source.reader import CassandraReader
    from cassandra_data_source.partitioning import TokenRangePartition
    from unittest.mock import MagicMock

    options = {
        "host": "127.0.0.1",
        "keyspace": "test_ks",
        "table": "test_table"
    }

    # Mock token ranges
    range1 = MagicMock()
    range1.start.value = 100
    range1.end.value = 200

    range2 = MagicMock()
    range2.start.value = 200
    range2.end.value = 300

    with patch("cassandra_data_source.reader.Cluster") as mock_cluster:
        mock_cluster_instance = MagicMock()
        mock_session = MagicMock()
        mock_cluster_instance.connect.return_value = mock_session
        mock_cluster_instance.metadata.keyspaces = {
            "test_ks": MagicMock(tables={"test_table": mock_table_metadata})
        }
        mock_cluster_instance.metadata.token_ranges.return_value = [range1, range2]
        mock_cluster.return_value = mock_cluster_instance

        reader = CassandraReader(options, None)
        partitions = reader.partitions()

        # Should have 2 partitions (one per token range)
        assert len(partitions) == 2
        assert all(isinstance(p, TokenRangePartition) for p in partitions)
```

**Step 2: Run test to verify it fails**

Run: `poetry run pytest tests/test_reader.py::test_reader_creates_partitions_from_token_ranges -v`

Expected: FAIL with "AssertionError" or "TypeError"

**Step 3: Implement partitions() method**

Update `src/cassandra_data_source/reader.py` - implement `partitions()` method:

```python
def partitions(self):
    """
    Return list of partitions for parallel reading.

    Returns one partition per Cassandra token range following
    the TokenRangesScan.java pattern.
    """
    if not self.token_ranges:
        # Empty table or no token ranges
        return []

    partitions = []
    sorted_ranges = sorted(self.token_ranges)

    # Get min token for wrap-around handling
    min_token = sorted_ranges[0].start if sorted_ranges else None

    for i, token_range in enumerate(sorted_ranges):
        start = token_range.start
        end = token_range.end

        # Determine if this is a wrap-around range
        is_wrap_around = (start == end)

        partition = TokenRangePartition(
            partition_id=i,
            start_token=start.value if hasattr(start, 'value') else str(start),
            end_token=end.value if hasattr(end, 'value') else str(end),
            pk_columns=self.pk_columns,
            is_wrap_around=is_wrap_around
        )

        partitions.append(partition)

    return partitions
```

**Step 4: Run test to verify it passes**

Run: `poetry run pytest tests/test_reader.py::test_reader_creates_partitions_from_token_ranges -v`

Expected: PASS

**Step 5: Commit**

```bash
git add src/cassandra_data_source/reader.py tests/test_reader.py
git commit -m "feat: implement token range partitioning logic"
```

---

## Task 5: Implement read() Method with Token Range Queries

**Files:**
- Modify: `src/cassandra_data_source/reader.py`
- Modify: `tests/test_reader.py`

**Step 1: Write failing test for read() method**

Add to `tests/test_reader.py`:

```python
def test_reader_read_executes_token_range_query(mock_table_metadata):
    """Test read() method executes query for token range."""
    from cassandra_data_source.reader import CassandraReader
    from cassandra_data_source.partitioning import TokenRangePartition
    from unittest.mock import MagicMock, call

    options = {
        "host": "127.0.0.1",
        "keyspace": "test_ks",
        "table": "test_table"
    }

    with patch("cassandra_data_source.reader.Cluster") as mock_cluster:
        # Setup mocks for initialization
        mock_cluster_instance = MagicMock()
        mock_session = MagicMock()
        mock_cluster_instance.connect.return_value = mock_session
        mock_cluster_instance.metadata.keyspaces = {
            "test_ks": MagicMock(tables={"test_table": mock_table_metadata})
        }
        mock_cluster_instance.metadata.token_ranges.return_value = []
        mock_cluster.return_value = mock_cluster_instance

        reader = CassandraReader(options, None)

        # Create a partition to read
        partition = TokenRangePartition(
            partition_id=0,
            start_token="100",
            end_token="200",
            pk_columns=["id"],
            is_wrap_around=False
        )

        # Mock Cluster again for read() execution
        with patch("cassandra_data_source.reader.Cluster") as mock_read_cluster:
            mock_read_cluster_instance = MagicMock()
            mock_read_session = MagicMock()
            mock_read_cluster_instance.connect.return_value = mock_read_session
            mock_read_cluster.return_value = mock_read_cluster_instance

            # Mock query execution to return test data
            mock_row = MagicMock()
            mock_row.__getitem__ = lambda self, key: f"value_{key}"
            mock_read_session.execute.return_value = [mock_row]

            # Execute read
            result = list(reader.read(partition))

            # Should have executed a query
            assert mock_read_session.execute.called
            # Should have returned rows
            assert len(result) > 0
```

**Step 2: Run test to verify it fails**

Run: `poetry run pytest tests/test_reader.py::test_reader_read_executes_token_range_query -v`

Expected: FAIL

**Step 3: Implement read() method**

Update `src/cassandra_data_source/reader.py` - implement `read()` method:

```python
def read(self, partition):
    """
    Read data from a partition using token range query.

    Args:
        partition: TokenRangePartition to read

    Yields:
        Tuples representing rows
    """
    # Import cassandra-driver on executor
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.query import ConsistencyLevel
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
    session = cluster.connect(self.keyspace)

    try:
        # Build SELECT clause with columns
        columns_str = ", ".join(self.columns)

        # Build WHERE clause with token range
        # Following TokenRangesScan.java pattern
        pk_cols_str = ", ".join(partition.pk_columns)

        if partition.is_wrap_around:
            # Wrap-around range (start == end)
            where_clause = f"token({pk_cols_str}) >= {partition.start_token}"
        elif partition.start_token == partition.end_token:
            # Single token
            where_clause = f"token({pk_cols_str}) = {partition.end_token}"
        else:
            # Normal range
            where_clause = (
                f"token({pk_cols_str}) > {partition.start_token} AND "
                f"token({pk_cols_str}) <= {partition.end_token}"
            )

        # Add optional filter
        if self.filter:
            where_clause = f"({where_clause}) AND ({self.filter})"

        # Build full query
        query = f"SELECT {columns_str} FROM {self.table} WHERE {where_clause}"

        # Execute query
        result_set = session.execute(query)

        # Yield rows as tuples
        for row in result_set:
            # Convert row to tuple matching schema order
            values = tuple(row[col] for col in self.columns)
            yield values

    finally:
        cluster.shutdown()
```

**Step 4: Run test to verify it passes**

Run: `poetry run pytest tests/test_reader.py::test_reader_read_executes_token_range_query -v`

Expected: PASS

**Step 5: Commit**

```bash
git add src/cassandra_data_source/reader.py tests/test_reader.py
git commit -m "feat: implement read() method with token range queries"
```

---

## Task 6: Update CassandraDataSource with Reader Methods

**Files:**
- Modify: `src/cassandra_data_source/data_source.py`
- Modify: `tests/test_options.py`

**Step 1: Write failing tests for reader() and schema() methods**

Add to `tests/test_options.py`:

```python
def test_data_source_reader_factory(mock_table_metadata):
    """Test data source creates reader."""
    from cassandra_data_source import CassandraDataSource
    from pyspark.sql.types import StructType, StructField, StringType

    options = {
        "host": "127.0.0.1",
        "keyspace": "test_ks",
        "table": "test_table"
    }
    schema = StructType([StructField("id", StringType())])

    with patch("cassandra_data_source.reader.Cluster") as mock_cluster:
        mock_cluster_instance = MagicMock()
        mock_session = MagicMock()
        mock_cluster_instance.connect.return_value = mock_session
        mock_cluster_instance.metadata.keyspaces = {
            "test_ks": MagicMock(tables={"test_table": mock_table_metadata})
        }
        mock_cluster_instance.metadata.token_ranges.return_value = []
        mock_cluster.return_value = mock_cluster_instance

        ds = CassandraDataSource(options)
        reader = ds.reader(schema)

        assert reader is not None


def test_data_source_schema_method(mock_table_metadata):
    """Test data source derives schema."""
    from cassandra_data_source import CassandraDataSource
    from pyspark.sql.types import StructType

    options = {
        "host": "127.0.0.1",
        "keyspace": "test_ks",
        "table": "test_table"
    }

    with patch("cassandra_data_source.reader.Cluster") as mock_cluster:
        mock_cluster_instance = MagicMock()
        mock_session = MagicMock()
        mock_cluster_instance.connect.return_value = mock_session
        mock_cluster_instance.metadata.keyspaces = {
            "test_ks": MagicMock(tables={"test_table": mock_table_metadata})
        }
        mock_cluster_instance.metadata.token_ranges.return_value = []
        mock_cluster.return_value = mock_cluster_instance

        ds = CassandraDataSource(options)
        schema = ds.schema()

        assert schema is not None
        assert isinstance(schema, StructType)
```

**Step 2: Run tests to verify they fail**

Run: `poetry run pytest tests/test_options.py::test_data_source_reader_factory -v`

Expected: FAIL with "AttributeError"

**Step 3: Implement reader() and schema() methods**

Update `src/cassandra_data_source/data_source.py`:

```python
"""PyCassandra Data Source implementation."""

from pyspark.sql.datasource import DataSource

from .reader import CassandraBatchReader
from .writer import CassandraBatchWriter, CassandraStreamWriter


class CassandraDataSource(DataSource):
    """PySpark Data Source for Apache Cassandra."""

    @classmethod
    def name(cls):
        """Return the data source format name."""
        return "pycassandra"

    def __init__(self, options):
        """Initialize data source with options."""
        self.options = options

    def schema(self):
        """
        Return the schema of the data source.

        Connects to Cassandra and derives schema from table metadata.
        """
        # Create a temporary reader to get schema
        reader = CassandraBatchReader(self.options, schema=None)
        return reader.schema

    def reader(self, schema):
        """Return a batch reader instance."""
        return CassandraBatchReader(self.options, schema)

    def writer(self, schema, save_mode):
        """Return a batch writer instance."""
        return CassandraBatchWriter(self.options, schema)

    def streamWriter(self, schema, overwrite):
        """Return a streaming writer instance."""
        return CassandraStreamWriter(self.options, schema)
```

**Step 4: Run tests to verify they pass**

Run: `poetry run pytest tests/test_options.py::test_data_source_reader_factory -v`

Expected: PASS

**Step 5: Commit**

```bash
git add src/cassandra_data_source/data_source.py tests/test_options.py
git commit -m "feat: add reader() and schema() methods to CassandraDataSource"
```

---

## Task 7: Add Integration Tests for Read Operations

**Files:**
- Modify: `tests/test_integration.py`

**Step 1: Write integration tests for read operations**

Add to `tests/test_integration.py`:

```python
@pytest.mark.integration
def test_read_from_cassandra_integration(spark, cassandra_setup):
    """Test reading data from Cassandra."""
    from cassandra_data_source import CassandraDataSource
    import uuid

    # Insert test data
    test_data = [
        (uuid.uuid4(), "Alice", 30, 100),
        (uuid.uuid4(), "Bob", 25, 200),
        (uuid.uuid4(), "Charlie", 35, 150)
    ]

    for row in test_data:
        cassandra_setup.execute(
            "INSERT INTO test_ks.test_table (id, name, age, score) VALUES (%s, %s, %s, %s)",
            row
        )

    time.sleep(1)  # Wait for data to be written

    # Register data source
    spark.dataSource.register(CassandraDataSource)

    # Read from Cassandra
    df = spark.read.format("pycassandra") \
        .option("host", "127.0.0.1") \
        .option("port", "9042") \
        .option("keyspace", "test_ks") \
        .option("table", "test_table") \
        .load()

    # Verify data was read
    count = df.count()
    assert count == 3

    # Verify schema
    assert "id" in df.columns
    assert "name" in df.columns
    assert "age" in df.columns
    assert "score" in df.columns


@pytest.mark.integration
def test_read_with_filter_integration(spark, cassandra_setup):
    """Test reading with filter expression."""
    from cassandra_data_source import CassandraDataSource
    import uuid

    # Insert test data
    test_data = [
        (uuid.uuid4(), "Alice", 30, 100),
        (uuid.uuid4(), "Bob", 25, 200),
        (uuid.uuid4(), "Charlie", 35, 150)
    ]

    for row in test_data:
        cassandra_setup.execute(
            "INSERT INTO test_ks.test_table (id, name, age, score) VALUES (%s, %s, %s, %s)",
            row
        )

    time.sleep(1)

    # Register data source
    spark.dataSource.register(CassandraDataSource)

    # Read with filter
    df = spark.read.format("pycassandra") \
        .option("host", "127.0.0.1") \
        .option("port", "9042") \
        .option("keyspace", "test_ks") \
        .option("table", "test_table") \
        .option("filter", "age >= 30 ALLOW FILTERING") \
        .load()

    # Verify filtered data
    count = df.count()
    assert count == 2  # Alice and Charlie


@pytest.mark.integration
def test_read_with_schema_projection_integration(spark, cassandra_setup):
    """Test reading with explicit schema (column projection)."""
    from cassandra_data_source import CassandraDataSource
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType
    import uuid

    # Insert test data
    test_data = [
        (uuid.uuid4(), "Alice", 30, 100),
        (uuid.uuid4(), "Bob", 25, 200)
    ]

    for row in test_data:
        cassandra_setup.execute(
            "INSERT INTO test_ks.test_table (id, name, age, score) VALUES (%s, %s, %s, %s)",
            row
        )

    time.sleep(1)

    # Register data source
    spark.dataSource.register(CassandraDataSource)

    # Define schema with subset of columns
    schema = StructType([
        StructField("id", StringType()),
        StructField("name", StringType())
    ])

    # Read with explicit schema
    df = spark.read.format("pycassandra") \
        .schema(schema) \
        .option("host", "127.0.0.1") \
        .option("port", "9042") \
        .option("keyspace", "test_ks") \
        .option("table", "test_table") \
        .load()

    # Verify only selected columns
    assert len(df.columns) == 2
    assert "id" in df.columns
    assert "name" in df.columns
    assert "age" not in df.columns
    assert "score" not in df.columns
```

**Step 2: Run integration tests**

Run: `cd tests && docker-compose up -d && cd .. && poetry run pytest tests/test_integration.py -v -m integration`

Expected: PASS (if Cassandra is running)

**Step 3: Commit**

```bash
git add tests/test_integration.py
git commit -m "test: add integration tests for read operations"
```

---

## Task 8: Update Documentation

**Files:**
- Modify: `README.md`

**Step 1: Update README with read examples**

Add to `README.md`:

```markdown
### Batch Read

```python
from pyspark.sql import SparkSession
from cassandra_data_source import CassandraDataSource

spark = SparkSession.builder.appName("pycassandra").getOrCreate()
spark.dataSource.register(CassandraDataSource)

# Read entire table
df = spark.read.format("pycassandra") \
    .option("host", "127.0.0.1") \
    .option("keyspace", "myks") \
    .option("table", "users") \
    .load()

df.show()
```

### Read with Filter

```python
# Apply server-side filter
df = spark.read.format("pycassandra") \
    .option("host", "127.0.0.1") \
    .option("keyspace", "myks") \
    .option("table", "users") \
    .option("filter", "age >= 18 ALLOW FILTERING") \
    .load()
```

### Read with Schema (Column Projection)

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("id", StringType()),
    StructField("name", StringType()),
    StructField("age", IntegerType())
])

df = spark.read.format("pycassandra") \
    .schema(schema) \
    .option("host", "127.0.0.1") \
    .option("keyspace", "myks") \
    .option("table", "users") \
    .load()
```

## Read Options

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `consistency` | No | LOCAL_ONE | Read consistency level |
| `filter` | No | - | Raw CQL WHERE clause (use ALLOW FILTERING if needed) |
```

**Step 2: Update Phase 2 status in README**

Update the Phase Status section:

```markdown
### Phase 2: Batch Read Operations ✅

**Status:** COMPLETE

- ✅ Token range-based partitioning
- ✅ Schema derivation from Cassandra metadata
- ✅ Explicit schema support (column projection)
- ✅ Optional filter expressions
- ✅ Connection management
- ✅ Integration tests
```

**Step 3: Run all tests to verify nothing broke**

Run: `poetry run pytest -v -m "not integration"`

Expected: All tests PASS

**Step 4: Commit**

```bash
git add README.md
git commit -m "docs: update README with read operations examples"
```

---

## Task 9: Final Verification

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

Expected: Success

**Step 4: Final commit**

```bash
git add .
git commit -m "chore: phase 2 complete - batch read operations implemented"
```

---

## Verification Checklist

After completing all tasks:

- [ ] All unit tests pass (40+ tests)
- [ ] Code formatted with ruff
- [ ] Type checking passes with mypy
- [ ] Package builds successfully
- [ ] Integration tests pass (if Cassandra available)
- [ ] README updated with read examples
- [ ] All commits follow conventional commit format

## Next Steps

After Phase 2 is complete:

- **Phase 3**: Investigate and implement streaming reads via CDC

## Notes for Implementer

- Follow TDD strictly - write tests first
- Use executor-side imports (inside read() method)
- Follow TokenRangesScan.java pattern for token range queries
- Test with real Cassandra using docker-compose
- Commit after each task
- Handle edge cases (empty tables, single partition, wrap-around ranges)

## References

- [TokenRangesScan.java](~/work/samples/cassandra-dse-playground/driver-1.x/src/main/java/com/datastax/alexott/demos/TokenRangesScan.java)
- [PySpark Data Source API](https://spark.apache.org/docs/latest/api/python/tutorial/sql/python_data_source.html)
- [Design Document](docs/plans/2025-12-29-pycassandra-design.md)
