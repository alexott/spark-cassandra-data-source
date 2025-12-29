import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from unittest.mock import MagicMock


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
