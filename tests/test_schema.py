from pyspark.sql.types import (
    StructType, StringType, IntegerType, LongType
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
    from cassandra import cqltypes

    # Mock table metadata
    table_meta = MagicMock()

    # Mock columns with proper type objects
    id_col = MagicMock()
    id_col.name = "id"
    id_col.type = cqltypes.UUIDType()  # Use actual type object
    id_col.cql_type = "uuid"

    name_col = MagicMock()
    name_col.name = "name"
    name_col.type = cqltypes.UTF8Type()  # Use actual type object
    name_col.cql_type = "text"

    age_col = MagicMock()
    age_col.name = "age"
    age_col.type = cqltypes.Int32Type()  # Use actual type object
    age_col.cql_type = "int"

    table_meta.columns = {
        "id": id_col,
        "name": name_col,
        "age": age_col
    }

    schema = derive_schema_from_table(table_meta)

    assert isinstance(schema, StructType)
    assert len(schema.fields) == 3
    assert schema.fields[0].name == "age"
    assert schema.fields[0].dataType == IntegerType()
    assert schema.fields[1].name == "id"
    assert schema.fields[1].dataType == StringType()
    assert schema.fields[2].name == "name"
    assert schema.fields[2].dataType == StringType()


def test_derive_schema_with_column_filter():
    """Test deriving schema with column filtering."""
    from cassandra_data_source.schema import derive_schema_from_table
    from cassandra import cqltypes

    # Mock table metadata
    table_meta = MagicMock()

    id_col = MagicMock()
    id_col.name = "id"
    id_col.type = cqltypes.UUIDType()  # Use actual type object
    id_col.cql_type = "uuid"

    name_col = MagicMock()
    name_col.name = "name"
    name_col.type = cqltypes.UTF8Type()  # Use actual type object
    name_col.cql_type = "text"

    age_col = MagicMock()
    age_col.name = "age"
    age_col.type = cqltypes.Int32Type()  # Use actual type object
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
