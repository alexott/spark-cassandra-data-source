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
