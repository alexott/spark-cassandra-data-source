"""Schema derivation utilities for Cassandra types."""

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, ShortType, ByteType,
    FloatType, DoubleType, BooleanType, TimestampType, DateType,
    BinaryType, DecimalType, ArrayType, MapType
)


def cassandra_to_spark_type(cassandra_type):
    """
    Convert Cassandra type to Spark type using cassandra.cqltypes objects.

    Args:
        cassandra_type: Cassandra type object (e.g., Int32Type, ListType, etc.)
                       or string (for backward compatibility with tests)

    Returns:
        PySpark DataType
    """
    # Import cassandra types here to avoid module-level import
    from cassandra import cqltypes

    # Handle string input for backward compatibility (used in tests)
    if isinstance(cassandra_type, str):
        return _cassandra_string_to_spark_type(cassandra_type)

    # Handle frozen types - unwrap and process inner type
    if isinstance(cassandra_type, cqltypes.FrozenType):
        return cassandra_to_spark_type(cassandra_type.subtype)

    # Numeric types with proper precision
    if isinstance(cassandra_type, cqltypes.ByteType):
        return ByteType()
    if isinstance(cassandra_type, cqltypes.ShortType):
        return ShortType()
    if isinstance(cassandra_type, cqltypes.Int32Type):
        return IntegerType()
    if isinstance(cassandra_type, (cqltypes.LongType, cqltypes.CounterColumnType)):
        return LongType()
    if isinstance(cassandra_type, cqltypes.FloatType):
        return FloatType()
    if isinstance(cassandra_type, cqltypes.DoubleType):
        return DoubleType()
    if isinstance(cassandra_type, cqltypes.DecimalType):
        return DecimalType()
    if isinstance(cassandra_type, cqltypes.IntegerType):
        return DecimalType()  # Variable precision integer (varint)

    # Boolean
    if isinstance(cassandra_type, cqltypes.BooleanType):
        return BooleanType()

    # String types
    if isinstance(cassandra_type, (cqltypes.UTF8Type, cqltypes.VarcharType, cqltypes.AsciiType)):
        return StringType()

    # UUID types - map to string (no native UUID in Spark)
    if isinstance(cassandra_type, (cqltypes.UUIDType, cqltypes.TimeUUIDType)):
        return StringType()

    # Date/Time types
    if isinstance(cassandra_type, cqltypes.TimestampType):
        return TimestampType()
    if isinstance(cassandra_type, cqltypes.DateType):
        return DateType()
    if isinstance(cassandra_type, cqltypes.TimeType):
        return LongType()  # Store as nanoseconds since midnight

    # Binary types
    if isinstance(cassandra_type, cqltypes.BytesType):
        return BinaryType()

    # INET - map to string
    if isinstance(cassandra_type, cqltypes.InetAddressType):
        return StringType()

    # Collection types
    if isinstance(cassandra_type, cqltypes.ListType):
        element_type = cassandra_to_spark_type(cassandra_type.subtypes[0])
        return ArrayType(element_type)

    if isinstance(cassandra_type, cqltypes.SetType):
        # Spark doesn't have Set type, use Array
        element_type = cassandra_to_spark_type(cassandra_type.subtypes[0])
        return ArrayType(element_type)

    if isinstance(cassandra_type, cqltypes.MapType):
        key_type = cassandra_to_spark_type(cassandra_type.subtypes[0])
        value_type = cassandra_to_spark_type(cassandra_type.subtypes[1])
        return MapType(key_type, value_type)

    # Tuple type - map to struct with generic field names
    if isinstance(cassandra_type, cqltypes.TupleType):
        fields = []
        for i, subtype in enumerate(cassandra_type.subtypes):
            spark_type = cassandra_to_spark_type(subtype)
            fields.append(StructField(f"_field{i}", spark_type, nullable=True))
        return StructType(fields)

    # User-defined type - map to struct with proper field names
    if isinstance(cassandra_type, cqltypes.UserType):
        fields = []
        for field_name, field_type in zip(cassandra_type.fieldnames, cassandra_type.subtypes):
            spark_type = cassandra_to_spark_type(field_type)
            fields.append(StructField(field_name, spark_type, nullable=True))
        return StructType(fields)

    # Default fallback
    return StringType()


def _cassandra_string_to_spark_type(cass_type_str):
    """
    Convert Cassandra type string to Spark type (backward compatibility for tests).

    Args:
        cass_type_str: Cassandra CQL type string (e.g., "int", "text")

    Returns:
        PySpark DataType
    """
    cass_type = cass_type_str.lower()

    # Numeric types - use proper precision
    if cass_type == "tinyint":
        return ByteType()
    if cass_type == "smallint":
        return ShortType()
    if cass_type == "int":
        return IntegerType()
    if cass_type in ("bigint", "counter"):
        return LongType()
    if cass_type == "float":
        return FloatType()
    if cass_type == "double":
        return DoubleType()
    if cass_type in ("decimal", "varint"):
        return DecimalType()

    # Boolean
    if cass_type == "boolean":
        return BooleanType()

    # String types
    if cass_type in ("text", "varchar", "ascii"):
        return StringType()

    # UUID types
    if cass_type in ("uuid", "timeuuid"):
        return StringType()

    # Date/Time types
    if cass_type == "timestamp":
        return TimestampType()
    if cass_type == "date":
        return DateType()

    # Binary types
    if cass_type == "blob":
        return BinaryType()

    # INET
    if cass_type == "inet":
        return StringType()

    # Complex types - can't parse from string, fallback to string
    if cass_type.startswith(("list", "set", "map", "tuple", "frozen")):
        return StringType()

    # Default fallback
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
        # Use col_meta.cql_type (string representation of type)
        spark_type = cassandra_to_spark_type(col_meta.cql_type)

        # All columns nullable in Spark (Cassandra allows nulls except in PKs)
        fields.append(StructField(col_name, spark_type, nullable=True))

    return StructType(fields)
