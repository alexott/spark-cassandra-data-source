"""Type conversion utilities for Cassandra data types."""

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
            if value < -(2**31) or value >= 2**31:
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
