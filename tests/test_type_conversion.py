import pytest
import uuid as uuid_lib


def test_string_to_uuid_valid():
    """Test converting valid UUID string."""
    from cassandra_data_source.type_conversion import convert_value

    uuid_str = "550e8400-e29b-41d4-a716-446655440000"
    result = convert_value(uuid_str, "uuid")

    assert isinstance(result, uuid_lib.UUID)
    assert str(result) == uuid_str


def test_string_to_uuid_invalid():
    """Test converting invalid UUID string raises error."""
    from cassandra_data_source.type_conversion import convert_value

    with pytest.raises(ValueError, match="Invalid UUID"):
        convert_value("not-a-uuid", "uuid")


def test_int_to_bigint():
    """Test converting int to bigint."""
    from cassandra_data_source.type_conversion import convert_value

    result = convert_value(42, "bigint")
    assert result == 42
    assert isinstance(result, int)


def test_long_to_int_in_range():
    """Test converting long to int within range."""
    from cassandra_data_source.type_conversion import convert_value

    result = convert_value(42, "int")
    assert result == 42


def test_long_to_int_overflow():
    """Test converting long to int with overflow raises error."""
    from cassandra_data_source.type_conversion import convert_value

    with pytest.raises(ValueError, match="overflow"):
        convert_value(2**32, "int")


def test_string_to_text():
    """Test string to text is pass-through."""
    from cassandra_data_source.type_conversion import convert_value

    result = convert_value("hello", "text")
    assert result == "hello"


def test_none_value():
    """Test None values pass through."""
    from cassandra_data_source.type_conversion import convert_value

    result = convert_value(None, "text")
    assert result is None
