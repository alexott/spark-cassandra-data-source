import pytest
from pyspark.sql.types import StructType, StructField, StringType


def test_missing_required_option_host():
    """Test that missing host option raises ValueError."""
    from cassandra_data_source import CassandraDataSource

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

    schema = StructType([StructField("id", StringType())])
    options = {
        "host": "127.0.0.1",
        "keyspace": "test_ks"
    }

    ds = CassandraDataSource(options)

    with pytest.raises(ValueError, match="table"):
        ds.writer(schema, None)


def test_invalid_consistency_level():
    """Test that invalid consistency level raises ValueError."""
    from cassandra_data_source import CassandraDataSource

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


def test_delete_flag_value_without_column():
    """Test that delete_flag_value without column raises ValueError."""
    from cassandra_data_source import CassandraDataSource

    schema = StructType([StructField("id", StringType())])
    options = {
        "host": "127.0.0.1",
        "keyspace": "test_ks",
        "table": "test_table",
        "delete_flag_value": "true"
        # delete_flag_column not provided
    }

    ds = CassandraDataSource(options)

    with pytest.raises(ValueError, match="must be specified together"):
        ds.writer(schema, None)


def test_default_port():
    """Test that port defaults to 9042."""
    from cassandra_data_source import CassandraDataSource

    schema = StructType([StructField("id", StringType())])
    options = {
        "host": "127.0.0.1",
        "keyspace": "test_ks",
        "table": "test_table"
    }

    ds = CassandraDataSource(options)

    # This test will verify the default port is used when writer is created
    # Since we don't connect in __init__, we just create the writer object
    # The actual port validation would happen in write() method
    writer = ds.writer(schema, None)

    # Verify default port was set
    assert writer.port == 9042


def test_default_consistency():
    """Test that consistency defaults to LOCAL_QUORUM."""
    from cassandra_data_source import CassandraDataSource

    schema = StructType([StructField("id", StringType())])
    options = {
        "host": "127.0.0.1",
        "keyspace": "test_ks",
        "table": "test_table"
    }

    ds = CassandraDataSource(options)
    writer = ds.writer(schema, None)

    # Verify default consistency was set
    assert writer.consistency == "LOCAL_QUORUM"


def test_valid_consistency_levels():
    """Test that valid consistency levels are accepted."""
    from cassandra_data_source import CassandraDataSource

    schema = StructType([StructField("id", StringType())])

    valid_levels = ["ONE", "TWO", "THREE", "QUORUM", "ALL",
                    "LOCAL_QUORUM", "EACH_QUORUM", "LOCAL_ONE"]

    for level in valid_levels:
        options = {
            "host": "127.0.0.1",
            "keyspace": "test_ks",
            "table": "test_table",
            "consistency": level
        }

        ds = CassandraDataSource(options)
        writer = ds.writer(schema, None)

        # Should not raise an error and should store the consistency level
        assert writer.consistency == level
