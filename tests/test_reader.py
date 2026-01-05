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

    # Mock Cluster to avoid actual connection - patch where it's imported
    with patch("cassandra.cluster.Cluster") as mock_cluster:
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

    with patch("cassandra.cluster.Cluster") as mock_cluster:
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

    with patch("cassandra.cluster.Cluster") as mock_cluster:
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
