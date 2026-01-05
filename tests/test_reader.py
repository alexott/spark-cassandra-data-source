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


def test_reader_read_executes_token_range_query(mock_table_metadata):
    """Test read() method executes query for token range."""
    from cassandra_data_source.reader import CassandraReader
    from cassandra_data_source.partitioning import TokenRangePartition

    options = {
        "host": "127.0.0.1",
        "keyspace": "test_ks",
        "table": "test_table"
    }

    with patch("cassandra.cluster.Cluster") as mock_cluster:
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

        # Create a partition to read (normal range: token > start AND token <= end)
        partition = TokenRangePartition(
            partition_id=0,
            start_token="100",
            end_token="200",
            pk_columns=["id"],
            is_wrap_around=False,
            min_token="-9223372036854775808"
        )

        # Mock Cluster again for read() execution
        with patch("cassandra.cluster.Cluster") as mock_read_cluster:
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


def test_reader_read_with_unbounded_start(mock_table_metadata):
    """Test read() with unbounded start token (start_token is None)."""
    from cassandra_data_source.reader import CassandraReader
    from cassandra_data_source.partitioning import TokenRangePartition

    options = {
        "host": "127.0.0.1",
        "keyspace": "test_ks",
        "table": "test_table"
    }

    with patch("cassandra.cluster.Cluster") as mock_cluster:
        mock_cluster_instance = MagicMock()
        mock_session = MagicMock()
        mock_cluster_instance.connect.return_value = mock_session
        mock_cluster_instance.metadata.keyspaces = {
            "test_ks": MagicMock(tables={"test_table": mock_table_metadata})
        }
        mock_cluster_instance.metadata.token_ranges.return_value = []
        mock_cluster.return_value = mock_cluster_instance

        reader = CassandraReader(options, None)

        # Partition with start_token = None (unbounded start)
        partition = TokenRangePartition(
            partition_id=0,
            start_token=None,
            end_token="-9223372036854775808",
            pk_columns=["id"],
            is_wrap_around=True,
            min_token="-9223372036854775808"
        )

        with patch("cassandra.cluster.Cluster") as mock_read_cluster:
            mock_read_cluster_instance = MagicMock()
            mock_read_session = MagicMock()
            mock_read_cluster_instance.connect.return_value = mock_read_session
            mock_read_cluster.return_value = mock_read_cluster_instance

            mock_read_session.execute.return_value = []

            # Execute read
            result = list(reader.read(partition))

            # Verify query was executed with correct WHERE clause
            call_args = mock_read_session.execute.call_args
            query = call_args[0][0]
            # Should use: token(id) <= end_token
            assert "token(id) <=" in query


def test_reader_read_with_unbounded_end(mock_table_metadata):
    """Test read() with unbounded end token (end_token is None)."""
    from cassandra_data_source.reader import CassandraReader
    from cassandra_data_source.partitioning import TokenRangePartition

    options = {
        "host": "127.0.0.1",
        "keyspace": "test_ks",
        "table": "test_table"
    }

    with patch("cassandra.cluster.Cluster") as mock_cluster:
        mock_cluster_instance = MagicMock()
        mock_session = MagicMock()
        mock_cluster_instance.connect.return_value = mock_session
        mock_cluster_instance.metadata.keyspaces = {
            "test_ks": MagicMock(tables={"test_table": mock_table_metadata})
        }
        mock_cluster_instance.metadata.token_ranges.return_value = []
        mock_cluster.return_value = mock_cluster_instance

        reader = CassandraReader(options, None)

        # Partition with end_token = None (unbounded end)
        partition = TokenRangePartition(
            partition_id=0,
            start_token="100",
            end_token=None,
            pk_columns=["id"],
            is_wrap_around=False,
            min_token="-9223372036854775808"
        )

        with patch("cassandra.cluster.Cluster") as mock_read_cluster:
            mock_read_cluster_instance = MagicMock()
            mock_read_session = MagicMock()
            mock_read_cluster_instance.connect.return_value = mock_read_session
            mock_read_cluster.return_value = mock_read_cluster_instance

            mock_read_session.execute.return_value = []

            # Execute read
            result = list(reader.read(partition))

            # Verify query was executed with correct WHERE clause
            call_args = mock_read_session.execute.call_args
            query = call_args[0][0]
            # Should use: token(id) > start_token (no upper bound)
            assert "token(id) >" in query
            assert "<=" not in query


def test_reader_read_with_filter(mock_table_metadata):
    """Test read() with optional filter."""
    from cassandra_data_source.reader import CassandraReader
    from cassandra_data_source.partitioning import TokenRangePartition

    options = {
        "host": "127.0.0.1",
        "keyspace": "test_ks",
        "table": "test_table",
        "filter": "age >= 18 ALLOW FILTERING"
    }

    with patch("cassandra.cluster.Cluster") as mock_cluster:
        mock_cluster_instance = MagicMock()
        mock_session = MagicMock()
        mock_cluster_instance.connect.return_value = mock_session
        mock_cluster_instance.metadata.keyspaces = {
            "test_ks": MagicMock(tables={"test_table": mock_table_metadata})
        }
        mock_cluster_instance.metadata.token_ranges.return_value = []
        mock_cluster.return_value = mock_cluster_instance

        reader = CassandraReader(options, None)

        partition = TokenRangePartition(
            partition_id=0,
            start_token="100",
            end_token="200",
            pk_columns=["id"],
            is_wrap_around=False,
            min_token="-9223372036854775808"
        )

        with patch("cassandra.cluster.Cluster") as mock_read_cluster:
            mock_read_cluster_instance = MagicMock()
            mock_read_session = MagicMock()
            mock_read_cluster_instance.connect.return_value = mock_read_session
            mock_read_cluster.return_value = mock_read_cluster_instance

            mock_read_session.execute.return_value = []

            # Execute read
            result = list(reader.read(partition))

            # Verify query includes filter
            call_args = mock_read_session.execute.call_args
            query = call_args[0][0]
            assert "age >= 18 ALLOW FILTERING" in query


def test_reader_read_returns_tuples_in_schema_order(mock_table_metadata):
    """Test read() returns tuples matching schema column order."""
    from cassandra_data_source.reader import CassandraReader
    from cassandra_data_source.partitioning import TokenRangePartition

    options = {
        "host": "127.0.0.1",
        "keyspace": "test_ks",
        "table": "test_table"
    }

    with patch("cassandra.cluster.Cluster") as mock_cluster:
        mock_cluster_instance = MagicMock()
        mock_session = MagicMock()
        mock_cluster_instance.connect.return_value = mock_session
        mock_cluster_instance.metadata.keyspaces = {
            "test_ks": MagicMock(tables={"test_table": mock_table_metadata})
        }
        mock_cluster_instance.metadata.token_ranges.return_value = []
        mock_cluster.return_value = mock_cluster_instance

        reader = CassandraReader(options, None)

        partition = TokenRangePartition(
            partition_id=0,
            start_token="100",
            end_token="200",
            pk_columns=["id"],
            is_wrap_around=False,
            min_token="-9223372036854775808"
        )

        with patch("cassandra.cluster.Cluster") as mock_read_cluster:
            mock_read_cluster_instance = MagicMock()
            mock_read_session = MagicMock()
            mock_read_cluster_instance.connect.return_value = mock_read_session
            mock_read_cluster.return_value = mock_read_cluster_instance

            # Mock row data
            mock_row = MagicMock()
            mock_row.__getitem__ = lambda self, key: {
                "age": 30,
                "id": "test-id",
                "name": "Alice",
                "score": 100
            }[key]
            mock_read_session.execute.return_value = [mock_row]

            # Execute read
            result = list(reader.read(partition))

            # Verify returns tuples
            assert len(result) == 1
            assert isinstance(result[0], tuple)
            # Verify tuple has correct number of elements (4 columns: age, id, name, score)
            assert len(result[0]) == 4
