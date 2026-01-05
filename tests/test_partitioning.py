import pytest
from unittest.mock import MagicMock, patch


def test_token_range_partition_creation():
    """Test creating a token range partition."""
    from cassandra_data_source.partitioning import TokenRangePartition

    partition = TokenRangePartition(
        partition_id=0,
        start_token="100",
        end_token="200",
        pk_columns=["id"],
        is_wrap_around=False,
        min_token="50"
    )

    assert partition.partition_id == 0
    assert partition.start_token == "100"
    assert partition.end_token == "200"
    assert partition.pk_columns == ["id"]
    assert partition.is_wrap_around is False
    assert partition.min_token == "50"


def test_token_range_partition_equality():
    """Test partition equality comparison."""
    from cassandra_data_source.partitioning import TokenRangePartition

    p1 = TokenRangePartition(0, "100", "200", ["id"], False, "50")
    p2 = TokenRangePartition(0, "100", "200", ["id"], False, "50")
    p3 = TokenRangePartition(1, "200", "300", ["id"], False, "50")

    assert p1 == p2
    assert p1 != p3


def test_reader_creates_partitions_from_token_ranges(mock_table_metadata):
    """Test reader creates partitions following TokenRangesScan.java pattern.

    The first range gets split into TWO partitions (wrap-around + normal),
    subsequent ranges get ONE partition each.
    """
    from cassandra_data_source.reader import CassandraReader
    from cassandra_data_source.partitioning import TokenRangePartition

    options = {
        "host": "127.0.0.1",
        "keyspace": "test_ks",
        "table": "test_table"
    }

    # Mock token ranges (need to be sortable)
    range1 = MagicMock()
    range1.start = MagicMock()
    range1.start.value = 100
    range1.end = MagicMock()
    range1.end.value = 200
    # Make sortable by implementing comparison
    range1.__lt__ = lambda self, other: self.start.value < other.start.value
    range1.__eq__ = lambda self, other: self.start.value == other.start.value

    range2 = MagicMock()
    range2.start = MagicMock()
    range2.start.value = 200
    range2.end = MagicMock()
    range2.end.value = 300
    range2.__lt__ = lambda self, other: self.start.value < other.start.value
    range2.__eq__ = lambda self, other: self.start.value == other.start.value

    with patch("cassandra.cluster.Cluster") as mock_cluster:
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

        # Should have 3 partitions: first range splits into 2, second range is 1
        assert len(partitions) == 3
        assert all(isinstance(p, TokenRangePartition) for p in partitions)

        # Partition 0: Wrap-around portion (token <= minToken)
        assert partitions[0].partition_id == 0
        assert partitions[0].start_token is None  # Unbounded
        assert partitions[0].end_token == "100"
        assert partitions[0].pk_columns == ["id"]
        assert partitions[0].is_wrap_around is True
        assert partitions[0].min_token == "100"

        # Partition 1: Normal portion of first range (token > 100 AND token <= 200)
        assert partitions[1].partition_id == 1
        assert partitions[1].start_token == "100"
        assert partitions[1].end_token == "200"
        assert partitions[1].pk_columns == ["id"]
        assert partitions[1].is_wrap_around is False

        # Partition 2: Second range (token > 200 AND token <= 300)
        assert partitions[2].partition_id == 2
        assert partitions[2].start_token == "200"
        assert partitions[2].end_token == "300"
        assert partitions[2].pk_columns == ["id"]
        assert partitions[2].is_wrap_around is False


def test_reader_handles_empty_token_ranges(mock_table_metadata):
    """Test reader handles case with no token ranges."""
    from cassandra_data_source.reader import CassandraReader

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
        partitions = reader.partitions()

        # Should return empty list
        assert partitions == []


def test_reader_detects_wrap_around_range(mock_table_metadata):
    """Test reader correctly handles degenerate single-node case.

    When start == end (entire ring), creates one partition with
    query: token >= minToken.
    """
    from cassandra_data_source.reader import CassandraReader

    options = {
        "host": "127.0.0.1",
        "keyspace": "test_ks",
        "table": "test_table"
    }

    # Mock degenerate range (start == end) - single node cluster
    wrap_range = MagicMock()
    wrap_range.start = MagicMock()
    wrap_range.start.value = -9223372036854775808  # Min token
    wrap_range.end = MagicMock()
    wrap_range.end.value = -9223372036854775808  # Same as start

    with patch("cassandra.cluster.Cluster") as mock_cluster:
        mock_cluster_instance = MagicMock()
        mock_session = MagicMock()
        mock_cluster_instance.connect.return_value = mock_session
        mock_cluster_instance.metadata.keyspaces = {
            "test_ks": MagicMock(tables={"test_table": mock_table_metadata})
        }
        mock_cluster_instance.metadata.token_ranges.return_value = [wrap_range]
        mock_cluster.return_value = mock_cluster_instance

        reader = CassandraReader(options, None)
        partitions = reader.partitions()

        # Should have 1 partition marked as wrap-around
        # Query will be: token >= minToken (unbounded end)
        assert len(partitions) == 1
        assert partitions[0].is_wrap_around is True
        assert partitions[0].start_token == "-9223372036854775808"
        assert partitions[0].end_token is None  # Unbounded
