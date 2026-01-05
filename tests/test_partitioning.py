import pytest


def test_token_range_partition_creation():
    """Test creating a token range partition."""
    from cassandra_data_source.partitioning import TokenRangePartition

    partition = TokenRangePartition(
        partition_id=0,
        start_token="100",
        end_token="200",
        pk_columns=["id"],
        is_wrap_around=False
    )

    assert partition.partition_id == 0
    assert partition.start_token == "100"
    assert partition.end_token == "200"
    assert partition.pk_columns == ["id"]
    assert partition.is_wrap_around is False


def test_token_range_partition_equality():
    """Test partition equality comparison."""
    from cassandra_data_source.partitioning import TokenRangePartition

    p1 = TokenRangePartition(0, "100", "200", ["id"], False)
    p2 = TokenRangePartition(0, "100", "200", ["id"], False)
    p3 = TokenRangePartition(1, "200", "300", ["id"], False)

    assert p1 == p2
    assert p1 != p3
