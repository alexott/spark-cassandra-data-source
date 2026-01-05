"""Partitioning utilities for Cassandra token ranges."""

from pyspark.sql.datasource import InputPartition


class TokenRangePartition(InputPartition):
    """
    Represents a Cassandra token range partition.

    Each partition corresponds to one Cassandra token range and will be
    read independently by a Spark executor.
    """

    def __init__(self, partition_id, start_token, end_token, pk_columns, is_wrap_around):
        """
        Initialize a token range partition.

        Args:
            partition_id: Unique partition identifier
            start_token: Start token (exclusive)
            end_token: End token (inclusive)
            pk_columns: List of partition key column names
            is_wrap_around: True if this is the wrap-around range (start > end)
        """
        self.partition_id = partition_id
        self.start_token = str(start_token)
        self.end_token = str(end_token)
        self.pk_columns = pk_columns
        self.is_wrap_around = is_wrap_around

    def __eq__(self, other):
        """Check equality based on partition content."""
        if not isinstance(other, TokenRangePartition):
            return False
        return (
            self.partition_id == other.partition_id and
            self.start_token == other.start_token and
            self.end_token == other.end_token and
            self.pk_columns == other.pk_columns and
            self.is_wrap_around == other.is_wrap_around
        )

    def __hash__(self):
        """Return hash for use in sets/dicts."""
        return hash((self.partition_id, self.start_token, self.end_token,
                    tuple(self.pk_columns), self.is_wrap_around))

    def __repr__(self):
        """Return string representation."""
        return (f"TokenRangePartition(id={self.partition_id}, "
                f"start={self.start_token}, end={self.end_token}, "
                f"pk={self.pk_columns}, wrap={self.is_wrap_around})")
