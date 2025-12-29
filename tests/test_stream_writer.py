import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import Row


def test_stream_writer_commit(basic_options, sample_schema, mock_table_metadata):
    """Test stream writer commit method."""
    from cassandra_data_source import CassandraDataSource

    # Create mock cluster and session
    mock_cluster = MagicMock()
    mock_session = MagicMock()
    mock_cluster.connect.return_value = mock_session
    mock_cluster.metadata.keyspaces = {"test_ks": MagicMock(tables={"test_table": mock_table_metadata})}

    with patch("cassandra.cluster.Cluster", return_value=mock_cluster):
        ds = CassandraDataSource(basic_options)
        writer = ds.streamWriter(sample_schema, False)

        # Commit should not raise error
        messages = [MagicMock()]
        writer.commit(messages)


def test_stream_writer_abort(basic_options, sample_schema, mock_table_metadata):
    """Test stream writer abort method."""
    from cassandra_data_source import CassandraDataSource

    # Create mock cluster and session
    mock_cluster = MagicMock()
    mock_session = MagicMock()
    mock_cluster.connect.return_value = mock_session
    mock_cluster.metadata.keyspaces = {"test_ks": MagicMock(tables={"test_table": mock_table_metadata})}

    with patch("cassandra.cluster.Cluster", return_value=mock_cluster):
        ds = CassandraDataSource(basic_options)
        writer = ds.streamWriter(sample_schema, False)

        # Abort should not raise error
        messages = [MagicMock()]
        writer.abort(messages)


def test_stream_writer_write(basic_options, sample_schema, mock_table_metadata):
    """Test that stream writer can write data."""
    from cassandra_data_source import CassandraDataSource

    # Create mock cluster and session
    mock_cluster = MagicMock()
    mock_session = MagicMock()
    mock_cluster.connect.return_value = mock_session
    mock_cluster.metadata.keyspaces = {"test_ks": MagicMock(tables={"test_table": mock_table_metadata})}

    # Mock prepared statement
    prepared_stmt = MagicMock()
    mock_session.prepare.return_value = prepared_stmt

    # Mock execute_concurrent_with_args
    def mock_execute(*args, **kwargs):
        return [MagicMock(success=True) for _ in range(len(args[2]))]

    with patch("cassandra.cluster.Cluster", return_value=mock_cluster):
        with patch("cassandra.concurrent.execute_concurrent_with_args", side_effect=mock_execute):
            ds = CassandraDataSource(basic_options)
            writer = ds.streamWriter(sample_schema, False)

            # Create test data
            rows = [
                Row(id="550e8400-e29b-41d4-a716-446655440000", name="Alice", age=30, score=100)
            ]

            # Execute write (inherited from base class)
            result = writer.write(iter(rows))

            # Should return WriterCommitMessage
            assert result is not None
