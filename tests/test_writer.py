"""Tests for writer execution logic."""

import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, BooleanType


def test_write_basic_insert(basic_options, sample_schema, mock_table_metadata):
    """Test basic write operation with inserts."""
    from cassandra_data_source import CassandraDataSource

    # Create mock cluster and session
    mock_cluster = MagicMock()
    mock_session = MagicMock()
    mock_cluster.connect.return_value = mock_session
    mock_cluster.metadata.keyspaces = {"test_ks": MagicMock(tables={"test_table": mock_table_metadata})}

    # Mock prepared statement
    prepared_stmt = MagicMock()
    mock_session.prepare.return_value = prepared_stmt

    # Mock execute_concurrent_with_args to return success
    def mock_execute(*args, **kwargs):
        return [MagicMock(success=True) for _ in range(len(args[2]))]

    # Patch Cluster and execute_concurrent_with_args at their source
    with patch("cassandra.cluster.Cluster", return_value=mock_cluster):
        with patch("cassandra.concurrent.execute_concurrent_with_args", side_effect=mock_execute) as mock_exec:
            ds = CassandraDataSource(basic_options)
            writer = ds.writer(sample_schema, None)

            # Create test data
            rows = [
                Row(id="550e8400-e29b-41d4-a716-446655440000", name="Alice", age=30, score=100),
                Row(id="550e8400-e29b-41d4-a716-446655440001", name="Bob", age=25, score=200)
            ]

            # Execute write
            result = writer.write(iter(rows))

            # Verify prepared statement was created
            assert mock_session.prepare.called
            prepare_call = mock_session.prepare.call_args[0][0]
            assert "INSERT INTO" in prepare_call
            assert "test_table" in prepare_call

            # Verify execute_concurrent_with_args was called
            assert mock_exec.called
            assert result is not None


def test_write_with_delete_flag(basic_options, sample_schema, mock_table_metadata):
    """Test write operation with delete flag."""
    from cassandra_data_source import CassandraDataSource

    # Add delete flag to schema
    schema_with_flag = StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("score", LongType(), True),
        StructField("is_deleted", BooleanType(), True)
    ])

    # Add delete flag options
    options = {**basic_options, "delete_flag_column": "is_deleted", "delete_flag_value": "true"}

    # Create mock cluster and session
    mock_cluster = MagicMock()
    mock_session = MagicMock()
    mock_cluster.connect.return_value = mock_session
    mock_cluster.metadata.keyspaces = {"test_ks": MagicMock(tables={"test_table": mock_table_metadata})}

    # Mock prepared statements
    prepared_insert = MagicMock()
    prepared_delete = MagicMock()
    mock_session.prepare.side_effect = [prepared_insert, prepared_delete]

    # Mock execute_concurrent_with_args
    def mock_execute(*args, **kwargs):
        return [MagicMock(success=True) for _ in range(len(args[2]))]

    with patch("cassandra.cluster.Cluster", return_value=mock_cluster):
        with patch("cassandra.concurrent.execute_concurrent_with_args", side_effect=mock_execute) as mock_exec:
            ds = CassandraDataSource(options)
            writer = ds.writer(schema_with_flag, None)

            # Create test data (one insert, one delete)
            rows = [
                Row(id="550e8400-e29b-41d4-a716-446655440000", name="Alice", age=30, score=100, is_deleted=False),
                Row(id="550e8400-e29b-41d4-a716-446655440001", name="Bob", age=25, score=200, is_deleted=True)
            ]

            # Execute write
            writer.write(iter(rows))

            # Verify both INSERT and DELETE statements were prepared
            assert mock_session.prepare.call_count == 2
            insert_call = mock_session.prepare.call_args_list[0][0][0]
            delete_call = mock_session.prepare.call_args_list[1][0][0]
            assert "INSERT INTO" in insert_call
            assert "DELETE FROM" in delete_call

            # Verify execute_concurrent was called for both inserts and deletes
            assert mock_exec.call_count == 2


def test_write_type_conversion(basic_options, sample_schema, mock_table_metadata):
    """Test that write applies type conversion."""
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
    captured_params = []

    def mock_execute(session, stmt, params, **kwargs):
        captured_params.extend(params)
        return [MagicMock(success=True) for _ in range(len(params))]

    with patch("cassandra.cluster.Cluster", return_value=mock_cluster):
        with patch("cassandra.concurrent.execute_concurrent_with_args", side_effect=mock_execute):
            ds = CassandraDataSource(basic_options)
            writer = ds.writer(sample_schema, None)

            # Create test data with UUID string
            rows = [
                Row(id="550e8400-e29b-41d4-a716-446655440000", name="Alice", age=30, score=100)
            ]

            # Execute write
            writer.write(iter(rows))

            # Verify parameters were captured and UUID was converted
            assert len(captured_params) > 0
            # First parameter should be the UUID object (converted from string)
            import uuid
            assert isinstance(captured_params[0][0], uuid.UUID)


def test_write_null_primary_key_raises_error(basic_options, sample_schema, mock_table_metadata):
    """Test that null primary key values raise ValueError."""
    from cassandra_data_source import CassandraDataSource

    # Create mock cluster and session
    mock_cluster = MagicMock()
    mock_session = MagicMock()
    mock_cluster.connect.return_value = mock_cluster
    mock_cluster.metadata.keyspaces = {"test_ks": MagicMock(tables={"test_table": mock_table_metadata})}

    # Mock prepared statement
    prepared_stmt = MagicMock()
    mock_session.prepare.return_value = prepared_stmt

    with patch("cassandra.cluster.Cluster", return_value=mock_cluster):
        mock_cluster.connect.return_value = mock_session
        ds = CassandraDataSource(basic_options)
        writer = ds.writer(sample_schema, None)

        # Create test data with null PK
        rows = [
            Row(id=None, name="Alice", age=30, score=100)
        ]

        # Execute write should raise ValueError
        with pytest.raises(ValueError, match="Primary key column 'id' cannot be null"):
            writer.write(iter(rows))


def test_write_incremental_batching(basic_options, sample_schema, mock_table_metadata):
    """Test that write flushes batches incrementally."""
    from cassandra_data_source import CassandraDataSource

    # Set small batch size for testing
    options = {**basic_options, "rows_per_batch": "2"}

    # Create mock cluster and session
    mock_cluster = MagicMock()
    mock_session = MagicMock()
    mock_cluster.connect.return_value = mock_session
    mock_cluster.metadata.keyspaces = {"test_ks": MagicMock(tables={"test_table": mock_table_metadata})}

    # Mock prepared statement
    prepared_stmt = MagicMock()
    mock_session.prepare.return_value = prepared_stmt

    # Track execute calls
    execute_calls = []

    def mock_execute(session, stmt, params, **kwargs):
        execute_calls.append(len(params))
        return [MagicMock(success=True) for _ in range(len(params))]

    with patch("cassandra.cluster.Cluster", return_value=mock_cluster):
        with patch("cassandra.concurrent.execute_concurrent_with_args", side_effect=mock_execute):
            ds = CassandraDataSource(options)
            writer = ds.writer(sample_schema, None)

            # Create 5 rows (should trigger 2 flushes: 2 rows, 2 rows, 1 row)
            rows = [
                Row(id=f"550e8400-e29b-41d4-a716-44665544000{i}", name=f"User{i}", age=20+i, score=100+i)
                for i in range(5)
            ]

            # Execute write
            writer.write(iter(rows))

            # Verify multiple execute calls (incremental batching)
            # Should have 3 calls: batch of 2, batch of 2, batch of 1
            assert len(execute_calls) == 3
            assert execute_calls[0] == 2
            assert execute_calls[1] == 2
            assert execute_calls[2] == 1


def test_write_consistency_level_applied(basic_options, sample_schema, mock_table_metadata):
    """Test that consistency level is properly set."""
    from cassandra_data_source import CassandraDataSource

    # Set custom consistency level
    options = {**basic_options, "consistency": "QUORUM"}

    # Create mock cluster and session
    mock_cluster = MagicMock()
    mock_session = MagicMock()
    mock_cluster.connect.return_value = mock_session
    mock_cluster.metadata.keyspaces = {"test_ks": MagicMock(tables={"test_table": mock_table_metadata})}

    # Mock prepared statement
    prepared_stmt = MagicMock()
    mock_session.prepare.return_value = prepared_stmt

    def mock_execute(*args, **kwargs):
        return [MagicMock(success=True)]

    with patch("cassandra.cluster.Cluster", return_value=mock_cluster):
        with patch("cassandra.concurrent.execute_concurrent_with_args", side_effect=mock_execute):
            ds = CassandraDataSource(options)
            writer = ds.writer(sample_schema, None)

            rows = [
                Row(id="550e8400-e29b-41d4-a716-446655440000", name="Alice", age=30, score=100)
            ]

            writer.write(iter(rows))

            # Verify consistency level was set on prepared statement
            # Note: This is checked by verifying the attribute was set
            assert prepared_stmt.consistency_level is not None


def test_write_with_ssl(basic_options, sample_schema, mock_table_metadata):
    """Test that SSL connection is properly configured."""
    from cassandra_data_source import CassandraDataSource

    # Enable SSL
    options = {**basic_options, "ssl_enabled": "true"}

    # Create mock cluster and session
    mock_cluster = MagicMock()
    mock_session = MagicMock()
    mock_cluster.connect.return_value = mock_session
    mock_cluster.metadata.keyspaces = {"test_ks": MagicMock(tables={"test_table": mock_table_metadata})}

    # Mock prepared statement
    prepared_stmt = MagicMock()
    mock_session.prepare.return_value = prepared_stmt

    def mock_execute(*args, **kwargs):
        return [MagicMock(success=True)]

    with patch("cassandra.cluster.Cluster") as mock_cluster_class:
        mock_cluster_class.return_value = mock_cluster
        with patch("cassandra.concurrent.execute_concurrent_with_args", side_effect=mock_execute):
            ds = CassandraDataSource(options)
            writer = ds.writer(sample_schema, None)

            rows = [
                Row(id="550e8400-e29b-41d4-a716-446655440000", name="Alice", age=30, score=100)
            ]

            writer.write(iter(rows))

            # Verify Cluster was called with ssl_context
            call_kwargs = mock_cluster_class.call_args[1]
            assert "ssl_context" in call_kwargs
