"""
Integration tests requiring a running Cassandra instance.

To run these tests:
1. Start Cassandra: cd tests && docker-compose up -d
2. Wait for ready: docker-compose exec cassandra cqlsh -e "describe keyspaces"
3. Run tests: poetry run pytest tests/test_integration.py -v
4. Stop Cassandra: cd tests && docker-compose down
"""

import pytest
from cassandra.cluster import Cluster
import time


@pytest.fixture(scope="module")
def cassandra_setup():
    """Setup test keyspace and table in Cassandra."""
    # Connect to Cassandra
    cluster = Cluster(["127.0.0.1"], port=9042)
    session = cluster.connect()

    # Create keyspace
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS test_ks
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """)

    # Create table
    session.execute("""
        CREATE TABLE IF NOT EXISTS test_ks.test_table (
            id uuid PRIMARY KEY,
            name text,
            age int,
            score bigint
        )
    """)

    # Truncate table to ensure clean state
    session.execute("TRUNCATE test_ks.test_table")

    yield session

    # Cleanup
    session.execute("DROP TABLE IF EXISTS test_ks.test_table")
    session.execute("DROP KEYSPACE IF EXISTS test_ks")
    cluster.shutdown()


@pytest.mark.integration
def test_write_and_read_integration(spark, cassandra_setup):
    """Test full write and read cycle with real Cassandra."""
    from cassandra_data_source import CassandraDataSource
    import uuid

    # Register data source
    spark.dataSource.register(CassandraDataSource)

    # Create test DataFrame
    data = [
        (str(uuid.uuid4()), "Alice", 30, 100),
        (str(uuid.uuid4()), "Bob", 25, 200),
        (str(uuid.uuid4()), "Charlie", 35, 150)
    ]
    df = spark.createDataFrame(data, ["id", "name", "age", "score"])

    # Write to Cassandra
    df.write.format("pycassandra") \
        .option("host", "127.0.0.1") \
        .option("port", "9042") \
        .option("keyspace", "test_ks") \
        .option("table", "test_table") \
        .save()

    # Verify data was written
    time.sleep(1)  # Give Cassandra time to flush

    rows = cassandra_setup.execute("SELECT COUNT(*) FROM test_ks.test_table")
    count = rows.one()[0]
    assert count == 3


@pytest.mark.integration
def test_write_with_delete_flag_integration(spark, cassandra_setup):
    """Test write with delete flag using real Cassandra."""
    from cassandra_data_source import CassandraDataSource
    import uuid

    # Register data source
    spark.dataSource.register(CassandraDataSource)

    # Insert initial data
    test_id = str(uuid.uuid4())
    cassandra_setup.execute(
        "INSERT INTO test_ks.test_table (id, name, age, score) VALUES (%s, %s, %s, %s)",
        (uuid.UUID(test_id), "ToDelete", 40, 300)
    )

    # Create DataFrame with delete flag
    data = [(test_id, "ToDelete", 40, 300, True)]
    df = spark.createDataFrame(data, ["id", "name", "age", "score", "is_deleted"])

    # Write with delete flag
    df.write.format("pycassandra") \
        .option("host", "127.0.0.1") \
        .option("port", "9042") \
        .option("keyspace", "test_ks") \
        .option("table", "test_table") \
        .option("delete_flag_column", "is_deleted") \
        .option("delete_flag_value", "true") \
        .save()

    # Verify row was deleted
    time.sleep(1)

    rows = cassandra_setup.execute(
        "SELECT COUNT(*) FROM test_ks.test_table WHERE id = %s",
        (uuid.UUID(test_id),)
    )
    count = rows.one()[0]
    assert count == 0


@pytest.mark.integration
@pytest.mark.manual
def test_write_streaming_integration():
    """
    Manual test for streaming writes.

    NOTE: This test is skipped because you cannot use createDataFrame with writeStream.
    createDataFrame creates a batch DataFrame, not a streaming DataFrame.

    To test streaming writes:
    1. Set up a real streaming source (Kafka, socket, rate source, etc.)
    2. Use that streaming source with pycassandra sink
    3. Verify data flows correctly

    Example with rate source:
        stream_df = spark.readStream.format("rate").load()
        stream_df.selectExpr("CAST(value AS STRING) as id", "timestamp as name") \
            .writeStream.format("pycassandra") \
            .option("host", "127.0.0.1") \
            .option("keyspace", "test_ks") \
            .option("table", "test_table") \
            .option("checkpointLocation", "/tmp/checkpoint") \
            .start()
    """
    pytest.skip("Manual test - requires actual streaming source. Cannot use createDataFrame with writeStream.")
