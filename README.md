# PyCassandra - Python Data Source for Apache Cassandra

Python Data Source for Apache Spark enabling batch and streaming reads/writes to Apache Cassandra.

## Features

- **Batch Writes**: Write DataFrames to Cassandra tables with concurrent execution
- **Streaming Writes**: Write streaming DataFrames with micro-batch processing
- **Type Conversion**: Automatic type mapping with validation (String <-> UUID, etc.)
- **Delete Flag Support**: Conditional row deletion during writes
- **SSL/TLS**: Secure connections to production clusters
- **Primary Key Validation**: Early validation ensures DataFrame contains all PK columns

## Installation

```bash
poetry install
```

## Quick Start

### Batch Write

```python
from pyspark.sql import SparkSession
from cassandra_data_source import CassandraDataSource

spark = SparkSession.builder.appName("pycassandra").getOrCreate()
spark.dataSource.register(CassandraDataSource)

df = spark.createDataFrame([
    ("550e8400-e29b-41d4-a716-446655440000", "Alice", 30),
    ("550e8400-e29b-41d4-a716-446655440001", "Bob", 25)
], ["id", "name", "age"])

df.write.format("pycassandra") \
    .option("host", "127.0.0.1") \
    .option("keyspace", "myks") \
    .option("table", "users") \
    .save()
```

### Streaming Write

```python
df.writeStream.format("pycassandra") \
    .option("host", "127.0.0.1") \
    .option("keyspace", "myks") \
    .option("table", "users") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start()
```

### Write with Delete Flag

```python
df.write.format("pycassandra") \
    .option("host", "127.0.0.1") \
    .option("keyspace", "myks") \
    .option("table", "users") \
    .option("delete_flag_column", "is_deleted") \
    .option("delete_flag_value", "true") \
    .save()
```

## Configuration Options

### Connection Options

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `host` | Yes | - | Cassandra contact point |
| `port` | No | 9042 | Cassandra port |
| `keyspace` | Yes | - | Target keyspace |
| `table` | Yes | - | Target table |
| `username` | No | - | Authentication username |
| `password` | No | - | Authentication password |
| `ssl_enabled` | No | false | Enable SSL/TLS |
| `ssl_ca_cert` | No | - | Path to CA certificate |

### Write Options

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `concurrency` | No | 100 | Number of concurrent requests for write operations |
| `rows_per_batch` | No | 1000 | Number of rows to buffer before flushing to Cassandra |
| `consistency` | No | LOCAL_QUORUM | Write consistency level (ONE, TWO, THREE, QUORUM, ALL, LOCAL_QUORUM, EACH_QUORUM, LOCAL_ONE) |
| `delete_flag_column` | No | - | Column indicating deletion (must be used with delete_flag_value) |
| `delete_flag_value` | No | - | Value triggering deletion (must be used with delete_flag_column) |

## Development

### Setup

```bash
poetry install
```

### Run Tests

```bash
# Unit tests only
poetry run pytest -v -m "not integration"

# Integration tests (requires Cassandra)
cd tests && docker-compose up -d
poetry run pytest -v -m integration
cd tests && docker-compose down
```

### Code Quality

```bash
poetry run ruff check src/
poetry run ruff format src/
poetry run mypy src/
```

## Design

See [Design Document](docs/plans/2025-12-29-pycassandra-design.md) for comprehensive architecture and implementation details.

## Phase 1 Status

- [x] Batch writes
- [x] Streaming writes
- [x] Connection management (host, port, auth, SSL)
- [x] Primary key validation
- [x] Type conversion (String <-> UUID, Int <-> BigInt)
- [x] Delete flag support
- [ ] Batch reads (Phase 3)
- [ ] Streaming reads (Phase 4)

## License

MIT
