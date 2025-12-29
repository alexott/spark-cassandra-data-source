# PyCassandra Design Document

**Date:** 2025-12-29
**Status:** Approved
**Version:** 1.1 (Updated to address inconsistencies)

## Overview

PyCassandra is a Python Data Source for Apache Spark that enables reading from and writing to Apache Cassandra databases. It addresses compatibility issues with the official Spark Cassandra Connector in Databricks runtimes by providing a pure Python implementation using the PySpark Data Source API.

## Motivation

The official Spark Cassandra Connector:
- Is not actively maintained
- Has compatibility problems with Databricks runtimes
- Requires JVM-based deployment

PyCassandra provides:
- Pure Python implementation (no JVM dependencies)
- Native PySpark Data Source API integration
- Compatibility with Databricks and modern Spark 4.0+ environments
- Minimal, maintainable codebase following SIMPLE principles

## Architecture Overview

PyCassandra follows the standard PySpark Data Source API pattern with a simple, flat structure:

### Core Components

1. **CassandraDataSource** - Entry point class implementing `DataSource`
   - Returns format name: `"pycassandra"`
   - Implements `schema()` - returns Spark schema (implemented in Phase 2 with readers)
   - Factory methods: `writer()`, `streamWriter()`, `reader()`, `streamReader()`

2. **CassandraWriter** - Base writer class with shared logic
   - Validates options in `__init__` (does NOT connect - validation happens in write())
   - Implements `write(iterator)` with executor-side imports, connection, and concurrent execution
   - Creates Cassandra session per partition inside `write()` method
   - All cassandra-driver imports happen inside `write()` for executor isolation

3. **CassandraBatchWriter(CassandraWriter, DataSourceWriter)** - Batch writes
   - Inherits all logic from base writer
   - No additional methods needed

4. **CassandraStreamWriter(CassandraWriter, DataSourceStreamWriter)** - Streaming writes
   - Inherits write logic from base writer
   - Implements `commit()` and `abort()` for micro-batches

5. **CassandraReader** - Base reader class with shared logic
   - Queries Cassandra for token ranges in `__init__`
   - Derives Spark schema from Cassandra table metadata (static method for reuse)
   - Implements `partitions()` - returns one partition per token range
   - Implements `read(partition)` - executes token range query

6. **CassandraBatchReader(CassandraReader, DataSourceReader)** - Batch reads
   - Inherits all logic from base reader

## Options and Configuration

### Connection Options

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `host` | Yes | - | Cassandra contact point (e.g., "127.0.0.1") |
| `port` | No | 9042 | Cassandra port |
| `keyspace` | Yes | - | Target keyspace |
| `table` | Yes | - | Target table name |
| `username` | No | - | Authentication username |
| `password` | No | - | Authentication password |
| `ssl_enabled` | No | false | Enable SSL/TLS |
| `ssl_ca_cert` | No | - | Path to CA certificate for SSL |

### Write Options

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `concurrency` | No | 100 | Number of concurrent requests in execute_concurrent_with_args |
| `rows_per_batch` | No | 1000 | Number of rows to buffer before flushing to Cassandra |
| `delete_flag_column` | No | - | Column name that indicates row should be deleted |
| `delete_flag_value` | No | - | Value that triggers deletion (e.g., "true", "1") |
| `consistency` | No | LOCAL_QUORUM | Write consistency level (ONE, QUORUM, LOCAL_QUORUM, ALL, etc.) |

### Read Options

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `consistency` | No | LOCAL_ONE | Read consistency level |
| `filter` | No | - | Raw CQL WHERE clause to add to token range queries |

### Validation Rules

**In `__init__` (fail fast before any work):**
- Required options checked (host, keyspace, table) - raise `ValueError` with clear message
- If `delete_flag_column` specified, `delete_flag_value` must also be provided (and vice versa)
- If `ssl_ca_cert` provided, `ssl_enabled` must be "true"
- Consistency level must be valid (ONE, TWO, THREE, QUORUM, ALL, LOCAL_QUORUM, EACH_QUORUM, LOCAL_ONE) - raise `ValueError` on unknown

**In `write()` method (first time executed on executor):**
- Connect to Cassandra and query table metadata
- Validate DataFrame contains all primary key columns
- Validate `delete_flag_column` (if specified) exists in DataFrame schema
- Validate all DataFrame columns exist in Cassandra table

## Type Mapping and Conversion

### Cassandra → Spark Type Mapping (Reads)

Aggressive mapping with safe fallbacks:

| Cassandra Type | Spark Type | Notes |
|----------------|------------|-------|
| INT, SMALLINT, TINYINT | IntegerType | Direct mapping |
| BIGINT, COUNTER | LongType | Direct mapping |
| FLOAT | FloatType | Direct mapping |
| DOUBLE | DoubleType | Direct mapping |
| BOOLEAN | BooleanType | Direct mapping |
| TEXT, VARCHAR, ASCII | StringType | Direct mapping |
| TIMESTAMP | TimestampType | Direct mapping |
| DATE | DateType | Direct mapping |
| DECIMAL | DecimalType | Direct mapping |
| BLOB | BinaryType | Direct mapping |
| UUID, TIMEUUID | StringType | Mapped to string (no native UUID type in Spark) |
| INET | StringType | Mapped to string (no native IP type in Spark) |
| UDT, TUPLE | StringType | JSON representation |
| LIST, SET, MAP | StringType | JSON representation |

### Spark → Cassandra Type Conversion (Writes)

Automatic conversion with validation:

| Conversion | Validation | Behavior |
|------------|------------|----------|
| StringType → UUID | uuid.UUID(value) | Fail if invalid format |
| StringType → INET | IP address format | Fail if invalid |
| IntegerType → BIGINT | Auto-promote | Always succeeds |
| LongType → INT | Range check | Fail if overflow |
| StringType → TEXT/VARCHAR | None | Direct pass-through |
| Other mappings | Exact type match | Fail if mismatch |

### Schema Handling

- If user provides schema via `.schema()`, validate it matches Cassandra table structure
- If no schema provided, auto-derive from Cassandra table metadata
- When user provides schema, only SELECT columns present in schema (projection pushdown)

## Write Operations

### Write Flow

1. **Initialization (`__init__`)**
   - Extract and validate all required options (fail fast)
   - Validate consistency level against known values
   - Validate delete flag options (both or neither)
   - Validate SSL options (ca_cert requires ssl_enabled)
   - Store options (no Cassandra connection yet)

2. **Write Execution (`write(iterator)` - runs on executor)**
   - Import cassandra-driver inside method (executor-side import)
   - Create Cassandra cluster and session for this partition
   - Query table metadata to get primary keys and column types
   - Validate DataFrame schema contains all PK columns
   - Validate delete flag column exists (if specified)
   - Prepare INSERT statement with all data columns (excluding delete flag)
   - If delete flag specified, also prepare DELETE statement
   - Process rows in batches:
     - Accumulate up to `rows_per_batch` rows
     - Separate normal rows from delete-flagged rows
     - Apply type conversions (String → UUID, etc.)
     - Execute concurrent inserts: `execute_concurrent_with_args(session, prepared_insert, insert_rows, concurrency=concurrency)`
     - Execute concurrent deletes: `execute_concurrent_with_args(session, prepared_delete, delete_rows, concurrency=concurrency)`
     - Fail fast on any error
   - Close session and return `SimpleCommitMessage`

3. **Type Conversion During Write**
   - Apply conversions based on Cassandra column types
   - String → UUID validation before passing to driver
   - Fail immediately on conversion errors with clear message

4. **Delete Flag Handling**
   - If row has `delete_flag_column == delete_flag_value`, generate DELETE
   - DELETE only uses PK columns: `DELETE FROM table WHERE pk1=? AND pk2=?`
   - Delete flag column is NOT included in INSERT or DELETE statements

### Example Usage

```python
# Batch write
df.write.format("pycassandra") \
    .option("host", "127.0.0.1") \
    .option("keyspace", "myks") \
    .option("table", "mytable") \
    .option("username", "cassandra") \
    .option("password", "cassandra") \
    .save()

# Streaming write
df.writeStream.format("pycassandra") \
    .option("host", "127.0.0.1") \
    .option("keyspace", "myks") \
    .option("table", "mytable") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start()

# Write with delete flag
df.write.format("pycassandra") \
    .option("host", "127.0.0.1") \
    .option("keyspace", "myks") \
    .option("table", "mytable") \
    .option("delete_flag_column", "is_deleted") \
    .option("delete_flag_value", "true") \
    .save()
```

## Read Operations (Token Range Scanning)

### Read Flow

1. **Initialization (`__init__`)**
   - Extract and validate required options (host, keyspace, table)
   - Create temporary Cassandra connection
   - Query table metadata to get:
     - Column definitions and types
     - Primary key structure (partition key columns)
     - Token ranges from cluster metadata
   - If user provided schema, validate it matches table structure
   - If no schema, derive Spark schema from Cassandra types
   - Sort and prepare token ranges (following TokenRangesScan.java pattern)
   - Close temporary connection

2. **Partitioning (`partitions()`)**
   - Return one `InputPartition` per token range
   - Each partition stores: token range start/end, query details
   - Total partitions = number of token ranges in cluster
   - Users can call `.coalesce(N)` to reduce partition count

3. **Read Execution (`read(partition)`)**
   - Import cassandra-driver (executor-side import)
   - Create Cassandra session for this partition
   - Build query following TokenRangesScan.java logic:
     ```sql
     SELECT <columns> FROM keyspace.table
     WHERE token(pk_cols) > start AND token(pk_cols) <= end
     ```
   - Handle edge cases (wrap-around token range, min token)
   - If `filter` option provided, append: `AND (filter_expression)`
   - Set routing token for query optimization
   - Execute query and yield rows as tuples
   - Convert Cassandra types to Spark types during iteration
   - Close session when done

4. **Schema Derivation**
   - Static method: `derive_schema_from_cassandra(table_metadata)`
   - Applies type mapping rules (see Type Mapping section)
   - Returns `StructType` with all columns (or subset if user schema provided)

### Token Range Logic

Following the reference implementation in `TokenRangesScan.java`:

```python
# Get all token ranges from cluster metadata
ranges = sorted(metadata.token_ranges())
min_token = ranges[0].start

# Generate query for each token range
for i, token_range in enumerate(ranges):
    start = token_range.start
    end = token_range.end

    if start == end:
        query = f"WHERE token({pk_cols}) >= {min_token}"
    elif i == 0:
        query = f"WHERE token({pk_cols}) <= {min_token}"
        # Also handle: token(pk) > start AND token(pk) <= end
    elif end == min_token:
        query = f"WHERE token({pk_cols}) > {start}"
    else:
        query = f"WHERE token({pk_cols}) > {start} AND token({pk_cols}) <= {end}"
```

### Example Usage

```python
# Batch read (full table scan)
df = spark.read.format("pycassandra") \
    .option("host", "127.0.0.1") \
    .option("keyspace", "myks") \
    .option("table", "mytable") \
    .load()

# Read with filter
df = spark.read.format("pycassandra") \
    .option("host", "127.0.0.1") \
    .option("keyspace", "myks") \
    .option("table", "mytable") \
    .option("filter", "col1 > 100 AND status = 'active'") \
    .load()

# Read with explicit schema (projection)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("id", StringType()),
    StructField("name", StringType()),
    StructField("age", IntegerType())
])

df = spark.read.format("pycassandra") \
    .schema(schema) \
    .option("host", "127.0.0.1") \
    .option("keyspace", "myks") \
    .option("table", "mytable") \
    .load()

# Reduce partitions after read
df = spark.read.format("pycassandra") \
    .option("host", "127.0.0.1") \
    .option("keyspace", "myks") \
    .option("table", "mytable") \
    .load() \
    .coalesce(10)  # Reduce to 10 partitions
```

## Implementation Phases

### Phase 1: Write Operations (Batch & Streaming with Delete Support)

**Deliverables:**
- Implement `CassandraDataSource`, `CassandraWriter` base class
- Implement `CassandraBatchWriter` and `CassandraStreamWriter`
- Connection management (host, port, username, password, SSL)
- Options validation in `__init__` (fail fast)
- Deferred PK validation in `write()` method
- Type conversion with validation (String → UUID, etc.)
- Delete flag support (`delete_flag_column` and `delete_flag_value`)
- Incremental batching with `rows_per_batch` and `concurrency` options
- Concurrent execution with `execute_concurrent_with_args`

**Testing:**
- Write to table with various data types
- Validate options and PK presence check
- Test type conversions (especially String → UUID)
- Test delete flag functionality
- Test both batch and streaming writes
- Integration tests with real Cassandra

### Phase 2: Batch Read Operations

**Deliverables:**
- Implement `CassandraReader` base class and `CassandraBatchReader`
- Implement `CassandraDataSource.schema()` method
- Token range scanning following TokenRangesScan.java pattern
- Schema derivation from Cassandra metadata
- Schema validation when user provides explicit schema
- Optional `filter` expression support
- Column projection (only read specified columns)

**Testing:**
- Full table scan with various data types
- Filtered reads
- Schema auto-derivation vs explicit schema
- Token range partitioning with real data
- Column projection

### Phase 3: Streaming Reads (Future)

**Deliverables:**
- Investigate Cassandra CDC integration
- Implement `CassandraStreamReader` with offset management
- Design offset tracking and checkpointing

**Status:** To be designed after CDC investigation

## Error Handling

### Fail Fast Validation

**In `__init__`:**
- Missing required options → `ValueError` with clear message
- Invalid consistency level → `ValueError` with list of valid levels
- Delete flag options mismatch → `ValueError` (must specify both or neither)
- SSL options mismatch → `ValueError` (ca_cert requires ssl_enabled=true)

**In `write()` method:**
- Missing PK columns in DataFrame → `ValueError` with list of missing columns
- Delete flag column not in DataFrame → `ValueError`
- DataFrame column not in Cassandra table → `ValueError`
- Null PK values → `ValueError` with row context

**In `read()` method:**
- Schema mismatch (when user provides schema) → `ValueError` with diff

### Connection Errors

- Cassandra unreachable → Let cassandra-driver raise native exception
- Authentication failure → Propagate driver exception with context
- SSL errors → Propagate with helpful message about cert paths

### Write Errors

- Type conversion failures → Fail immediately with row details (which row, which column, which conversion)
- Concurrent write failures → Fail fast on first error (`raise_on_first_error=True`)
- Include partial error context in exception message

### Read Errors

- Query execution failures → Propagate exception
- Type conversion during read → Log warning, return null or string fallback
- Token range errors → Fail with query details

## Testing Strategy

### Unit Tests (pytest)

- Mock cassandra-driver Cluster/Session objects
- Test option validation (required vs optional)
- Test type conversion functions in isolation
- Test PK extraction from metadata
- Test query generation (token range logic, filter append)

### Integration Tests (pytest with actual Cassandra)

- Require local Cassandra instance (docker-compose recommended)
- Test full write/read cycle
- Test delete flag functionality
- Test various data types (UUID, INET, collections)
- Test token range partitioning with real data
- Test filtered reads

### Test Structure

```
tests/
├── conftest.py                    # Shared fixtures (Spark session, Cassandra connection)
├── test_cassandra_writer.py       # Write operations
├── test_cassandra_reader.py       # Read operations
└── test_type_mapping.py           # Type conversions
```

## Dependencies

**Required:**
- `cassandra-driver` 3.29.3 (already in pyproject.toml)
- `pyspark` 4.0.1+ (already in pyproject.toml)

**No additional dependencies needed** - use standard library for:
- UUID validation
- SSL context creation
- JSON serialization (for complex types)

## Design Principles

Following the AGENTS.md guidelines:

1. **SIMPLE over CLEVER** - No abstract base classes beyond what PySpark API requires
2. **EXPLICIT over IMPLICIT** - Direct implementations, clear option names
3. **FLAT over NESTED** - Single-level inheritance (DataSource → Writer → Batch/Stream)
4. **Imports inside methods** - Import cassandra-driver within `write()` and `read()` for partition-level execution
5. **Fail fast** - Validate early in `__init__`, not during execution

## Future Enhancements (Not in Initial Scope)

- DataStax Astra support (token-based auth)
- Additional auth providers (Kerberos, AWS Sigv4)
- Write-time TTL support
- Conditional updates (IF NOT EXISTS, IF EXISTS)
- Counter column support
- Predicate pushdown for automatic filter conversion
- Streaming reads via CDC
- Per-column consistency levels
- Retry policies and backoff strategies

## References

- [PySpark Data Source API Documentation](https://spark.apache.org/docs/latest/api/python/tutorial/sql/python_data_source.html)
- [Databricks Python Data Sources](https://docs.databricks.com/aws/en/pyspark/datasources)
- [Apache Cassandra Documentation](https://cassandra.apache.org/doc/latest/)
- [Python Driver Documentation](https://docs.datastax.com/en/developer/python-driver/3.29/index.html)
- [Original Spark Cassandra Connector](https://github.com/apache/cassandra-spark-connector)
- [TokenRangesScan.java Reference](~/work/samples/cassandra-dse-playground/driver-1.x/src/main/java/com/datastax/alexott/demos/TokenRangesScan.java)
- [Example Data Sources](https://github.com/alexott/cyber-spark-data-connectors)
