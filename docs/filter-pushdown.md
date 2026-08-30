# Filter Pushdown for Cassandra Data Source

## Overview

Starting with Spark 4.1.0, the Cassandra data source supports **filter pushdown** through the `pushFilters` API. This feature allows Spark to push supported filters down to Cassandra, reducing the amount of data transferred and improving query performance.

**Note**: Right now, this functionality requires that `spark.sql.python.filterPushdown.enabled` is set to `true`.

## Features

- **Automatic Filter Translation**: Spark filters are automatically converted to Cassandra CQL WHERE clauses
- **Cassandra-Aware Validation**: Filters are validated according to Cassandra's predicate pushdown restrictions
- **Backward Compatible**: Works seamlessly with Spark 4.0 (where filters can still be specified via the `filter` option)
- **Combines with Manual Filters**: Pushed filters are combined with any manual `filter` option using AND logic

## Supported Filter Types

The following Spark filter types are supported:

| Spark Filter | CQL Equivalent | Example |
|--------------|----------------|---------|
| `EqualTo` | `=` | `col = 'value'` |
| `GreaterThan` | `>` | `col > 10` |
| `GreaterThanOrEqual` | `>=` | `col >= 10` |
| `LessThan` | `<` | `col < 100` |
| `LessThanOrEqual` | `<=` | `col <= 100` |
| `In` | `IN` | `col IN ('a', 'b', 'c')` |
| `IsNull` | `IS NULL` | `col IS NULL` |
| `IsNotNull` | `IS NOT NULL` | `col IS NOT NULL` |

## Cassandra Predicate Pushdown Rules

Cassandra has specific restrictions on which predicates can be pushed down. The implementation follows the same rules as the [official Spark Cassandra Connector](https://github.com/apache/cassandra-spark-connector/blob/trunk/doc/data_source_v1.md#full-list-of-predicate-pushdown-restrictions):

### 1. Partition Key Columns
- **Allowed**: `=` or `IN` predicates only
- **Not Allowed**: Range predicates (`>`, `<`, `>=`, `<=`)
- **Example**: `user_id = 'alice'` or `user_id IN ('alice', 'bob')`

### 2. Clustering Columns
- Must be specified in order (can't skip columns)
- Only the last clustering column can use range predicates
- Preceding clustering columns must use `=` or `IN`
- **Example**: If columns are `[timestamp, sensor_id]`:
  - ✅ `timestamp = 1000 AND sensor_id > 500` (valid)
  - ❌ `sensor_id = 'temp1'` without `timestamp` (invalid - skips timestamp)
  - ❌ `timestamp > 1000 AND sensor_id = 'temp1'` (invalid - range not on last column)

### 3. Regular (Non-Key) Columns
- Require at least one indexed column with `=` predicate
- Cannot use `IN` predicates
- **Exception**: If `allow_filtering=true`, these restrictions are relaxed
- **Example**: If `email` is indexed:
  - ✅ `email = 'alice@example.com' AND age > 25`
  - ❌ `age > 25` without indexed column filter (unless `allow_filtering=true`)
- **Example with allow_filtering**:
  - ✅ `age > 25` with `option("allow_filtering", "true")` (no index needed)

### 4. Other Restrictions
- No `OR` conditions (not supported by Cassandra)
- No `NOT` conditions
- No multiple predicates on same column if any is `=` or `IN`

## Usage Examples

### Spark 4.1+ with Automatic Filter Pushdown

```python
from cassandra_data_source import CassandraDataSource
from pyspark.sql import SparkSession

# Register the data source
spark = SparkSession.builder.appName("example").getOrCreate()
spark.dataSource.register(CassandraDataSource)

# Read with filters - automatically pushed down to Cassandra
df = (spark.read
    .format("pycassandra")
    .option("host", "127.0.0.1")
    .option("keyspace", "my_keyspace")
    .option("table", "users")
    .load()
    .filter("user_id = 'alice'")  # Pushed to Cassandra
    .filter("age > 25")           # Pushed to Cassandra (if age is indexed or allow_filtering=true)
)

# Filters are automatically converted to CQL:
# SELECT * FROM users WHERE token(user_id, ...) > X AND token(user_id, ...) <= Y 
#   AND user_id = 'alice' AND age > 25
```

### Complex Filtering with Clustering Columns

```python
# Table: CREATE TABLE sensor_data (
#   device_id text,
#   timestamp bigint,
#   sensor_id text,
#   value double,
#   PRIMARY KEY (device_id, timestamp, sensor_id)
# )

df = (spark.read
    .format("pycassandra")
    .option("host", "127.0.0.1")
    .option("keyspace", "iot")
    .option("table", "sensor_data")
    .load()
    .filter("device_id = 'device1'")       # Partition key - pushed
    .filter("timestamp >= 1000000")         # First clustering column - pushed
    .filter("timestamp < 2000000")          # Range on last filter - pushed
)
```

### Filtering Non-Indexed Columns with allow_filtering

When `allow_filtering=true`, indexes are **not required** for filter pushdown:

```python
# Without allow_filtering: age filter would not be pushed (not indexed)
df_without = (spark.read
    .format("pycassandra")
    .option("host", "127.0.0.1")
    .option("keyspace", "my_keyspace")
    .option("table", "users")
    .load()
    .filter("user_id = 'alice'")  # Pushed to Cassandra
    .filter("age > 25")            # NOT pushed (age not indexed) - Spark evaluates
)

# With allow_filtering: both filters are pushed
df_with = (spark.read
    .format("pycassandra")
    .option("host", "127.0.0.1")
    .option("keyspace", "my_keyspace")
    .option("table", "users")
    .option("allow_filtering", "true")  # Enable ALLOW FILTERING
    .load()
    .filter("user_id = 'alice'")  # Pushed to Cassandra
    .filter("age > 25")            # PUSHED to Cassandra (no index needed!)
    .filter("city = 'Seattle'")    # Also pushed (no index needed!)
)

# Generated CQL with allow_filtering:
# SELECT * FROM users WHERE token(...) > X AND token(...) <= Y 
#   AND user_id = 'alice' AND age > 25 AND city = 'Seattle' ALLOW FILTERING
```

### Combining Manual and Automatic Filters

You can combine the manual `filter` option with automatic filter pushdown:

```python
df = (spark.read
    .format("pycassandra")
    .option("host", "127.0.0.1")
    .option("keyspace", "my_keyspace")
    .option("table", "users")
    .option("filter", "status = 'active'")  # Manual filter
    .load()
    .filter("user_id = 'alice'")            # Automatic pushdown
)

# Both filters are combined with AND:
# WHERE ... AND (status = 'active') AND (user_id = 'alice')
```

### Spark 4.0 Compatibility

The code works seamlessly with Spark 4.0, where filter pushdown is not available:

```python
# Spark 4.0 - use manual filter option
df = (spark.read
    .format("pycassandra")
    .option("host", "127.0.0.1")
    .option("keyspace", "my_keyspace")
    .option("table", "users")
    .option("filter", "user_id = 'alice' AND age > 25")  # Manual CQL filter
    .load()
)
```

## Unsupported Filters

Filters that cannot be pushed down to Cassandra are automatically evaluated by Spark after reading:

```python
df = (spark.read
    .format("pycassandra")
    .option("host", "127.0.0.1")
    .option("keyspace", "my_keyspace")
    .option("table", "users")
    .load()
    .filter("user_id = 'alice'")        # Pushed to Cassandra
    .filter("age > 25 OR age < 18")     # NOT pushed (OR condition) - evaluated by Spark
)
```

## Performance Considerations

### Benefits of Filter Pushdown

1. **Reduced Data Transfer**: Only matching rows are fetched from Cassandra
2. **Lower Memory Usage**: Less data needs to be held in Spark memory
3. **Improved Query Time**: Cassandra can use indexes and partition pruning
4. **Better Parallelism**: Token range partitioning combined with filtering

### When to Use Manual Filters

Use the `filter` option instead of automatic pushdown when:
- You need complex CQL expressions not supported by Spark filters
- You want to use Cassandra-specific functions (e.g., `token()`, `writetime()`)
- You're targeting Spark 4.0 specifically

### Best Practices

1. **Always Filter Partition Keys**: This enables partition pruning
2. **Use Indexed Columns or allow_filtering**: For non-key column filters:
   - Prefer indexed columns for better performance
   - Use `allow_filtering=true` if indexes cannot be created
3. **Specify Clustering Columns in Order**: To enable clustering column filtering
4. **Avoid OR Conditions**: These cannot be pushed down
5. **Test with EXPLAIN**: Use `df.explain()` to verify filters are pushed down
6. **Use allow_filtering Judiciously**: Can cause full table scans; monitor performance

## Implementation Details

### Filter Conversion

Spark filters are converted to CQL literals with proper escaping:
- Strings: Single quotes, escaped internal quotes (`'O''Brien'`)
- Numbers: Direct conversion (`42`, `3.14`)
- Booleans: CQL keywords (`true`, `false`)
- Arrays: CQL list syntax (`[1, 2, 3]`)
- NULL: CQL `NULL` keyword

### Validation Logic

The `validate_cassandra_filters` function implements Cassandra's predicate pushdown rules:
1. Groups filters by column
2. Checks partition key restrictions
3. Validates clustering column ordering
4. Verifies indexed column requirements (relaxed if `allow_filtering=true`)
5. Returns supported and unsupported filters

### Integration with Token Range Queries

Pushed filters are combined with token range queries:
```cql
SELECT columns FROM table 
WHERE token(pk_cols) > start_token 
  AND token(pk_cols) <= end_token
  AND (pushed_filters)
  AND (manual_filter_option)
```

## Troubleshooting

### Filter Not Being Pushed Down

If a filter isn't being pushed to Cassandra:

1. Check if the column is a non-key column without an index (enable `allow_filtering=true` to bypass)
2. Verify clustering columns are specified in order
3. Ensure no OR/NOT conditions are used
4. Check for multiple predicates on the same column

### Using `allow_filtering` for Non-Indexed Columns

**Key Point**: When `allow_filtering=true`, indexes are **not required** for filter pushdown on regular columns.

If you need to filter on non-indexed columns:

```python
df = (spark.read
    .format("pycassandra")
    .option("host", "127.0.0.1")
    .option("keyspace", "my_keyspace")
    .option("table", "users")
    .option("allow_filtering", "true")  # Enable ALLOW FILTERING
    .load()
    .filter("age > 25")  # Pushed to Cassandra even without index
    .filter("city = 'Seattle'")  # Also pushed, no index needed
)
```

**Benefits**:
- Filters are pushed to Cassandra, reducing data transfer
- No need to create secondary indexes
- Simplifies schema management

**Warning**: `ALLOW FILTERING` can cause full table scans and impact cluster performance. Use when:
- Working with small datasets
- Filtering is highly selective
- Creating indexes is not feasible
- Development/testing environments

**Performance Tip**: Even with `allow_filtering=true`, always filter on partition keys when possible to enable partition pruning.

## References

- [Spark 4.1 Python Data Source API](https://spark.apache.org/docs/latest/api/python/tutorial/sql/python_data_source.html)
- [Cassandra Spark Connector - Predicate Pushdown](https://github.com/apache/cassandra-spark-connector/blob/trunk/doc/data_source_v1.md#full-list-of-predicate-pushdown-restrictions)
- [Cassandra CQL WHERE Clause](https://cassandra.apache.org/doc/latest/cassandra/developing/cql/dml.html#where-clause)
- [Spark Filter Class Documentation](https://github.com/apache/spark/blob/master/python/pyspark/sql/datasource.py)
