"""Tests for filter conversion and validation."""

import pytest
from unittest.mock import MagicMock

# Try importing Spark 4.1+ filter classes
try:
    from pyspark.sql.datasource import (
        EqualTo, GreaterThan, GreaterThanOrEqual,
        LessThan, LessThanOrEqual, In, IsNull, IsNotNull, Not
    )
    SPARK_41_AVAILABLE = True
except ImportError:
    SPARK_41_AVAILABLE = False


@pytest.mark.skipif(not SPARK_41_AVAILABLE, reason="Requires Spark 4.1+")
class TestFilterConversion:
    """Test conversion of Spark filters to CQL."""

    def test_equal_to_filter(self):
        """Test EqualTo filter conversion."""
        from cassandra_data_source.filters import convert_filter_to_cql

        f = EqualTo(("name",), "Alice")
        cql, supported = convert_filter_to_cql(f)

        assert supported is True
        assert cql == "name = 'Alice'"

    def test_greater_than_filter(self):
        """Test GreaterThan filter conversion."""
        from cassandra_data_source.filters import convert_filter_to_cql

        f = GreaterThan(("age",), 25)
        cql, supported = convert_filter_to_cql(f)

        assert supported is True
        assert cql == "age > 25"

    def test_greater_than_or_equal_filter(self):
        """Test GreaterThanOrEqual filter conversion."""
        from cassandra_data_source.filters import convert_filter_to_cql

        f = GreaterThanOrEqual(("age",), 25)
        cql, supported = convert_filter_to_cql(f)

        assert supported is True
        assert cql == "age >= 25"

    def test_less_than_filter(self):
        """Test LessThan filter conversion."""
        from cassandra_data_source.filters import convert_filter_to_cql

        f = LessThan(("age",), 65)
        cql, supported = convert_filter_to_cql(f)

        assert supported is True
        assert cql == "age < 65"

    def test_less_than_or_equal_filter(self):
        """Test LessThanOrEqual filter conversion."""
        from cassandra_data_source.filters import convert_filter_to_cql

        f = LessThanOrEqual(("age",), 65)
        cql, supported = convert_filter_to_cql(f)

        assert supported is True
        assert cql == "age <= 65"

    def test_in_filter(self):
        """Test IN filter conversion."""
        from cassandra_data_source.filters import convert_filter_to_cql

        f = In(("status",), ("active", "pending"))
        cql, supported = convert_filter_to_cql(f)

        assert supported is True
        assert cql == "status IN ('active', 'pending')"

    def test_is_null_filter(self):
        """Test IsNull filter conversion."""
        from cassandra_data_source.filters import convert_filter_to_cql

        f = IsNull(("email",))
        cql, supported = convert_filter_to_cql(f)

        assert supported is True
        assert cql == "email IS NULL"

    def test_is_not_null_filter(self):
        """Test IsNotNull filter conversion."""
        from cassandra_data_source.filters import convert_filter_to_cql

        f = IsNotNull(("email",))
        cql, supported = convert_filter_to_cql(f)

        assert supported is True
        assert cql == "email IS NOT NULL"

    def test_not_filter_unsupported(self):
        """Test that NOT filter is not supported."""
        from cassandra_data_source.filters import convert_filter_to_cql

        f = Not(EqualTo(("active",), True))
        cql, supported = convert_filter_to_cql(f)

        assert supported is False
        assert cql is None

    def test_nested_column_name(self):
        """Test filter with nested column name."""
        from cassandra_data_source.filters import convert_filter_to_cql

        f = EqualTo(("user", "address", "city"), "Seattle")
        cql, supported = convert_filter_to_cql(f)

        assert supported is True
        assert cql == "user.address.city = 'Seattle'"

    def test_string_escaping(self):
        """Test proper escaping of single quotes in strings."""
        from cassandra_data_source.filters import convert_filter_to_cql

        f = EqualTo(("name",), "O'Brien")
        cql, supported = convert_filter_to_cql(f)

        assert supported is True
        assert cql == "name = 'O''Brien'"

    def test_boolean_values(self):
        """Test boolean value conversion."""
        from cassandra_data_source.filters import convert_filter_to_cql

        f_true = EqualTo(("active",), True)
        cql_true, supported = convert_filter_to_cql(f_true)
        assert cql_true == "active = true"

        f_false = EqualTo(("active",), False)
        cql_false, supported = convert_filter_to_cql(f_false)
        assert cql_false == "active = false"

    def test_numeric_values(self):
        """Test numeric value conversion."""
        from cassandra_data_source.filters import convert_filter_to_cql

        # Integer
        f_int = EqualTo(("count",), 42)
        cql_int, _ = convert_filter_to_cql(f_int)
        assert cql_int == "count = 42"

        # Float
        f_float = EqualTo(("price",), 19.99)
        cql_float, _ = convert_filter_to_cql(f_float)
        assert cql_float == "price = 19.99"


@pytest.mark.skipif(not SPARK_41_AVAILABLE, reason="Requires Spark 4.1+")
class TestFilterValidation:
    """Test Cassandra filter validation rules."""

    def test_partition_key_with_equality(self):
        """Test partition key with equality predicate (supported)."""
        from cassandra_data_source.filters import validate_cassandra_filters

        filters = [EqualTo(("user_id",), "alice")]
        unsupported, cql = validate_cassandra_filters(
            filters,
            partition_key_columns=["user_id"],
            clustering_columns=[],
            indexed_columns=[]
        )

        assert len(unsupported) == 0
        assert len(cql) == 1
        assert "user_id = 'alice'" in cql

    def test_partition_key_with_in(self):
        """Test partition key with IN predicate (supported)."""
        from cassandra_data_source.filters import validate_cassandra_filters

        filters = [In(("user_id",), ("alice", "bob"))]
        unsupported, cql = validate_cassandra_filters(
            filters,
            partition_key_columns=["user_id"],
            clustering_columns=[],
            indexed_columns=[]
        )

        assert len(unsupported) == 0
        assert len(cql) == 1
        assert "user_id IN" in cql[0]

    def test_partition_key_with_range_unsupported(self):
        """Test partition key with range predicate (unsupported)."""
        from cassandra_data_source.filters import validate_cassandra_filters

        filters = [GreaterThan(("user_id",), "alice")]
        unsupported, cql = validate_cassandra_filters(
            filters,
            partition_key_columns=["user_id"],
            clustering_columns=[],
            indexed_columns=[]
        )

        assert len(unsupported) == 1
        assert len(cql) == 0

    def test_clustering_column_ordering(self):
        """Test clustering column must follow ordering."""
        from cassandra_data_source.filters import validate_cassandra_filters

        # Valid: first clustering column with EQ
        filters1 = [
            EqualTo(("user_id",), "alice"),
            EqualTo(("timestamp",), 1000)
        ]
        unsupported1, cql1 = validate_cassandra_filters(
            filters1,
            partition_key_columns=["user_id"],
            clustering_columns=["timestamp", "sensor_id"],
            indexed_columns=[]
        )
        assert len(unsupported1) == 0
        assert len(cql1) == 2

        # Invalid: second clustering column without first
        filters2 = [
            EqualTo(("user_id",), "alice"),
            EqualTo(("sensor_id",), "temp1")  # Skip timestamp
        ]
        unsupported2, cql2 = validate_cassandra_filters(
            filters2,
            partition_key_columns=["user_id"],
            clustering_columns=["timestamp", "sensor_id"],
            indexed_columns=[]
        )
        # sensor_id filter should be unsupported because timestamp is missing
        assert any(isinstance(f, EqualTo) and f.attribute == ("sensor_id",) 
                   for f in unsupported2)

    def test_clustering_column_range_must_be_last(self):
        """Test range predicate on clustering column must be last."""
        from cassandra_data_source.filters import validate_cassandra_filters

        # Valid: range on last clustering column
        filters1 = [
            EqualTo(("user_id",), "alice"),
            EqualTo(("timestamp",), 1000),
            GreaterThan(("value",), 50)  # Last clustering column
        ]
        unsupported1, cql1 = validate_cassandra_filters(
            filters1,
            partition_key_columns=["user_id"],
            clustering_columns=["timestamp", "value"],
            indexed_columns=[]
        )
        assert len(unsupported1) == 0
        assert len(cql1) == 3

        # Invalid: range on non-last clustering column
        filters2 = [
            EqualTo(("user_id",), "alice"),
            GreaterThan(("timestamp",), 1000),  # Range on first clustering column
            EqualTo(("value",), 50)  # Another clustering column follows
        ]
        unsupported2, cql2 = validate_cassandra_filters(
            filters2,
            partition_key_columns=["user_id"],
            clustering_columns=["timestamp", "value"],
            indexed_columns=[]
        )
        # timestamp range filter should be unsupported
        assert any(isinstance(f, GreaterThan) and f.attribute == ("timestamp",) 
                   for f in unsupported2)

    def test_not_filter_unsupported(self):
        """Test NOT filter is always unsupported."""
        from cassandra_data_source.filters import validate_cassandra_filters

        filters = [Not(EqualTo(("active",), True))]
        unsupported, cql = validate_cassandra_filters(
            filters,
            partition_key_columns=["user_id"],
            clustering_columns=[],
            indexed_columns=[]
        )

        assert len(unsupported) == 1
        assert len(cql) == 0

    def test_multiple_predicates_same_column_with_equality(self):
        """Test multiple predicates on same column with equality (unsupported)."""
        from cassandra_data_source.filters import validate_cassandra_filters

        # Multiple predicates on age, one is equality
        filters = [
            EqualTo(("age",), 25),
            GreaterThan(("age",), 20)
        ]
        unsupported, cql = validate_cassandra_filters(
            filters,
            partition_key_columns=["user_id"],
            clustering_columns=[],
            indexed_columns=["age"]
        )

        # Both should be unsupported
        assert len(unsupported) == 2
        assert len(cql) == 0

    def test_regular_column_needs_indexed_column(self):
        """Test regular column filter requires indexed column with EQ."""
        from cassandra_data_source.filters import validate_cassandra_filters

        # Regular column without indexed column (unsupported)
        filters1 = [
            EqualTo(("user_id",), "alice"),
            EqualTo(("email",), "alice@example.com")  # Regular column
        ]
        unsupported1, cql1 = validate_cassandra_filters(
            filters1,
            partition_key_columns=["user_id"],
            clustering_columns=[],
            indexed_columns=[]  # No indexed columns
        )
        # email filter should be unsupported
        assert any(isinstance(f, EqualTo) and f.attribute == ("email",) 
                   for f in unsupported1)

        # Regular column with indexed column (supported)
        filters2 = [
            EqualTo(("user_id",), "alice"),
            EqualTo(("email",), "alice@example.com")  # email is indexed
        ]
        unsupported2, cql2 = validate_cassandra_filters(
            filters2,
            partition_key_columns=["user_id"],
            clustering_columns=[],
            indexed_columns=["email"]  # email is indexed
        )
        assert len(unsupported2) == 0
        assert len(cql2) == 2

    def test_regular_column_with_allow_filtering(self):
        """Test regular column filter works with allow_filtering=true."""
        from cassandra_data_source.filters import validate_cassandra_filters

        # Regular column without indexed column but with allow_filtering
        filters = [
            EqualTo(("user_id",), "alice"),
            GreaterThan(("age",), 25),  # Regular column, not indexed
            EqualTo(("city",), "Seattle")  # Regular column, not indexed
        ]
        unsupported, cql = validate_cassandra_filters(
            filters,
            partition_key_columns=["user_id"],
            clustering_columns=[],
            indexed_columns=[],  # No indexed columns
            allow_filtering=True  # But allow_filtering is enabled
        )
        
        # All filters should be supported with allow_filtering
        assert len(unsupported) == 0
        assert len(cql) == 3
        assert "age > 25" in cql
        assert "city = 'Seattle'" in cql


@pytest.mark.skipif(not SPARK_41_AVAILABLE, reason="Requires Spark 4.1+")
class TestPushFiltersIntegration:
    """Test pushFilters integration with CassandraReader."""

    def test_push_filters_method_exists(self, mock_table_metadata):
        """Test that pushFilters method exists on reader."""
        from cassandra_data_source.reader import CassandraReader
        from unittest.mock import patch

        options = {
            "host": "127.0.0.1",
            "keyspace": "test_ks",
            "table": "test_table"
        }

        with patch("cassandra.cluster.Cluster") as mock_cluster:
            mock_cluster_instance = MagicMock()
            mock_cluster_instance.connect.return_value = MagicMock()
            mock_cluster_instance.metadata.keyspaces = {
                "test_ks": MagicMock(tables={"test_table": mock_table_metadata})
            }
            mock_cluster_instance.metadata.token_map = None
            mock_cluster.return_value = mock_cluster_instance

            reader = CassandraReader(options, schema=None)

            # Check method exists
            assert hasattr(reader, "pushFilters")
            assert callable(reader.pushFilters)

    def test_push_filters_stores_cql(self, mock_table_metadata):
        """Test that pushFilters stores CQL clauses."""
        from cassandra_data_source.reader import CassandraReader
        from unittest.mock import patch

        options = {
            "host": "127.0.0.1",
            "keyspace": "test_ks",
            "table": "test_table"
        }

        with patch("cassandra.cluster.Cluster") as mock_cluster:
            mock_cluster_instance = MagicMock()
            mock_cluster_instance.connect.return_value = MagicMock()
            
            # Setup table metadata with partition key (reuse existing mock_table_metadata)
            # The fixture already has partition_key, clustering_key, and indexes set
            mock_cluster_instance.metadata.keyspaces = {
                "test_ks": MagicMock(tables={"test_table": mock_table_metadata})
            }
            mock_cluster_instance.metadata.token_map = None
            mock_cluster.return_value = mock_cluster_instance

            reader = CassandraReader(options, schema=None)

            # Push some filters
            filters = [EqualTo(("id",), "123")]
            unsupported = list(reader.pushFilters(filters))

            # Check that CQL was stored
            assert len(reader.pushed_filters_cql) > 0
            assert len(unsupported) == 0

    def test_push_filters_returns_unsupported(self, mock_table_metadata):
        """Test that pushFilters returns unsupported filters."""
        from cassandra_data_source.reader import CassandraReader
        from unittest.mock import patch

        options = {
            "host": "127.0.0.1",
            "keyspace": "test_ks",
            "table": "test_table"
        }

        with patch("cassandra.cluster.Cluster") as mock_cluster:
            mock_cluster_instance = MagicMock()
            mock_cluster_instance.connect.return_value = MagicMock()
            
            # Use existing mock_table_metadata (has partition_key, clustering_key, indexes)
            mock_cluster_instance.metadata.keyspaces = {
                "test_ks": MagicMock(tables={"test_table": mock_table_metadata})
            }
            mock_cluster_instance.metadata.token_map = None
            mock_cluster.return_value = mock_cluster_instance

            reader = CassandraReader(options, schema=None)

            # Push a NOT filter (unsupported)
            filters = [Not(EqualTo(("id",), "123"))]
            unsupported = list(reader.pushFilters(filters))

            # Check that filter was returned as unsupported
            assert len(unsupported) == 1
            assert isinstance(unsupported[0], Not)


def test_backward_compatibility_spark_40():
    """Test that code works with Spark 4.0 (no pushFilters)."""
    from cassandra_data_source.reader import CassandraReader
    from unittest.mock import patch, MagicMock

    options = {
        "host": "127.0.0.1",
        "keyspace": "test_ks",
        "table": "test_table"
    }

    # Create mock table metadata
    mock_table_metadata = MagicMock()
    mock_table_metadata.partition_key = [MagicMock(name="id")]
    mock_table_metadata.clustering_key = []
    mock_table_metadata.columns = {
        "id": MagicMock(name="id", cql_type="text"),
        "name": MagicMock(name="name", cql_type="text")
    }
    mock_table_metadata.indexes = {}

    with patch("cassandra.cluster.Cluster") as mock_cluster:
        mock_cluster_instance = MagicMock()
        mock_cluster_instance.connect.return_value = MagicMock()
        mock_cluster_instance.metadata.keyspaces = {
            "test_ks": MagicMock(tables={"test_table": mock_table_metadata})
        }
        mock_cluster_instance.metadata.token_map = None
        mock_cluster.return_value = mock_cluster_instance

        # Should be able to create reader without errors
        reader = CassandraReader(options, schema=None)

        # pushed_filters_cql should be initialized to empty list
        assert reader.pushed_filters_cql == []

        # Even if pushFilters doesn't exist in Spark 4.0, the method
        # should exist on our reader (it just won't be called by Spark)
        assert hasattr(reader, "pushFilters")
