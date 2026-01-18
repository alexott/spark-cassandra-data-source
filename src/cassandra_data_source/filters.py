"""Filter conversion and validation for Cassandra predicate pushdown."""

from typing import List, Tuple, Optional


def convert_filter_to_cql(filter_obj) -> Tuple[Optional[str], bool]:
    """
    Convert a Spark Filter to a CQL WHERE clause fragment.

    Args:
        filter_obj: Spark Filter object (EqualTo, GreaterThan, etc.)

    Returns:
        Tuple of (cql_clause, is_supported)
        - cql_clause: CQL WHERE clause fragment or None if unsupported
        - is_supported: True if filter can be pushed to Cassandra

    Note:
        Cassandra has specific restrictions on predicate pushdown:
        - Partition key columns: Only = or IN predicates
        - Clustering columns: Must be in order, last can be range (<, >, <=, >=)
        - Regular columns: Require indexed column with = predicate
    """
    # Import here to avoid module-level dependency (Spark 4.1+ only)
    try:
        from pyspark.sql.datasource import (
            EqualTo, GreaterThan, GreaterThanOrEqual,
            LessThan, LessThanOrEqual, In, IsNull, IsNotNull, Not
        )
    except ImportError:
        # Spark 4.0 doesn't have these classes
        return None, False

    column_name = None
    value = None

    # Handle different filter types
    if isinstance(filter_obj, EqualTo):
        column_name = ".".join(filter_obj.attribute)
        value = filter_obj.value
        # Convert Python value to CQL literal
        cql_value = _python_to_cql_literal(value)
        return f"{column_name} = {cql_value}", True

    elif isinstance(filter_obj, GreaterThan):
        column_name = ".".join(filter_obj.attribute)
        value = filter_obj.value
        cql_value = _python_to_cql_literal(value)
        return f"{column_name} > {cql_value}", True

    elif isinstance(filter_obj, GreaterThanOrEqual):
        column_name = ".".join(filter_obj.attribute)
        value = filter_obj.value
        cql_value = _python_to_cql_literal(value)
        return f"{column_name} >= {cql_value}", True

    elif isinstance(filter_obj, LessThan):
        column_name = ".".join(filter_obj.attribute)
        value = filter_obj.value
        cql_value = _python_to_cql_literal(value)
        return f"{column_name} < {cql_value}", True

    elif isinstance(filter_obj, LessThanOrEqual):
        column_name = ".".join(filter_obj.attribute)
        value = filter_obj.value
        cql_value = _python_to_cql_literal(value)
        return f"{column_name} <= {cql_value}", True

    elif isinstance(filter_obj, In):
        column_name = ".".join(filter_obj.attribute)
        values = filter_obj.value  # It's 'value', not 'values'
        # Convert list of Python values to CQL tuple
        cql_values = ", ".join(_python_to_cql_literal(v) for v in values)
        return f"{column_name} IN ({cql_values})", True

    elif isinstance(filter_obj, IsNull):
        column_name = ".".join(filter_obj.attribute)
        return f"{column_name} IS NULL", True

    elif isinstance(filter_obj, IsNotNull):
        column_name = ".".join(filter_obj.attribute)
        return f"{column_name} IS NOT NULL", True

    elif isinstance(filter_obj, Not):
        # NOT is only supported for simple cases
        # Cassandra doesn't support NOT directly, so we can't push it down
        return None, False

    else:
        # Unsupported filter type
        return None, False


def _python_to_cql_literal(value) -> str:
    """
    Convert Python value to CQL literal string.

    Args:
        value: Python value (int, str, float, bool, list, etc.)

    Returns:
        CQL literal representation as string
    """
    if value is None:
        return "NULL"
    elif isinstance(value, bool):
        # Must come before int check (bool is subclass of int)
        return "true" if value else "false"
    elif isinstance(value, (int, float)):
        return str(value)
    elif isinstance(value, str):
        # Escape single quotes in strings
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
    elif isinstance(value, list):
        # Array literal
        elements = ", ".join(_python_to_cql_literal(v) for v in value)
        return f"[{elements}]"
    elif isinstance(value, dict):
        # Map literal
        pairs = ", ".join(f"{_python_to_cql_literal(k)}: {_python_to_cql_literal(v)}" 
                         for k, v in value.items())
        return f"{{{pairs}}}"
    else:
        # Try to convert to string as fallback
        return f"'{str(value)}'"


def validate_cassandra_filters(
    filters: List,
    partition_key_columns: List[str],
    clustering_columns: List[str],
    indexed_columns: List[str],
    allow_filtering: bool = False
) -> Tuple[List, List[str]]:
    """
    Validate and filter predicates according to Cassandra pushdown restrictions.

    Cassandra restrictions (from Spark Cassandra Connector docs):
    1. Only push down non-PK column predicates with =, >, <, >=, <= predicate
    2. Only push down PK column predicates with = or IN predicate
    3. If regular columns in pushdown, need ≥1 EQ on indexed column, no IN
       (unless allow_filtering=True, which relaxes this requirement)
    4. All partition columns must be included; each can be EQ or IN (one per column)
    5. For clustering columns: only last can be RANGE; preceding must be EQ/IN
    6. No OR or NOT IN conditions
    7. No multiple predicates for same column if any is equality or IN

    Args:
        filters: List of Spark Filter objects
        partition_key_columns: List of partition key column names
        clustering_columns: List of clustering column names  
        indexed_columns: List of indexed column names
        allow_filtering: If True, relaxes requirement for indexed columns on regular columns

    Returns:
        Tuple of (unsupported_filters, cql_clauses)
        - unsupported_filters: Filters that can't be pushed down
        - cql_clauses: List of CQL WHERE clause fragments that can be pushed
    """
    # Import here to avoid module-level dependency
    try:
        from pyspark.sql.datasource import (  # noqa: F401
            EqualTo, GreaterThan, GreaterThanOrEqual,
            LessThan, LessThanOrEqual, In, Not
        )
    except ImportError:
        # Spark 4.0 - no filters available, return all as unsupported
        return filters, []

    supported_cql = []
    unsupported = []

    # Track which columns have filters
    column_filters = {}  # column_name -> list of (filter_obj, filter_type)

    # Group filters by column
    for f in filters:
        if isinstance(f, Not):
            # Rule 6: No NOT conditions
            unsupported.append(f)
            continue

        column_name = _extract_column_name(f)
        if column_name is None:
            unsupported.append(f)
            continue

        filter_type = _classify_filter(f)
        if filter_type is None:
            unsupported.append(f)
            continue

        if column_name not in column_filters:
            column_filters[column_name] = []
        column_filters[column_name].append((f, filter_type))

    # Rule 7: Check for multiple predicates on same column with equality/IN
    for column_name, column_filter_list in column_filters.items():
        if len(column_filter_list) > 1:
            has_eq_or_in = any(ft in ("EQ", "IN") for _, ft in column_filter_list)
            if has_eq_or_in:
                # Can't push down multiple predicates on same column if any is EQ/IN
                for f, _ in column_filter_list:
                    unsupported.append(f)
                continue

        # Process each filter for this column
        for f, filter_type in column_filter_list:
            # Rule 2: Partition key columns - only = or IN
            if column_name in partition_key_columns:
                if filter_type not in ("EQ", "IN"):
                    unsupported.append(f)
                    continue

            # Rule 5: Clustering columns - check ordering
            if column_name in clustering_columns:
                col_idx = clustering_columns.index(column_name)
                # Check if all preceding clustering columns have filters
                preceding_have_filters = all(
                    col in column_filters for col in clustering_columns[:col_idx]
                )
                if not preceding_have_filters:
                    unsupported.append(f)
                    continue

                # Check if this is the last clustering column with a filter
                is_last_clustering_filter = all(
                    col not in column_filters for col in clustering_columns[col_idx + 1:]
                )

                if not is_last_clustering_filter:
                    # Not last - must be EQ or IN
                    if filter_type not in ("EQ", "IN"):
                        unsupported.append(f)
                        continue
                # else: is last, can be RANGE

            # Rule 3: Regular columns - need indexed column with EQ, no IN
            # (unless allow_filtering=True)
            if (column_name not in partition_key_columns and 
                column_name not in clustering_columns):
                # It's a regular column
                if not allow_filtering:
                    # Without ALLOW FILTERING, need indexed column with EQ
                    has_indexed_eq = any(
                        col in indexed_columns and any(
                            ft == "EQ" for _, ft in column_filters.get(col, [])
                        )
                        for col in column_filters.keys()
                    )
                    if not has_indexed_eq:
                        unsupported.append(f)
                        continue

                    if filter_type == "IN":
                        unsupported.append(f)
                        continue
                # else: allow_filtering=True, can push any filter type

            # Filter is supported - convert to CQL
            cql_clause, is_supported = convert_filter_to_cql(f)
            if is_supported and cql_clause:
                supported_cql.append(cql_clause)
            else:
                unsupported.append(f)

    return unsupported, supported_cql


def _extract_column_name(filter_obj) -> Optional[str]:
    """Extract column name from filter object."""
    if hasattr(filter_obj, "attribute"):
        return ".".join(filter_obj.attribute)
    return None


def _classify_filter(filter_obj) -> Optional[str]:
    """
    Classify filter as EQ, IN, or RANGE.

    Returns:
        "EQ", "IN", "RANGE", or None
    """
    try:
        from pyspark.sql.datasource import (
            EqualTo, In, GreaterThan, GreaterThanOrEqual,
            LessThan, LessThanOrEqual
        )
    except ImportError:
        return None

    if isinstance(filter_obj, EqualTo):
        return "EQ"
    elif isinstance(filter_obj, In):
        return "IN"
    elif isinstance(filter_obj, (GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual)):
        return "RANGE"
    else:
        return None
