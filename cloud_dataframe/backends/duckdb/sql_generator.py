"""
SQL generator for DuckDB.

This module provides functions to generate SQL for DuckDB from DataFrame objects.
"""
from typing import Any, Dict, List, Optional, Union, cast

from ...core.dataframe import (
    DataFrame, TableReference, SubquerySource, JoinOperation, 
    JoinType, OrderByClause, SortDirection, FilterCondition,
    BinaryOperation, UnaryOperation, CommonTableExpression
)
from ...type_system.column import (
    Column, ColumnReference, LiteralExpression, FunctionExpression,
    AggregateFunction, WindowFunction
)


def generate_sql(df: DataFrame) -> str:
    """
    Generate SQL for DuckDB from a DataFrame.
    
    Args:
        df: The DataFrame to generate SQL for
        
    Returns:
        The generated SQL string
    """
    # Generate CTEs if present
    cte_sql = _generate_ctes(df.ctes) if df.ctes else ""
    
    # Generate the main query
    query_sql = _generate_query(df)
    
    # Combine CTEs and main query
    if cte_sql:
        return f"{cte_sql}\n{query_sql}"
    else:
        return query_sql


def _generate_ctes(ctes: List[CommonTableExpression]) -> str:
    """
    Generate SQL for Common Table Expressions (CTEs).
    
    Args:
        ctes: The list of CTEs to generate SQL for
        
    Returns:
        The generated SQL string for the WITH clause
    """
    cte_parts = []
    
    for cte in ctes:
        if isinstance(cte.query, DataFrame):
            query_sql = generate_sql(cte.query)
        else:
            query_sql = cte.query
        
        columns_sql = f"({', '.join(cte.columns)})" if cte.columns else ""
        recursive_sql = "RECURSIVE " if cte.is_recursive else ""
        
        cte_parts.append(f"{cte.name}{columns_sql} AS (\n{query_sql}\n)")
    
    if cte_parts:
        return f"WITH {recursive_sql}{', '.join(cte_parts)}"
    else:
        return ""


def _generate_query(df: DataFrame) -> str:
    """
    Generate SQL for a DataFrame query.
    
    Args:
        df: The DataFrame to generate SQL for
        
    Returns:
        The generated SQL string
    """
    # Generate SELECT clause
    select_sql = _generate_select(df)
    
    # Generate FROM clause
    from_sql = _generate_from(df)
    
    # Generate WHERE clause
    where_sql = _generate_where(df)
    
    # Generate GROUP BY clause
    group_by_sql = _generate_group_by(df)
    
    # Generate HAVING clause
    having_sql = _generate_having(df)
    
    # Generate ORDER BY clause
    order_by_sql = _generate_order_by(df)
    
    # Generate LIMIT and OFFSET clauses
    limit_offset_sql = _generate_limit_offset(df)
    
    # Combine all clauses
    query_parts = [select_sql, from_sql]
    
    if where_sql:
        query_parts.append(where_sql)
    
    if group_by_sql:
        query_parts.append(group_by_sql)
    
    if having_sql:
        query_parts.append(having_sql)
    
    if order_by_sql:
        query_parts.append(order_by_sql)
    
    if limit_offset_sql:
        query_parts.append(limit_offset_sql)
    
    return "\n".join(query_parts)


def _generate_select(df: DataFrame) -> str:
    """
    Generate SQL for the SELECT clause.
    
    Args:
        df: The DataFrame to generate SQL for
        
    Returns:
        The generated SQL string for the SELECT clause
    """
    distinct_sql = "DISTINCT " if df.distinct else ""
    
    if not df.columns:
        # If no columns are specified, select all columns
        return f"SELECT {distinct_sql}*"
    
    column_parts = []
    
    for col in df.columns:
        column_sql = _generate_column(col)
        column_parts.append(column_sql)
    
    return f"SELECT {distinct_sql}{', '.join(column_parts)}"


def _generate_column(col: Column) -> str:
    """
    Generate SQL for a column.
    
    Args:
        col: The column to generate SQL for
        
    Returns:
        The generated SQL string for the column
    """
    expr_sql = _generate_expression(col.expression)
    
    if col.alias:
        return f"{expr_sql} AS {col.alias}"
    else:
        return expr_sql


def _generate_expression(expr: Any) -> str:
    """
    Generate SQL for an expression.
    
    Args:
        expr: The expression to generate SQL for
        
    Returns:
        The generated SQL string for the expression
    """
    if isinstance(expr, ColumnReference):
        if expr.table_alias:
            return f"{expr.table_alias}.{expr.name}"
        else:
            return expr.name
    
    elif isinstance(expr, LiteralExpression):
        if expr.value is None:
            return "NULL"
        elif isinstance(expr.value, str):
            # Escape single quotes in string literals
            escaped_value = str(expr.value).replace("'", "''")
            return f"'{escaped_value}'"
        elif isinstance(expr.value, bool):
            return "TRUE" if expr.value else "FALSE"
        else:
            return str(expr.value)
    
    elif isinstance(expr, BinaryOperation):
        left_sql = _generate_expression(expr.left)
        right_sql = _generate_expression(expr.right)
        
        # Handle special cases for certain operators
        if expr.operator.upper() in ("IN", "NOT IN"):
            if isinstance(expr.right, list):
                values_sql = ", ".join(_generate_expression(val) for val in expr.right)
                return f"{left_sql} {expr.operator} ({values_sql})"
            else:
                return f"{left_sql} {expr.operator} ({right_sql})"
        else:
            return f"{left_sql} {expr.operator} {right_sql}"
    
    elif isinstance(expr, UnaryOperation):
        expr_sql = _generate_expression(expr.expression)
        return f"{expr.operator} ({expr_sql})"
    
    elif isinstance(expr, FunctionExpression):
        if isinstance(expr, AggregateFunction):
            return _generate_aggregate_function(expr)
        elif isinstance(expr, WindowFunction):
            return _generate_window_function(expr)
        else:
            return _generate_function(expr)
    
    else:
        # For other types of expressions, convert to string
        return str(expr)


def _generate_aggregate_function(func: AggregateFunction) -> str:
    """
    Generate SQL for an aggregate function.
    
    Args:
        func: The aggregate function to generate SQL for
        
    Returns:
        The generated SQL string for the aggregate function
    """
    params_sql = ", ".join(_generate_expression(param) for param in func.parameters)
    
    # Handle special case for COUNT(*)
    if func.function_name.upper() == "COUNT" and (not func.parameters or func.parameters[0] == "*"):
        return "COUNT(*)"
    
    return f"{func.function_name}({params_sql})"


def _generate_window_function(func: WindowFunction) -> str:
    """
    Generate SQL for a window function.
    
    Args:
        func: The window function to generate SQL for
        
    Returns:
        The generated SQL string for the window function
    """
    params_sql = ", ".join(_generate_expression(param) for param in func.parameters)
    
    partition_by_sql = ""
    if func.window.partition_by:
        partition_by_cols = ", ".join(_generate_expression(col) for col in func.window.partition_by)
        partition_by_sql = f"PARTITION BY {partition_by_cols}"
    
    order_by_sql = ""
    if func.window.order_by:
        order_by_cols = ", ".join(_generate_expression(col) for col in func.window.order_by)
        order_by_sql = f"ORDER BY {order_by_cols}"
    
    window_sql = ""
    if partition_by_sql or order_by_sql:
        window_parts = []
        if partition_by_sql:
            window_parts.append(partition_by_sql)
        if order_by_sql:
            window_parts.append(order_by_sql)
        window_sql = f" OVER ({' '.join(window_parts)})"
    
    return f"{func.function_name}({params_sql}){window_sql}"


def _generate_function(func: FunctionExpression) -> str:
    """
    Generate SQL for a function.
    
    Args:
        func: The function to generate SQL for
        
    Returns:
        The generated SQL string for the function
    """
    params_sql = ", ".join(_generate_expression(param) for param in func.parameters)
    return f"{func.function_name}({params_sql})"


def _generate_from(df: DataFrame) -> str:
    """
    Generate SQL for the FROM clause.
    
    Args:
        df: The DataFrame to generate SQL for
        
    Returns:
        The generated SQL string for the FROM clause
    """
    if not df.source:
        return ""
    
    source_sql = _generate_source(df.source)
    return f"FROM {source_sql}"


def _generate_source(source: Any) -> str:
    """
    Generate SQL for a data source.
    
    Args:
        source: The data source to generate SQL for
        
    Returns:
        The generated SQL string for the data source
    """
    if isinstance(source, TableReference):
        table_sql = source.table_name
        if source.schema:
            table_sql = f"{source.schema}.{table_sql}"
        
        if source.alias:
            return f"{table_sql} AS {source.alias}"
        else:
            return table_sql
    
    elif isinstance(source, SubquerySource):
        subquery_sql = generate_sql(source.dataframe)
        return f"({subquery_sql}) AS {source.alias}"
    
    elif isinstance(source, JoinOperation):
        left_sql = _generate_source(source.left)
        right_sql = _generate_source(source.right)
        
        join_type_sql = source.join_type.value
        
        if source.join_type == JoinType.CROSS:
            return f"{left_sql} CROSS JOIN {right_sql}"
        else:
            condition_sql = _generate_expression(source.condition)
            return f"{left_sql} {join_type_sql} JOIN {right_sql} ON {condition_sql}"
    
    else:
        # For other types of sources, convert to string
        return str(source)


def _generate_where(df: DataFrame) -> str:
    """
    Generate SQL for the WHERE clause.
    
    Args:
        df: The DataFrame to generate SQL for
        
    Returns:
        The generated SQL string for the WHERE clause
    """
    if not df.filter_condition:
        return ""
    
    condition_sql = _generate_expression(df.filter_condition)
    return f"WHERE {condition_sql}"


def _generate_group_by(df: DataFrame) -> str:
    """
    Generate SQL for the GROUP BY clause.
    
    Args:
        df: The DataFrame to generate SQL for
        
    Returns:
        The generated SQL string for the GROUP BY clause
    """
    if not df.group_by_clause or not df.group_by_clause.columns:
        return ""
    
    group_by_cols = []
    for col in df.group_by_clause.columns:
        col_sql = _generate_expression(col)
        group_by_cols.append(col_sql)
    
    return f"GROUP BY {', '.join(group_by_cols)}"


def _generate_having(df: DataFrame) -> str:
    """
    Generate SQL for the HAVING clause.
    
    Args:
        df: The DataFrame to generate SQL for
        
    Returns:
        The generated SQL string for the HAVING clause
    """
    if not df.having:
        return ""
    
    condition_sql = _generate_expression(df.having)
    return f"HAVING {condition_sql}"


def _generate_order_by(df: DataFrame) -> str:
    """
    Generate SQL for the ORDER BY clause.
    
    Args:
        df: The DataFrame to generate SQL for
        
    Returns:
        The generated SQL string for the ORDER BY clause
    """
    if not df.order_by_clauses:
        return ""
    
    order_by_parts = []
    
    for clause in df.order_by_clauses:
        expr_sql = _generate_expression(clause.expression)
        direction_sql = clause.direction.value
        order_by_parts.append(f"{expr_sql} {direction_sql}")
    
    return f"ORDER BY {', '.join(order_by_parts)}"


def _generate_limit_offset(df: DataFrame) -> str:
    """
    Generate SQL for the LIMIT and OFFSET clauses.
    
    Args:
        df: The DataFrame to generate SQL for
        
    Returns:
        The generated SQL string for the LIMIT and OFFSET clauses
    """
    limit_sql = f"LIMIT {df.limit_value}" if df.limit_value is not None else ""
    offset_sql = f"OFFSET {df.offset_value}" if df.offset_value is not None else ""
    
    if limit_sql and offset_sql:
        return f"{limit_sql} {offset_sql}"
    elif limit_sql:
        return limit_sql
    elif offset_sql:
        return offset_sql
    else:
        return ""
