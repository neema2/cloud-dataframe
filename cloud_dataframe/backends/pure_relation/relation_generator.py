"""
Pure Relation language generator.

This module provides functions to generate Pure Relation language from DataFrame objects.
"""
from typing import Any, Dict, List, Optional, Union, cast

from ...core.dataframe import (
    DataFrame, TableReference, SubquerySource, JoinOperation, 
    JoinType, OrderByClause, Sort, FilterCondition,
    BinaryOperation, UnaryOperation, CommonTableExpression
)
from ...type_system.column import (
    Column, ColumnReference, Expression, LiteralExpression, FunctionExpression,
    AggregateFunction, WindowFunction, CountFunction
)
from ...functions.base import ScalarFunction


def generate_relation(df: DataFrame) -> str:
    """
    Generate Pure Relation language from a DataFrame.
    
    Args:
        df: The DataFrame to generate Pure Relation language for
        
    Returns:
        The generated Pure Relation language string
    """
    cte_relation = _generate_ctes(df.ctes) if df.ctes else ""
    
    query_relation = _generate_query(df)
    
    if cte_relation:
        return f"{cte_relation}\n{query_relation}"
    else:
        return query_relation


def _generate_ctes(ctes: List[CommonTableExpression]) -> str:
    """
    Generate Pure Relation language for Common Table Expressions (CTEs).
    
    Args:
        ctes: The list of CTEs to generate Pure Relation language for
        
    Returns:
        The generated Pure Relation language string for the CTEs
    """
    if not ctes:
        return ""
        
    cte_parts = []
    
    for cte in ctes:
        if isinstance(cte.query, DataFrame):
            df_copy = cte.query.copy()
            saved_ctes = df_copy.ctes
            df_copy.ctes = []  # Temporarily remove CTEs to avoid recursion
            query_relation = _generate_query(df_copy)  # Generate only the query part
            df_copy.ctes = saved_ctes  # Restore CTEs
        else:
            query_relation = cte.query
        
        columns_str = f"({', '.join(cte.columns)})" if cte.columns else ""
        
        cte_parts.append(f"let {cte.name}{columns_str} = {query_relation};")
    
    return "\n".join(cte_parts)


def _is_join_operation(df: DataFrame) -> bool:
    """
    Check if the DataFrame has a join operation as its source.
    
    Args:
        df: The DataFrame to check
        
    Returns:
        True if the DataFrame has a join operation, False otherwise
    """
    return hasattr(df, 'source') and isinstance(df.source, JoinOperation)


def _generate_query(df: DataFrame) -> str:
    """
    Generate Pure Relation language for a DataFrame query.
    
    Args:
        df: The DataFrame to generate Pure Relation language for
        
    Returns:
        The generated Pure Relation language string
    """
    relation_parts = []
    
    source_relation = _generate_source(df.source) if df.source else "#TDS\n#"
    relation_parts.append(source_relation)
    
    if df.filter_condition:
        filter_relation = _generate_filter(df)
        relation_parts.append(filter_relation)
    
    if df.columns:
        select_relation = _generate_select(df)
        relation_parts.append(select_relation)
    
    if df.group_by:
        group_by_relation = _generate_group_by(df)
        relation_parts.append(group_by_relation)
    
    if df.having_condition:
        having_relation = _generate_having(df)
        relation_parts.append(having_relation)
    
    if df.qualify_condition:
        qualify_relation = _generate_qualify(df)
        relation_parts.append(qualify_relation)
    
    if df.order_by:
        order_by_relation = _generate_order_by(df)
        relation_parts.append(order_by_relation)
    
    if df.limit is not None or df.offset is not None:
        limit_offset_relation = _generate_limit_offset(df)
        relation_parts.append(limit_offset_relation)
    
    return "->".join(relation_parts)


def _generate_select(df: DataFrame) -> str:
    """
    Generate Pure Relation language for the select operation.
    
    Args:
        df: The DataFrame to generate Pure Relation language for
        
    Returns:
        The generated Pure Relation language string for the select operation
    """
    if not df.columns:
        return "select()"
    
    column_parts = []
    
    for col in df.columns:
        column_relation = _generate_column(col, df)
        column_parts.append(column_relation)
    
    if df.distinct:
        return f"select(~[{', '.join(column_parts)}])->distinct()"
    else:
        if len(column_parts) == 1:
            return f"select(~{column_parts[0]})"
        else:
            return f"select(~[{', '.join(column_parts)}])"


def _generate_column(col: Union[Column, ColumnReference, Expression], df: Optional[DataFrame] = None) -> str:
    """
    Generate Pure Relation language for a column.
    
    Args:
        col: The column to generate Pure Relation language for
        df: Optional DataFrame context
        
    Returns:
        The generated Pure Relation language string for the column
    """
    if isinstance(col, Column):
        expr_relation = _generate_expression(col.expression)
        
        if col.alias:
            return f"{col.alias} : {expr_relation}"
        else:
            return expr_relation
    elif isinstance(col, ColumnReference) or isinstance(col, Expression):
        return _generate_expression(col)
    else:
        return str(col)


def _generate_expression(expr: Any) -> str:
    """
    Generate Pure Relation language for an expression.
    
    Args:
        expr: The expression to generate Pure Relation language for
        
    Returns:
        The generated Pure Relation language string for the expression
    """
    if isinstance(expr, ColumnReference):
        if expr.name == "*":
            return "*"
            
        source_alias = expr.table_alias
        
        if not source_alias:
            source_alias = "x"
            
        return f"${source_alias}.{expr.name}"
    
    elif isinstance(expr, LiteralExpression):
        if expr.value is None:
            return "null"
        elif isinstance(expr.value, str):
            escaped_value = str(expr.value).replace("'", "''")
            return f"'{escaped_value}'"
        elif isinstance(expr.value, bool):
            return "true" if expr.value else "false"
        else:
            return str(expr.value)
    
    elif isinstance(expr, BinaryOperation):
        left_relation = _generate_expression(expr.left)
        right_relation = _generate_expression(expr.right)
        
        if expr.operator == "=":
            return f"{left_relation} == {right_relation}"
        elif expr.operator == "!=":
            return f"{left_relation} != {right_relation}"
        elif expr.operator == "AND":
            return f"{left_relation} && {right_relation}"
        elif expr.operator == "OR":
            return f"{left_relation} || {right_relation}"
        elif expr.operator == "CASE":
            condition = expr.left
            condition_relation = _generate_expression(condition)
            
            if isinstance(expr.right, BinaryOperation) and expr.right.operator == "ELSE":
                then_expr = expr.right.left
                else_expr = expr.right.right
                
                then_relation = _generate_expression(then_expr)
                else_relation = _generate_expression(else_expr)
                
                return f"if({condition_relation}, {then_relation}, {else_relation})"
            else:
                return f"if({condition_relation}, {right_relation}, null)"
        
        elif expr.operator.upper() in ("IN", "NOT IN"):
            if isinstance(expr.right, list):
                values_relation = ", ".join(_generate_expression(val) for val in expr.right)
                if expr.operator.upper() == "IN":
                    return f"[{values_relation}]->contains({left_relation})"
                else:
                    return f"![{values_relation}]->contains({left_relation})"
            else:
                if expr.operator.upper() == "IN":
                    return f"{right_relation}->contains({left_relation})"
                else:
                    return f"!{right_relation}->contains({left_relation})"
        else:
            return f"{left_relation} {expr.operator} {right_relation}"
    
    elif isinstance(expr, UnaryOperation):
        expr_relation = _generate_expression(expr.expression)
        if expr.operator == "NOT":
            return f"!{expr_relation}"
        else:
            return f"{expr.operator}({expr_relation})"
    
    elif isinstance(expr, FunctionExpression):
        if isinstance(expr, ScalarFunction):
            return _convert_scalar_function_to_pure(expr)
        elif isinstance(expr, AggregateFunction):
            return _generate_aggregate_function(expr)
        elif isinstance(expr, WindowFunction):
            return _generate_window_function(expr)
        else:
            return _generate_function(expr)
    
    else:
        return str(expr)


def _convert_scalar_function_to_pure(func: ScalarFunction) -> str:
    """
    Convert a scalar function to Pure Relation language.
    
    Args:
        func: The scalar function to convert
        
    Returns:
        The Pure Relation language representation of the function
    """
    function_map = {
        "UPPER": "toUpper",
        "LOWER": "toLower",
        "CONCAT": "joinStrings",
        "SUBSTRING": "substring",
        "LENGTH": "length",
        "ABS": "abs",
        "ROUND": "round",
        "CEIL": "ceiling",
        "FLOOR": "floor",
        "CAST": "cast",
    }
    
    pure_function_name = function_map.get(func.function_name.upper(), func.function_name.lower())
    
    params_relation = ", ".join(_generate_expression(param) for param in func.parameters)
    
    if pure_function_name == "joinStrings" and len(func.parameters) > 1:
        return f"[{params_relation}]->joinStrings('')"
    elif pure_function_name == "substring":
        if len(func.parameters) == 3:
            string_expr = _generate_expression(func.parameters[0])
            start_expr = _generate_expression(func.parameters[1])
            length_expr = _generate_expression(func.parameters[2])
            return f"{string_expr}->substring({start_expr}, {length_expr})"
        else:
            return f"{params_relation}->substring()"
    else:
        return f"{pure_function_name}({params_relation})"


def _generate_aggregate_function(func: AggregateFunction) -> str:
    """
    Generate Pure Relation language for an aggregate function.
    
    Args:
        func: The aggregate function to generate Pure Relation language for
        
    Returns:
        The generated Pure Relation language string for the aggregate function
    """
    agg_function_map = {
        "COUNT": "count",
        "SUM": "sum",
        "AVG": "average",
        "MIN": "min",
        "MAX": "max",
    }
    
    pure_function_name = agg_function_map.get(func.function_name.upper(), func.function_name.lower())
    
    if isinstance(func, CountFunction) and (not func.parameters or 
                                           (len(func.parameters) == 1 and 
                                            isinstance(func.parameters[0], LiteralExpression) and 
                                            func.parameters[0].value == 1)):
        return "count()"
    
    params_relation = ", ".join(_generate_expression(param) for param in func.parameters)
    
    if isinstance(func, CountFunction) and func.distinct:
        return f"distinct()->count()"
    
    return f"{params_relation}->{pure_function_name}()"


def _generate_window_function(func: WindowFunction) -> str:
    """
    Generate Pure Relation language for a window function.
    
    Args:
        func: The window function to generate Pure Relation language for
        
    Returns:
        The generated Pure Relation language string for the window function
    """
    window_function_map = {
        "ROW_NUMBER": "rowNumber",
        "RANK": "rank",
        "DENSE_RANK": "denseRank",
        "LEAD": "lead",
        "LAG": "lag",
    }
    
    pure_function_name = window_function_map.get(func.function_name.upper(), func.function_name.lower())
    
    params_relation = ", ".join(_generate_expression(param) for param in func.parameters)
    
    window_parts = []
    
    if func.window.partition_by:
        partition_by_cols = ", ".join(_generate_expression(col) for col in func.window.partition_by)
        window_parts.append(f"~[{partition_by_cols}]")
    else:
        window_parts.append("[]")
    
    if func.window.order_by:
        order_by_parts = []
        for clause in func.window.order_by:
            if isinstance(clause, OrderByClause):
                expr_relation = _generate_expression(clause.expression)
                direction = "ascending" if clause.direction.value == "ASC" else "descending"
                order_by_parts.append(f"~{expr_relation}->{direction}()")
            elif isinstance(clause, tuple) and len(clause) == 2:
                col_expr, sort_dir = clause
                col_relation = _generate_expression(col_expr)
                direction = "descending" if sort_dir == "DESC" or (hasattr(sort_dir, "value") and sort_dir.value == "DESC") else "ascending"
                order_by_parts.append(f"~{col_relation}->{direction}()")
            else:
                order_by_parts.append(_generate_expression(clause))
        
        window_parts.append(f"[{', '.join(order_by_parts)}]")
    else:
        window_parts.append("[]")
    
    if func.window.frame:
        frame = func.window.frame
        frame_type = frame.type.lower()
        
        if frame.is_unbounded_start and frame.is_unbounded_end:
            window_parts.append("null")  # Default frame
        else:
            start_boundary = "null" if frame.is_unbounded_start else str(frame.start)
            end_boundary = "null" if frame.is_unbounded_end else str(frame.end)
            window_parts.append(f"^Frame(type='{frame_type}', start={start_boundary}, end={end_boundary})")
    else:
        window_parts.append("null")  # Default frame
    
    if params_relation:
        return f"{params_relation}->{pure_function_name}({', '.join(window_parts)})"
    else:
        return f"{pure_function_name}({', '.join(window_parts)})"


def _generate_function(func: FunctionExpression) -> str:
    """
    Generate Pure Relation language for a function.
    
    Args:
        func: The function to generate Pure Relation language for
        
    Returns:
        The generated Pure Relation language string for the function
    """   
    params_relation = ", ".join(_generate_expression(param) for param in func.parameters)
    
    return f"{func.function_name.lower()}({params_relation})"


def _generate_source(source: Any) -> str:
    """
    Generate Pure Relation language for a data source.
    
    Args:
        source: The data source to generate Pure Relation language for
        
    Returns:
        The generated Pure Relation language string for the data source
    """
    if isinstance(source, TableReference):
        if source.alias:
            return f"let {source.alias} = {source.table_name}"
        else:
            return source.table_name
    
    elif isinstance(source, SubquerySource):
        subquery_relation = generate_relation(source.dataframe)
        return f"let {source.alias} = {subquery_relation}"
    
    elif isinstance(source, JoinOperation):
        left_relation = _generate_source(source.left)
        right_relation = _generate_source(source.right)
        
        join_type_map = {
            JoinType.INNER: "INNER",
            JoinType.LEFT: "LEFT",
            JoinType.RIGHT: "RIGHT",
            JoinType.FULL: "FULL",
            JoinType.CROSS: "CROSS"
        }
        
        join_type = join_type_map.get(source.join_type, "INNER")
        
        if source.join_type == JoinType.CROSS:
            return f"{left_relation}->join({right_relation}, JoinKind.INNER, {{x,y| true}})"
        else:
            condition_relation = _generate_expression(source.condition)
            return f"{left_relation}->join({right_relation}, JoinKind.{join_type}, {{x,y| {condition_relation}}})"
    
    else:
        return str(source)


def _generate_filter(df: DataFrame) -> str:
    """
    Generate Pure Relation language for the filter operation.
    
    Args:
        df: The DataFrame to generate Pure Relation language for
        
    Returns:
        The generated Pure Relation language string for the filter operation
    """
    if not df.filter_condition:
        return ""
    
    condition_relation = _generate_expression(df.filter_condition)
    return f"filter(x|{condition_relation})"


def _generate_group_by(df: DataFrame) -> str:
    """
    Generate Pure Relation language for the group by operation.
    
    Args:
        df: The DataFrame to generate Pure Relation language for
        
    Returns:
        The generated Pure Relation language string for the group by operation
    """
    if not df.group_by:
        return ""
    
    group_by_cols = []
    group_by_cols.append("x")
    
    agg_cols = []
    for col in df.columns:
        if isinstance(col, Column) and isinstance(col.expression, AggregateFunction):
            agg_relation = _generate_column(col)
            agg_cols.append(agg_relation)
    
    if len(group_by_cols) == 1:
        group_by_part = f"~{group_by_cols[0]}"
    else:
        group_by_part = f"~[{', '.join(group_by_cols)}]"
    
    if agg_cols:
        if len(agg_cols) == 1:
            agg_part = f"~{agg_cols[0]}"
        else:
            agg_part = f"~[{', '.join(agg_cols)}]"
        
        return f"groupBy({group_by_part}, {agg_part})"
    else:
        return f"groupBy({group_by_part})"


def _generate_having(df: DataFrame) -> str:
    """
    Generate Pure Relation language for the having operation.
    
    Args:
        df: The DataFrame to generate Pure Relation language for
        
    Returns:
        The generated Pure Relation language string for the having operation
    """
    if not df.having_condition:
        return ""
    
    condition_relation = _generate_expression(df.having_condition)
    return f"filter(x|{condition_relation})"


def _generate_qualify(df: DataFrame) -> str:
    """
    Generate Pure Relation language for the qualify operation.
    
    Args:
        df: The DataFrame to generate Pure Relation language for
        
    Returns:
        The generated Pure Relation language string for the qualify operation
    """
    if not df.qualify_condition:
        return ""
    
    condition_relation = _generate_expression(df.qualify_condition)
    return f"filter(x|{condition_relation})"


def _generate_order_by(df: DataFrame) -> str:
    """
    Generate Pure Relation language for the order by operation.
    
    Args:
        df: The DataFrame to generate Pure Relation language for
        
    Returns:
        The generated Pure Relation language string for the order by operation
    """
    if not df.order_by:
        return ""
    
    order_by_parts = []
    
    order_by_parts.append("~x->ascending()")
    
    if len(order_by_parts) == 1:
        return f"sort({order_by_parts[0]})"
    else:
        return f"sort([{', '.join(order_by_parts)}])"


def _generate_limit_offset(df: DataFrame) -> str:
    """
    Generate Pure Relation language for the limit and offset operations.
    
    Args:
        df: The DataFrame to generate Pure Relation language for
        
    Returns:
        The generated Pure Relation language string for the limit and offset operations
    """
    if df.limit is None and df.offset is None:
        return ""
    
    if df.limit is not None and df.offset is None:
        return f"limit({df.limit})"
    elif df.limit is None and df.offset is not None:
        return f"drop({df.offset})"
    else:
        return f"drop({df.offset})->limit({df.limit})"
