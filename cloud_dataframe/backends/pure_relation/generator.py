"""
Pure Relation generator for cloud-dataframe.

This module provides functions to generate Pure Relation language code from DataFrame objects.
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


def generate_pure_relation(df: DataFrame) -> str:
    """
    Generate Pure Relation language code from a DataFrame.
    
    Args:
        df: The DataFrame to generate Pure Relation code for
        
    Returns:
        The generated Pure Relation code string
    """
    cte_code = _generate_ctes(df.ctes) if df.ctes else ""
    
    query_code = _generate_query(df)
    
    if cte_code:
        return f"{cte_code}\n{query_code}"
    else:
        return query_code


def _generate_ctes(ctes: List[CommonTableExpression]) -> str:
    """
    Generate Pure Relation code for Common Table Expressions (CTEs).
    
    Args:
        ctes: The list of CTEs to generate code for
        
    Returns:
        The generated Pure Relation code string for CTEs
    """
    return "// CTEs are not directly supported in Pure Relation language"


def _generate_query(df: DataFrame) -> str:
    """
    Generate Pure Relation code for a DataFrame query.
    
    Args:
        df: The DataFrame to generate code for
        
    Returns:
        The generated Pure Relation code string
    """
    relation_code = _generate_source(df.source) if df.source else "Relation.empty()"
    
    if df.filter_condition:
        relation_code = _apply_filter(relation_code, df.filter_condition)
        
    if df.columns:
        relation_code = _apply_select(relation_code, df.columns)
        
    if hasattr(df, 'group_by_clauses') and df.group_by_clauses:
        relation_code = _apply_group_by(relation_code, df.group_by_clauses, df.columns)
        
    if hasattr(df, 'having_condition') and df.having_condition:
        relation_code = _apply_having(relation_code, df.having_condition)
        
    if df.order_by_clauses:
        relation_code = _apply_order_by(relation_code, df.order_by_clauses)
        
    if df.limit_value is not None:
        relation_code = _apply_limit(relation_code, df.limit_value)
        
    if df.offset_value is not None:
        relation_code = _apply_offset(relation_code, df.offset_value)
        
    return relation_code


def _generate_source(source: Any) -> str:
    """
    Generate Pure Relation code for a data source.
    
    Args:
        source: The data source to generate code for
        
    Returns:
        The generated Pure Relation code string for the data source
    """
    if isinstance(source, TableReference):
        table_name = source.table_name
        if source.schema:
            table_name = f"{source.schema}::{table_name}"
        
        return f"${table_name}"
        
    elif isinstance(source, SubquerySource):
        subquery_code = generate_pure_relation(source.dataframe)
        return f"({subquery_code})"
        
    elif isinstance(source, JoinOperation):
        left_code = _generate_source(source.left)
        right_code = _generate_source(source.right)
        
        if source.join_type == JoinType.CROSS:
            return f"{left_code}->crossJoin({right_code})"
        else:
            join_type = "INNER" if source.join_type == JoinType.INNER else "LEFT"
            
            condition_code = _generate_expression(source.condition)
            
            return f"{left_code}->join({right_code}, JoinKind.{join_type}, {{x, y | {condition_code.replace('left.', '$x.').replace('right.', '$y.')}}})"
    else:
        return str(source)


def _apply_filter(relation_code: str, filter_condition: FilterCondition) -> str:
    """
    Apply a filter operation to a relation.
    
    Args:
        relation_code: The code for the relation to filter
        filter_condition: The filter condition to apply
        
    Returns:
        The code for the filtered relation
    """
    if hasattr(filter_condition, 'condition'):
        condition_code = _generate_expression(filter_condition.condition)
    else:
        condition_code = _generate_expression(filter_condition)
        
    return f"{relation_code}->filter(x | {condition_code.replace('x.', '$x.')})"


def _apply_select(relation_code: str, columns: List[Column]) -> str:
    """
    Apply a select operation to a relation.
    
    Args:
        relation_code: The code for the relation to select from
        columns: The columns to select
        
    Returns:
        The code for the relation with columns selected
    """
    cols = []
    for col in columns:
        if isinstance(col, Column):
            if col.alias:
                cols.append(col.alias)
            else:
                expr = col.expression
                if isinstance(expr, ColumnReference):
                    cols.append(expr.name)
                else:
                    cols.append(_generate_expression(expr))
        elif isinstance(col, ColumnReference):
            cols.append(col.name)
        else:
            cols.append(_generate_expression(col))
            
    cols_code = ", ".join(cols)
    
    return f"{relation_code}->select(~[{cols_code}])"


def _apply_group_by(relation_code: str, group_by_clauses: List[Expression], columns: List[Column]) -> str:
    """
    Apply a group by operation to a relation.
    
    Args:
        relation_code: The code for the relation to group
        group_by_clauses: The grouping columns
        columns: The columns with aggregations
        
    Returns:
        The code for the grouped relation
    """
    key_cols = []
    for col in group_by_clauses:
        if isinstance(col, ColumnReference):
            key_cols.append(col.name)
        else:
            key_cols.append(_generate_expression(col))
            
    keys_code = ", ".join(key_cols)
    
    agg_cols = []
    for col in columns:
        if isinstance(col, Column) and col.alias and isinstance(col.expression, AggregateFunction):
            agg_func = col.expression
            agg_cols.append(f"{col.alias}:x|$x.{_generate_function_parameters(agg_func)}")
            
    aggs_code = ", ".join(agg_cols)
    
    if agg_cols:
        return f"{relation_code}->groupBy(~[{keys_code}], ~[{aggs_code}])"
    else:
        return f"{relation_code}->groupBy(~[{keys_code}])"


def _apply_having(relation_code: str, having_condition: FilterCondition) -> str:
    """
    Apply a having operation to a relation.
    
    Args:
        relation_code: The code for the relation to filter after grouping
        having_condition: The having condition to apply
        
    Returns:
        The code for the relation with having applied
    """
    if hasattr(having_condition, 'condition'):
        condition_code = _generate_expression(having_condition.condition)
    else:
        condition_code = _generate_expression(having_condition)
        
    condition_code = condition_code.replace("df.", "")
    
    return f"{relation_code}->filter(x | {condition_code.replace('x.', '$x.')})"


def _apply_order_by(relation_code: str, order_by_clauses: List[OrderByClause]) -> str:
    """
    Apply an order by operation to a relation.
    
    Args:
        relation_code: The code for the relation to order
        order_by_clauses: The ordering clauses
        
    Returns:
        The code for the ordered relation
    """
    order_exprs = []
    for clause in order_by_clauses:
        if isinstance(clause, OrderByClause):
            expr = clause.expression
            if isinstance(expr, ColumnReference):
                col_name = expr.name
                direction = "ascending" if clause.direction == Sort.ASC else "descending"
                order_exprs.append(f"{direction}(~{col_name})")
        
    orders_code = ", ".join(order_exprs)
    
    return f"{relation_code}->sort({orders_code})"


def _apply_limit(relation_code: str, limit: int) -> str:
    """
    Apply a limit operation to a relation.
    
    Args:
        relation_code: The code for the relation to limit
        limit: The maximum number of rows to return
        
    Returns:
        The code for the limited relation
    """
    return f"{relation_code}->limit({limit})"


def _apply_offset(relation_code: str, offset: int) -> str:
    """
    Apply an offset operation to a relation.
    
    Args:
        relation_code: The code for the relation to offset
        offset: The number of rows to skip
        
    Returns:
        The code for the relation with offset applied
    """
    return f"// Offset is not directly supported in Pure Relation language\n{relation_code}"


def _generate_expression(expr: Any) -> str:
    """
    Generate Pure Relation code for an expression.
    
    Args:
        expr: The expression to generate code for
        
    Returns:
        The generated Pure Relation code string for the expression
    """
    if isinstance(expr, ColumnReference):
        if expr.name == "*":
            return "*"
            
        source_alias = expr.table_alias or "x"
        return f"${source_alias}.{expr.name}"
    
    elif isinstance(expr, LiteralExpression):
        if expr.value is None:
            return "null"
        elif isinstance(expr.value, str):
            escaped_value = str(expr.value).replace('"', '\\"')
            return f'"{escaped_value}"'
        elif isinstance(expr.value, bool):
            return "true" if expr.value else "false"
        else:
            return str(expr.value)
    
    elif isinstance(expr, BinaryOperation):
        if expr.operator == "AS" and isinstance(expr.right, LiteralExpression):
            return expr.right.value
            
        left_code = _generate_expression(expr.left)
        right_code = _generate_expression(expr.right)
        
        operator_map = {
            "=": "==",
            "<>": "!=",
            "AND": "&&",
            "OR": "||",
            "+": "+",
            "-": "-",
            "*": "*",
            "/": "/",
        }
        
        op = operator_map.get(expr.operator, expr.operator)
        
        if expr.operator == "CASE":
            condition = _generate_expression(expr.left)
            
            if isinstance(expr.right, BinaryOperation) and expr.right.operator == "ELSE":
                then_expr = _generate_expression(expr.right.left)
                else_expr = _generate_expression(expr.right.right)
                
                return f"if({condition}, {then_expr}, {else_expr})"
            else:
                return f"if({condition}, {right_code}, null)"
        
        if hasattr(expr, 'needs_parentheses') and expr.needs_parentheses:
            return f"({left_code} {op} {right_code})"
        else:
            return f"{left_code} {op} {right_code}"
    
    elif isinstance(expr, UnaryOperation):
        expr_code = _generate_expression(expr.expression)
        
        operator_map = {
            "NOT": "!",
            "-": "-",
            "+": "+",
        }
        
        op = operator_map.get(expr.operator, expr.operator)
        
        return f"{op}({expr_code})"
    
    elif isinstance(expr, FunctionExpression):
        if isinstance(expr, ScalarFunction):
            return expr.to_sql({"backend": "pure_relation"})
        elif isinstance(expr, AggregateFunction):
            return _generate_aggregate_function(expr)
        elif isinstance(expr, WindowFunction):
            return _generate_window_function(expr)
        else:
            return _generate_function(expr)
    
    else:
        return str(expr)


def _generate_aggregate_function(func: AggregateFunction) -> str:
    """
    Generate Pure Relation code for an aggregate function.
    
    Args:
        func: The aggregate function to generate code for
        
    Returns:
        The generated Pure Relation code string for the aggregate function
    """
    if isinstance(func, CountFunction) and (not func.parameters or 
                                       (len(func.parameters) == 1 and 
                                        isinstance(func.parameters[0], LiteralExpression) and 
                                        func.parameters[0].value == 1)):
        return "x | $x->count()"
    
    params_code = _generate_function_parameters(func)
    
    function_map = {
        "COUNT": "count",
        "SUM": "sum",
        "AVG": "average",
        "MIN": "min",
        "MAX": "max",
    }
    
    func_name = function_map.get(func.function_name, func.function_name.lower())
    
    if isinstance(func, CountFunction) and func.distinct:
        return f"x | $x.{params_code}->distinct()->count()"
    
    return f"x | $x.{params_code}->{func_name}()"


def _generate_window_function(func: WindowFunction) -> str:
    """
    Generate Pure Relation code for a window function.
    
    Args:
        func: The window function to generate code for
        
    Returns:
        The generated Pure Relation code string for the window function
    """
    function_map = {
        "ROW_NUMBER": "rowNumber",
        "RANK": "rank",
        "DENSE_RANK": "denseRank",
        "NTILE": "ntile",
    }
    
    func_name = function_map.get(func.function_name, func.function_name.lower())
    
    window_code = []
    
    if func.window.partition_by:
        partition_cols = []
        for col in func.window.partition_by:
            if isinstance(col, ColumnReference):
                partition_cols.append(col.name)
            else:
                partition_cols.append(_generate_expression(col))
        
        partition_code = ", ".join(partition_cols)
        window_code.append(f"~[{partition_code}]")
    else:
        window_code.append("~[]")
    
    if func.window.order_by:
        order_parts = []
        for clause in func.window.order_by:
            if isinstance(clause, OrderByClause):
                col = clause.expression
                if isinstance(col, ColumnReference):
                    direction = "ascending" if clause.direction == Sort.ASC else "descending"
                    order_parts.append(f"{direction}(~{col.name})")
        
        if order_parts:
            window_code.append(", ".join(order_parts))
    
    window_spec = ", ".join(window_code)
    
    return f"{func_name}(->over({window_spec}))"


def _generate_function(func: FunctionExpression) -> str:
    """
    Generate Pure Relation code for a function.
    
    Args:
        func: The function to generate code for
        
    Returns:
        The generated Pure Relation code string for the function
    """
    params_code = ", ".join(_generate_expression(param) for param in func.parameters)
    
    return f"{func.function_name.lower()}({params_code})"


def _generate_function_parameters(func: FunctionExpression) -> str:
    """
    Generate Pure Relation code for function parameters.
    
    Args:
        func: The function to generate parameters for
        
    Returns:
        The generated Pure Relation code string for the function parameters
    """
    if not func.parameters:
        return "*"
    
    param = func.parameters[0]
    if isinstance(param, ColumnReference):
        return param.name
    else:
        return _generate_expression(param)
