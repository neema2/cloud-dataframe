"""
SQL generator for DuckDB backend.
"""
import re
from typing import Any, Dict, List, Optional, Tuple, Union

from cloud_dataframe.type_system.column import (
    Expression, ColumnReference, LiteralExpression, BinaryOperation, UnaryOperation,
    FunctionExpression, CountFunction, SumFunction, AvgFunction, MinFunction, MaxFunction,
    WindowFunction, WindowSpecification
)


def generate_sql(query: Dict[str, Any]) -> str:
    """
    Generate SQL for DuckDB from a query dictionary.
    
    Args:
        query: Query dictionary with select, from, where, etc.
        
    Returns:
        SQL string for DuckDB
    """
    sql_parts = []
    
    # Handle WITH clause for CTEs
    if query.get('ctes'):
        cte_parts = []
        for cte in query['ctes']:
            cte_sql = generate_sql(cte['query'])
            cte_parts.append(f"{cte['name']} AS (\n{cte_sql}\n)")
        
        sql_parts.append(f"WITH {', '.join(cte_parts)}")
    
    # Handle SELECT clause
    select_clause = "SELECT "
    if query.get('distinct'):
        select_clause += "DISTINCT "
    
    if not query.get('select'):
        select_clause += "*"
    else:
        select_exprs = []
        for expr in query['select']:
            select_exprs.append(_generate_expression(expr))
        
        select_clause += ", ".join(select_exprs)
    
    sql_parts.append(select_clause)
    
    # Handle FROM clause
    if query.get('from'):
        from_clause = f"FROM {query['from']['table']}"
        if query['from'].get('alias'):
            from_clause += f" {query['from']['alias']}"
        
        sql_parts.append(from_clause)
    
    # Handle JOIN clauses
    if query.get('joins'):
        for join in query['joins']:
            join_type = join['type'].upper()
            join_clause = f"{join_type} JOIN {join['table']}"
            
            if join.get('alias'):
                join_clause += f" {join['alias']}"
            
            if join.get('on'):
                join_clause += f" ON {_generate_expression(join['on'])}"
            
            sql_parts.append(join_clause)
    
    # Handle WHERE clause
    if query.get('where'):
        sql_parts.append(f"WHERE {_generate_expression(query['where'])}")
    
    # Handle GROUP BY clause
    if query.get('group_by'):
        group_exprs = []
        for expr in query['group_by']:
            group_exprs.append(_generate_expression(expr))
        
        sql_parts.append(f"GROUP BY {', '.join(group_exprs)}")
    
    # Handle HAVING clause
    if query.get('having'):
        sql_parts.append(f"HAVING {_generate_expression(query['having'])}")
    
    # Handle ORDER BY clause
    if query.get('order_by'):
        order_exprs = []
        for expr, direction in query['order_by']:
            order_expr = _generate_expression(expr)
            if direction == 'desc':
                order_expr += " DESC"
            else:
                order_expr += " ASC"
            
            order_exprs.append(order_expr)
        
        sql_parts.append(f"ORDER BY {', '.join(order_exprs)}")
    
    # Handle LIMIT clause
    if query.get('limit') is not None:
        sql_parts.append(f"LIMIT {query['limit']}")
    
    # Handle OFFSET clause
    if query.get('offset') is not None:
        sql_parts.append(f"OFFSET {query['offset']}")
    
    return "\n".join(sql_parts)


def _generate_expression(expr: Expression) -> str:
    """
    Generate SQL for an expression.
    
    Args:
        expr: Expression to generate SQL for
        
    Returns:
        SQL string for the expression
    """
    if isinstance(expr, ColumnReference):
        if expr.table_alias:
            return f"{expr.table_alias}.{expr.name}"
        return expr.name
    
    elif isinstance(expr, LiteralExpression):
        return _format_literal(expr.value)
    
    elif isinstance(expr, BinaryOperation):
        left = _generate_expression(expr.left)
        right = _generate_expression(expr.right)
        
        # Handle special operators
        if expr.operator == 'IN':
            if isinstance(right, str) and right.startswith('(') and right.endswith(')'):
                return f"{left} IN {right}"
            else:
                return f"{left} IN ({right})"
        
        elif expr.operator == 'NOT IN':
            if isinstance(right, str) and right.startswith('(') and right.endswith(')'):
                return f"{left} NOT IN {right}"
            else:
                return f"{left} NOT IN ({right})"
        
        elif expr.operator == 'AS':
            return f"{left} AS {right}"
        
        else:
            return f"({left} {expr.operator} {right})"
    
    elif isinstance(expr, UnaryOperation):
        operand = _generate_expression(expr.operand)
        
        if expr.operator == 'NOT':
            return f"NOT {operand}"
        else:
            return f"{expr.operator} {operand}"
    
    elif isinstance(expr, FunctionExpression):
        args = [_generate_expression(arg) for arg in expr.args]
        
        if isinstance(expr, CountFunction):
            if expr.distinct:
                return f"COUNT(DISTINCT {', '.join(args)})"
            else:
                return f"COUNT({', '.join(args)})"
        
        elif isinstance(expr, SumFunction):
            return f"SUM({', '.join(args)})"
        
        elif isinstance(expr, AvgFunction):
            return f"AVG({', '.join(args)})"
        
        elif isinstance(expr, MinFunction):
            return f"MIN({', '.join(args)})"
        
        elif isinstance(expr, MaxFunction):
            return f"MAX({', '.join(args)})"
        
        elif isinstance(expr, WindowFunction):
            function_expr = f"{expr.function_name}({', '.join(args)})"
            
            if expr.window_spec:
                window_parts = []
                
                if expr.window_spec.partition_by:
                    partition_exprs = [_generate_expression(e) for e in expr.window_spec.partition_by]
                    window_parts.append(f"PARTITION BY {', '.join(partition_exprs)}")
                
                if expr.window_spec.order_by:
                    order_exprs = []
                    for e, direction in expr.window_spec.order_by:
                        order_expr = _generate_expression(e)
                        if direction == 'desc':
                            order_expr += " DESC"
                        else:
                            order_expr += " ASC"
                        
                        order_exprs.append(order_expr)
                    
                    window_parts.append(f"ORDER BY {', '.join(order_exprs)}")
                
                if expr.window_spec.frame_clause:
                    window_parts.append(expr.window_spec.frame_clause)
                
                return f"{function_expr} OVER ({' '.join(window_parts)})"
            else:
                return f"{function_expr} OVER ()"
        
        else:
            return f"{expr.function_name}({', '.join(args)})"
    
    else:
        raise ValueError(f"Unsupported expression type: {type(expr)}")


def _format_literal(value: Any) -> str:
    """
    Format a literal value for SQL.
    
    Args:
        value: Value to format
        
    Returns:
        Formatted value as a string
    """
    if value is None:
        return "NULL"
    elif isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    elif isinstance(value, (int, float)):
        return str(value)
    elif isinstance(value, str):
        # Escape single quotes
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
    elif isinstance(value, list):
        formatted_items = [_format_literal(item) for item in value]
        return f"({', '.join(formatted_items)})"
    else:
        raise ValueError(f"Unsupported literal type: {type(value)}")
