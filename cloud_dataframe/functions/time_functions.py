"""
Time functions for the cloud-dataframe DSL.

This module provides time-related scalar functions for the DataFrame DSL.
"""
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Union, Callable, Type

from ..type_system.column import ScalarFunction, Expression, LiteralExpression
from . import register_function

@register_function
@dataclass
class DatePart(ScalarFunction):
    """
    date_part scalar function.
    
    Extracts a part (year, month, day, etc.) from a date/time value.
    
    Input Parameters:
        part: String specifying the part to extract (year, month, day, etc.)
        date: Date/time expression to extract from
        
    Return Type:
        Integer or string representing the extracted part
    """
    function_name: str = "date_part"
    input_types: List[Type] = field(default_factory=lambda: [Expression, Expression])
    return_type: Type = Union[int, str]
    
    def generate_sql(self, dialect: str, sql_generator: Callable[[Expression], str]) -> str:
        """
        Generate SQL for this function in the specified dialect.
        
        Args:
            dialect: The SQL dialect to generate
            sql_generator: The SQL generator to use for parameter expressions
            
        Returns:
            The generated SQL string
        """
        if len(self.parameters) != 2:
            raise ValueError("date_part function requires exactly 2 parameters")
        
        part = sql_generator(self.parameters[0])
        date_expr = sql_generator(self.parameters[1])
        
        if part.startswith("'") and part.endswith("'"):
            part = part[1:-1]
        
        if dialect.lower() == "duckdb":
            return self.generate_duckdb_sql(part, date_expr)
        elif dialect.lower() == "postgres":
            return self.generate_postgres_sql(part, date_expr)
        elif dialect.lower() == "mysql":
            return self.generate_mysql_sql(part, date_expr)
        
        return self.generate_default_sql(part, date_expr)
    
    def generate_default_sql(self, part: str, date_expr: str) -> str:
        """Default SQL generation for date_part."""
        return f"DATE_PART('{part}', {date_expr})"
    
    def generate_duckdb_sql(self, part: str, date_expr: str) -> str:
        """DuckDB-specific SQL generation for date_part."""
        return f"DATE_PART('{part}', {date_expr})"
    
    def generate_postgres_sql(self, part: str, date_expr: str) -> str:
        """PostgreSQL-specific SQL generation for date_part."""
        return f"EXTRACT({part} FROM {date_expr})"
    
    def generate_mysql_sql(self, part: str, date_expr: str) -> str:
        """MySQL-specific SQL generation for date_part."""
        if part.lower() in ('year', 'month', 'day', 'hour', 'minute', 'second'):
            return f"{part.upper()}({date_expr})"
        else:
            return f"EXTRACT({part} FROM {date_expr})"

@register_function
@dataclass
class DateTrunc(ScalarFunction):
    """
    date_trunc scalar function.
    
    Truncates a date/time value to the specified precision.
    
    Input Parameters:
        precision: String specifying the precision to truncate to (year, month, day, etc.)
        date: Date/time expression to truncate
        
    Return Type:
        Date value truncated to the specified precision
    """
    function_name: str = "date_trunc"
    input_types: List[Type] = field(default_factory=lambda: [Expression, Expression])
    return_type: Type = str
    
    def generate_sql(self, dialect: str, sql_generator: Callable[[Expression], str]) -> str:
        """
        Generate SQL for this function in the specified dialect.
        
        Args:
            dialect: The SQL dialect to generate
            sql_generator: The SQL generator to use for parameter expressions
            
        Returns:
            The generated SQL string
        """
        if len(self.parameters) != 2:
            raise ValueError("date_trunc function requires exactly 2 parameters")
        
        precision = sql_generator(self.parameters[0])
        date_expr = sql_generator(self.parameters[1])
        
        if precision.startswith("'") and precision.endswith("'"):
            precision = precision[1:-1]
        
        if dialect.lower() == "duckdb":
            return self.generate_duckdb_sql(precision, date_expr)
        elif dialect.lower() == "postgres":
            return self.generate_postgres_sql(precision, date_expr)
        elif dialect.lower() == "mysql":
            return self.generate_mysql_sql(precision, date_expr)
        
        return self.generate_default_sql(precision, date_expr)
    
    def generate_default_sql(self, precision: str, date_expr: str) -> str:
        """Default SQL generation for date_trunc."""
        return f"DATE_TRUNC('{precision}', {date_expr})"
    
    def generate_duckdb_sql(self, precision: str, date_expr: str) -> str:
        """DuckDB-specific SQL generation for date_trunc."""
        return f"DATE_TRUNC('{precision}', {date_expr})"
    
    def generate_postgres_sql(self, precision: str, date_expr: str) -> str:
        """PostgreSQL-specific SQL generation for date_trunc."""
        return f"DATE_TRUNC('{precision}', {date_expr})"
    
    def generate_mysql_sql(self, precision: str, date_expr: str) -> str:
        """MySQL-specific SQL generation for date_trunc."""
        if precision.lower() == 'year':
            return f"STR_TO_DATE(CONCAT(YEAR({date_expr}), '-01-01'), '%Y-%m-%d')"
        elif precision.lower() == 'month':
            return f"STR_TO_DATE(CONCAT(YEAR({date_expr}), '-', MONTH({date_expr}), '-01'), '%Y-%m-%d')"
        elif precision.lower() == 'day':
            return f"DATE({date_expr})"
        else:
            return f"DATE_TRUNC('{precision}', {date_expr})"


def date_part(part: Union[str, Callable, Expression], expr: Union[Callable, Expression]) -> DatePart:
    """
    Create a DATE_PART scalar function.
    
    Args:
        part: The part to extract (year, month, day, etc.)
              Example: 'year', lambda x: x.part_name
        expr: Date/time expression to extract from (lambda function or Expression)
              Example: lambda x: x.date_column
        
    Returns:
        A DatePart expression
    """
    from ..utils.lambda_parser import parse_lambda
    
    if isinstance(part, str):
        part_expr = LiteralExpression(value=part)
    elif callable(part) and not isinstance(part, Expression):
        part_expr = parse_lambda(part)
    else:
        part_expr = part
    
    if callable(expr) and not isinstance(expr, Expression):
        parsed_expr = parse_lambda(expr)
    else:
        parsed_expr = expr
    
    return DatePart(parameters=[part_expr, parsed_expr])

def date_trunc(precision: Union[str, Callable, Expression], expr: Union[Callable, Expression]) -> DateTrunc:
    """
    Create a DATE_TRUNC scalar function.
    
    Args:
        precision: The precision to truncate to (year, month, day, etc.)
                  Example: 'month', lambda x: x.precision
        expr: Date/time expression to truncate (lambda function or Expression)
              Example: lambda x: x.date_column
        
    Returns:
        A DateTrunc expression
    """
    from ..utils.lambda_parser import parse_lambda
    
    if isinstance(precision, str):
        precision_expr = LiteralExpression(value=precision)
    elif callable(precision) and not isinstance(precision, Expression):
        precision_expr = parse_lambda(precision)
    else:
        precision_expr = precision
    
    if callable(expr) and not isinstance(expr, Expression):
        parsed_expr = parse_lambda(expr)
    else:
        parsed_expr = expr
    
    return DateTrunc(parameters=[precision_expr, parsed_expr])
