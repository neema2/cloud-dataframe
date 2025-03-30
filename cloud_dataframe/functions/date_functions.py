"""
Date functions for the cloud-dataframe DSL.

This module provides date-related scalar functions for the DataFrame DSL.
"""
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Union, Callable, Type

from ..type_system.column import ScalarFunction, Expression
from . import register_function

@register_function
@dataclass
class DateDiff(ScalarFunction):
    """
    date_diff scalar function.
    
    Calculates the difference between two dates in days.
    
    Input Parameters:
        date1: Date expression (first date)
        date2: Date expression (second date)
        
    Return Type:
        Integer representing the number of days between the dates
    """
    function_name: str = "date_diff"
    input_types: List[Type] = field(default_factory=lambda: [Expression, Expression])
    return_type: Type = int
    
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
            raise ValueError("date_diff function requires exactly 2 parameters")
        
        params = [sql_generator(param) for param in self.parameters]
        
        if dialect.lower() == "duckdb":
            return self.generate_duckdb_sql(params)
        elif dialect.lower() == "postgres":
            return self.generate_postgres_sql(params)
        elif dialect.lower() == "mysql":
            return self.generate_mysql_sql(params)
        
        return self.generate_default_sql(params)
    
    def generate_default_sql(self, params: List[str]) -> str:
        """Default SQL generation for date_diff."""
        return f"DATEDIFF({params[0]}, {params[1]})"
    
    def generate_duckdb_sql(self, params: List[str]) -> str:
        """DuckDB-specific SQL generation for date_diff."""
        return f"DATEDIFF('day', CAST({params[0]} AS DATE), CAST({params[1]} AS DATE))"
    
    def generate_postgres_sql(self, params: List[str]) -> str:
        """PostgreSQL-specific SQL generation for date_diff."""
        return f"({params[1]}::date - {params[0]}::date)"
    
    def generate_mysql_sql(self, params: List[str]) -> str:
        """MySQL-specific SQL generation for date_diff."""
        return f"DATEDIFF({params[1]}, {params[0]})"

def date_diff(expr1: Union[Callable, Expression], expr2: Union[Callable, Expression]) -> DateDiff:
    """
    Create a date_diff scalar function.
    
    Calculates the difference between two dates in days.
    
    Args:
        expr1: First date expression (lambda function or Expression)
              Example: lambda x: x.start_date
        expr2: Second date expression (lambda function or Expression)
              Example: lambda x: x.end_date
        
    Returns:
        A DateDiff expression that evaluates to an integer
    """
    from ..utils.lambda_parser import parse_lambda
    
    if callable(expr1) and not isinstance(expr1, Expression):
        parsed_expr1 = parse_lambda(expr1)
    else:
        parsed_expr1 = expr1
    
    if callable(expr2) and not isinstance(expr2, Expression):
        parsed_expr2 = parse_lambda(expr2)
    else:
        parsed_expr2 = expr2
    
    return DateDiff(parameters=[parsed_expr1, parsed_expr2])
