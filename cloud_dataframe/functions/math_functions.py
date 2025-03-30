"""
Math functions for the cloud-dataframe DSL.

This module provides math-related scalar functions for the DataFrame DSL.
"""
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Union, Callable, Type

from ..type_system.column import ScalarFunction, Expression
from . import register_function

@register_function
@dataclass
class Round(ScalarFunction):
    """
    round scalar function.
    
    Rounds a number to the specified number of decimal places.
    
    Input Parameters:
        value: Numeric expression to round
        decimal_places: (Optional) Number of decimal places to round to (default: 0)
        
    Return Type:
        Numeric value rounded to the specified number of decimal places
    """
    function_name: str = "round"
    input_types: List[Type] = field(default_factory=lambda: [Expression, Optional[Expression]])
    return_type: Type = float
    
    def generate_sql(self, dialect: str, sql_generator: Callable[[Expression], str]) -> str:
        """
        Generate SQL for this function in the specified dialect.
        
        Args:
            dialect: The SQL dialect to generate
            sql_generator: The SQL generator to use for parameter expressions
            
        Returns:
            The generated SQL string
        """
        if len(self.parameters) < 1 or len(self.parameters) > 2:
            raise ValueError("round function requires 1 or 2 parameters")
        
        params = [sql_generator(param) for param in self.parameters]
        
        decimal_places = "0"
        if len(params) > 1:
            decimal_places = params[1]
        
        if dialect.lower() == "duckdb":
            return self.generate_duckdb_sql(params[0], decimal_places)
        elif dialect.lower() == "postgres":
            return self.generate_postgres_sql(params[0], decimal_places)
        elif dialect.lower() == "mysql":
            return self.generate_mysql_sql(params[0], decimal_places)
        
        return self.generate_default_sql(params[0], decimal_places)
    
    def generate_default_sql(self, value: str, decimal_places: str) -> str:
        """Default SQL generation for round."""
        return f"ROUND({value}, {decimal_places})"
    
    def generate_duckdb_sql(self, value: str, decimal_places: str) -> str:
        """DuckDB-specific SQL generation for round."""
        return f"ROUND({value}, {decimal_places})"
    
    def generate_postgres_sql(self, value: str, decimal_places: str) -> str:
        """PostgreSQL-specific SQL generation for round."""
        return f"ROUND({value}::numeric, {decimal_places})"
    
    def generate_mysql_sql(self, value: str, decimal_places: str) -> str:
        """MySQL-specific SQL generation for round."""
        return f"ROUND({value}, {decimal_places})"

@register_function
@dataclass
class Ceil(ScalarFunction):
    """
    ceil scalar function.
    
    Returns the smallest integer greater than or equal to a number.
    
    Input Parameters:
        value: Numeric expression to ceil
        
    Return Type:
        Integer value representing the ceiling of the input
    """
    function_name: str = "ceil"
    input_types: List[Type] = field(default_factory=lambda: [Expression])
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
        if len(self.parameters) != 1:
            raise ValueError("ceil function requires exactly 1 parameter")
        
        param = sql_generator(self.parameters[0])
        
        if dialect.lower() == "duckdb":
            return self.generate_duckdb_sql(param)
        elif dialect.lower() == "postgres":
            return self.generate_postgres_sql(param)
        elif dialect.lower() == "mysql":
            return self.generate_mysql_sql(param)
        
        return self.generate_default_sql(param)
    
    def generate_default_sql(self, param: str) -> str:
        """Default SQL generation for ceil."""
        return f"CEIL({param})"
    
    def generate_duckdb_sql(self, param: str) -> str:
        """DuckDB-specific SQL generation for ceil."""
        return f"CEIL({param})"
    
    def generate_postgres_sql(self, param: str) -> str:
        """PostgreSQL-specific SQL generation for ceil."""
        return f"CEILING({param}::numeric)"
    
    def generate_mysql_sql(self, param: str) -> str:
        """MySQL-specific SQL generation for ceil."""
        return f"CEILING({param})"

@register_function
@dataclass
class Floor(ScalarFunction):
    """
    floor scalar function.
    
    Returns the largest integer less than or equal to a number.
    
    Input Parameters:
        value: Numeric expression to floor
        
    Return Type:
        Integer value representing the floor of the input
    """
    function_name: str = "floor"
    input_types: List[Type] = field(default_factory=lambda: [Expression])
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
        if len(self.parameters) != 1:
            raise ValueError("floor function requires exactly 1 parameter")
        
        param = sql_generator(self.parameters[0])
        
        if dialect.lower() == "duckdb":
            return self.generate_duckdb_sql(param)
        elif dialect.lower() == "postgres":
            return self.generate_postgres_sql(param)
        elif dialect.lower() == "mysql":
            return self.generate_mysql_sql(param)
        
        return self.generate_default_sql(param)
    
    def generate_default_sql(self, param: str) -> str:
        """Default SQL generation for floor."""
        return f"FLOOR({param})"
    
    def generate_duckdb_sql(self, param: str) -> str:
        """DuckDB-specific SQL generation for floor."""
        return f"FLOOR({param})"
    
    def generate_postgres_sql(self, param: str) -> str:
        """PostgreSQL-specific SQL generation for floor."""
        return f"FLOOR({param}::numeric)"
    
    def generate_mysql_sql(self, param: str) -> str:
        """MySQL-specific SQL generation for floor."""
        return f"FLOOR({param})"


def round(expr: Union[Callable, Expression], decimal_places: Union[Callable, Expression, int] = 0) -> Round:
    """
    Create a ROUND scalar function.
    
    Args:
        expr: Numeric expression to round (lambda function or Expression)
              Example: lambda x: x.price
        decimal_places: Number of decimal places to round to (default: 0)
              Example: 2, lambda x: x.precision
        
    Returns:
        A Round expression
    """
    from ..utils.lambda_parser import parse_lambda
    from ..type_system.column import LiteralExpression
    
    if callable(expr) and not isinstance(expr, Expression):
        parsed_expr = parse_lambda(expr)
    else:
        parsed_expr = expr
    
    params = [parsed_expr]
    
    if isinstance(decimal_places, int):
        params.append(LiteralExpression(value=decimal_places))
    elif callable(decimal_places) and not isinstance(decimal_places, Expression):
        params.append(parse_lambda(decimal_places))
    elif isinstance(decimal_places, Expression):
        params.append(decimal_places)
    
    return Round(parameters=params)

def ceil(expr: Union[Callable, Expression]) -> Ceil:
    """
    Create a CEIL scalar function.
    
    Args:
        expr: Numeric expression to ceil (lambda function or Expression)
              Example: lambda x: x.price
        
    Returns:
        A Ceil expression
    """
    from ..utils.lambda_parser import parse_lambda
    
    if callable(expr) and not isinstance(expr, Expression):
        parsed_expr = parse_lambda(expr)
    else:
        parsed_expr = expr
    
    return Ceil(parameters=[parsed_expr])

def floor(expr: Union[Callable, Expression]) -> Floor:
    """
    Create a FLOOR scalar function.
    
    Args:
        expr: Numeric expression to floor (lambda function or Expression)
              Example: lambda x: x.price
        
    Returns:
        A Floor expression
    """
    from ..utils.lambda_parser import parse_lambda
    
    if callable(expr) and not isinstance(expr, Expression):
        parsed_expr = parse_lambda(expr)
    else:
        parsed_expr = expr
    
    return Floor(parameters=[parsed_expr])
