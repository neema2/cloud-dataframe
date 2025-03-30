"""
String functions for the cloud-dataframe DSL.

This module provides string-related scalar functions for the DataFrame DSL.
"""
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Union, Callable, Type

from ..type_system.column import ScalarFunction, Expression
from . import register_function

@register_function
@dataclass
class Concat(ScalarFunction):
    """
    concat scalar function.
    
    Concatenates two or more strings together.
    
    Input Parameters:
        *strings: Two or more string expressions to concatenate
        
    Return Type:
        String containing the concatenated result
    """
    function_name: str = "concat"
    input_types: List[Type] = field(default_factory=lambda: [Expression])
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
        if len(self.parameters) < 2:
            raise ValueError("concat function requires at least 2 parameters")
        
        params = [sql_generator(param) for param in self.parameters]
        
        if dialect.lower() == "duckdb":
            return self.generate_duckdb_sql(params)
        elif dialect.lower() == "postgres":
            return self.generate_postgres_sql(params)
        elif dialect.lower() == "mysql":
            return self.generate_mysql_sql(params)
        
        return self.generate_default_sql(params)
    
    def generate_default_sql(self, params: List[str]) -> str:
        """Default SQL generation for concat."""
        return f"CONCAT({', '.join(params)})"
    
    def generate_duckdb_sql(self, params: List[str]) -> str:
        """DuckDB-specific SQL generation for concat."""
        return f"CONCAT({', '.join(params)})"
    
    def generate_postgres_sql(self, params: List[str]) -> str:
        """PostgreSQL-specific SQL generation for concat."""
        return f"CONCAT({', '.join(params)})"
    
    def generate_mysql_sql(self, params: List[str]) -> str:
        """MySQL-specific SQL generation for concat."""
        return f"CONCAT({', '.join(params)})"

@register_function
@dataclass
class Upper(ScalarFunction):
    """
    upper scalar function.
    
    Converts a string to uppercase.
    
    Input Parameters:
        string: String expression to convert to uppercase
        
    Return Type:
        String containing the uppercase result
    """
    function_name: str = "upper"
    input_types: List[Type] = field(default_factory=lambda: [Expression])
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
        if len(self.parameters) != 1:
            raise ValueError("upper function requires exactly 1 parameter")
        
        param = sql_generator(self.parameters[0])
        
        if dialect.lower() == "duckdb":
            return self.generate_duckdb_sql(param)
        elif dialect.lower() == "postgres":
            return self.generate_postgres_sql(param)
        elif dialect.lower() == "mysql":
            return self.generate_mysql_sql(param)
        
        return self.generate_default_sql(param)
    
    def generate_default_sql(self, param: str) -> str:
        """Default SQL generation for upper."""
        return f"UPPER({param})"
    
    def generate_duckdb_sql(self, param: str) -> str:
        """DuckDB-specific SQL generation for upper."""
        return f"UPPER({param})"
    
    def generate_postgres_sql(self, param: str) -> str:
        """PostgreSQL-specific SQL generation for upper."""
        return f"UPPER({param})"
    
    def generate_mysql_sql(self, param: str) -> str:
        """MySQL-specific SQL generation for upper."""
        return f"UPPER({param})"

@register_function
@dataclass
class Lower(ScalarFunction):
    """
    lower scalar function.
    
    Converts a string to lowercase.
    
    Input Parameters:
        string: String expression to convert to lowercase
        
    Return Type:
        String containing the lowercase result
    """
    function_name: str = "lower"
    input_types: List[Type] = field(default_factory=lambda: [Expression])
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
        if len(self.parameters) != 1:
            raise ValueError("lower function requires exactly 1 parameter")
        
        param = sql_generator(self.parameters[0])
        
        if dialect.lower() == "duckdb":
            return self.generate_duckdb_sql(param)
        elif dialect.lower() == "postgres":
            return self.generate_postgres_sql(param)
        elif dialect.lower() == "mysql":
            return self.generate_mysql_sql(param)
        
        return self.generate_default_sql(param)
    
    def generate_default_sql(self, param: str) -> str:
        """Default SQL generation for lower."""
        return f"LOWER({param})"
    
    def generate_duckdb_sql(self, param: str) -> str:
        """DuckDB-specific SQL generation for lower."""
        return f"LOWER({param})"
    
    def generate_postgres_sql(self, param: str) -> str:
        """PostgreSQL-specific SQL generation for lower."""
        return f"LOWER({param})"
    
    def generate_mysql_sql(self, param: str) -> str:
        """MySQL-specific SQL generation for lower."""
        return f"LOWER({param})"


def concat(*exprs: Union[Expression, Any]) -> Concat:
    """
    Create a CONCAT scalar function.
    
    Args:
        *exprs: String expressions to concatenate (column references, expressions, or literals)
              Example: x.first_name, " ", x.last_name
        
    Returns:
        A Concat expression
    """
    from ..type_system.column import LiteralExpression
    
    params = []
    for expr in exprs:
        if isinstance(expr, str):
            params.append(LiteralExpression(value=expr))
        else:
            params.append(expr)
    
    return Concat(parameters=params)

def upper(expr: Union[Expression, Any]) -> Upper:
    """
    Create an UPPER scalar function.
    
    Args:
        expr: String expression to convert to uppercase
              Example: x.name
        
    Returns:
        An Upper expression
    """
    return Upper(parameters=[expr])

def lower(expr: Union[Expression, Any]) -> Lower:
    """
    Create a LOWER scalar function.
    
    Args:
        expr: String expression to convert to lowercase
              Example: x.name
        
    Returns:
        A Lower expression
    """
    return Lower(parameters=[expr])
