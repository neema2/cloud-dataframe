"""
Backend modules for the cloud-dataframe DSL.

This package contains the backend modules for different database systems.
Each backend module provides SQL generation functions for a specific database.
"""
from typing import Dict, Type, Callable
from ..core.dataframe import DataFrame

# Registry of SQL generators for different database backends
SQL_GENERATORS: Dict[str, Callable[[DataFrame], str]] = {}


def register_sql_generator(dialect: str, generator: Callable[[DataFrame], str]) -> None:
    """
    Register a SQL generator for a database dialect.
    
    Args:
        dialect: The name of the database dialect
        generator: The SQL generator function
    """
    SQL_GENERATORS[dialect.lower()] = generator


def get_sql_generator(dialect: str) -> Callable[[DataFrame], str]:
    """
    Get the SQL generator for a database dialect.
    
    Args:
        dialect: The name of the database dialect
        
    Returns:
        The SQL generator function
        
    Raises:
        ValueError: If no SQL generator is registered for the dialect
    """
    generator = SQL_GENERATORS.get(dialect.lower())
    if not generator:
        raise ValueError(f"No SQL generator registered for dialect: {dialect}")
    
    return generator


# Import and register the DuckDB SQL generator
from .duckdb.sql_generator import generate_sql as generate_duckdb_sql
register_sql_generator("duckdb", generate_duckdb_sql)

from .pure_relation.relation_generator import generate_relation as generate_pure_relation
register_sql_generator("pure_relation", generate_pure_relation)
