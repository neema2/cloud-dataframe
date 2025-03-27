"""
Debug script for lambda parser in cloud-dataframe.

This script helps debug issues with the lambda parser by extracting and printing
the source code of lambda functions, parsing them into an AST, and debugging
complex nested conditions and chained filters.
"""

import ast
import inspect
import textwrap
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from cloud_dataframe.utils.lambda_parser import LambdaParser
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.type_system.column import (
    Expression, LiteralExpression, ColumnReference, 
    SumFunction, AvgFunction, CountFunction, MinFunction, MaxFunction
)
from cloud_dataframe.core.dataframe import BinaryOperation


def extract_lambda_source(lambda_func: Callable) -> str:
    """
    Extract the source code of a lambda function.
    
    Args:
        lambda_func: The lambda function to extract source from
        
    Returns:
        The source code of the lambda function as a string
    """
    try:
        source = inspect.getsource(lambda_func)
        
        if "\\" in source:
            source = source.replace("\\", "").strip()
        
        lambda_start = source.find("lambda")
        if lambda_start >= 0:
            source = source[lambda_start:]
            
            assign_pos = source.find("=")
            if assign_pos > 0 and assign_pos < source.find(":"):
                source = source[assign_pos+1:].strip()
                lambda_start = source.find("lambda")
                if lambda_start >= 0:
                    source = source[lambda_start:]
        
        return source.strip()
    except (OSError, TypeError):
        code = lambda_func.__code__
        return f"lambda {', '.join(code.co_varnames[:code.co_argcount])}: <body>"


def print_ast(node, indent=0):
    """
    Print an AST node and its children with indentation.
    
    Args:
        node: The AST node to print
        indent: The indentation level
    """
    if isinstance(node, ast.AST):
        print(' ' * indent + node.__class__.__name__)
        for field, value in ast.iter_fields(node):
            if isinstance(value, list):
                print(' ' * (indent + 2) + field + ':')
                for item in value:
                    print_ast(item, indent + 4)
            elif isinstance(value, ast.AST):
                print(' ' * (indent + 2) + field + ':')
                print_ast(value, indent + 4)
            else:
                print(' ' * (indent + 2) + field + ': ' + str(value))


def debug_lambda(lambda_func: Callable, schema=None):
    """
    Debug a lambda function by printing its source code and AST.
    
    Args:
        lambda_func: The lambda function to debug
        schema: Optional schema for parsing the lambda
    """
    source = extract_lambda_source(lambda_func)
    print(f"Lambda source: {source}")
    
    try:
        tree = ast.parse(source.strip())
        print("\nAST:")
        print_ast(tree)
        
        if schema:
            print("\nParsed expression:")
            expr = LambdaParser.parse_lambda(lambda_func, schema)
            print(expr)
    except SyntaxError as e:
        print(f"Syntax error: {e}")


def debug_complex_condition():
    """Debug a complex nested condition."""
    schema = TableSchema(
        name="Employee",
        columns={
            "department": str,
            "salary": float,
            "is_manager": bool,
            "age": int
        }
    )
    
    lambda_func = lambda x: (x.department == "Engineering" and x.salary > 80000) or (x.department == "Sales" and x.salary > 60000) or (x.is_manager == True and x.age > 40)
    
    debug_lambda(lambda_func, schema)


def debug_chained_filters():
    """Debug chained filters."""
    schema = TableSchema(
        name="Employee",
        columns={
            "salary": float,
            "department": str,
            "age": int
        }
    )
    
    lambda_func = lambda x: x.salary > 50000 and x.department == "Engineering" and x.age > 30
    
    debug_lambda(lambda_func, schema)


if __name__ == "__main__":
    print("Debugging complex nested condition:")
    debug_complex_condition()
    
    print("\n" + "="*80 + "\n")
    
    print("Debugging chained filters:")
    debug_chained_filters()
