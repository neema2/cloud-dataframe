"""
Lambda parser for the cloud-dataframe DSL.

This module provides utilities for parsing Python lambda functions
and converting them to SQL expressions.
"""
import ast
import inspect
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from ..type_system.column import (
    Expression, LiteralExpression, ColumnReference
)
from ..core.dataframe import BinaryOperation


class LambdaParser:
    """
    Parser for converting Python lambda functions to SQL expressions.
    
    This class provides methods to analyze a lambda function's AST
    and convert it to an equivalent SQL expression.
    """
    
    @staticmethod
    def parse_lambda(lambda_func: Callable, table_schema=None) -> Expression:
        """
        Parse a lambda function and convert it to an Expression.
        
        Args:
            lambda_func: The lambda function to parse
            table_schema: Optional schema for type checking
            
        Returns:
            An Expression representing the lambda function
        """
        # Get the source code of the lambda function
        source = inspect.getsource(lambda_func)
        
        # Parse the source code into an AST
        tree = ast.parse(source.strip())
        
        # Find the lambda expression in the AST
        lambda_node = None
        for node in ast.walk(tree):
            if isinstance(node, ast.Lambda):
                lambda_node = node
                break
        
        if not lambda_node:
            raise ValueError("Could not find lambda expression in source code")
        
        # Parse the lambda body
        return LambdaParser._parse_expression(lambda_node.body, lambda_node.args.args, table_schema)
    
    @staticmethod
    def _parse_expression(node: ast.AST, args: List[ast.arg], table_schema=None) -> Expression:
        """
        Parse an AST node and convert it to an Expression.
        
        Args:
            node: The AST node to parse
            args: The lambda function arguments
            table_schema: Optional schema for type checking
            
        Returns:
            An Expression representing the AST node
        """
        # Handle different types of AST nodes
        if isinstance(node, ast.Compare):
            # Handle comparison operations (e.g., x > 5, y == 'value')
            left = LambdaParser._parse_expression(node.left, args, table_schema)
            
            # We only handle the first comparator for simplicity
            # In a real implementation, we would handle multiple comparators
            op = node.ops[0]
            right = LambdaParser._parse_expression(node.comparators[0], args, table_schema)
            
            operator = LambdaParser._get_comparison_operator(op)
            
            return BinaryOperation(left=left, operator=operator, right=right)
        
        elif isinstance(node, ast.BoolOp):
            # Handle boolean operations (e.g., x and y, x or y)
            values = [LambdaParser._parse_expression(val, args, table_schema) for val in node.values]
            
            # Combine the values with the appropriate operator
            operator = "AND" if isinstance(node.op, ast.And) else "OR"
            
            # Start with the first two values
            result = BinaryOperation(left=values[0], operator=operator, right=values[1])
            
            # Add the remaining values
            for value in values[2:]:
                result = BinaryOperation(left=result, operator=operator, right=value)
            
            return result
        
        elif isinstance(node, ast.Attribute):
            # Handle attribute access (e.g., x.name, x.age)
            if isinstance(node.value, ast.Name) and node.value.id == args[0].arg:
                # This is accessing an attribute of the lambda parameter (e.g., x.name)
                return ColumnReference(name=node.attr)
            else:
                # This is a more complex attribute access
                # In a real implementation, we would handle this more robustly
                raise ValueError(f"Unsupported attribute access: {ast.dump(node)}")
        
        elif isinstance(node, ast.Constant):
            # Handle literal values (e.g., 5, 'value', True)
            return LiteralExpression(value=node.value)
        
        elif isinstance(node, ast.Name):
            # Handle variable names (e.g., x, y)
            if node.id == args[0].arg:
                # This is the lambda parameter itself
                # In a real implementation, we would handle this more robustly
                raise ValueError("Cannot use the lambda parameter directly")
            else:
                # This is a variable reference
                # In a real implementation, we would handle this more robustly
                raise ValueError(f"Unsupported variable reference: {node.id}")
        
        else:
            # Handle other types of AST nodes
            # In a real implementation, we would handle more types of nodes
            raise ValueError(f"Unsupported AST node type: {type(node)}")
    
    @staticmethod
    def _get_comparison_operator(op: ast.cmpop) -> str:
        """
        Convert an AST comparison operator to a SQL operator.
        
        Args:
            op: The AST comparison operator
            
        Returns:
            The equivalent SQL operator
        """
        if isinstance(op, ast.Eq):
            return "="
        elif isinstance(op, ast.NotEq):
            return "!="
        elif isinstance(op, ast.Lt):
            return "<"
        elif isinstance(op, ast.LtE):
            return "<="
        elif isinstance(op, ast.Gt):
            return ">"
        elif isinstance(op, ast.GtE):
            return ">="
        elif isinstance(op, ast.In):
            return "IN"
        elif isinstance(op, ast.NotIn):
            return "NOT IN"
        else:
            raise ValueError(f"Unsupported comparison operator: {type(op)}")
