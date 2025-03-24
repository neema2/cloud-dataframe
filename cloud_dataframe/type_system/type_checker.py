"""
Type checking utilities for the cloud-dataframe DSL.

This module provides utilities for type checking dataframe operations
to ensure type safety at both development and runtime.
"""
from __future__ import annotations
from typing import Any, Dict, List, Optional, Type, TypeVar, Union, get_type_hints
import inspect
from dataclasses import is_dataclass

from .schema import TableSchema, ColSpec
from .column import Expression, ColumnReference, FunctionExpression

T = TypeVar('T')


class TypeChecker:
    """
    Type checker for dataframe operations.
    
    This class provides methods to validate the types of expressions
    and ensure type safety in dataframe operations.
    """
    
    @staticmethod
    def validate_column_reference(col_ref: ColumnReference, table_schema: Optional[TableSchema] = None) -> bool:
        """
        Validate that a column reference exists in the schema.
        
        Args:
            col_ref: The column reference to validate
            table_schema: The schema to validate against
            
        Returns:
            True if the column reference is valid, False otherwise
        """
        if not table_schema:
            # If no schema is provided, we can't validate the column
            return True
        
        return table_schema.validate_column(col_ref.name)
    
    @staticmethod
    def validate_function_expression(func_expr: FunctionExpression, table_schema: Optional[TableSchema] = None) -> bool:
        """
        Validate that a function expression has valid parameters.
        
        Args:
            func_expr: The function expression to validate
            table_schema: The schema to validate against
            
        Returns:
            True if the function expression is valid, False otherwise
        """
        # Validate each parameter
        for param in func_expr.parameters:
            if isinstance(param, ColumnReference):
                if not TypeChecker.validate_column_reference(param, table_schema):
                    return False
            elif isinstance(param, FunctionExpression):
                if not TypeChecker.validate_function_expression(param, table_schema):
                    return False
        
        return True
    
    @staticmethod
    def validate_expression(expr: Expression, table_schema: Optional[TableSchema] = None) -> bool:
        """
        Validate that an expression is valid.
        
        Args:
            expr: The expression to validate
            table_schema: The schema to validate against
            
        Returns:
            True if the expression is valid, False otherwise
        """
        if isinstance(expr, ColumnReference):
            return TypeChecker.validate_column_reference(expr, table_schema)
        elif isinstance(expr, FunctionExpression):
            return TypeChecker.validate_function_expression(expr, table_schema)
        else:
            # Other expression types are assumed to be valid
            return True
    
    @staticmethod
    def get_expression_type(expr: Expression, table_schema: Optional[TableSchema] = None) -> Optional[Type]:
        """
        Get the type of an expression.
        
        Args:
            expr: The expression to get the type of
            table_schema: The schema to get the type from
            
        Returns:
            The type of the expression, or None if the type cannot be determined
        """
        if isinstance(expr, ColumnReference):
            if table_schema:
                return table_schema.get_column_type(expr.name)
            else:
                return None
        elif isinstance(expr, FunctionExpression):
            # For function expressions, we would need to know the return type of the function
            # This is a simplification - in a real implementation, we would have a registry of function return types
            return None
        else:
            # For other expression types, we can't determine the type
            return None


def validate_dataclass_schema(cls: Type) -> bool:
    """
    Validate that a dataclass can be used as a table schema.
    
    Args:
        cls: The dataclass to validate
        
    Returns:
        True if the dataclass is valid as a schema, False otherwise
    """
    if not is_dataclass(cls):
        return False
    
    # Check that all fields have type annotations
    type_hints = get_type_hints(cls)
    return len(type_hints) > 0


def create_schema_from_dataclass(cls: Type, name: Optional[str] = None) -> TableSchema:
    """
    Create a TableSchema from a dataclass.
    
    Args:
        cls: The dataclass to create a schema from
        name: Optional name for the schema (defaults to the class name)
        
    Returns:
        A TableSchema created from the dataclass
    """
    if not validate_dataclass_schema(cls):
        raise ValueError(f"Class {cls.__name__} is not a valid dataclass schema")
    
    type_hints = get_type_hints(cls)
    
    # Check if the class has a custom name from the decorator
    custom_name = None
    if hasattr(cls, "__table_schema__") and getattr(cls, "__table_schema__").name != cls.__name__:
        custom_name = getattr(cls, "__table_schema__").name
    
    return TableSchema(
        name=name or custom_name or cls.__name__,
        columns=type_hints
    )


def col_spec_from_dataclass_field(cls: Type, field_name: str) -> ColSpec:
    """
    Create a ColSpec from a dataclass field.
    
    Args:
        cls: The dataclass containing the field
        field_name: The name of the field
        
    Returns:
        A ColSpec for the field
    """
    if not validate_dataclass_schema(cls):
        raise ValueError(f"Class {cls.__name__} is not a valid dataclass schema")
    
    type_hints = get_type_hints(cls)
    
    if field_name not in type_hints:
        raise ValueError(f"Field {field_name} not found in class {cls.__name__}")
    
    schema = create_schema_from_dataclass(cls)
    
    return ColSpec(name=field_name, table_schema=schema)
