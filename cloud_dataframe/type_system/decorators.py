"""
Decorators for type-safe dataframe operations.

This module provides decorators that can be used to create type-safe
dataframe operations using Python's type hints.
"""
from __future__ import annotations
from typing import Any, Callable, Dict, List, Optional, Type, TypeVar, Union, get_type_hints
from dataclasses import dataclass, is_dataclass
import inspect
import functools

from .schema import TableSchema, ColSpec
from .type_checker import create_schema_from_dataclass

T = TypeVar('T')
R = TypeVar('R')


def type_safe(func: Callable) -> Callable:
    """
    Decorator to ensure type safety in dataframe operations.
    
    This decorator checks that the arguments passed to a function
    match the expected types, and that the function returns a value
    of the expected type.
    
    Args:
        func: The function to decorate
        
    Returns:
        The decorated function
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Get the type hints for the function
        type_hints = get_type_hints(func)
        
        # Check the types of the arguments
        sig = inspect.signature(func)
        bound_args = sig.bind(*args, **kwargs)
        bound_args.apply_defaults()
        
        for param_name, param_value in bound_args.arguments.items():
            if param_name in type_hints:
                expected_type = type_hints[param_name]
                
                # Skip type checking for None values
                if param_value is None:
                    continue
                
                # Check if the value is of the expected type
                if not isinstance(param_value, expected_type):
                    raise TypeError(f"Argument '{param_name}' must be of type {expected_type}, got {type(param_value)}")
        
        # Call the function
        result = func(*args, **kwargs)
        
        # Check the return type
        if 'return' in type_hints:
            expected_return_type = type_hints['return']
            
            # Skip type checking for None values
            if result is None:
                return result
            
            # Check if the result is of the expected type
            if not isinstance(result, expected_return_type):
                raise TypeError(f"Return value must be of type {expected_return_type}, got {type(result)}")
        
        return result
    
    return wrapper


def table_schema_from_dataclass(cls: Type) -> TableSchema:
    """
    Create a TableSchema from a dataclass.
    
    This function creates a TableSchema from a dataclass, using the
    field names and types as column names and types.
    
    Args:
        cls: The dataclass to create a schema from
        
    Returns:
        A TableSchema created from the dataclass
    """
    return create_schema_from_dataclass(cls)


def dataclass_to_schema(name: Optional[str] = None) -> Callable[[Type], Type]:
    """
    Decorator to create a TableSchema from a dataclass.
    
    This decorator creates a TableSchema from a dataclass and attaches
    it to the dataclass as a class attribute.
    
    Args:
        name: Optional name for the schema (defaults to the class name)
        
    Returns:
        A decorator function
    """
    def decorator(cls: Type) -> Type:
        # If the class is not a dataclass, make it one
        if not is_dataclass(cls):
            cls = dataclass(cls)
        
        # Create a TableSchema from the dataclass
        schema = create_schema_from_dataclass(cls, name)
        
        # Attach the schema to the class
        setattr(cls, '__table_schema__', schema)
        
        return cls
    
    return decorator


def col(field_name: str) -> Callable[[Type], ColSpec]:
    """
    Create a ColSpec from a dataclass field.
    
    This function creates a ColSpec from a dataclass field, which can
    be used for type-safe column references.
    
    Args:
        field_name: The name of the field
        
    Returns:
        A function that creates a ColSpec from a dataclass
    """
    def create_col_spec(cls: Type) -> ColSpec:
        if not is_dataclass(cls):
            raise ValueError(f"Class {cls.__name__} is not a dataclass")
        
        type_hints = get_type_hints(cls)
        
        if field_name not in type_hints:
            raise ValueError(f"Field {field_name} not found in class {cls.__name__}")
        
        schema = create_schema_from_dataclass(cls)
        
        return ColSpec(name=field_name, table_schema=schema)
    
    return create_col_spec
