"""
Schema definitions for the cloud-dataframe type system.

This module defines the schema classes used to provide type safety
in dataframe operations.
"""
from __future__ import annotations
from typing import Any, Dict, Generic, List, Optional, Type, TypeVar, Union, get_type_hints
from dataclasses import dataclass, field, make_dataclass

T = TypeVar('T')


@dataclass
class TableSchema:
    """
    Schema definition for a database table.
    
    A TableSchema defines the columns and their types for a table,
    enabling type-safe operations on the table.
    """
    name: str
    columns: Dict[str, Type] = field(default_factory=dict)
    
    def validate_column(self, column_name: str) -> bool:
        """
        Validate that a column exists in the schema.
        
        Args:
            column_name: The name of the column to validate
            
        Returns:
            True if the column exists, False otherwise
        """
        return column_name in self.columns
    
    def get_column_type(self, column_name: str) -> Optional[Type]:
        """
        Get the type of a column.
        
        Args:
            column_name: The name of the column
            
        Returns:
            The type of the column, or None if the column doesn't exist
        """
        return self.columns.get(column_name)


@dataclass
class ColSpec(Generic[T]):
    """
    Type-safe column specification.
    
    A ColSpec represents a column with type information,
    enabling type-safe operations on the column.
    """
    name: str
    table_schema: Optional[TableSchema] = None
    
    def __post_init__(self):
        """Validate the column exists in the schema if provided."""
        if self.table_schema and not self.table_schema.validate_column(self.name):
            raise ValueError(f"Column '{self.name}' not found in table schema '{self.table_schema.name}'")


def col_spec(name: str, table_schema: Optional[TableSchema] = None) -> ColSpec:
    """
    Create a type-safe column specification.
    
    Args:
        name: The name of the column
        table_schema: Optional schema for the table
        
    Returns:
        A ColSpec object
    """
    return ColSpec(name=name, table_schema=table_schema)


# Decorator for creating type-safe table schemas from dataclasses
def table_schema(cls=None, *, name: Optional[str] = None):
    """
    Decorator to create a TableSchema from a dataclass.
    
    This decorator allows you to define a table schema using a Python dataclass,
    which provides better type checking and IDE support.
    
    Args:
        cls: The dataclass to convert to a TableSchema
        name: Optional name for the table (defaults to the class name)
        
    Returns:
        A TableSchema object
    """
    def wrap(cls):
        table_name = name or cls.__name__
        type_hints = get_type_hints(cls)
        
        schema = TableSchema(
            name=table_name,
            columns={field_name: field_type for field_name, field_type in type_hints.items()}
        )
        
        # Attach the schema to the class for reference
        setattr(cls, '__table_schema__', schema)
        return cls
    
    if cls is None:
        return wrap
    
    return wrap(cls)


def create_dynamic_dataclass_from_schema(table_name: str, schema: TableSchema) -> Type:
    """
    Create a dynamic dataclass from a TableSchema.
    
    Args:
        table_name: The name of the table
        schema: The schema to create a dataclass from
        
    Returns:
        A dynamically generated dataclass
    """
    # Create fields for the dataclass
    fields = []
    for col_name, col_type in schema.columns.items():
        # Skip wildcard columns
        if col_name == "*":
            continue
        
        # Use the actual type from the schema, not Any
        # If the type is not specified, default to Any
        field_type = col_type if col_type != type(Any) else Any
        
        # Add the field to the dataclass
        fields.append((col_name, field_type))
    
    # Create the dataclass
    cls = make_dataclass(
        cls_name=f"{table_name.title().replace('_', '')}Table", 
        fields=fields,
        frozen=True  # Make it immutable for safety
    )
    
    # Attach the schema to the class
    setattr(cls, '__table_schema__', schema)
    
    # Add a validate_column method to check if a column exists
    def validate_column(cls, column_name: str) -> bool:
        """Check if a column exists in the schema."""
        return column_name in schema.columns or column_name == "*"
    
    setattr(cls, 'validate_column', classmethod(validate_column))
    
    return cls
