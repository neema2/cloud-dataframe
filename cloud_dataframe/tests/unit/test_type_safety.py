"""
Unit tests for type safety features.

This module contains tests for the type safety mechanisms in the cloud-dataframe DSL.
"""
import unittest
from dataclasses import dataclass
from typing import Optional, List, Dict, Any, Type, cast

from cloud_dataframe.type_system.schema import TableSchema, ColSpec
from cloud_dataframe.type_system.column import (
    Expression, ColumnReference, LiteralExpression, FunctionExpression,
    col, literal, as_column
)
from cloud_dataframe.type_system.type_checker import (
    TypeChecker, validate_dataclass_schema, create_schema_from_dataclass,
    col_spec_from_dataclass_field
)
from cloud_dataframe.type_system.decorators import (
    type_safe, dataclass_to_schema, col
)


@dataclass_to_schema(name="EmployeeTable")
class Employee:
    """Employee dataclass for testing type-safe operations."""
    id: int
    name: str
    department: str
    salary: float
    manager_id: Optional[int] = None


class TestTypeChecker(unittest.TestCase):
    """Test cases for the TypeChecker class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.schema = TableSchema(
            name="employees",
            columns={
                "id": int,
                "name": str,
                "department": str,
                "salary": float,
                "manager_id": Optional[int]
            }
        )
        self.type_checker = TypeChecker()
    
    def test_validate_column_reference(self):
        """Test validating a column reference."""
        # Valid column reference
        col_ref = ColumnReference(name="id")
        self.assertTrue(self.type_checker.validate_column_reference(col_ref, self.schema))
        
        # Invalid column reference
        col_ref = ColumnReference(name="invalid_column")
        self.assertFalse(self.type_checker.validate_column_reference(col_ref, self.schema))
    
    def test_validate_function_expression(self):
        """Test validating a function expression."""
        # Valid function expression
        func_expr = FunctionExpression(
            function_name="SUM",
            parameters=[ColumnReference(name="salary")]
        )
        self.assertTrue(self.type_checker.validate_function_expression(func_expr, self.schema))
        
        # Invalid function expression
        func_expr = FunctionExpression(
            function_name="SUM",
            parameters=[ColumnReference(name="invalid_column")]
        )
        self.assertFalse(self.type_checker.validate_function_expression(func_expr, self.schema))
    
    def test_get_expression_type(self):
        """Test getting the type of an expression."""
        # Column reference
        col_ref = ColumnReference(name="id")
        self.assertEqual(self.type_checker.get_expression_type(col_ref, self.schema), int)
        
        # Literal expression
        lit_expr = LiteralExpression(value=42)
        self.assertIsNone(self.type_checker.get_expression_type(lit_expr, self.schema))


class TestDataclassSchema(unittest.TestCase):
    """Test cases for dataclass schema utilities."""
    
    def test_validate_dataclass_schema(self):
        """Test validating a dataclass schema."""
        # Valid dataclass schema
        self.assertTrue(validate_dataclass_schema(Employee))
        
        # Invalid dataclass schema (not a dataclass)
        class NotADataclass:
            id: int
            name: str
        
        self.assertFalse(validate_dataclass_schema(NotADataclass))
    
    def test_create_schema_from_dataclass(self):
        """Test creating a schema from a dataclass."""
        schema = create_schema_from_dataclass(Employee)
        
        self.assertEqual(schema.name, "EmployeeTable")
        self.assertEqual(len(schema.columns), 5)
        self.assertEqual(schema.columns["id"], int)
        self.assertEqual(schema.columns["name"], str)
        self.assertEqual(schema.columns["department"], str)
        self.assertEqual(schema.columns["salary"], float)
        self.assertEqual(schema.columns["manager_id"], Optional[int])
    
    def test_col_spec_from_dataclass_field(self):
        """Test creating a ColSpec from a dataclass field."""
        col_spec = col_spec_from_dataclass_field(Employee, "salary")
        
        self.assertEqual(col_spec.name, "salary")
        self.assertEqual(col_spec.table_schema.name, "EmployeeTable")
        
        # Invalid field
        with self.assertRaises(ValueError):
            col_spec_from_dataclass_field(Employee, "invalid_field")


class TestTypeDecorators(unittest.TestCase):
    """Test cases for type decorators."""
    
    def test_type_safe_decorator(self):
        """Test the type_safe decorator."""
        @type_safe
        def add_numbers(a: int, b: int) -> int:
            return a + b
        
        # Valid call
        self.assertEqual(add_numbers(1, 2), 3)
        
        # Invalid call (wrong argument type)
        with self.assertRaises(TypeError):
            add_numbers("1", 2)
        
        # Invalid call (wrong return type)
        @type_safe
        def return_wrong_type(a: int) -> str:
            return a  # type: ignore
        
        with self.assertRaises(TypeError):
            return_wrong_type(1)
    
    def test_dataclass_to_schema_decorator(self):
        """Test the dataclass_to_schema decorator."""
        # Check that the decorator adds the __table_schema__ attribute
        self.assertTrue(hasattr(Employee, "__table_schema__"))
        
        # Check that the schema has the correct name
        schema = getattr(Employee, "__table_schema__")
        self.assertEqual(schema.name, "EmployeeTable")
    
    def test_col_function(self):
        """Test the col function."""
        # Create a column specification
        employee_id = col("id")(Employee)
        
        self.assertEqual(employee_id.name, "id")
        self.assertEqual(employee_id.table_schema.name, "EmployeeTable")
        
        # Invalid column
        with self.assertRaises(ValueError):
            col("invalid_column")(Employee)


if __name__ == "__main__":
    unittest.main()
