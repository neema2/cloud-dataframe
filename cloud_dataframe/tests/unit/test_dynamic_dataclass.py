"""
Unit tests for dynamic dataclass generation.

This module contains tests for the dynamic dataclass generation
and typed property access in the cloud-dataframe DSL.
"""
import unittest
from typing import Optional

from cloud_dataframe.core.dataframe import DataFrame
from cloud_dataframe.type_system.schema import TableSchema, create_dynamic_dataclass_from_schema
from cloud_dataframe.type_system.column import as_column, avg


class TestDynamicDataclass(unittest.TestCase):
    """Test cases for dynamic dataclass generation."""
    
    def test_dynamic_dataclass_generation(self):
        """Test generating a dynamic dataclass from a schema."""
        schema = TableSchema(
            name="Employee",
            columns={
                "id": int,
                "name": str,
                "department": str,
                "salary": float
            }
        )
        
        # Create a dynamic dataclass
        DynamicEmployee = create_dynamic_dataclass_from_schema("employees", schema)
        
        # Test creating an instance
        emp = DynamicEmployee(id=1, name="John", department="Engineering", salary=75000.0)
        
        # Test access to properties
        self.assertEqual(emp.id, 1)
        self.assertEqual(emp.name, "John")
        self.assertEqual(emp.department, "Engineering")
        self.assertEqual(emp.salary, 75000.0)
        
        # Test that the schema is attached to the class
        self.assertTrue(hasattr(DynamicEmployee, '__table_schema__'))
        self.assertEqual(DynamicEmployee.__table_schema__, schema)
        
        # Test the validate_column method
        self.assertTrue(DynamicEmployee.validate_column("id"))
        self.assertTrue(DynamicEmployee.validate_column("name"))
        self.assertFalse(DynamicEmployee.validate_column("invalid_column"))


class TestDataframeWithTypedProperties(unittest.TestCase):
    """Test cases for using a DataFrame with typed properties."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.schema = TableSchema(
            name="Employee",
            columns={
                "id": int,
                "name": str,
                "department": str,
                "salary": float,
                "is_manager": bool,
                "manager_id": Optional[int]
            }
        )
        
        # Create a DataFrame with typed properties
        self.df = DataFrame.from_table_schema("employees", self.schema)
    
    def test_filter_with_typed_properties(self):
        """Test filtering with typed properties."""
        # Test filter with lambda using typed properties
        filtered_df = self.df.filter(lambda x: x.salary > 50000)
        
        # Check the SQL generation
        sql = filtered_df.to_sql(dialect="duckdb")
        expected_sql = "SELECT *\nFROM employees\nWHERE salary > 50000"
        self.assertEqual(sql.strip(), expected_sql.strip())
        
        # Test filter with multiple conditions
        filtered_df = self.df.filter(lambda x: (x.salary > 50000) and (x.department == "Engineering"))
        
        # Check the SQL generation
        sql = filtered_df.to_sql(dialect="duckdb")
        expected_sql = "SELECT *\nFROM employees\nWHERE salary > 50000 AND department = 'Engineering'"
        self.assertEqual(sql.strip(), expected_sql.strip())
    
    def test_select_with_typed_properties(self):
        """Test selecting with typed properties."""
        # Test select with lambda using typed properties
        selected_df = self.df.select(
            lambda x: x.name,
            lambda x: x.department,
            lambda x: x.salary
        )
        
        # Check the SQL generation
        sql = selected_df.to_sql(dialect="duckdb")
        expected_sql = "SELECT name, department, salary\nFROM employees"
        self.assertEqual(sql.strip(), expected_sql.strip())
    
    def test_group_by_with_typed_properties(self):
        """Test grouping with typed properties."""
        # Test group_by with lambda using typed properties
        grouped_df = self.df.group_by(lambda x: x.department).select(
            lambda x: x.department,
            as_column(avg(lambda x: x.salary), "avg_salary")
        )
        
        # Check the SQL generation
        sql = grouped_df.to_sql(dialect="duckdb")
        expected_sql = "SELECT department, AVG(salary) AS avg_salary\nFROM employees\nGROUP BY department"
        self.assertEqual(sql.strip(), expected_sql.strip())
    
    def test_order_by_with_typed_properties(self):
        """Test ordering with typed properties."""
        # Test order_by with lambda using typed properties
        ordered_df = self.df.order_by(lambda x: x.salary, desc=True)
        
        # Check the SQL generation
        sql = ordered_df.to_sql(dialect="duckdb")
        expected_sql = "SELECT *\nFROM employees\nORDER BY salary DESC"
        self.assertEqual(sql.strip(), expected_sql.strip())
        
        # Test order_by with multiple columns
        ordered_df = self.df.order_by(
            lambda x: x.department,
            lambda x: x.salary, 
            desc=True
        )
        
        # Check the SQL generation
        sql = ordered_df.to_sql(dialect="duckdb")
        expected_sql = "SELECT *\nFROM employees\nORDER BY salary DESC, department DESC, salary DESC"
        self.assertEqual(sql.strip(), expected_sql.strip())
    
    def test_get_table_class(self):
        """Test getting the table class from a DataFrame."""
        # Test get_table_class method
        table_class = self.df.get_table_class()
        
        # Check that the table class exists
        self.assertIsNotNone(table_class)
        
        # Check that the table class has the correct fields
        self.assertTrue(hasattr(table_class, '__annotations__'))
        annotations = table_class.__annotations__
        
        self.assertEqual(annotations.get('id'), int)
        self.assertEqual(annotations.get('name'), str)
        self.assertEqual(annotations.get('department'), str)
        self.assertEqual(annotations.get('salary'), float)
        self.assertEqual(annotations.get('is_manager'), bool)


if __name__ == "__main__":
    unittest.main()
