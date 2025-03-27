"""
Unit tests for conditional expressions in lambda parser.
"""
import unittest
from typing import Optional

from cloud_dataframe.core.dataframe import DataFrame
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.type_system.column import sum, avg


class TestConditionalExpressions(unittest.TestCase):
    """Test cases for conditional expressions in lambda parser."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.schema = TableSchema(
            name="Employee",
            columns={
                "id": int,
                "name": str,
                "department": str,
                "salary": float,
                "bonus": float,
                "is_manager": bool,
                "age": int
            }
        )
        
        self.df = DataFrame.from_table_schema("employees", self.schema, alias="x")
    
    def test_simple_if_else(self):
        """Test simple if-else conditional expression."""
        query = self.df.select(
            lambda x: x.name,
            lambda x: x.department,
            lambda x: "High" if x.salary > 80000 else "Low"
        )
        
        sql = query.to_sql(dialect="duckdb")
        expected = "SELECT x.name, x.department, CASE WHEN x.salary > 80000 THEN 'High' ELSE 'Low' END\nFROM employees x"
        self.assertEqual(sql.strip(), expected)
    
    def test_nested_if_else(self):
        """Test nested if-else conditional expression."""
        query = self.df.select(
            lambda x: x.name,
            lambda x: "High" if x.salary > 80000 else ("Medium" if x.salary > 50000 else "Low")
        )
        
        sql = query.to_sql(dialect="duckdb")
        expected = "SELECT x.name, CASE WHEN x.salary > 80000 THEN 'High' ELSE CASE WHEN x.salary > 50000 THEN 'Medium' ELSE 'Low' END END\nFROM employees x"
        self.assertEqual(sql.strip(), expected)
    
    def test_if_else_with_column_references(self):
        """Test if-else with column references."""
        query = self.df.select(
            lambda x: x.name,
            lambda x: x.bonus if x.is_manager else x.salary
        )
        
        sql = query.to_sql(dialect="duckdb")
        expected = "SELECT x.name, CASE WHEN x.is_manager THEN x.bonus ELSE x.salary END\nFROM employees x"
        self.assertEqual(sql.strip(), expected)
    
    def test_if_else_with_expressions(self):
        """Test if-else with expressions."""
        query = self.df.select(
            lambda x: x.name,
            lambda x: (x.salary * 1.1) if x.department == "Engineering" else (x.salary * 1.05)
        )
        
        sql = query.to_sql(dialect="duckdb")
        expected = "SELECT x.name, CASE WHEN x.department = 'Engineering' THEN (x.salary * 1.1) ELSE (x.salary * 1.05) END\nFROM employees x"
        self.assertEqual(sql.strip(), expected)


if __name__ == "__main__":
    unittest.main()
