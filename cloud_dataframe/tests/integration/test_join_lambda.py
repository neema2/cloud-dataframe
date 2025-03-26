"""
Integration tests for lambda-based join operations.

This module contains tests for joining DataFrames using lambda expressions.
"""
import unittest
from dataclasses import dataclass
from typing import Optional

from cloud_dataframe.core.dataframe import DataFrame
from cloud_dataframe.type_system.column import col, literal, as_column
from cloud_dataframe.type_system.decorators import dataclass_to_schema


@dataclass
@dataclass_to_schema()
class Employee:
    """Employee dataclass for testing join operations."""
    id: int
    name: str
    department_id: int
    salary: float


@dataclass
@dataclass_to_schema()
class Department:
    """Department dataclass for testing join operations."""
    id: int
    name: str
    location: str


class TestJoinWithLambda(unittest.TestCase):
    """Test cases for lambda-based join operations."""
    
    def test_simple_join(self):
        """Test a simple join with lambda."""
        employees = DataFrame.from_("employees", alias="e")
        departments = DataFrame.from_("departments", alias="d")
        
        joined_df = employees.join(
            departments,
            lambda e, d: e.department_id == d.id
        )
        
        sql = joined_df.to_sql(dialect="duckdb")
        expected_sql = "SELECT *\nFROM employees e INNER JOIN departments d ON e.department_id = d.id"
        self.assertEqual(sql.strip(), expected_sql)
    
    def test_left_join(self):
        """Test a LEFT JOIN with lambda."""
        employees = DataFrame.from_("employees", alias="e")
        departments = DataFrame.from_("departments", alias="d")
        
        joined_df = employees.left_join(
            departments,
            lambda e, d: e.department_id == d.id
        )
        
        sql = joined_df.to_sql(dialect="duckdb")
        expected_sql = "SELECT *\nFROM employees e LEFT JOIN departments d ON e.department_id = d.id"
        self.assertEqual(sql.strip(), expected_sql)
    
    def test_right_join(self):
        """Test a RIGHT JOIN with lambda."""
        employees = DataFrame.from_("employees", alias="e")
        departments = DataFrame.from_("departments", alias="d")
        
        joined_df = employees.right_join(
            departments,
            lambda e, d: e.department_id == d.id
        )
        
        sql = joined_df.to_sql(dialect="duckdb")
        expected_sql = "SELECT *\nFROM employees e RIGHT JOIN departments d ON e.department_id = d.id"
        self.assertEqual(sql.strip(), expected_sql)
    
    def test_full_join(self):
        """Test a FULL JOIN with lambda."""
        employees = DataFrame.from_("employees", alias="e")
        departments = DataFrame.from_("departments", alias="d")
        
        joined_df = employees.full_join(
            departments,
            lambda e, d: e.department_id == d.id
        )
        
        sql = joined_df.to_sql(dialect="duckdb")
        expected_sql = "SELECT *\nFROM employees e FULL JOIN departments d ON e.department_id = d.id"
        self.assertEqual(sql.strip(), expected_sql)
    
    def test_complex_join_condition(self):
        """Test a join with complex condition."""
        employees = DataFrame.from_("employees", alias="e")
        departments = DataFrame.from_("departments", alias="d")
        
        joined_df = employees.join(
            departments,
            lambda e, d: (e.department_id == d.id) and (e.salary > 50000)
        )
        
        sql = joined_df.to_sql(dialect="duckdb")
        expected_sql = "SELECT *\nFROM employees e INNER JOIN departments d ON e.department_id = d.id AND e.salary > 50000"
        self.assertEqual(sql.strip(), expected_sql)


if __name__ == "__main__":
    unittest.main()
