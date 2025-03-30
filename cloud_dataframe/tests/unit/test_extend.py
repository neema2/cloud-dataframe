"""
Unit tests for the extend() function.

This module contains tests to ensure that the extend() function correctly
adds columns to a DataFrame while preserving existing columns.
"""
import unittest
from dataclasses import dataclass
from typing import Optional

from cloud_dataframe.core.dataframe import DataFrame
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.type_system.column import col, sum, avg, count
from cloud_dataframe.type_system.decorators import dataclass_to_schema


@dataclass
@dataclass_to_schema()
class Employee:
    """Employee dataclass for testing type-safe operations."""
    id: int
    name: str
    department: str
    salary: float
    manager_id: Optional[int] = None


class TestExtendFunction(unittest.TestCase):
    """Test cases for the extend() function."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.schema = TableSchema(
            name="Employee",
            columns={
                "id": int,
                "name": str,
                "department": str,
                "salary": float,
                "manager_id": Optional[int]
            }
        )
        self.df = DataFrame.from_table_schema("employees", self.schema, alias="e")
    
    def test_extend_with_simple_column(self):
        """Test extending with a simple column."""
        df = self.df.select(lambda e: e.id)
        
        extended_df = df.extend(lambda e: (department := e.department))
        
        self.assertEqual(len(extended_df.columns), 2)
        
        sql = extended_df.to_sql(dialect="duckdb")
        expected_sql = "SELECT e.id, e.department AS department\nFROM employees e"
        self.assertEqual(sql.strip(), expected_sql.strip())
    
    def test_extend_with_computed_column(self):
        """Test extending with a computed column."""
        df = self.df.select(lambda e: e.id)
        
        extended_df = df.extend(lambda e: (salary_bonus := e.salary * 1.1))
        
        self.assertEqual(len(extended_df.columns), 2)
        
        sql = extended_df.to_sql(dialect="duckdb")
        expected_sql = "SELECT e.id, (e.salary * 1.1) AS salary_bonus\nFROM employees e"
        self.assertEqual(sql.strip(), expected_sql.strip())
    
    def test_extend_with_multiple_columns(self):
        """Test extending with multiple columns at once."""
        df = self.df.select(lambda e: e.id)
        
        extended_df = df.extend(
            lambda e: (name := e.name),
            lambda e: (department := e.department)
        )
        
        self.assertEqual(len(extended_df.columns), 3)
        
        sql = extended_df.to_sql(dialect="duckdb")
        expected_sql = "SELECT e.id, e.name AS name, e.department AS department\nFROM employees e"
        self.assertEqual(sql.strip(), expected_sql.strip())
    
    def test_extend_with_array_lambda(self):
        """Test extending with an array lambda."""
        df = self.df.select(lambda e: e.id)
        
        extended_df = df.extend(lambda e: [
            (name := e.name),
            (department := e.department)
        ])
        
        self.assertEqual(len(extended_df.columns), 3)
        
        sql = extended_df.to_sql(dialect="duckdb")
        expected_sql = "SELECT e.id, e.name AS name, e.department AS department\nFROM employees e"
        self.assertEqual(sql.strip(), expected_sql.strip())
    
    def test_extend_with_join(self):
        """Test extending with a joined table."""
        dept_schema = TableSchema(
            name="Department",
            columns={
                "id": int,
                "name": str,
                "budget": float
            }
        )
        df_dept = DataFrame.from_table_schema("departments", dept_schema, alias="d")
        
        joined_df = self.df.join(
            df_dept,
            lambda e, d: e.department == d.name
        ).select(
            lambda e, d: e.id,
            lambda e, d: e.name,
            lambda e, d: d.budget
        )
        
        extended_df = joined_df.extend(
            lambda e, d: (budget_per_employee := d.budget / e.salary)
        )
        
        self.assertEqual(len(extended_df.columns), 4)
        
        sql = extended_df.to_sql(dialect="duckdb")
        expected_sql = "SELECT e.id, e.name, d.budget, (d.budget / e.salary) AS budget_per_employee\nFROM employees e INNER JOIN departments d ON e.department = d.name"
        self.assertEqual(sql.strip(), expected_sql.strip())
    
    def test_extend_with_sort_tuple(self):
        """Test extending with a tuple containing Sort enum."""
        from cloud_dataframe.core.dataframe import Sort
        
        df = self.df.select(lambda e: e.id)
        
        extended_df = df.extend(lambda e: (salary := (e.salary, Sort.DESC)))
        
        self.assertEqual(len(extended_df.columns), 2)
        
        sql = extended_df.to_sql(dialect="duckdb")
        expected_sql = "SELECT e.id, e.salary AS salary\nFROM employees e"
        self.assertEqual(sql.strip(), expected_sql.strip())
    
    def test_extend_with_array_mixed_formats(self):
        """Test extending with an array containing mixed formats."""
        from cloud_dataframe.core.dataframe import Sort
        
        df = self.df.select(lambda e: e.id)
        
        extended_df = df.extend(lambda e: [
            (name := e.name),
            (salary := (e.salary, Sort.DESC)),
            e.department
        ])
        
        self.assertEqual(len(extended_df.columns), 4)
        
        sql = extended_df.to_sql(dialect="duckdb")
        expected_sql = "SELECT e.id, e.name AS name, e.salary AS salary, e.department\nFROM employees e"
        self.assertEqual(sql.strip(), expected_sql.strip())


if __name__ == "__main__":
    unittest.main()
