"""
Integration tests for join examples with DuckDB.

This module contains tests for join operations using lambda expressions
with the cloud-dataframe library.
"""
import unittest
import pandas as pd
import duckdb
from typing import Optional, Dict

from cloud_dataframe.core.dataframe import DataFrame
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.type_system.column import as_column, sum, avg, count


class TestJoinExamples(unittest.TestCase):
    """Test cases for join operations with lambda expressions."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Create a DuckDB connection
        self.conn = duckdb.connect(":memory:")
        
        # Create test data for employees
        employees_data = pd.DataFrame({
            "id": [1, 2, 3, 4, 5, 6],
            "name": ["Alice", "Bob", "Charlie", "David", "Eve", "Frank"],
            "department_id": [1, 1, 2, 2, 3, 3],
            "salary": [80000.0, 90000.0, 70000.0, 75000.0, 65000.0, 60000.0],
        })
        
        # Create test data for departments
        departments_data = pd.DataFrame({
            "id": [1, 2, 3, 4],
            "name": ["Engineering", "Sales", "Marketing", "HR"],
            "location": ["New York", "San Francisco", "Chicago", "Boston"],
            "budget": [1000000.0, 800000.0, 600000.0, 400000.0],
        })
        
        # Create the tables in DuckDB
        self.conn.register("employees", employees_data)
        self.conn.register("departments", departments_data)
        
        # Create schemas for the tables
        self.employee_schema = TableSchema(
            name="Employee",
            columns={
                "id": int,
                "name": str,
                "department_id": int,
                "salary": float,
            }
        )
        
        self.department_schema = TableSchema(
            name="Department",
            columns={
                "id": int,
                "name": str,
                "location": str,
                "budget": float,
            }
        )
        
        # Create DataFrames with typed properties
        self.employees_df = DataFrame.from_table_schema("employees", self.employee_schema)
        self.departments_df = DataFrame.from_table_schema("departments", self.department_schema)
    
    def tearDown(self):
        """Tear down test fixtures."""
        self.conn.close()
    
    def test_inner_join(self):
        """Test inner join with lambda expressions."""
        # Build query with inner join
        query = self.employees_df.join(
            self.departments_df,
            lambda e, d: e.department_id == d.id
        ).select(
            lambda x: x.employees.id,
            lambda x: x.employees.name,
            lambda x: x.departments.name.alias("department_name"),
            lambda x: x.departments.location,
            lambda x: x.employees.salary
        )
        
        # Generate SQL
        sql = query.to_sql(dialect="duckdb")
        
        # Expected SQL (may vary based on implementation)
        expected_sql_pattern = "SELECT employees.id, employees.name, departments.name AS department_name, departments.location, employees.salary"
        self.assertIn(expected_sql_pattern, sql.replace("\n", " "))
        
        # Execute query
        result = self.conn.execute(sql).fetchdf()
        
        # Verify result
        self.assertEqual(len(result), 6)  # All employees should match
        self.assertIn("department_name", result.columns)
        self.assertIn("location", result.columns)
    
    def test_left_join(self):
        """Test left join with lambda expressions."""
        # Build query with left join
        query = self.employees_df.left_join(
            self.departments_df,
            lambda e, d: e.department_id == d.id
        ).select(
            lambda x: x.employees.id,
            lambda x: x.employees.name,
            lambda x: x.departments.name.alias("department_name"),
            lambda x: x.departments.location,
            lambda x: x.employees.salary
        )
        
        # Generate SQL
        sql = query.to_sql(dialect="duckdb")
        
        # Expected SQL (may vary based on implementation)
        expected_sql_pattern = "LEFT JOIN"
        self.assertIn(expected_sql_pattern, sql.replace("\n", " "))
        
        # Execute query
        result = self.conn.execute(sql).fetchdf()
        
        # Verify result
        self.assertEqual(len(result), 6)  # All employees should be included
    
    def test_join_with_aggregation(self):
        """Test join with aggregation."""
        # Build query with join and aggregation
        query = self.employees_df.join(
            self.departments_df,
            lambda e, d: e.department_id == d.id
        ).group_by(
            lambda x: x.departments.name
        ).select(
            lambda x: x.departments.name.alias("department_name"),
            as_column(lambda x: count(x.employees.id), "employee_count"),
            as_column(lambda x: sum(x.employees.salary), "total_salary"),
            as_column(lambda x: avg(x.employees.salary), "avg_salary")
        ).order_by(
            lambda x: x.departments.name
        )
        
        # Generate SQL
        sql = query.to_sql(dialect="duckdb")
        
        # Execute query
        result = self.conn.execute(sql).fetchdf()
        
        # Verify result
        self.assertEqual(len(result), 3)  # Three departments with employees
        self.assertIn("department_name", result.columns)
        self.assertIn("employee_count", result.columns)
        self.assertIn("total_salary", result.columns)
        self.assertIn("avg_salary", result.columns)


if __name__ == "__main__":
    unittest.main()
