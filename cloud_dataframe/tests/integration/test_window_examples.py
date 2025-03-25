"""
Integration tests for window function examples with DuckDB.

This module contains tests for window functions using lambda expressions
with the cloud-dataframe library.
"""
import unittest
import pandas as pd
import duckdb
from typing import Optional, Dict

from cloud_dataframe.core.dataframe import DataFrame
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.type_system.column import as_column, sum, avg, count
from cloud_dataframe.type_system.window_functions import rank, dense_rank, row_number


class TestWindowExamples(unittest.TestCase):
    """Test cases for window functions with lambda expressions."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Create a DuckDB connection
        self.conn = duckdb.connect(":memory:")
        
        # Create test data for employees
        employees_data = pd.DataFrame({
            "id": [1, 2, 3, 4, 5, 6, 7, 8],
            "name": ["Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Heidi"],
            "department": ["Engineering", "Engineering", "Sales", "Sales", "Marketing", "Marketing", "HR", "HR"],
            "salary": [80000.0, 90000.0, 70000.0, 75000.0, 65000.0, 60000.0, 55000.0, 58000.0],
            "hire_date": ["2020-01-15", "2019-05-10", "2021-02-20", "2018-11-05", "2022-03-15", "2017-08-22", "2020-07-10", "2019-12-01"],
        })
        
        # Create the employees table in DuckDB
        self.conn.execute("CREATE TABLE employees AS SELECT * FROM employees_data")
        self.conn.register("employees_data", employees_data)
        
        # Create schema for the employees table
        self.schema = TableSchema(
            name="Employee",
            columns={
                "id": int,
                "name": str,
                "department": str,
                "salary": float,
                "hire_date": str,
            }
        )
        
        # Create a DataFrame with typed properties
        self.df = DataFrame.from_table_schema("employees", self.schema)
    
    def tearDown(self):
        """Tear down test fixtures."""
        self.conn.close()
    
    def test_rank_window_function(self):
        """Test rank window function with lambda expressions."""
        # Build query with rank window function
        query = self.df.select(
            lambda x: x.id,
            lambda x: x.name,
            lambda x: x.department,
            lambda x: x.salary,
            as_column(
                rank().over().partition_by(lambda x: x.department).order_by(lambda x: x.salary, desc=True),
                "salary_rank"
            )
        ).order_by(
            lambda x: x.department,
            lambda x: x.salary_rank
        )
        
        # Generate SQL
        sql = query.to_sql(dialect="duckdb")
        
        # Execute query
        result = self.conn.execute(sql).fetchdf()
        
        # Verify result
        self.assertEqual(len(result), 8)  # All employees
        self.assertIn("salary_rank", result.columns)
        
        # Check that ranks are correct within departments
        eng_rows = result[result["department"] == "Engineering"]
        self.assertEqual(eng_rows.iloc[0]["salary_rank"], 1)
        self.assertEqual(eng_rows.iloc[1]["salary_rank"], 2)
    
    def test_row_number_window_function(self):
        """Test row_number window function with lambda expressions."""
        # Build query with row_number window function
        query = self.df.select(
            lambda x: x.id,
            lambda x: x.name,
            lambda x: x.department,
            lambda x: x.salary,
            as_column(
                row_number().over().partition_by(lambda x: x.department).order_by(lambda x: x.salary, desc=True),
                "row_num"
            )
        ).order_by(
            lambda x: x.department,
            lambda x: x.row_num
        )
        
        # Generate SQL
        sql = query.to_sql(dialect="duckdb")
        
        # Execute query
        result = self.conn.execute(sql).fetchdf()
        
        # Verify result
        self.assertEqual(len(result), 8)  # All employees
        self.assertIn("row_num", result.columns)
    
    def test_window_function_with_filter(self):
        """Test window function with filter."""
        # Build query with window function and filter
        query = self.df.select(
            lambda x: x.id,
            lambda x: x.name,
            lambda x: x.department,
            lambda x: x.salary,
            as_column(
                rank().over().partition_by(lambda x: x.department).order_by(lambda x: x.salary, desc=True),
                "salary_rank"
            )
        ).filter(
            lambda x: x.salary_rank == 1
        ).order_by(
            lambda x: x.department
        )
        
        # Generate SQL
        sql = query.to_sql(dialect="duckdb")
        
        # Execute query
        result = self.conn.execute(sql).fetchdf()
        
        # Verify result
        self.assertEqual(len(result), 4)  # One top employee per department
        self.assertIn("salary_rank", result.columns)
        for rank in result["salary_rank"]:
            self.assertEqual(rank, 1)
    
    def test_multiple_window_functions(self):
        """Test multiple window functions in the same query."""
        # Build query with multiple window functions
        query = self.df.select(
            lambda x: x.id,
            lambda x: x.name,
            lambda x: x.department,
            lambda x: x.salary,
            as_column(
                rank().over().partition_by(lambda x: x.department).order_by(lambda x: x.salary, desc=True),
                "salary_rank"
            ),
            as_column(
                dense_rank().over().partition_by(lambda x: x.department).order_by(lambda x: x.salary, desc=True),
                "dense_rank"
            ),
            as_column(
                row_number().over().partition_by(lambda x: x.department).order_by(lambda x: x.salary, desc=True),
                "row_num"
            )
        ).order_by(
            lambda x: x.department,
            lambda x: x.salary_rank
        )
        
        # Generate SQL
        sql = query.to_sql(dialect="duckdb")
        
        # Execute query
        result = self.conn.execute(sql).fetchdf()
        
        # Verify result
        self.assertEqual(len(result), 8)  # All employees
        self.assertIn("salary_rank", result.columns)
        self.assertIn("dense_rank", result.columns)
        self.assertIn("row_num", result.columns)


if __name__ == "__main__":
    unittest.main()
