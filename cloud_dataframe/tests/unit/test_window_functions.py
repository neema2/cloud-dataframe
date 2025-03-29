"""
Unit tests for window functions with lambda-based column references.

This module contains tests for using lambda functions in window function
partition_by and order_by parameters.
"""
import unittest
from typing import Optional

from cloud_dataframe.core.dataframe import DataFrame
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.type_system.column import (
    col, over, row_number, rank, dense_rank, window
)


class TestWindowFunctions(unittest.TestCase):
    """Test cases for window functions with lambda-based column references."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.schema = TableSchema(
            name="Employee",
            columns={
                "id": int,
                "name": str,
                "department": str,
                "location": str,
                "salary": float,
                "is_manager": bool,
                "manager_id": Optional[int]
            }
        )
        
        # Create a DataFrame with typed properties
        self.df = DataFrame.from_table_schema("employees", self.schema)
    
    def test_window_with_lambda_partition_by(self):
        """Test window functions with lambda-based partition_by."""
        # Test window function with lambda-based partition_by
        df_with_rank = self.df.select(
            lambda x: x.id,
            lambda x: x.name,
            lambda x: x.department,
            lambda x: x.salary,
            lambda x: (salary_rank := window(func=row_number(), partition=x.department, order_by=x.salary))
        )
        
        # Check the SQL generation
        sql = df_with_rank.to_sql(dialect="duckdb")
        expected_sql = "SELECT x.id, x.name, x.department, x.salary, ROW_NUMBER() OVER (PARTITION BY x.department ORDER BY x.salary ASC) AS salary_rank\nFROM employees x"
        self.assertEqual(sql.strip(), expected_sql.strip())
    
    def test_window_with_array_lambda_partition_by(self):
        """Test window functions with array lambda-based partition_by."""
        # Test window function with array lambda-based partition_by
        df_with_rank = self.df.select(
            lambda x: x.id,
            lambda x: x.name,
            lambda x: x.department,
            lambda x: x.location,
            lambda x: x.salary,
            lambda x: (salary_rank := window(func=row_number(), partition=[x.department, x.location], order_by=x.salary))
        )
        
        # Check the SQL generation
        sql = df_with_rank.to_sql(dialect="duckdb")
        expected_sql = "SELECT x.id, x.name, x.department, x.location, x.salary, ROW_NUMBER() OVER (PARTITION BY x.department, x.location ORDER BY x.salary ASC) AS salary_rank\nFROM employees x"
        self.assertEqual(sql.strip(), expected_sql.strip())
    
    def test_window_with_lambda_order_by(self):
        """Test window functions with lambda-based order_by."""
        # Test window function with lambda-based order_by
        df_with_rank = self.df.select(
            lambda x: x.id,
            lambda x: x.name,
            lambda x: x.department,
            lambda x: x.salary,
            lambda x: (salary_rank := window(func=rank(), partition=x.department, order_by=x.salary))
        )
        
        # Check the SQL generation
        sql = df_with_rank.to_sql(dialect="duckdb")
        expected_sql = "SELECT x.id, x.name, x.department, x.salary, RANK() OVER (PARTITION BY x.department ORDER BY x.salary ASC) AS salary_rank\nFROM employees x"
        self.assertEqual(sql.strip(), expected_sql.strip())
    
    def test_window_with_array_lambda_order_by(self):
        """Test window functions with array lambda-based order_by."""
        # Test window function with array lambda-based order_by
        df_with_rank = self.df.select(
            lambda x: x.id,
            lambda x: x.name,
            lambda x: x.department,
            lambda x: x.salary,
            lambda x: (salary_rank := window(func=dense_rank(), partition=x.department, order_by=[x.salary, x.id]))
        )
        
        # Check the SQL generation
        sql = df_with_rank.to_sql(dialect="duckdb")
        expected_sql = "SELECT x.id, x.name, x.department, x.salary, DENSE_RANK() OVER (PARTITION BY x.department ORDER BY x.salary ASC, x.id ASC) AS salary_rank\nFROM employees x"
        self.assertEqual(sql.strip(), expected_sql.strip())
    
    def test_multiple_window_functions(self):
        """Test multiple window functions with lambda-based parameters."""
        # Test multiple window functions
        df_with_ranks = self.df.select(
            lambda x: x.id,
            lambda x: x.name,
            lambda x: x.department,
            lambda x: x.salary,
            lambda x: (row_num := window(func=row_number(), partition=x.department, order_by=x.salary)),
            lambda x: (rank := window(func=rank(), partition=x.department, order_by=x.salary)),
            lambda x: (dense_rank := window(func=dense_rank(), partition=x.department, order_by=x.salary))
        )
        
        # Check the SQL generation
        sql = df_with_ranks.to_sql(dialect="duckdb")
        expected_sql = "SELECT x.id, x.name, x.department, x.salary, ROW_NUMBER() OVER (PARTITION BY x.department ORDER BY x.salary ASC) AS row_num, RANK() OVER (PARTITION BY x.department ORDER BY x.salary ASC) AS rank, DENSE_RANK() OVER (PARTITION BY x.department ORDER BY x.salary ASC) AS dense_rank\nFROM employees x"
        self.assertEqual(sql.strip(), expected_sql.strip())


if __name__ == "__main__":
    unittest.main()
