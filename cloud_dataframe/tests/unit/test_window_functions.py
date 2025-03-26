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
    as_column, col, over, row_number, rank, dense_rank
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
            as_column(
                over(
                    row_number(),
                    partition_by=lambda x: x.department,
                    order_by=lambda x: x.salary
                ),
                "salary_rank"
            )
        )
        
        # Check the SQL generation
        sql = df_with_rank.to_sql(dialect="duckdb")
        expected_sql = "SELECT id, name, department, salary, ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary ASC) AS row_num\nFROM employees"
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
            as_column(
                over(
                    row_number(),
                    partition_by=lambda x: [x.department, x.location],
                    order_by=lambda x: x.salary
                ),
                "salary_rank"
            )
        )
        
        # Check the SQL generation
        sql = df_with_rank.to_sql(dialect="duckdb")
        expected_sql = "SELECT id, name, department, location, salary, ROW_NUMBER() OVER (PARTITION BY department, location ORDER BY salary ASC) AS row_num\nFROM employees"
        self.assertEqual(sql.strip(), expected_sql.strip())
    
    def test_window_with_lambda_order_by(self):
        """Test window functions with lambda-based order_by."""
        # Test window function with lambda-based order_by
        df_with_rank = self.df.select(
            lambda x: x.id,
            lambda x: x.name,
            lambda x: x.department,
            lambda x: x.salary,
            as_column(
                over(
                    rank(),
                    partition_by=lambda x: x.department,
                    order_by=lambda x: x.salary
                ),
                "salary_rank"
            )
        )
        
        # Check the SQL generation
        sql = df_with_rank.to_sql(dialect="duckdb")
        expected_sql = "SELECT id, name, department, salary, RANK() OVER (PARTITION BY department ORDER BY salary ASC) AS rank\nFROM employees"
        self.assertEqual(sql.strip(), expected_sql.strip())
    
    def test_window_with_array_lambda_order_by(self):
        """Test window functions with array lambda-based order_by."""
        # Test window function with array lambda-based order_by
        df_with_rank = self.df.select(
            lambda x: x.id,
            lambda x: x.name,
            lambda x: x.department,
            lambda x: x.salary,
            as_column(
                over(
                    dense_rank(),
                    partition_by=lambda x: x.department,
                    order_by=lambda x: [x.salary, x.id]
                ),
                "salary_rank"
            )
        )
        
        # Check the SQL generation
        sql = df_with_rank.to_sql(dialect="duckdb")
        expected_sql = "SELECT id, name, department, salary, DENSE_RANK() OVER (PARTITION BY department ORDER BY salary ASC, id ASC) AS dense_rank\nFROM employees"
        self.assertEqual(sql.strip(), expected_sql.strip())
    
    def test_multiple_window_functions(self):
        """Test multiple window functions with lambda-based parameters."""
        # Test multiple window functions
        df_with_ranks = self.df.select(
            lambda x: x.id,
            lambda x: x.name,
            lambda x: x.department,
            lambda x: x.salary,
            as_column(
                over(
                    row_number(),
                    partition_by=lambda x: x.department,
                    order_by=lambda x: x.salary
                ),
                "row_num"
            ),
            as_column(
                over(
                    rank(),
                    partition_by=lambda x: x.department,
                    order_by=lambda x: x.salary
                ),
                "rank"
            ),
            as_column(
                over(
                    dense_rank(),
                    partition_by=lambda x: x.department,
                    order_by=lambda x: x.salary
                ),
                "dense_rank"
            )
        )
        
        # Check the SQL generation
        sql = df_with_ranks.to_sql(dialect="duckdb")
        expected_sql = "SELECT id, name, department, salary, ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary ASC) AS row_num, RANK() OVER (PARTITION BY department ORDER BY salary ASC) AS rank, DENSE_RANK() OVER (PARTITION BY department ORDER BY salary ASC) AS dense_rank\nFROM employees"
        self.assertEqual(sql.strip(), expected_sql.strip())


if __name__ == "__main__":
    unittest.main()
