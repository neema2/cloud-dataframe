"""
Unit tests for window functions with frame specifications.

This module contains tests for using frame specifications with window functions.
"""
import unittest
from typing import Optional

from cloud_dataframe.core.dataframe import DataFrame
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.type_system.column import (
    col, row_number, rank, dense_rank,
    row, range, unbounded, window
)


class TestWindowFrames(unittest.TestCase):
    """Test cases for window functions with frame specifications."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.schema = TableSchema(
            name="Employee",
            columns={
                "id": int,
                "name": str,
                "department": str,
                "salary": float,
            }
        )
        
        # Create a DataFrame with typed properties
        self.df = DataFrame.from_table_schema("employees", self.schema)
    
    def test_row_frame_current_preceding(self):
        """Test ROWS frame with current row to preceding rows."""
        df_with_frame = self.df.select(
            lambda x: x.id,
            lambda x: x.salary,
            lambda x: (row_num := window(func=row_number(), partition=x.department, order_by=x.salary, frame=row(2, 0)))  # 2 preceding to current row
        )
        
        # Check the SQL generation
        sql = df_with_frame.to_sql(dialect="duckdb")
        expected_sql = "SELECT x.id, x.salary, ROW_NUMBER() OVER (PARTITION BY x.department ORDER BY x.salary ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS row_num\nFROM employees AS x"
        self.assertEqual(sql.strip(), expected_sql.strip())
    
    def test_row_frame_preceding_following(self):
        """Test ROWS frame with preceding to following rows."""
        df_with_frame = self.df.select(
            lambda x: x.id,
            lambda x: x.salary,
            lambda x: (rank_val := window(func=rank(), partition=x.department, order_by=x.salary, frame=row(2, 2)))  # 2 preceding to 2 following
        )
        
        # Check the SQL generation
        sql = df_with_frame.to_sql(dialect="duckdb")
        expected_sql = "SELECT x.id, x.salary, RANK() OVER (PARTITION BY x.department ORDER BY x.salary ASC ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS rank_val\nFROM employees AS x"
        self.assertEqual(sql.strip(), expected_sql.strip())
    
    def test_row_frame_current_following(self):
        """Test ROWS frame with current row to following rows."""
        df_with_frame = self.df.select(
            lambda x: x.id,
            lambda x: x.salary,
            lambda x: (dense_rank_val := window(func=dense_rank(), partition=x.department, order_by=x.salary, frame=row(0, 2)))  # current row to 2 following
        )
        
        # Check the SQL generation
        sql = df_with_frame.to_sql(dialect="duckdb")
        expected_sql = "SELECT x.id, x.salary, DENSE_RANK() OVER (PARTITION BY x.department ORDER BY x.salary ASC ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING) AS dense_rank_val\nFROM employees AS x"
        self.assertEqual(sql.strip(), expected_sql.strip())
    
    def test_row_frame_unbounded_preceding(self):
        """Test ROWS frame with unbounded preceding to current row."""
        df_with_frame = self.df.select(
            lambda x: x.id,
            lambda x: x.salary,
            lambda x: (row_num := window(func=row_number(), partition=x.department, order_by=x.salary, frame=row(unbounded(), 0)))  # unbounded preceding to current row
        )
        
        # Check the SQL generation
        sql = df_with_frame.to_sql(dialect="duckdb")
        expected_sql = "SELECT x.id, x.salary, ROW_NUMBER() OVER (PARTITION BY x.department ORDER BY x.salary ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS row_num\nFROM employees AS x"
        self.assertEqual(sql.strip(), expected_sql.strip())
    
    def test_row_frame_preceding_unbounded(self):
        """Test ROWS frame with preceding to unbounded following."""
        df_with_frame = self.df.select(
            lambda x: x.id,
            lambda x: x.salary,
            lambda x: (rank_val := window(func=rank(), partition=x.department, order_by=x.salary, frame=row(2, unbounded())))  # 2 preceding to unbounded following
        )
        
        # Check the SQL generation
        sql = df_with_frame.to_sql(dialect="duckdb")
        expected_sql = "SELECT x.id, x.salary, RANK() OVER (PARTITION BY x.department ORDER BY x.salary ASC ROWS BETWEEN 2 PRECEDING AND UNBOUNDED FOLLOWING) AS rank_val\nFROM employees AS x"
        self.assertEqual(sql.strip(), expected_sql.strip())
    
    def test_range_frame(self):
        """Test RANGE frame specification."""
        df_with_frame = self.df.select(
            lambda x: x.id,
            lambda x: x.salary,
            lambda x: (dense_rank_val := window(func=dense_rank(), partition=x.department, order_by=x.salary, frame=range(unbounded(), 0)))  # unbounded preceding to current row
        )
        
        # Check the SQL generation
        sql = df_with_frame.to_sql(dialect="duckdb")
        expected_sql = "SELECT x.id, x.salary, DENSE_RANK() OVER (PARTITION BY x.department ORDER BY x.salary ASC RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS dense_rank_val\nFROM employees AS x"
        self.assertEqual(sql.strip(), expected_sql.strip())

    def test_lambda_function_window(self):
        """Test lambda function expression in window()."""
        from cloud_dataframe.type_system.column import sum
        
        df_with_lambda = self.df.select(
            lambda x: x.id,
            lambda x: x.salary,
            lambda x: (sum_salary := window(func=sum(x.salary), partition=x.department, frame=row(unbounded(), 0)))
        )
        
        # Check the SQL generation
        sql = df_with_lambda.to_sql(dialect="duckdb")
        expected_sql = "SELECT x.id, x.salary, SUM(x.salary) OVER (PARTITION BY x.department ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS sum_salary\nFROM employees AS x"
        self.assertEqual(sql.strip(), expected_sql.strip())


if __name__ == "__main__":
    unittest.main()
