"""
Unit tests for per-column sort direction in order_by clauses.

This module contains tests for using tuples to specify sort direction
for individual columns in order_by clauses.
"""
import unittest
from typing import Optional

from cloud_dataframe.core.dataframe import DataFrame, Sort, OrderByClause
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.type_system.column import (
    col, row_number, rank, dense_rank, window
)


class TestPerColumnSort(unittest.TestCase):
    """Test cases for per-column sort direction in order_by clauses."""
    
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
    
    def test_order_by_with_per_column_sort_direction(self):
        """Test ordering with per-column sort direction."""
        # Test order_by with per-column sort direction
        ordered_df = self.df.order_by(
            lambda x: [
                (x.department, Sort.DESC),  # Department in descending order
                (x.salary, Sort.ASC),       # Salary in ascending order
                x.name                   # Name in default ascending order
            ]
        )
        
        # Check the SQL generation
        sql = ordered_df.to_sql(dialect="duckdb")
        expected_sql = "SELECT *\nFROM employees x\nORDER BY x.department DESC, x.salary ASC, x.name ASC"
        self.assertEqual(sql.strip(), expected_sql.strip())
    
    def test_mixed_sort_direction_specifications(self):
        """Test mix of tuple and non-tuple specifications."""
        # Test mix of tuple and non-tuple specifications
        ordered_df = self.df.order_by(
            lambda x: [(x.department, Sort.DESC)],  # Department in descending order
            lambda x: x.salary,                  # Salary in default order
            desc=True                            # Default direction is DESC for non-tuple columns
        )
        
        # Check the SQL generation
        sql = ordered_df.to_sql(dialect="duckdb")
        expected_sql = "SELECT *\nFROM employees x\nORDER BY x.department DESC, x.salary DESC"
        self.assertEqual(sql.strip(), expected_sql.strip())
    
    def test_enum_sort_direction(self):
        """Test using Sort enum in tuples."""
        # Test using Sort enum directly
        ordered_df = self.df.order_by(
            lambda x: [
                (x.department, Sort.DESC),  # Department in descending order
                (x.salary, Sort.ASC)        # Salary in ascending order
            ]
        )
        
        # Check the SQL generation
        # Note: The SQL generator will convert Sort enum to string values
        sql = ordered_df.to_sql(dialect="duckdb")
        expected_sql = "SELECT *\nFROM employees x\nORDER BY x.department DESC, x.salary ASC"
        self.assertEqual(sql.strip(), expected_sql.strip())
    
    def test_window_function_with_per_column_sort(self):
        """Test window functions with per-column sort order."""
        # Test window function with per-column sort order
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
    
    def test_multiple_window_functions_with_per_column_sort(self):
        """Test multiple window functions with per-column sort order."""
        # Test multiple window functions with different sort orders
        df_with_ranks = self.df.select(
            lambda x: x.id,
            lambda x: x.name,
            lambda x: x.department,
            lambda x: x.salary,
            lambda x: (row_num := window(func=row_number(), partition=x.department, order_by=x.salary)),
            lambda x: (rank_val := window(func=rank(), partition=[x.department, x.location], order_by=[x.salary, x.id]))
        )
        
        # Check the SQL generation
        sql = df_with_ranks.to_sql(dialect="duckdb")
        expected_sql = "SELECT x.id, x.name, x.department, x.salary, ROW_NUMBER() OVER (PARTITION BY x.department ORDER BY x.salary ASC) AS row_num, RANK() OVER (PARTITION BY x.department, x.location ORDER BY x.salary ASC, x.id ASC) AS rank_val\nFROM employees x"
        self.assertEqual(sql.strip(), expected_sql.strip())


if __name__ == "__main__":
    unittest.main()
