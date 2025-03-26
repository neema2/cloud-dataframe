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
    as_column, col, over, row_number, rank, dense_rank
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
        expected_sql = "SELECT *\nFROM employees\nORDER BY department DESC, salary ASC, name ASC"
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
        expected_sql = "SELECT *\nFROM employees\nORDER BY department DESC, salary DESC"
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
        expected_sql = "SELECT *\nFROM employees\nORDER BY department DESC, salary ASC"
        self.assertEqual(sql.strip(), expected_sql.strip())
    
    def test_window_function_with_per_column_sort(self):
        """Test window functions with per-column sort order."""
        # Test window function with per-column sort order
        df_with_rank = self.df.select(
            lambda x: x.id,
            lambda x: x.name,
            lambda x: x.department,
            lambda x: x.salary,
            as_column(
                over(
                    dense_rank(),
                    partition_by=lambda x: x.department,
                    order_by=lambda x: [
                        (x.salary, Sort.DESC),  # Salary in descending order
                        (x.id, Sort.ASC)        # ID in ascending order
                    ]
                ),
                "salary_rank"
            )
        )
        
        # Check the SQL generation
        sql = df_with_rank.to_sql(dialect="duckdb")
        expected_sql = "SELECT id, name, department, salary, DENSE_RANK() OVER (PARTITION BY department ORDER BY salary ASC, id ASC) AS dense_rank\nFROM employees"
        self.assertEqual(sql.strip(), expected_sql.strip())
    
    def test_multiple_window_functions_with_per_column_sort(self):
        """Test multiple window functions with per-column sort order."""
        # Test multiple window functions with different sort orders
        df_with_ranks = self.df.select(
            lambda x: x.id,
            lambda x: x.name,
            lambda x: x.department,
            lambda x: x.salary,
            as_column(
                over(
                    row_number(),
                    partition_by=lambda x: x.department,
                    order_by=lambda x: [(x.salary, Sort.DESC)]
                ),
                "row_num"
            ),
            as_column(
                over(
                    rank(),
                    partition_by=lambda x: [x.department, x.location],
                    order_by=lambda x: [(x.salary, Sort.ASC), (x.id, Sort.DESC)]
                ),
                "rank"
            )
        )
        
        # Check the SQL generation
        sql = df_with_ranks.to_sql(dialect="duckdb")
        expected_sql = "SELECT id, name, department, salary, ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary ASC) AS row_num, RANK() OVER (PARTITION BY department, location ORDER BY salary ASC, id ASC) AS rank\nFROM employees"
        self.assertEqual(sql.strip(), expected_sql.strip())


if __name__ == "__main__":
    unittest.main()
