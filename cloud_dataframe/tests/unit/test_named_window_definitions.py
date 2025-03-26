"""
Unit tests for named window definitions.

This module contains tests for defining and using named windows in the DataFrame DSL.
"""
import unittest
from typing import Optional

from cloud_dataframe.core.dataframe import DataFrame
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.type_system.column import (
    as_column, col, over, row_number, rank, dense_rank
)


class TestNamedWindowDefinitions(unittest.TestCase):
    """Test cases for named window definitions."""
    
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
        
        self.df = DataFrame.from_table_schema("employees", self.schema)
    
    def test_named_window_definition(self):
        """Test named window definitions."""
        df_with_window = self.df.window(
            "dept_window",
            partition_by=lambda x: x.department,
            order_by=lambda x: x.salary
        )
        
        df_with_ranks = df_with_window.select(
            lambda x: x.id,
            lambda x: x.name,
            lambda x: x.department,
            lambda x: x.salary,
            as_column(
                over(row_number(), window_name="dept_window"),
                "row_num"
            ),
            as_column(
                over(rank(), window_name="dept_window"),
                "rank"
            ),
            as_column(
                over(dense_rank(), window_name="dept_window"),
                "dense_rank"
            )
        )
        
        sql = df_with_ranks.to_sql(dialect="duckdb")
        expected_sql = """SELECT id, name, department, salary, ROW_NUMBER() OVER dept_window AS row_num, RANK() OVER dept_window AS rank, DENSE_RANK() OVER dept_window AS dense_rank
FROM employees
WINDOW dept_window AS (PARTITION BY department ORDER BY salary ASC)"""
        self.assertEqual(sql.strip(), expected_sql.strip())
    
    def test_standalone_window_definition(self):
        """Test standalone window definitions without aggregate functions."""
        df_with_window = self.df.window(
            "dept_window",
            partition_by=lambda x: x.department,
            order_by=lambda x: x.salary
        )
        
        df_with_window_ref = df_with_window.select(
            lambda x: x.id,
            lambda x: x.name,
            lambda x: x.department,
            lambda x: x.salary,
            as_column(
                over(window_name="dept_window"),
                "window_ref"
            )
        )
        
        sql = df_with_window_ref.to_sql(dialect="duckdb")
        expected_sql = """SELECT id, name, department, salary, WINDOW_REF() OVER dept_window AS window_ref
FROM employees
WINDOW dept_window AS (PARTITION BY department ORDER BY salary ASC)"""
        self.assertEqual(sql.strip(), expected_sql.strip())
    
    def test_multiple_window_definitions(self):
        """Test multiple named window definitions."""
        df_with_windows = self.df.window(
            "dept_window",
            partition_by=lambda x: x.department,
            order_by=lambda x: x.salary
        ).window(
            "location_window",
            partition_by=lambda x: x.location,
            order_by=lambda x: x.salary
        )
        
        df_with_ranks = df_with_windows.select(
            lambda x: x.id,
            lambda x: x.name,
            lambda x: x.department,
            lambda x: x.location,
            lambda x: x.salary,
            as_column(
                over(row_number(), window_name="dept_window"),
                "dept_rank"
            ),
            as_column(
                over(row_number(), window_name="location_window"),
                "location_rank"
            )
        )
        
        sql = df_with_ranks.to_sql(dialect="duckdb")
        expected_sql = """SELECT id, name, department, location, salary, ROW_NUMBER() OVER dept_window AS row_num, ROW_NUMBER() OVER location_window AS location_rank
FROM employees
WINDOW dept_window AS (PARTITION BY department ORDER BY salary ASC), location_window AS (PARTITION BY location ORDER BY salary ASC)"""
        self.assertEqual(sql.strip(), expected_sql.strip())
    
    def test_standalone_over_clause(self):
        """Test standalone OVER clause without aggregate function."""
        df_with_window = self.df.select(
            lambda x: x.id,
            lambda x: x.name,
            lambda x: x.department,
            lambda x: x.salary,
            as_column(
                over(
                    partition_by=lambda x: x.department,
                    order_by=lambda x: x.salary
                ),
                "window_spec"
            )
        )
        
        sql = df_with_window.to_sql(dialect="duckdb")
        expected_sql = """SELECT id, name, department, salary, WINDOW_DEF() OVER (PARTITION BY department ORDER BY salary ASC) AS window_spec
FROM employees"""
        self.assertEqual(sql.strip(), expected_sql.strip())


if __name__ == "__main__":
    unittest.main()
