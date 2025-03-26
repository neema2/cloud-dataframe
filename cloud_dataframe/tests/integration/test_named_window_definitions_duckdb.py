"""
Integration tests for named window definitions using DuckDB.

This module contains tests for defining and using named windows in the DataFrame DSL
with DuckDB as the backend.
"""
import unittest
import duckdb
from typing import Optional

from cloud_dataframe.core.dataframe import DataFrame
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.type_system.column import (
    as_column, col, over, row_number, rank, dense_rank, sum, avg
)


class TestNamedWindowDefinitionsDuckDB(unittest.TestCase):
    """Test cases for named window definitions using DuckDB."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.conn = duckdb.connect(":memory:")
        
        self.conn.execute("""
            CREATE TABLE employees (
                id INTEGER,
                name VARCHAR,
                department VARCHAR,
                location VARCHAR,
                salary FLOAT,
                is_manager BOOLEAN,
                manager_id INTEGER
            )
        """)
        
        self.conn.execute("""
            INSERT INTO employees VALUES
            (1, 'Alice', 'Engineering', 'New York', 120000, true, NULL),
            (2, 'Bob', 'Engineering', 'San Francisco', 110000, false, 1),
            (3, 'Charlie', 'Engineering', 'New York', 95000, false, 1),
            (4, 'David', 'Sales', 'Chicago', 85000, true, NULL),
            (5, 'Eve', 'Sales', 'Chicago', 90000, false, 4),
            (6, 'Frank', 'Marketing', 'New York', 105000, true, NULL),
            (7, 'Grace', 'Marketing', 'San Francisco', 95000, false, 6),
            (8, 'Heidi', 'HR', 'Chicago', 80000, true, NULL)
        """)
        
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
    
    def tearDown(self):
        """Clean up test fixtures."""
        self.conn.close()
    
    def test_named_window_definitions(self):
        """Test named window definitions with DuckDB."""
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
        
        result = self.conn.execute(sql).fetchall()
        
        self.assertEqual(len(result), 8)  # Should have 8 rows
        
        dept_results = {}
        for row in result:
            dept = row[2]  # department is at index 2
            if dept not in dept_results:
                dept_results[dept] = []
            dept_results[dept].append(row)
        
        eng_results = sorted(dept_results["Engineering"], key=lambda x: x[3])  # sort by salary
        
        self.assertEqual(eng_results[0][4], 1)  # row_num
        self.assertEqual(eng_results[0][5], 1)  # rank
        self.assertEqual(eng_results[0][6], 1)  # dense_rank
        
        self.assertEqual(eng_results[1][4], 2)  # row_num
        self.assertEqual(eng_results[1][5], 2)  # rank
        self.assertEqual(eng_results[1][6], 2)  # dense_rank
        
        self.assertEqual(eng_results[2][4], 3)  # row_num
        self.assertEqual(eng_results[2][5], 3)  # rank
        self.assertEqual(eng_results[2][6], 3)  # dense_rank
    
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
                over(row_number(), window_name="dept_window"),
                "window_ref"
            )
        )
        
        sql = df_with_window_ref.to_sql(dialect="duckdb")
        
        result = self.conn.execute(sql).fetchall()
        
        self.assertEqual(len(result), 8)  # Should have 8 rows
    
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
        
        result = self.conn.execute(sql).fetchall()
        
        self.assertEqual(len(result), 8)  # Should have 8 rows
        
        dept_results = {}
        for row in result:
            dept = row[2]  # department is at index 2
            if dept not in dept_results:
                dept_results[dept] = []
            dept_results[dept].append(row)
        
        loc_results = {}
        for row in result:
            loc = row[3]  # location is at index 3
            if loc not in loc_results:
                loc_results[loc] = []
            loc_results[loc].append(row)
        
        eng_results = sorted(dept_results["Engineering"], key=lambda x: x[4])  # sort by salary
        self.assertEqual(len(eng_results), 3)
        self.assertEqual(eng_results[0][5], 1)  # First dept_rank
        self.assertEqual(eng_results[1][5], 2)  # Second dept_rank
        self.assertEqual(eng_results[2][5], 3)  # Third dept_rank
        
        ny_results = sorted(loc_results["New York"], key=lambda x: x[4])  # sort by salary
        self.assertEqual(len(ny_results), 3)
        self.assertEqual(ny_results[0][6], 1)  # First location_rank
        self.assertEqual(ny_results[1][6], 2)  # Second location_rank
        self.assertEqual(ny_results[2][6], 3)  # Third location_rank
    
    def test_reusing_window_definitions(self):
        """Test reusing named window definitions across multiple queries."""
        df_with_window = self.df.window(
            "dept_window",
            partition_by=lambda x: x.department,
            order_by=lambda x: x.salary
        )
        
        df_with_row_nums = df_with_window.select(
            lambda x: x.id,
            lambda x: x.name,
            lambda x: x.department,
            lambda x: x.salary,
            as_column(
                over(row_number(), window_name="dept_window"),
                "row_num"
            )
        )
        
        df_with_ranks = df_with_window.select(
            lambda x: x.id,
            lambda x: x.name,
            lambda x: x.department,
            lambda x: x.salary,
            as_column(
                over(rank(), window_name="dept_window"),
                "rank"
            )
        )
        
        sql1 = df_with_row_nums.to_sql(dialect="duckdb")
        result1 = self.conn.execute(sql1).fetchall()
        
        sql2 = df_with_ranks.to_sql(dialect="duckdb")
        result2 = self.conn.execute(sql2).fetchall()
        
        self.assertEqual(len(result1), 8)  # Should have 8 rows
        self.assertEqual(len(result2), 8)  # Should have 8 rows
        
        dept_results1 = {}
        for row in result1:
            dept = row[2]  # department is at index 2
            if dept not in dept_results1:
                dept_results1[dept] = []
            dept_results1[dept].append(row)
        
        dept_results2 = {}
        for row in result2:
            dept = row[2]  # department is at index 2
            if dept not in dept_results2:
                dept_results2[dept] = []
            dept_results2[dept].append(row)
        
        eng_results1 = sorted(dept_results1["Engineering"], key=lambda x: x[3])  # sort by salary
        self.assertEqual(eng_results1[0][4], 1)  # First row_num
        self.assertEqual(eng_results1[1][4], 2)  # Second row_num
        self.assertEqual(eng_results1[2][4], 3)  # Third row_num
        
        eng_results2 = sorted(dept_results2["Engineering"], key=lambda x: x[3])  # sort by salary
        self.assertEqual(eng_results2[0][4], 1)  # First rank
        self.assertEqual(eng_results2[1][4], 2)  # Second rank
        self.assertEqual(eng_results2[2][4], 3)  # Third rank


if __name__ == "__main__":
    unittest.main()
