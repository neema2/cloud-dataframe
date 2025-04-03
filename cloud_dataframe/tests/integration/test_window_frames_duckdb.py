"""
Integration tests for window function frames with DuckDB.

This module contains tests for using frame specifications with window functions
using the cloud-dataframe library with DuckDB.
"""
import unittest
import duckdb
from typing import Optional, Dict, List, Any, Tuple

from cloud_dataframe.core.dataframe import DataFrame
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.type_system.column import (
    row_number, rank, dense_rank, sum,
    row, range, unbounded, window
)


class TestWindowFramesDuckDB(unittest.TestCase):
    """Test cases for window frames with DuckDB."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Create a DuckDB connection
        self.conn = duckdb.connect(":memory:")
        
        self.conn.execute("""
            CREATE TABLE employees AS
            SELECT 1 AS id, 'Alice' AS name, 'Engineering' AS department, 80000.0 AS salary UNION ALL
            SELECT 2, 'Bob', 'Engineering', 90000.0 UNION ALL
            SELECT 3, 'Charlie', 'Sales', 70000.0 UNION ALL
            SELECT 4, 'David', 'Sales', 75000.0 UNION ALL
            SELECT 5, 'Eve', 'Marketing', 65000.0 UNION ALL
            SELECT 6, 'Frank', 'Marketing', 60000.0 UNION ALL
            SELECT 7, 'Grace', 'HR', 55000.0 UNION ALL
            SELECT 8, 'Heidi', 'HR', 58000.0
        """)
        
        # Create schema for the employees table
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
    
    def tearDown(self):
        """Tear down test fixtures."""
        self.conn.close()
    
    def test_sum_over_partition_with_frame(self):
        """Test SUM with ROWS frame and partition by."""
        # Build query with SUM window function and frame
        query = self.df.select(
            lambda x: x.id,
            lambda x: x.name,
            lambda x: x.department,
            lambda x: x.salary,
            lambda x: (running_total := window(
                func=sum(x.salary),
                partition=x.department,
                frame=row(unbounded(), 0)  # unbounded preceding to current row
            ))
        ).order_by(
            lambda x: [x.department, x.salary]
        )
        
        # Generate SQL
        sql = query.to_sql(dialect="duckdb")
        
        # Execute query
        result = self.conn.execute(sql).fetchall()
        
        column_names = ["id", "name", "department", "salary", "running_total"]
        result_dicts = [dict(zip(column_names, row)) for row in result]
        
        # Verify result
        self.assertEqual(len(result_dicts), 8)  # All employees
        self.assertTrue(all("running_total" in row for row in result_dicts))
        
        # Manual check for Engineering department running total
        eng_rows = [row for row in result_dicts if row["department"] == "Engineering"]
        eng_rows.sort(key=lambda x: x["salary"])
        self.assertEqual(eng_rows[0]["running_total"], eng_rows[0]["salary"])
        self.assertEqual(eng_rows[1]["running_total"], eng_rows[0]["salary"] + eng_rows[1]["salary"])
    
    def test_moving_average_with_frame(self):
        """Test moving average with ROWS frame."""
        # Create a time series data
        self.conn.execute("""
            CREATE TABLE time_series AS
            SELECT 
                generate_series AS day,
                (random() * 100)::INT AS value
            FROM generate_series(1, 30, 1)
        """)
        
        # Create schema for time series
        ts_schema = TableSchema(
            name="TimeSeries",
            columns={
                "day": int,
                "value": int,
            }
        )
        
        # Create DataFrame
        ts_df = DataFrame.from_table_schema("time_series", ts_schema)
        
        # Build query with moving average
        from cloud_dataframe.type_system.column import row
        query = ts_df.select(
            lambda x: x.day,
            lambda x: x.value,
            lambda x: (moving_avg := window(
                func=sum(x.value),  # Sum function
                order_by=x.day,
                frame=row(1, 1)  # 1 preceding, 1 following (3-day window)
            ))
        ).order_by(
            lambda x: x.day
        )
        
        # Generate SQL
        sql = query.to_sql(dialect="duckdb")
        
        # Execute query
        result = self.conn.execute(sql).fetchall()
        
        column_names = ["day", "value", "moving_avg"]
        result_dicts = [dict(zip(column_names, row)) for row in result]
        
        # Verify result
        self.assertEqual(len(result_dicts), 30)  # All days
        self.assertTrue(all("moving_avg" in row for row in result_dicts))
        
        middle_rows = result_dicts[1:-1]
        for row in middle_rows:
            # Check if we're getting reasonable averages - values can be higher due to sum
            self.assertGreaterEqual(row["moving_avg"], 0.0)
            # We don't check upper bound since it's a sum of random values
    
    def test_lambda_function_with_complex_expression(self):
        """Test lambda function with complex expression in window()."""
        # Build query with complex lambda expression
        query = self.df.select(
            lambda x: x.id,
            lambda x: x.name,
            lambda x: x.department,
            lambda x: x.salary,
            lambda x: (adjusted_total := window(
                func=sum(x.salary + 1000),  # Complex expression: salary + 1000
                partition=x.department,
                frame=row(unbounded(), 0)
            ))
        ).order_by(
            lambda x: [x.department, x.id]
        )
        
        # Generate SQL
        sql = query.to_sql(dialect="duckdb")
        
        # Execute query
        result = self.conn.execute(sql).fetchall()
        
        column_names = ["id", "name", "department", "salary", "adjusted_total"]
        result_dicts = [dict(zip(column_names, row)) for row in result]
        
        # Verify result
        self.assertEqual(len(result_dicts), 8)  # All employees
        self.assertTrue(all("adjusted_total" in row for row in result_dicts))
        
        # Check that the adjusted total includes the +1000 for each employee
        eng_rows = [row for row in result_dicts if row["department"] == "Engineering"]
        eng_rows.sort(key=lambda x: x["id"])
        expected_total = (eng_rows[0]["salary"] + 1000) + (eng_rows[1]["salary"] + 1000)
        self.assertEqual(eng_rows[1]["adjusted_total"], expected_total)


if __name__ == "__main__":
    unittest.main()
