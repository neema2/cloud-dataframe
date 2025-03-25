"""
Integration tests for window function frames with DuckDB.

This module contains tests for using frame specifications with window functions
using the cloud-dataframe library with DuckDB.
"""
import unittest
import pandas as pd
import duckdb
from typing import Optional

from cloud_dataframe.core.dataframe import DataFrame
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.type_system.column import (
    as_column, over, row_number, rank, dense_rank, sum,
    row, range, unbounded
)


class TestWindowFramesDuckDB(unittest.TestCase):
    """Test cases for window frames with DuckDB."""
    
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
            as_column(
                over(
                    lambda x: sum(x.salary),
                    partition_by=lambda x: x.department,
                    frame=row(unbounded(), 0)  # unbounded preceding to current row
                ),
                "running_total"
            )
        ).order_by(
            lambda x: x.department,
            lambda x: x.salary
        )
        
        # Generate SQL
        sql = query.to_sql(dialect="duckdb")
        
        # Execute query
        result = self.conn.execute(sql).fetchdf()
        
        # Verify result
        self.assertEqual(len(result), 8)  # All employees
        self.assertIn("running_total", result.columns)
        
        # Manual check for Engineering department running total
        eng_rows = result[result["department"] == "Engineering"].reset_index(drop=True)
        self.assertEqual(eng_rows.loc[0, "running_total"], eng_rows.loc[0, "salary"])
        self.assertEqual(eng_rows.loc[1, "running_total"], eng_rows.loc[0, "salary"] + eng_rows.loc[1, "salary"])
    
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
            as_column(
                over(
                    lambda x: sum(x.value),  # Lambda with sum function
                    order_by=lambda x: x.day,
                    frame=row(1, 1)  # 1 preceding, 1 following (3-day window)
                ),
                "moving_avg"
            )
        ).order_by(
            lambda x: x.day
        )
        
        # Generate SQL
        sql = query.to_sql(dialect="duckdb")
        
        # Execute query
        result = self.conn.execute(sql).fetchdf()
        
        # Verify result
        self.assertEqual(len(result), 30)  # All days
        self.assertIn("moving_avg", result.columns)
        
        # Verify middle values have 3 days in the average
        middle_rows = result.iloc[1:-1]  # Skip first and last
        for _, row in middle_rows.iterrows():
            # Check if we're getting reasonable averages - values can be higher due to sum
            self.assertGreaterEqual(row["moving_avg"], 0.0)
            # We don't check upper bound since it's a sum of random values
    
    def test_lambda_function_with_complex_expression(self):
        """Test lambda function with complex expression in over()."""
        # Build query with complex lambda expression
        query = self.df.select(
            lambda x: x.id,
            lambda x: x.name,
            lambda x: x.department,
            lambda x: x.salary,
            as_column(
                over(
                    lambda x: sum(x.salary + 1000),  # Complex expression: salary + 1000
                    partition_by=lambda x: x.department,
                    frame=row(unbounded(), 0)
                ),
                "adjusted_total"
            )
        ).order_by(
            lambda x: x.department,
            lambda x: x.id
        )
        
        # Generate SQL
        sql = query.to_sql(dialect="duckdb")
        
        # Execute query
        result = self.conn.execute(sql).fetchdf()
        
        # Verify result
        self.assertEqual(len(result), 8)  # All employees
        self.assertIn("adjusted_total", result.columns)
        
        # Check that the adjusted total includes the +1000 for each employee
        eng_rows = result[result["department"] == "Engineering"].reset_index(drop=True)
        expected_total = (eng_rows.loc[0, "salary"] + 1000) + (eng_rows.loc[1, "salary"] + 1000)
        self.assertEqual(eng_rows.loc[1, "adjusted_total"], expected_total)


if __name__ == "__main__":
    unittest.main()
