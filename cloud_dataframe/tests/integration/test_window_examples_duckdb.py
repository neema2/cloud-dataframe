"""
Integration tests for window function examples with DuckDB.

This module contains tests for using frame specifications with window functions
using the cloud-dataframe library with DuckDB, demonstrating real-world examples.
"""
import unittest
import duckdb
from typing import Optional, Dict, List, Any, Tuple

from cloud_dataframe.core.dataframe import DataFrame
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.type_system.column import (
    row_number, rank, dense_rank, sum, avg,
    row, range, unbounded, window
)


class TestWindowExamplesDuckDB(unittest.TestCase):
    """Test cases for window function examples with DuckDB."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Create a DuckDB connection
        self.conn = duckdb.connect(":memory:")
        
        self.conn.execute("""
            CREATE TABLE sales AS
            SELECT 1 AS product_id, '2023-01-01' AS date, 'East' AS region, 100 AS sales UNION ALL
            SELECT 2, '2023-01-01', 'East', 150 UNION ALL
            SELECT 3, '2023-01-01', 'West', 200 UNION ALL
            SELECT 1, '2023-01-02', 'East', 120 UNION ALL
            SELECT 2, '2023-01-02', 'West', 160 UNION ALL
            SELECT 3, '2023-01-02', 'West', 210 UNION ALL
            SELECT 1, '2023-01-03', 'East', 130 UNION ALL
            SELECT 2, '2023-01-03', 'West', 170 UNION ALL
            SELECT 3, '2023-01-03', 'East', 220
        """)
        
        # Create schema for the sales table
        self.schema = TableSchema(
            name="Sales",
            columns={
                "product_id": int,
                "date": str,
                "region": str,
                "sales": int,
            }
        )
        
        # Create a DataFrame with typed properties
        self.df = DataFrame.from_table_schema("sales", self.schema, alias="x")
    
    def tearDown(self):
        """Tear down test fixtures."""
        self.conn.close()
    
    def test_running_total_with_unbounded_preceding(self):
        """Test running total of sales by product_id with unbounded preceding frame."""
        # Build query with running total
        query = self.df.select(
            lambda x: x.product_id,
            lambda x: x.date,
            lambda x: x.sales,
            lambda x: (running_total := window(func=sum(x.sales), partition=x.product_id, order_by=x.date, frame=row(unbounded(), 0)))
        ).order_by(
            lambda x: [x.product_id, x.date]
        )
        
        # Generate SQL
        sql = query.to_sql(dialect="duckdb")
        expected_sql = "SELECT x.product_id, x.date, x.sales, SUM(x.sales) OVER (PARTITION BY x.product_id ORDER BY x.date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total\nFROM sales AS x\nORDER BY x.product_id ASC, x.date ASC"
        self.assertEqual(sql.strip(), expected_sql.strip())
        
        # Execute query
        result = self.conn.execute(sql).fetchall()
        
        column_names = ["product_id", "date", "sales", "running_total"]
        result_dicts = [dict(zip(column_names, row)) for row in result]
        
        # Verify result
        self.assertEqual(len(result_dicts), 9)  # All sales records
        
        # Check running totals for product_id=1
        product1_rows = [row for row in result_dicts if row["product_id"] == 1]
        product1_rows.sort(key=lambda x: x["date"])
        self.assertEqual(product1_rows[0]["running_total"], 100)
        self.assertEqual(product1_rows[1]["running_total"], 220)
        self.assertEqual(product1_rows[2]["running_total"], 350)
        
        # Check running totals for product_id=2
        product2_rows = [row for row in result_dicts if row["product_id"] == 2]
        product2_rows.sort(key=lambda x: x["date"])
        self.assertEqual(product2_rows[0]["running_total"], 150)
        self.assertEqual(product2_rows[1]["running_total"], 310)
        self.assertEqual(product2_rows[2]["running_total"], 480)
    
    def test_moving_average_with_preceding_following(self):
        """Test moving average with preceding and following rows."""
        # Build query with moving average
        query = self.df.select(
            lambda x: x.product_id,
            lambda x: x.date,
            lambda x: x.sales,
            lambda x: (moving_avg := window(func=avg(x.sales), partition=x.product_id, order_by=x.date, frame=row(1, 1)))
        ).order_by(
            lambda x: [x.product_id, x.date]
        )
        
        # Generate SQL
        sql = query.to_sql(dialect="duckdb")
        expected_sql = "SELECT x.product_id, x.date, x.sales, AVG(x.sales) OVER (PARTITION BY x.product_id ORDER BY x.date ASC ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS moving_avg\nFROM sales AS x\nORDER BY x.product_id ASC, x.date ASC"
        self.assertEqual(sql.strip(), expected_sql.strip())
        
        # Execute query
        result = self.conn.execute(sql).fetchall()
        
        column_names = ["product_id", "date", "sales", "moving_avg"]
        result_dicts = [dict(zip(column_names, row)) for row in result]
        
        # Verify result
        self.assertEqual(len(result_dicts), 9)  # All sales records
        
        # Check moving averages for product_id=1
        product1_rows = [row for row in result_dicts if row["product_id"] == 1]
        product1_rows.sort(key=lambda x: x["date"])
        self.assertAlmostEqual(product1_rows[0]["moving_avg"], 110.0)  # Avg of [100, 120]
        self.assertAlmostEqual(product1_rows[1]["moving_avg"], 116.666667, places=5)  # Avg of [100, 120, 130]
        self.assertAlmostEqual(product1_rows[2]["moving_avg"], 125.0)  # Avg of [120, 130]
    
    def test_complex_expression_in_lambda(self):
        """Test complex expression in lambda function."""
        # Build query with complex lambda expression
        query = self.df.select(
            lambda x: x.product_id,
            lambda x: x.region,
            lambda x: x.sales,
            lambda x: (adjusted_total := window(func=sum(x.sales + 10), partition=x.region, frame=range(unbounded(), 0)))
        ).order_by(
            lambda x: [x.region, x.product_id]
        )
        
        # Generate SQL
        sql = query.to_sql(dialect="duckdb")
        expected_sql = "SELECT x.product_id, x.region, x.sales, SUM((x.sales + 10)) OVER (PARTITION BY x.region RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS adjusted_total\nFROM sales AS x\nORDER BY x.region ASC, x.product_id ASC"
        self.assertEqual(sql.strip(), expected_sql.strip())
        
        # Execute query
        result = self.conn.execute(sql).fetchall()
        
        column_names = ["product_id", "region", "sales", "adjusted_total"]
        result_dicts = [dict(zip(column_names, row)) for row in result]
        
        # Verify result
        self.assertEqual(len(result_dicts), 9)  # All sales records
        
        # Check adjusted totals for East region
        east_rows = [row for row in result_dicts if row["region"] == "East"]
        east_rows.sort(key=lambda x: (x["product_id"]))
        # Sum of (100+10, 120+10, 130+10, 150+10, 220+10) = 770
        self.assertEqual(east_rows[0]["adjusted_total"], 770.0)
        
        # Check adjusted totals for West region
        west_rows = [row for row in result_dicts if row["region"] == "West"]
        west_rows.sort(key=lambda x: (x["product_id"]))
        # Sum of (160+10, 170+10, 200+10, 210+10) = 780
        self.assertEqual(west_rows[0]["adjusted_total"], 780.0)


if __name__ == "__main__":
    unittest.main()
