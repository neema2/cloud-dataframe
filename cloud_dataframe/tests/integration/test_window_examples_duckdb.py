"""
Integration tests for window function examples with DuckDB.

This module contains tests for using frame specifications with window functions
using the cloud-dataframe library with DuckDB, demonstrating real-world examples.
"""
import unittest
import pandas as pd
import duckdb
from typing import Optional

from cloud_dataframe.core.dataframe import DataFrame
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.type_system.column import (
    as_column, over, row_number, rank, dense_rank, sum, avg,
    row, range, unbounded
)


class TestWindowExamplesDuckDB(unittest.TestCase):
    """Test cases for window function examples with DuckDB."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Create a DuckDB connection
        self.conn = duckdb.connect(":memory:")
        
        # Create sample data
        sales_data = pd.DataFrame({
            "product_id": [1, 2, 3, 1, 2, 3, 1, 2, 3],
            "date": ["2023-01-01", "2023-01-01", "2023-01-01", 
                    "2023-01-02", "2023-01-02", "2023-01-02",
                    "2023-01-03", "2023-01-03", "2023-01-03"],
            "region": ["East", "East", "West", "East", "West", "West", "East", "West", "East"],
            "sales": [100, 150, 200, 120, 160, 210, 130, 170, 220]
        })
        
        # Register the data in DuckDB
        self.conn.register("sales_data", sales_data)
        self.conn.execute("CREATE TABLE sales AS SELECT * FROM sales_data")
        
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
        self.df = DataFrame.from_table_schema("sales", self.schema)
    
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
            as_column(
                over(
                    lambda x: sum(x.sales),  # Lambda expression with sum
                    partition_by=lambda x: x.product_id,
                    order_by=lambda x: x.date,
                    frame=row(unbounded(), 0)  # ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ),
                "running_total"
            )
        ).order_by(
            lambda x: x.product_id,
            lambda x: x.date
        )
        
        # Generate SQL
        sql = query.to_sql(dialect="duckdb")
        expected_sql = "SELECT product_id, date, sales, SUM(sales) OVER (PARTITION BY product_id ORDER BY date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total\nFROM sales\nORDER BY product_id ASC, date ASC"
        self.assertEqual(sql.strip(), expected_sql.strip())
        
        # Execute query
        result = self.conn.execute(sql).fetchdf()
        
        # Verify result
        self.assertEqual(len(result), 9)  # All sales records
        
        # Check running totals for product_id=1
        product1_rows = result[result["product_id"] == 1].reset_index(drop=True)
        self.assertEqual(product1_rows.loc[0, "running_total"], 100)
        self.assertEqual(product1_rows.loc[1, "running_total"], 220)
        self.assertEqual(product1_rows.loc[2, "running_total"], 350)
        
        # Check running totals for product_id=2
        product2_rows = result[result["product_id"] == 2].reset_index(drop=True)
        self.assertEqual(product2_rows.loc[0, "running_total"], 150)
        self.assertEqual(product2_rows.loc[1, "running_total"], 310)
        self.assertEqual(product2_rows.loc[2, "running_total"], 480)
    
    def test_moving_average_with_preceding_following(self):
        """Test moving average with preceding and following rows."""
        # Build query with moving average
        query = self.df.select(
            lambda x: x.product_id,
            lambda x: x.date,
            lambda x: x.sales,
            as_column(
                over(
                    lambda x: avg(x.sales),  # Lambda expression with avg
                    partition_by=lambda x: x.product_id,
                    order_by=lambda x: x.date,
                    frame=row(1, 1)  # ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
                ),
                "moving_avg"
            )
        ).order_by(
            lambda x: x.product_id,
            lambda x: x.date
        )
        
        # Generate SQL
        sql = query.to_sql(dialect="duckdb")
        expected_sql = "SELECT product_id, date, sales, AVG(sales) OVER (PARTITION BY product_id ORDER BY date ASC ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS moving_avg\nFROM sales\nORDER BY product_id ASC, date ASC"
        self.assertEqual(sql.strip(), expected_sql.strip())
        
        # Execute query
        result = self.conn.execute(sql).fetchdf()
        
        # Verify result
        self.assertEqual(len(result), 9)  # All sales records
        
        # Check moving averages for product_id=1
        product1_rows = result[result["product_id"] == 1].reset_index(drop=True)
        self.assertAlmostEqual(product1_rows.loc[0, "moving_avg"], 110.0)  # Avg of [100, 120]
        self.assertAlmostEqual(product1_rows.loc[1, "moving_avg"], 116.666667, places=5)  # Avg of [100, 120, 130]
        self.assertAlmostEqual(product1_rows.loc[2, "moving_avg"], 125.0)  # Avg of [120, 130]
    
    def test_complex_expression_in_lambda(self):
        """Test complex expression in lambda function."""
        # Build query with complex lambda expression
        query = self.df.select(
            lambda x: x.product_id,
            lambda x: x.region,
            lambda x: x.sales,
            as_column(
                over(
                    lambda x: sum(x.sales + 10),  # Complex expression: sales + 10
                    partition_by=lambda x: x.region,
                    frame=range(unbounded(), 0)  # RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ),
                "adjusted_total"
            )
        ).order_by(
            lambda x: x.region,
            lambda x: x.product_id
        )
        
        # Generate SQL
        sql = query.to_sql(dialect="duckdb")
        expected_sql = "SELECT product_id, region, sales, SUM((sales + 10)) OVER (PARTITION BY region RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS adjusted_total\nFROM sales\nORDER BY region ASC, product_id ASC"
        self.assertEqual(sql.strip(), expected_sql.strip())
        
        # Execute query
        result = self.conn.execute(sql).fetchdf()
        
        # Verify result
        self.assertEqual(len(result), 9)  # All sales records
        
        # Check adjusted totals for East region
        east_rows = result[result["region"] == "East"].reset_index(drop=True)
        # Sum of (100+10, 120+10, 130+10, 150+10, 220+10) = 770
        self.assertEqual(east_rows.loc[0, "adjusted_total"], 770.0)
        
        # Check adjusted totals for West region
        west_rows = result[result["region"] == "West"].reset_index(drop=True)
        # Sum of (160+10, 170+10, 200+10, 210+10) = 780
        self.assertEqual(west_rows.loc[0, "adjusted_total"], 780.0)


if __name__ == "__main__":
    unittest.main()
