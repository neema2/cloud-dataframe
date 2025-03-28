"""
Integration tests for window functions with walrus operator syntax in DuckDB.

This module tests the walrus operator syntax with window functions
in select() method, focusing on the over() functionality with partition_by and order_by clauses.
"""
import unittest
import pandas as pd
import duckdb
from typing import Optional

from cloud_dataframe.core.dataframe import DataFrame
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.type_system.column import (
    as_column, over, sum, avg, count, min, max, row_number, rank, dense_rank,
    row, range, unbounded
)


class TestWalrusWindowFunctionsDuckDB(unittest.TestCase):
    """Test cases for window functions with walrus operator syntax in DuckDB."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.conn = duckdb.connect(":memory:")
        
        sales_data = pd.DataFrame({
            "product_id": [1, 1, 1, 2, 2, 2, 3, 3, 3],
            "region": ["North", "South", "East", "North", "South", "East", "North", "South", "East"],
            "quarter": [1, 2, 3, 1, 2, 3, 1, 2, 3],
            "sales": [1000, 1200, 1400, 800, 900, 1000, 1500, 1600, 1700]
        })
        
        self.conn.execute("CREATE TABLE sales AS SELECT * FROM sales_data")
        self.conn.register("sales_data", sales_data)
        
        self.schema = TableSchema(
            name="Sales",
            columns={
                "product_id": int,
                "region": str,
                "quarter": int,
                "sales": int
            }
        )
        
        self.df = DataFrame.from_table_schema("sales", self.schema)
    
    def tearDown(self):
        """Tear down test fixtures."""
        self.conn.close()
        
    def test_basic_window_function(self):
        """Test basic window function with running total."""
        query = self.df.select(
            lambda x: x.product_id,
            lambda x: x.region,
            lambda x: x.quarter,
            lambda x: x.sales,
            as_column(
                over(
                    lambda x: sum(x.sales),
                    partition_by=lambda x: x.product_id,
                    order_by=lambda x: x.quarter
                ),
                "running_total"
            )
        ).order_by(
            lambda x: x.product_id,
            lambda x: x.quarter
        )
        
        sql = query.to_sql(dialect="duckdb")
        
        self.assertIn("running_total", sql)
        self.assertIn("SUM(", sql)
        self.assertIn("OVER (PARTITION BY", sql)
        
        result = self.conn.execute(sql).fetchdf()
        
        self.assertEqual(len(result), 9)  # All sales rows
        self.assertIn("running_total", result.columns)
        
        product1_rows = result[result["product_id"] == 1].sort_values("quarter").reset_index(drop=True)
        self.assertEqual(product1_rows.loc[0, "running_total"], 1000)  # Q1 sales
        self.assertEqual(product1_rows.loc[1, "running_total"], 2200)  # Q1 + Q2 sales
        self.assertEqual(product1_rows.loc[2, "running_total"], 3600)  # Q1 + Q2 + Q3 sales
        
    def test_multi_column_partition_by(self):
        """Test window function with multiple partition by columns."""
        query = self.df.select(
            lambda x: x.product_id,
            lambda x: x.region,
            lambda x: x.quarter,
            lambda x: x.sales,
            as_column(
                over(
                    lambda x: sum(x.sales),
                    partition_by=lambda x: [x.region, x.product_id]
                ),
                "region_product_total"
            )
        ).order_by(
            lambda x: x.region,
            lambda x: x.product_id,
            lambda x: x.quarter
        )
        
        sql = query.to_sql(dialect="duckdb")
        
        self.assertIn("region_product_total", sql)
        self.assertIn("SUM(", sql)
        self.assertIn("OVER (PARTITION BY", sql)
        self.assertIn("region", sql.lower())
        self.assertIn("product_id", sql.lower())
        
        result = self.conn.execute(sql).fetchdf()
        
        self.assertEqual(len(result), 9)  # All sales rows
        self.assertIn("region_product_total", result.columns)
        
        north_product1 = result[(result["region"] == "North") & (result["product_id"] == 1)]["region_product_total"].iloc[0]
        self.assertEqual(north_product1, 1000)  # Only one row for North/Product 1
        
        east_product1 = result[(result["region"] == "East") & (result["product_id"] == 1)]["region_product_total"].iloc[0]
        self.assertEqual(east_product1, 1400)  # Only one row for East/Product 1
        
    def test_multi_column_order_by(self):
        """Test window function with multiple order_by columns."""
        query = self.df.select(
            lambda x: x.product_id,
            lambda x: x.region,
            lambda x: x.quarter,
            lambda x: x.sales,
            as_column(
                over(
                    rank(),
                    partition_by=lambda x: x.product_id,
                    order_by=lambda x: [x.region, x.sales]
                ),
                "rank_by_region_sales"
            )
        ).order_by(
            lambda x: x.product_id,
            lambda x: x.region,
            lambda x: x.sales
        )
        
        sql = query.to_sql(dialect="duckdb")
        
        self.assertIn("rank_by_region_sales", sql)
        self.assertIn("RANK()", sql)
        self.assertIn("OVER (PARTITION BY", sql)
        self.assertIn("ORDER BY", sql)
        self.assertIn("region", sql.lower())
        self.assertIn("sales", sql.lower())
        
        result = self.conn.execute(sql).fetchdf()
        
        self.assertEqual(len(result), 9)  # All sales rows
        self.assertIn("rank_by_region_sales", result.columns)
        
        product1_rows = result[result["product_id"] == 1].sort_values(["region", "sales"]).reset_index(drop=True)
        
        self.assertEqual(product1_rows.loc[0, "rank_by_region_sales"], 1)  # First row for product 1
        self.assertEqual(product1_rows.loc[1, "rank_by_region_sales"], 2)  # Second row for product 1
        self.assertEqual(product1_rows.loc[2, "rank_by_region_sales"], 3)  # Third row for product 1
        
    def test_specific_syntax(self):
        """Test the specific syntax requested in the requirements."""
        query = self.df.select(
            lambda x: x.product_id,
            lambda x: x.region,
            lambda x: x.quarter,
            lambda x: x.sales,
            as_column(
                over(
                    lambda x: sum(x.sales),
                    partition_by=lambda x: [x.product_id, x.region],
                    order_by=lambda x: x.quarter
                ),
                "new_col"
            )
        ).order_by(
            lambda x: x.product_id,
            lambda x: x.region,
            lambda x: x.quarter
        )
        
        sql = query.to_sql(dialect="duckdb")
        
        self.assertIn("new_col", sql)
        self.assertIn("SUM(", sql)
        self.assertIn("OVER (PARTITION BY", sql)
        self.assertIn("ORDER BY", sql)
        
        result = self.conn.execute(sql).fetchdf()
        
        self.assertEqual(len(result), 9)  # All sales rows
        self.assertIn("new_col", result.columns)
        
        north_product1_rows = result[(result["region"] == "North") & (result["product_id"] == 1)].sort_values("quarter").reset_index(drop=True)
        self.assertEqual(len(north_product1_rows), 1)  # Only one row for North/Product 1
        self.assertEqual(north_product1_rows.loc[0, "new_col"], north_product1_rows.loc[0, "sales"])  # Running total equals sales for single row
        
    def test_window_function_with_frame(self):
        """Test window function with frame specification."""
        query = self.df.select(
            lambda x: x.product_id,
            lambda x: x.region,
            lambda x: x.quarter,
            lambda x: x.sales,
            as_column(
                over(
                    lambda x: avg(x.sales),
                    partition_by=lambda x: x.product_id,
                    order_by=lambda x: x.quarter,
                    frame=row(1, 1)  # 1 preceding, 1 following
                ),
                "moving_avg"
            )
        ).order_by(
            lambda x: x.product_id,
            lambda x: x.quarter
        )
        
        sql = query.to_sql(dialect="duckdb")
        
        self.assertIn("moving_avg", sql)
        self.assertIn("AVG(", sql)
        self.assertIn("OVER (PARTITION BY", sql)
        self.assertIn("ORDER BY", sql)
        self.assertIn("ROWS BETWEEN", sql)
        self.assertIn("PRECEDING AND", sql)
        self.assertIn("FOLLOWING", sql)
        
        result = self.conn.execute(sql).fetchdf()
        
        self.assertEqual(len(result), 9)  # All sales rows
        self.assertIn("moving_avg", result.columns)
        
        product1_rows = result[result["product_id"] == 1].sort_values("quarter").reset_index(drop=True)
        q2_row = product1_rows[product1_rows["quarter"] == 2]
        
        q1_sales = product1_rows[product1_rows["quarter"] == 1]["sales"].iloc[0]
        q2_sales = product1_rows[product1_rows["quarter"] == 2]["sales"].iloc[0]
        q3_sales = product1_rows[product1_rows["quarter"] == 3]["sales"].iloc[0]
        expected_avg = (q1_sales + q2_sales + q3_sales) / 3
        
        self.assertAlmostEqual(q2_row["moving_avg"].iloc[0], expected_avg, places=2)


if __name__ == "__main__":
    unittest.main()
