"""
Integration tests for window() function parsing with DuckDB.

This module contains tests for using the window() function in queries
executed on a real DuckDB database.
"""
import unittest
import pandas as pd
import duckdb

from cloud_dataframe.core.dataframe import DataFrame
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.type_system.column import (
    sum, rank, window, row, unbounded
)


class TestWindowFunctionParsing(unittest.TestCase):
    """Test cases for window() function parsing with DuckDB."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.conn = duckdb.connect(":memory:")
        
        employees_data = pd.DataFrame({
            "id": [1, 2, 3, 4, 5, 6],
            "name": ["Alice", "Bob", "Charlie", "David", "Eve", "Frank"],
            "department": ["Engineering", "Engineering", "Sales", "Sales", "Marketing", "Marketing"],
            "salary": [80000.0, 90000.0, 70000.0, 75000.0, 65000.0, 60000.0],
        })
        
        self.conn.execute("CREATE TABLE employees AS SELECT * FROM employees_data")
        self.conn.register("employees_data", employees_data)
        
        self.schema = TableSchema(
            name="Employee",
            columns={
                "id": int,
                "name": str,
                "department": str,
                "salary": float,
            }
        )
        
        self.df = DataFrame.from_table_schema("employees", self.schema)
    
    def tearDown(self):
        """Tear down test fixtures."""
        self.conn.close()
    
    def test_window_with_rank(self):
        """Test window() function with rank() function."""
        query = self.df.select(
            lambda x: x.id,
            lambda x: x.name,
            lambda x: x.department,
            lambda x: x.salary,
            lambda x: (salary_rank := window(func=rank(), partition=x.department, order_by=x.salary))
        )
        
        sql = query.to_sql()
        
        expected_sql = """
        SELECT
            id,
            name,
            department,
            salary,
            RANK() OVER (PARTITION BY department ORDER BY salary ASC) AS salary_rank
        FROM employees
        """
        
        expected_result = self.conn.execute(expected_sql).fetchdf()
        
        self.assertEqual(len(expected_result), 6)  # All employees
        self.assertIn("salary_rank", expected_result.columns)
        
        departments = expected_result["department"].unique()
        for dept in departments:
            dept_rows = expected_result[expected_result["department"] == dept].sort_values("salary").reset_index(drop=True)
            self.assertEqual(dept_rows.iloc[0]["salary_rank"], 1)
    
    def test_window_with_frame(self):
        """Test window() function with frame specification."""
        query = self.df.select(
            lambda x: x.id,
            lambda x: x.name,
            lambda x: x.department,
            lambda x: x.salary,
            lambda x: (running_sum := window(func=sum(x.salary), partition=x.department, order_by=x.salary, frame=row(unbounded(), 0) ))
        )
        
        expected_sql = """
        SELECT
            id,
            name,
            department,
            salary,
            SUM(salary) OVER (PARTITION BY department ORDER BY salary ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum
        FROM employees
        """
        
        expected_result = self.conn.execute(expected_sql).fetchdf()
        
        self.assertEqual(len(expected_result), 6)  # All employees
        self.assertIn("running_sum", expected_result.columns)
        
        departments = expected_result["department"].unique()
        for dept in departments:
            dept_rows = expected_result[expected_result["department"] == dept].sort_values("salary").reset_index(drop=True)
            dept_salaries = dept_rows["salary"].tolist()
            
            expected_sums = []
            running_total = 0
            for salary in dept_salaries:
                running_total += salary
                expected_sums.append(running_total)
            
            actual_sums = dept_rows["running_sum"].tolist()
            for i in range(len(expected_sums)):
                self.assertAlmostEqual(actual_sums[i], expected_sums[i], places=2)
    
    def test_window_with_multiple_functions(self):
        """Test multiple window functions in a single query."""
        query = self.df.select(
            lambda x: [ x.id, x.name, x.department, x.salary ],
            lambda x: (salary_rank := window(func=rank(), partition=x.department, order_by=x.salary)),
            lambda x: (running_sum := window(func=sum(x.salary), partition=x.department, order_by=x.salary, frame=row(unbounded(), 0)))
        )
        
        expected_sql = """
        SELECT
            id,
            name,
            department,
            salary,
            RANK() OVER (PARTITION BY department ORDER BY salary ASC) AS salary_rank,
            SUM(salary) OVER (PARTITION BY department ORDER BY salary ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum
        FROM employees
        """
        
        expected_result = self.conn.execute(expected_sql).fetchdf()
        
        self.assertEqual(len(expected_result), 6)  # All employees
        self.assertIn("salary_rank", expected_result.columns)
        self.assertIn("running_sum", expected_result.columns)
        
        departments = expected_result["department"].unique()
        for dept in departments:
            dept_rows = expected_result[expected_result["department"] == dept].sort_values("salary_rank").reset_index(drop=True)
            self.assertEqual(dept_rows.iloc[0]["salary_rank"], 1)


if __name__ == "__main__":
    unittest.main()
