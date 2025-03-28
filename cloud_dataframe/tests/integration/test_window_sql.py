"""
Integration tests for window functions using direct SQL with DuckDB.

This module contains tests for window functions executed on a real DuckDB database
using direct SQL queries. These tests verify that the window function syntax works
correctly with DuckDB before implementing the DSL parsing.
"""
import unittest
import pandas as pd
import duckdb


class TestWindowSQL(unittest.TestCase):
    """Test cases for window functions using direct SQL with DuckDB."""
    
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
    
    def tearDown(self):
        """Tear down test fixtures."""
        self.conn.close()
    
    def test_window_with_rank(self):
        """Test window function with rank() function using DSL."""
        from cloud_dataframe.core.dataframe import DataFrame
        from cloud_dataframe.type_system.schema import TableSchema
        from cloud_dataframe.type_system.column import as_column, rank, window
        
        schema = TableSchema(
            name="Employee",
            columns={
                "id": int,
                "name": str,
                "department": str,
                "salary": float,
            }
        )
        
        df = DataFrame.from_table_schema("employees", schema)
        query = df.select(
            lambda x: x.id,
            lambda x: x.name,
            lambda x: x.department,
            lambda x: x.salary,
            as_column(lambda x: window(func=rank(), partition=x.department, order_by=x.salary), "salary_rank")
        )
        
        result = self.conn.execute(query.to_sql()).fetchdf()
        
        self.assertEqual(len(result), 6)  # All employees
        self.assertIn("salary_rank", result.columns)
        
        departments = result["department"].unique()
        for dept in departments:
            dept_rows = result[result["department"] == dept].sort_values("salary_rank").reset_index(drop=True)
            self.assertEqual(dept_rows.iloc[0]["salary_rank"], 1)
    
    def test_window_with_frame(self):
        """Test window function with frame specification using DSL."""
        from cloud_dataframe.core.dataframe import DataFrame
        from cloud_dataframe.type_system.schema import TableSchema
        from cloud_dataframe.type_system.column import as_column, sum, window, row, unbounded
        
        schema = TableSchema(
            name="Employee",
            columns={
                "id": int,
                "name": str,
                "department": str,
                "salary": float,
            }
        )
        
        df = DataFrame.from_table_schema("employees", schema)
        query = df.select(
            lambda x: x.id,
            lambda x: x.name,
            lambda x: x.department,
            lambda x: x.salary,
            as_column(
                lambda x: window(
                    func=sum(x.salary),
                    partition=x.department,
                    order_by=x.salary,
                    frame=row(unbounded(), 0)
                ),
                "running_sum"
            )
        )
        
        result = self.conn.execute(query.to_sql()).fetchdf()
        
        self.assertEqual(len(result), 6)  # All employees
        self.assertIn("running_sum", result.columns)
        
        departments = result["department"].unique()
        for dept in departments:
            dept_rows = result[result["department"] == dept].sort_values("salary").reset_index(drop=True)
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
        """Test multiple window functions in a single query using DSL."""
        from cloud_dataframe.core.dataframe import DataFrame
        from cloud_dataframe.type_system.schema import TableSchema
        from cloud_dataframe.type_system.column import as_column, sum, rank, window, row, unbounded
        
        schema = TableSchema(
            name="Employee",
            columns={
                "id": int,
                "name": str,
                "department": str,
                "salary": float,
            }
        )
        
        df = DataFrame.from_table_schema("employees", schema)
        query = df.select(
            lambda x: x.id,
            lambda x: x.name,
            lambda x: x.department,
            lambda x: x.salary,
            as_column(lambda x: window(func=rank(), partition=x.department, order_by=x.salary), "salary_rank"),
            as_column(
                lambda x: window(
                    func=sum(x.salary),
                    partition=x.department,
                    order_by=x.salary,
                    frame=row(unbounded(), 0)
                ),
                "running_sum"
            )
        )
        
        result = self.conn.execute(query.to_sql()).fetchdf()
        
        self.assertEqual(len(result), 6)  # All employees
        self.assertIn("salary_rank", result.columns)
        self.assertIn("running_sum", result.columns)
        
        departments = result["department"].unique()
        for dept in departments:
            dept_rows = result[result["department"] == dept].sort_values("salary_rank").reset_index(drop=True)
            self.assertEqual(dept_rows.iloc[0]["salary_rank"], 1)


if __name__ == "__main__":
    unittest.main()
