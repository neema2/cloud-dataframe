"""
Integration tests for window() function parsing with DuckDB.

This module contains tests for using the window() function in queries
executed on a real DuckDB database.
"""
import unittest
import duckdb
from typing import Dict, List, Any, Tuple

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
        
        self.conn.execute("""
            CREATE TABLE employees AS
            SELECT 1 AS id, 'Alice' AS name, 'Engineering' AS department, 80000.0 AS salary UNION ALL
            SELECT 2, 'Bob', 'Engineering', 90000.0 UNION ALL
            SELECT 3, 'Charlie', 'Sales', 70000.0 UNION ALL
            SELECT 4, 'David', 'Sales', 75000.0 UNION ALL
            SELECT 5, 'Eve', 'Marketing', 65000.0 UNION ALL
            SELECT 6, 'Frank', 'Marketing', 60000.0
        """)
        
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
        
        expected_result = self.conn.execute(expected_sql).fetchall()
        
        column_names = ["id", "name", "department", "salary", "salary_rank"]
        result_dicts = [dict(zip(column_names, row)) for row in expected_result]
        
        self.assertEqual(len(result_dicts), 6)  # All employees
        self.assertTrue(all("salary_rank" in row for row in result_dicts))
        
        departments = set(row["department"] for row in result_dicts)
        
        for dept in departments:
            dept_rows = [row for row in result_dicts if row["department"] == dept]
            dept_rows.sort(key=lambda x: x["salary"])
            
            self.assertEqual(dept_rows[0]["salary_rank"], 1)
    
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
        
        expected_result = self.conn.execute(expected_sql).fetchall()
        
        column_names = ["id", "name", "department", "salary", "running_sum"]
        result_dicts = [dict(zip(column_names, row)) for row in expected_result]
        
        self.assertEqual(len(result_dicts), 6)  # All employees
        self.assertTrue(all("running_sum" in row for row in result_dicts))
        
        departments = set(row["department"] for row in result_dicts)
        
        for dept in departments:
            dept_rows = [row for row in result_dicts if row["department"] == dept]
            dept_rows.sort(key=lambda x: x["salary"])
            
            dept_salaries = [row["salary"] for row in dept_rows]
            expected_sums = []
            running_total = 0
            for salary in dept_salaries:
                running_total += salary
                expected_sums.append(running_total)
            
            actual_sums = [row["running_sum"] for row in dept_rows]
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
        
        expected_result = self.conn.execute(expected_sql).fetchall()
        
        column_names = ["id", "name", "department", "salary", "salary_rank", "running_sum"]
        result_dicts = [dict(zip(column_names, row)) for row in expected_result]
        
        self.assertEqual(len(result_dicts), 6)  # All employees
        self.assertTrue(all("salary_rank" in row for row in result_dicts))
        self.assertTrue(all("running_sum" in row for row in result_dicts))
        
        departments = set(row["department"] for row in result_dicts)
        
        for dept in departments:
            dept_rows = [row for row in result_dicts if row["department"] == dept]
            dept_rows.sort(key=lambda x: x["salary_rank"])
            
            self.assertEqual(dept_rows[0]["salary_rank"], 1)


if __name__ == "__main__":
    unittest.main()
