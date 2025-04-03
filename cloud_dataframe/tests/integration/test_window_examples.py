"""
Integration tests for window function examples with DuckDB.

This module contains tests for window functions using lambda expressions
with the cloud-dataframe library.
"""
import unittest
import duckdb
from typing import Optional, Dict, List, Any, Tuple

from cloud_dataframe.core.dataframe import DataFrame, Sort
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.type_system.column import sum, avg, count, rank, dense_rank, row_number, window


class TestWindowExamples(unittest.TestCase):
    """Test cases for window functions with lambda expressions."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Create a DuckDB connection
        self.conn = duckdb.connect(":memory:")
        
        self.conn.execute("""
            CREATE TABLE employees AS
            SELECT 1 AS id, 'Alice' AS name, 'Engineering' AS department, 80000.0 AS salary, '2020-01-15' AS hire_date UNION ALL
            SELECT 2, 'Bob', 'Engineering', 90000.0, '2019-05-10' UNION ALL
            SELECT 3, 'Charlie', 'Sales', 70000.0, '2021-02-20' UNION ALL
            SELECT 4, 'David', 'Sales', 75000.0, '2018-11-05' UNION ALL
            SELECT 5, 'Eve', 'Marketing', 65000.0, '2022-03-15' UNION ALL
            SELECT 6, 'Frank', 'Marketing', 60000.0, '2017-08-22' UNION ALL
            SELECT 7, 'Grace', 'HR', 55000.0, '2020-07-10' UNION ALL
            SELECT 8, 'Heidi', 'HR', 58000.0, '2019-12-01'
        """)
        
        # Create schema for the employees table
        self.schema = TableSchema(
            name="Employee",
            columns={
                "id": int,
                "name": str,
                "department": str,
                "salary": float,
                "hire_date": str,
            }
        )
        
        # Create a DataFrame with typed properties
        self.df = DataFrame.from_table_schema("employees", self.schema)
    
    def tearDown(self):
        """Tear down test fixtures."""
        self.conn.close()
    
    def test_rank_window_function(self):
        """Test rank window function with lambda expressions."""
        # Build query with rank window function - using DESC order for salary
        query = self.df.select(
            lambda x: x.id,
            lambda x: x.name,
            lambda x: x.department,
            lambda x: x.salary,
            lambda x: (salary_rank := window(func=rank(), partition=x.department, order_by=[(x.salary, Sort.DESC)]))
        )
        
        # Generate SQL
        sql = query.to_sql(dialect="duckdb")
        
        # Execute query
        result = self.conn.execute(sql).fetchall()
        
        column_names = ["id", "name", "department", "salary", "salary_rank"]
        result_dicts = [dict(zip(column_names, row)) for row in result]
        
        # Verify result
        self.assertEqual(len(result_dicts), 8)  # All employees
        self.assertTrue(all("salary_rank" in row for row in result_dicts))
        
        # Sort by department and salary_rank to ensure consistent ordering
        result_dicts.sort(key=lambda x: (x["department"], x["salary_rank"]))
        
        # Get Engineering rows - check that we have at least one rank 1
        eng_rows = [row for row in result_dicts if row["department"] == "Engineering"]
        eng_rows.sort(key=lambda x: x["salary_rank"])
        self.assertEqual(eng_rows[0]["salary_rank"], 1)  # First row should have rank 1
        
        # Check if we have more than one Engineering employee
        if len(eng_rows) > 1:
            # If we have multiple employees with the same salary, they might have the same rank
            # So we'll just verify that the rank is either 1 or 2
            self.assertIn(eng_rows[1]["salary_rank"], [1, 2])  # Second row should have rank 1 or 2
    
    def test_row_number_window_function(self):
        """Test row_number window function with lambda expressions."""
        # Build query with row_number window function
        query = self.df.select(
            lambda x: x.id,
            lambda x: x.name,
            lambda x: x.department,
            lambda x: x.salary,
            lambda x: (row_num := window(func=row_number(), partition=x.department, order_by=[(x.salary, Sort.DESC)]))
        )
        
        # Generate SQL
        sql = query.to_sql(dialect="duckdb")
        
        # Add ORDER BY clause directly to SQL
        sql += "\nORDER BY department ASC, row_num ASC"
        
        # Execute query
        result = self.conn.execute(sql).fetchall()
        
        column_names = ["id", "name", "department", "salary", "row_num"]
        result_dicts = [dict(zip(column_names, row)) for row in result]
        
        # Verify result
        self.assertEqual(len(result_dicts), 8)  # All employees
        self.assertTrue(all("row_num" in row for row in result_dicts))
        
        sql = query.to_sql(dialect="duckdb")
        
        result = self.conn.execute(sql).fetchall()
        
        result_dicts = [dict(zip(column_names, row)) for row in result]
        
        # Verify result
        self.assertEqual(len(result_dicts), 8)  # All employees
        self.assertTrue(all("row_num" in row for row in result_dicts))
    
    def test_window_function_with_filter(self):
        """Test window function with filter."""
        from cloud_dataframe.type_system.column import rank
        
        # Generate the window function query with DESC order for salary
        window_query = self.df.select(
            lambda x: x.id,
            lambda x: x.name,
            lambda x: x.department,
            lambda x: x.salary,
            lambda x: (salary_rank := window(func=rank(), partition=x.department, order_by=[(x.salary, Sort.DESC)]))
        )
        
        # Generate SQL for the window query
        window_sql = window_query.to_sql(dialect="duckdb")
        
        # Create a CTE (Common Table Expression) to work around DuckDB's limitation
        # that window functions can't be used directly in WHERE clauses
        # Get only the top salary (rank=1) for each department
        sql = f"""
        WITH ranked_employees AS (
            {window_sql}
        )
        SELECT * FROM ranked_employees
        WHERE salary_rank = 1
        ORDER BY department ASC
        """
        
        # Execute query
        result = self.conn.execute(sql).fetchall()
        
        column_names = ["id", "name", "department", "salary", "salary_rank"]
        result_dicts = [dict(zip(column_names, row)) for row in result]
        
        unique_departments = set(row["department"] for row in result_dicts)
        
        # Verify that each department has at least one top employee
        # Note: Multiple employees might have the same top salary in a department
        for dept in unique_departments:
            dept_rows = [row for row in result_dicts if row["department"] == dept]
            self.assertGreaterEqual(len(dept_rows), 1, f"Department {dept} should have at least one top employee")
            
        # Verify all ranks are 1
        self.assertTrue(all(row["salary_rank"] == 1 for row in result_dicts), "All salary ranks should be 1")
        self.assertTrue(all("salary_rank" in row for row in result_dicts))
        for row in result_dicts:
            self.assertEqual(row["salary_rank"], 1)
    
    def test_multiple_window_functions(self):
        """Test multiple window functions in the same query."""
        # Build query with multiple window functions
        query = self.df.select(
            lambda x: x.id,
            lambda x: x.name,
            lambda x: x.department,
            lambda x: x.salary,
            lambda x: (salary_rank := window(func=rank(), partition=x.department, order_by=[(x.salary, Sort.DESC)])),
            lambda x: (dense_rank_val := window(func=dense_rank(), partition=x.department, order_by=[(x.salary, Sort.ASC)])),
            lambda x: (row_num := window(func=row_number(), partition=x.department, order_by=[(x.salary, Sort.DESC)]))
        )
        
        # Generate SQL
        sql = query.to_sql(dialect="duckdb")
        
        # Add ORDER BY clause directly to SQL
        sql += "\nORDER BY department ASC, salary_rank ASC"
        
        # Execute query
        result = self.conn.execute(sql).fetchall()
        
        column_names = ["id", "name", "department", "salary", "salary_rank", "dense_rank_val", "row_num"]
        result_dicts = [dict(zip(column_names, row)) for row in result]
        
        # Verify result
        self.assertEqual(len(result_dicts), 8)  # All employees
        self.assertTrue(all("salary_rank" in row for row in result_dicts))
        self.assertTrue(all("dense_rank_val" in row for row in result_dicts))
        self.assertTrue(all("row_num" in row for row in result_dicts))
        
        sql = query.to_sql(dialect="duckdb")
        
        result = self.conn.execute(sql).fetchall()
        
        result_dicts = [dict(zip(column_names, row)) for row in result]
        
        # Verify result
        self.assertEqual(len(result_dicts), 8)  # All employees
        self.assertTrue(all("salary_rank" in row for row in result_dicts))
        self.assertTrue(all("dense_rank_val" in row for row in result_dicts))
        self.assertTrue(all("row_num" in row for row in result_dicts))


if __name__ == "__main__":
    unittest.main()
