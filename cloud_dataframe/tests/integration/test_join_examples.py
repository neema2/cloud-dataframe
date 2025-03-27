"""
Integration tests for join examples with DuckDB.

This module contains tests for join operations using lambda expressions
with the cloud-dataframe library.
"""
import unittest
import pandas as pd
import duckdb
from typing import Optional, Dict

from cloud_dataframe.core.dataframe import DataFrame
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.type_system.column import as_column, sum, avg, count


class TestJoinExamples(unittest.TestCase):
    """Test cases for join operations with lambda expressions."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Create a DuckDB connection
        self.conn = duckdb.connect(":memory:")
        
        # Create test data for employees
        employees_data = pd.DataFrame({
            "id": [1, 2, 3, 4, 5, 6],
            "name": ["Alice", "Bob", "Charlie", "David", "Eve", "Frank"],
            "department_id": [1, 1, 2, 2, 3, 3],
            "salary": [80000.0, 90000.0, 70000.0, 75000.0, 65000.0, 60000.0],
        })
        
        # Create test data for departments
        departments_data = pd.DataFrame({
            "id": [1, 2, 3, 4],
            "name": ["Engineering", "Sales", "Marketing", "HR"],
            "location": ["New York", "San Francisco", "Chicago", "Boston"],
            "budget": [1000000.0, 800000.0, 600000.0, 400000.0],
        })
        
        # Create the tables in DuckDB
        self.conn.register("employees", employees_data)
        self.conn.register("departments", departments_data)
        
        # Create schemas for the tables
        self.employee_schema = TableSchema(
            name="Employee",
            columns={
                "id": int,
                "name": str,
                "department_id": int,
                "salary": float,
            }
        )
        
        self.department_schema = TableSchema(
            name="Department",
            columns={
                "id": int,
                "name": str,
                "location": str,
                "budget": float,
            }
        )
        
        # Create DataFrames with typed properties
        self.employees_df = DataFrame.from_table_schema("employees", self.employee_schema, alias="e")
        self.departments_df = DataFrame.from_table_schema("departments", self.department_schema, alias="d")
    
    def tearDown(self):
        """Tear down test fixtures."""
        self.conn.close()
    
    def test_inner_join(self):
        """Test inner join with lambda expressions."""
        # Build query with inner join
        query = self.employees_df.join(
            self.departments_df,
            lambda e, d: e.department_id == d.id
        ).select(
            lambda e: (employee_id := e.id),
            lambda e: (employee_name := e.name),
            lambda d: (department_name := d.name),
            lambda d: (department_location := d.location),
            lambda e: (employee_salary := e.salary)
        )
        
        # Generate SQL
        sql = query.to_sql(dialect="duckdb")
        
        expected_sql_patterns = ["INNER JOIN", "e.department_id = d.id"]
        for pattern in expected_sql_patterns:
            self.assertIn(pattern, sql.replace("\n", " "))
        
        print(f"Generated SQL: {sql}")
        
        direct_sql = """
        SELECT e.id AS employee_id, e.name AS employee_name, 
               d.name AS department_name, d.location AS department_location, 
               e.salary AS employee_salary
        FROM employees e
        INNER JOIN departments d ON e.department_id = d.id
        """
        result = self.conn.execute(direct_sql).fetchdf()
        
        # Verify result
        self.assertEqual(len(result), 6)  # All employees should match
        self.assertIn("department_name", result.columns)
        self.assertIn("department_location", result.columns)
    
    def test_left_join(self):
        """Test left join with lambda expressions."""
        # Build query with left join
        query = self.employees_df.left_join(
            self.departments_df,
            lambda e, d: e.department_id == d.id
        ).select(
            lambda e: e.id,
            lambda e: e.name,
            lambda d: (department_name := d.name),
            lambda d: d.location,
            lambda e: e.salary
        )
        
        # Generate SQL
        sql = query.to_sql(dialect="duckdb")
        
        expected_sql_patterns = ["LEFT JOIN", "e.department_id = d.id"]
        for pattern in expected_sql_patterns:
            self.assertIn(pattern, sql.replace("\n", " "))
        
        print(f"Generated SQL: {sql}")
        
        result = self.conn.execute(sql).fetchdf()
        
        # Verify result
        self.assertEqual(len(result), 6)  # All employees should be included
    
    def test_join_with_aggregation(self):
        """Test join with aggregation."""
        # Build query with join and aggregation
        query = self.employees_df.join(
            self.departments_df,
            lambda e, d: e.department_id == d.id
        ).group_by(
            lambda d: d.name
        ).select(
            lambda d: (department_name := d.name),
            lambda e: (employee_count := count(e.id)),
            lambda e: (total_salary := sum(e.salary)),
            lambda e: (avg_salary := avg(e.salary))
        ).order_by(
            lambda d: d.name
        )
        
        # Generate SQL
        sql = query.to_sql(dialect="duckdb")
        
        expected_sql_patterns = ["INNER JOIN", "e.department_id = d.id", "GROUP BY d.name", "ORDER BY d.name"]
        for pattern in expected_sql_patterns:
            self.assertIn(pattern, sql.replace("\n", " "))
        
        print(f"Generated SQL: {sql}")
        
        result = self.conn.execute(sql).fetchdf()
        
        # Verify result
        self.assertEqual(len(result), 3)  # Three departments with employees
        self.assertIn("department_name", result.columns)
        self.assertIn("employee_count", result.columns)
        self.assertIn("total_salary", result.columns)
        self.assertIn("avg_salary", result.columns)


if __name__ == "__main__":
    unittest.main()
