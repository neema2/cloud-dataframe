"""
Integration tests for join examples with DuckDB.

This module contains tests for join operations using lambda expressions
with the cloud-dataframe library.
"""
import unittest
import duckdb
from typing import Optional, Dict, List, Any, Tuple

from cloud_dataframe.core.dataframe import DataFrame
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.type_system.column import sum, avg, count


class TestJoinExamples(unittest.TestCase):
    """Test cases for join operations with lambda expressions."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Create a DuckDB connection
        self.conn = duckdb.connect(":memory:")
        
        self.conn.execute("""
            CREATE TABLE employees AS
            SELECT 1 AS id, 'Alice' AS name, 1 AS department_id, 80000.0 AS salary UNION ALL
            SELECT 2, 'Bob', 1, 90000.0 UNION ALL
            SELECT 3, 'Charlie', 2, 70000.0 UNION ALL
            SELECT 4, 'David', 2, 75000.0 UNION ALL
            SELECT 5, 'Eve', 3, 65000.0 UNION ALL
            SELECT 6, 'Frank', 3, 60000.0
        """)
        
        self.conn.execute("""
            CREATE TABLE departments AS
            SELECT 1 AS id, 'Engineering' AS name, 'New York' AS location, 1000000.0 AS budget UNION ALL
            SELECT 2, 'Sales', 'San Francisco', 800000.0 UNION ALL
            SELECT 3, 'Marketing', 'Chicago', 600000.0 UNION ALL
            SELECT 4, 'HR', 'Boston', 400000.0
        """)
        
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
        result = self.conn.execute(direct_sql).fetchall()
        
        column_names = ["employee_id", "employee_name", "department_name", "department_location", "employee_salary"]
        result_dicts = [dict(zip(column_names, row)) for row in result]
        
        # Verify result
        self.assertEqual(len(result_dicts), 6)  # All employees should match
        self.assertTrue(all("department_name" in row for row in result_dicts))
        self.assertTrue(all("department_location" in row for row in result_dicts))
    
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
        
        result = self.conn.execute(sql).fetchall()
        
        column_names = ["id", "name", "department_name", "location", "salary"]
        result_dicts = [dict(zip(column_names, row)) for row in result]
        
        # Verify result
        self.assertEqual(len(result_dicts), 6)  # All employees should be included
    
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
        
        result = self.conn.execute(sql).fetchall()
        
        column_names = ["department_name", "employee_count", "total_salary", "avg_salary"]
        result_dicts = [dict(zip(column_names, row)) for row in result]
        
        # Verify result
        self.assertEqual(len(result_dicts), 3)  # Three departments with employees
        self.assertTrue(all("department_name" in row for row in result_dicts))
        self.assertTrue(all("employee_count" in row for row in result_dicts))
        self.assertTrue(all("total_salary" in row for row in result_dicts))
        self.assertTrue(all("avg_salary" in row for row in result_dicts))


if __name__ == "__main__":
    unittest.main()
