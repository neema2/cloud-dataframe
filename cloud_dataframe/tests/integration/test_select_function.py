"""
Integration tests for select() function with DuckDB.

This module contains tests to demonstrate the column naming behavior in select(),
including the issue with duplicate column references in the generated SQL.
"""
import unittest
import duckdb
from typing import Optional

from cloud_dataframe.core.dataframe import DataFrame
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.type_system.column import (
    sum, avg, count, min, max
)


class TestSelectFunctionDuckDB(unittest.TestCase):
    """Test cases for select() function with DuckDB."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.conn = duckdb.connect(":memory:")
        
        self.conn.execute("""
            CREATE TABLE employees (
                id INTEGER,
                name VARCHAR,
                department VARCHAR,
                location VARCHAR,
                salary FLOAT
            )
        """)
        
        self.conn.execute("""
            INSERT INTO employees VALUES
            (1, 'Alice', 'Engineering', 'New York', 120000),
            (2, 'Bob', 'Engineering', 'San Francisco', 110000),
            (3, 'Charlie', 'Engineering', 'New York', 95000),
            (4, 'David', 'Sales', 'Chicago', 85000),
            (5, 'Eve', 'Sales', 'Chicago', 90000)
        """)
        
        self.employee_schema = TableSchema(
            name="Employee",
            columns={
                "id": int,
                "name": str,
                "department": str,
                "location": str,
                "salary": float
            }
        )
        
        self.df_employees = DataFrame.from_table_schema("employees", self.employee_schema, alias="e")
    
    def tearDown(self):
        """Clean up test fixtures."""
        self.conn.close()
    
    def test_select_single_column(self):
        """Test select() with a single column."""
        df = self.df_employees.select(lambda e: e.id)
        
        sql = df.to_sql(dialect="duckdb")
        expected_sql = "SELECT e.id\nFROM employees e"
        self.assertEqual(sql.strip(), expected_sql.strip())
        
        result = self.conn.execute(sql).fetchall()
        self.assertEqual(len(result), 5)  # Should have 5 rows
        for row in result:
            self.assertEqual(len(row), 1)  # Should have 1 column
    
    def test_select_multiple_columns(self):
        """Test select() with multiple columns."""
        df = self.df_employees.select(
            lambda e: e.id,
            lambda e: e.name,
            lambda e: e.salary
        )
        
        sql = df.to_sql(dialect="duckdb")
        
        expected_sql = "SELECT e.id, e.name, e.salary\nFROM employees e"
        self.assertEqual(sql.strip(), expected_sql.strip())
        
        result = self.conn.execute(sql).fetchall()
        self.assertEqual(len(result), 5)  # Should have 5 rows
        for row in result:
            self.assertEqual(len(row), 3)  # Should have 3 columns
    
    def test_select_with_aliases(self):
        """Test select() with column aliases."""
        df = self.df_employees.select(
            lambda e: (employee_id := e.id),
            lambda e: (employee_name := e.name),
            lambda e: (employee_salary := e.salary)
        )
        
        sql = df.to_sql(dialect="duckdb")
        
        expected_sql = "SELECT e.id AS employee_id, e.name AS employee_name, e.salary AS employee_salary\nFROM employees e"
        self.assertEqual(sql.strip(), expected_sql.strip())
        
        result = self.conn.execute(sql).fetchall()
        self.assertEqual(len(result), 5)  # Should have 5 rows
        for row in result:
            self.assertEqual(len(row), 3)  # Should have 3 columns
    
    def test_select_with_computed_columns(self):
        """Test select() with computed columns."""
        df = self.df_employees.select(
            lambda e: e.id,
            lambda e: e.name,
            lambda e: (bonus := e.salary * 0.1)
        )
        
        sql = df.to_sql(dialect="duckdb")
        
        expected_sql = "SELECT e.id, e.name, (e.salary * 0.1) AS bonus\nFROM employees e"
        self.assertEqual(sql.strip(), expected_sql.strip())
        
        result = self.conn.execute(sql).fetchall()
        self.assertEqual(len(result), 5)  # Should have 5 rows
        for row in result:
            self.assertEqual(len(row), 3)  # Should have 3 columns
            self.assertTrue(isinstance(row[2], float))  # Bonus should be a float
    
    def test_select_with_boolean_expressions(self):
        """Test select() with boolean expressions."""
        df = self.df_employees.select(
            lambda e: e.id,
            lambda e: e.name,
            lambda e: (high_salary := e.salary > 100000)
        )
        
        sql = df.to_sql(dialect="duckdb")
        
        expected_sql = "SELECT e.id, e.name, e.salary > 100000 AS high_salary\nFROM employees e"
        self.assertEqual(sql.strip(), expected_sql.strip())
        
        result = self.conn.execute(sql).fetchall()
        self.assertEqual(len(result), 5)  # Should have 5 rows
        for row in result:
            self.assertEqual(len(row), 3)  # Should have 3 columns
            self.assertTrue(isinstance(row[2], bool))  # high_salary should be a boolean
    
    def test_select_with_array_lambda(self):
        """Test select() with an array lambda."""
        df = self.df_employees.select(lambda e: [
            e.id,
            e.name,
            (high_salary := e.salary > 100000)
        ])
        
        sql = df.to_sql(dialect="duckdb")
        
        expected_sql = "SELECT e.id, e.name, e.salary > 100000 AS high_salary\nFROM employees e"
        self.assertEqual(sql.strip(), expected_sql.strip())
        
        result = self.conn.execute(sql).fetchall()
        self.assertEqual(len(result), 5)  # Should have 5 rows
        for row in result:
            self.assertEqual(len(row), 3)  # Should have 3 columns
    
    def test_select_with_aggregates(self):
        """Test select() with aggregate functions."""
        df = self.df_employees.group_by(
            lambda e: e.department
        ).select(
            lambda e: e.department,
            lambda e: (avg_salary := avg(e.salary)),
            lambda e: (emp_count := count(e.id))
        )
        
        sql = df.to_sql(dialect="duckdb")
        
        expected_sql = "SELECT e.department, AVG(e.salary) AS avg_salary, COUNT(e.id) AS emp_count\nFROM employees e\nGROUP BY e.department"
        self.assertEqual(sql.strip(), expected_sql.strip())
        
        result = self.conn.execute(sql).fetchall()
        self.assertEqual(len(result), 2)  # Should have 2 departments
        for row in result:
            self.assertEqual(len(row), 3)  # Should have 3 columns


if __name__ == "__main__":
    unittest.main()
