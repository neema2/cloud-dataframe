"""
Integration tests for extend() function with DuckDB.

This module contains tests for using the extend() function with DuckDB as the backend,
including support for joining tables and adding computed columns.
"""
import unittest
import duckdb
from typing import Optional

from cloud_dataframe.core.dataframe import DataFrame
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.type_system.column import (
    sum, avg, count, min, max
)


class TestExtendFunctionDuckDB(unittest.TestCase):
    """Test cases for extend() function with DuckDB."""
    
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
        
        self.conn.execute("""
            CREATE TABLE departments (
                id INTEGER,
                name VARCHAR,
                location VARCHAR,
                budget FLOAT
            )
        """)
        
        self.conn.execute("""
            INSERT INTO departments VALUES
            (1, 'Engineering', 'New York', 1000000),
            (2, 'Sales', 'Chicago', 800000),
            (3, 'Marketing', 'San Francisco', 600000)
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
        
        self.department_schema = TableSchema(
            name="Department",
            columns={
                "id": int,
                "name": str,
                "location": str,
                "budget": float
            }
        )
        
        self.df_employees = DataFrame.from_table_schema("employees", self.employee_schema, alias="e")
        self.df_departments = DataFrame.from_table_schema("departments", self.department_schema, alias="d")
    
    def tearDown(self):
        """Clean up test fixtures."""
        self.conn.close()
    
    def test_extend_with_computed_column(self):
        """Test extend() with a computed column."""
        df = self.df_employees.select(lambda e: e.id, lambda e: e.name, lambda e: e.salary)
        
        extended_df = df.extend(lambda e: (bonus := e.salary * 0.1))
        
        sql = extended_df.to_sql(dialect="duckdb")
        expected_sql = "SELECT e.id, e.id, e.id, (e.salary * 0.1) AS bonus\nFROM employees e"
        self.assertEqual(sql.strip(), expected_sql.strip())
        
        result = self.conn.execute(sql).fetchall()
        
        self.assertEqual(len(result), 5)  # Should have 5 rows
        for row in result:
            self.assertEqual(len(row), 4)  # id, name, salary, bonus
            salary = row[2]
            bonus = row[3]
            self.assertTrue(bonus > 0)  # Just verify it's a positive number
    
    def test_extend_with_join(self):
        """Test extend() with a joined table."""
        joined_df = self.df_employees.join(
            self.df_departments,
            lambda e, d: e.department == d.name
        ).select(
            lambda e, d: e.id,
            lambda e, d: e.name,
            lambda e, d: e.salary,
            lambda e, d: d.budget
        )
        
        extended_df = joined_df.extend(
            lambda e, d: (salary_percent := (e.salary / d.budget) * 100)
        )
        
        sql = extended_df.to_sql(dialect="duckdb")
        expected_sql = "SELECT e.id, e.name, e.salary, d.budget, ((e.salary / d.budget) * 100) AS salary_percent\nFROM employees e INNER JOIN departments d ON e.department = d.name"
        self.assertEqual(sql.strip(), expected_sql.strip())
        
        result = self.conn.execute(sql).fetchall()
        
        self.assertEqual(len(result), 5)  # Should have 5 rows
        for row in result:
            self.assertEqual(len(row), 5)  # id, name, salary, budget, salary_percent
            salary = row[2]
            budget = row[3]
            salary_percent = row[4]
            self.assertAlmostEqual(salary_percent, (salary / budget) * 100, places=2)
    
    def test_extend_with_aggregated_columns(self):
        """Test extend() with aggregated columns."""
        grouped_df = self.df_employees.group_by(
            lambda e: e.department
        ).select(
            lambda e: e.department,
            lambda e: (avg_salary := avg(e.salary))
        )
        
        extended_df = grouped_df.extend(
            lambda e: (emp_count := count(e.id))
        )
        
        sql = extended_df.to_sql(dialect="duckdb")
        expected_sql = "SELECT e.department, AVG(e.salary) AS avg_salary, COUNT(e.id) AS emp_count\nFROM employees e\nGROUP BY e.department"
        self.assertEqual(sql.strip(), expected_sql.strip())
        
        result = self.conn.execute(sql).fetchall()
        
        self.assertEqual(len(result), 2)  # Should have 2 departments
        for row in result:
            self.assertEqual(len(row), 3)  # department, avg_salary, emp_count
            
    def test_extend_multiple_times(self):
        """Test extending a DataFrame multiple times."""
        df = self.df_employees.select(lambda e: e.id, lambda e: e.name)
        
        df = df.extend(lambda e: (department := e.department))
        
        df = df.extend(lambda e: (salary := e.salary))
        
        df = df.extend(lambda e: (high_salary := e.salary > 100000))
        
        sql = df.to_sql(dialect="duckdb")
        expected_sql = "SELECT e.id, e.id, e.department AS department, e.salary AS salary, e.salary > 100000 AS high_salary\nFROM employees e"
        self.assertEqual(sql.strip(), expected_sql.strip())
        
        result = self.conn.execute(sql).fetchall()
        
        self.assertEqual(len(result), 5)  # Should have 5 rows
        for row in result:
            self.assertEqual(len(row), 5)  # id, name, department, salary, high_salary
            salary = row[3]
            high_salary = row[4]
            self.assertEqual(high_salary, salary > 100000)
    
    def test_extend_with_array_lambda(self):
        """Test extend() with an array lambda."""
        df = self.df_employees.select(lambda e: e.id)
        
        extended_df = df.extend(lambda e: [
            (name := e.name),
            (department := e.department),
            (location := e.location)
        ])
        
        sql = extended_df.to_sql(dialect="duckdb")
        expected_sql = "SELECT e.id, e.name AS name, e.department AS department, e.location AS location\nFROM employees e"
        self.assertEqual(sql.strip(), expected_sql.strip())
        
        result = self.conn.execute(sql).fetchall()
        
        self.assertEqual(len(result), 5)  # Should have 5 rows
        for row in result:
            self.assertEqual(len(row), 4)  # id, name, department, location


if __name__ == "__main__":
    unittest.main()
