"""
Integration tests for having() function with DuckDB.

This module contains tests for using different lambda formats in having() function
with DuckDB as the backend, including support for referencing new columns with df parameter.
"""
import unittest
import duckdb
from typing import Optional

from cloud_dataframe.core.dataframe import DataFrame
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.type_system.column import (
    sum, avg, count, min, max
)


class TestHavingFunctionDuckDB(unittest.TestCase):
    """Test cases for having() function with DuckDB."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.conn = duckdb.connect(":memory:")
        
        self.conn.execute("""
            CREATE TABLE employees (
                id INTEGER,
                name VARCHAR,
                department VARCHAR,
                location VARCHAR,
                salary FLOAT,
                hire_date DATE,
                is_manager BOOLEAN,
                manager_id INTEGER
            )
        """)
        
        self.conn.execute("""
            INSERT INTO employees VALUES
            (1, 'Alice', 'Engineering', 'New York', 120000, '2020-01-15', true, NULL),
            (2, 'Bob', 'Engineering', 'San Francisco', 110000, '2021-03-10', false, 1),
            (3, 'Charlie', 'Engineering', 'New York', 95000, '2022-05-20', false, 1),
            (4, 'David', 'Sales', 'Chicago', 85000, '2019-11-05', true, NULL),
            (5, 'Eve', 'Sales', 'Chicago', 90000, '2020-08-12', false, 4),
            (6, 'Frank', 'Marketing', 'New York', 105000, '2021-02-28', true, NULL),
            (7, 'Grace', 'Marketing', 'San Francisco', 95000, '2022-01-10', false, 6),
            (8, 'Heidi', 'HR', 'Chicago', 80000, '2019-06-15', true, NULL)
        """)
        
        self.schema = TableSchema(
            name="Employee",
            columns={
                "id": int,
                "name": str,
                "department": str,
                "location": str,
                "salary": float,
                "hire_date": str,
                "is_manager": bool,
                "manager_id": Optional[int]
            }
        )
        
        self.df = DataFrame.from_table_schema("employees", self.schema)
    
    def tearDown(self):
        """Clean up test fixtures."""
        self.conn.close()
    
    def test_having_with_original_column_reference(self):
        """Test having() with original column reference (lambda x: x.column)."""
        df_with_having = self.df.select(
            lambda x: x.department,
            lambda x: (avg_salary := avg(x.salary))
        ).group_by(
            lambda x: x.department
        ).having(
            lambda x: x.department != 'HR'
        )
        
        sql = df_with_having.to_sql(dialect="duckdb")
        expected_sql = "SELECT x.department, AVG(x.salary) AS avg_salary\nFROM employees x\nGROUP BY x.department\nHAVING x.department != 'HR'"
        self.assertEqual(sql.strip(), expected_sql.strip())
        
        result = self.conn.execute(sql).fetchall()
        self.assertEqual(len(result), 3)  # Should have 3 departments (Engineering, Sales, Marketing)
        
        departments = [row[0] for row in result]
        self.assertNotIn('HR', departments)
    
    def test_having_with_aggregate_function(self):
        """Test having() with aggregate function (lambda x: avg(x.salary) > 100000)."""
        df_with_having = self.df.select(
            lambda x: x.department,
            lambda x: (avg_salary := avg(x.salary))
        ).group_by(
            lambda x: x.department
        ).having(
            lambda x: avg(x.salary) > 100000
        )
        
        sql = df_with_having.to_sql(dialect="duckdb")
        expected_sql = "SELECT x.department, AVG(x.salary) AS avg_salary\nFROM employees x\nGROUP BY x.department\nHAVING AVG(x.salary) > 100000"
        self.assertEqual(sql.strip(), expected_sql.strip())
        
        result = self.conn.execute(sql).fetchall()
        self.assertEqual(len(result), 1)  # Should have 1 department (Engineering)
        
        self.assertEqual(result[0][0], 'Engineering')
    
    def test_having_with_new_column_reference(self):
        """Test having() with new column reference (lambda df: df.new_col > value)."""
        df_with_having = self.df.select(
            lambda x: x.department,
            lambda x: (avg_salary := avg(x.salary))
        ).group_by(
            lambda x: x.department
        ).having(
            lambda df: df.avg_salary > 100000
        )
        
        sql = df_with_having.to_sql(dialect="duckdb")
        expected_sql = "SELECT x.department, AVG(x.salary) AS avg_salary\nFROM employees x\nGROUP BY x.department\nHAVING avg_salary > 100000"
        self.assertEqual(sql.strip(), expected_sql.strip())
        
        result = self.conn.execute(sql).fetchall()
        self.assertEqual(len(result), 1)  # Should have 1 department (Engineering)
        
        self.assertEqual(result[0][0], 'Engineering')
    
    def test_having_with_multiple_new_column_references(self):
        """Test having() with multiple new column references."""
        df_with_having = self.df.select(
            lambda x: x.department,
            lambda x: (avg_salary := avg(x.salary)),
            lambda x: (emp_count := count(x.id))
        ).group_by(
            lambda x: x.department
        ).having(
            lambda df: (df.avg_salary > 85000) and (df.emp_count > 1)
        )
        
        sql = df_with_having.to_sql(dialect="duckdb")
        expected_sql = "SELECT x.department, AVG(x.salary) AS avg_salary, COUNT(x.id) AS emp_count\nFROM employees x\nGROUP BY x.department\nHAVING avg_salary > 85000 AND emp_count > 1"
        self.assertEqual(sql.strip(), expected_sql.strip())
        
        result = self.conn.execute(sql).fetchall()
        self.assertEqual(len(result), 3)  # Should have 3 departments (Engineering, Sales, Marketing)
        
        departments = [row[0] for row in result]
        self.assertIn('Engineering', departments)
        self.assertIn('Sales', departments)
    
    def test_having_with_mixed_references(self):
        """Test having() with both original and new column references (lambda df, x: df.new_col > value and x.column == value)."""
        df_with_having = self.df.select(
            lambda x: x.department,
            lambda x: x.location,
            lambda x: (avg_salary := avg(x.salary)),
            lambda x: (emp_count := count(x.id))
        ).group_by(
            lambda x: x.department,
            lambda x: x.location
        ).having(
            lambda df, x: (df.avg_salary > 90000) and (x.location == 'New York')
        )
        
        sql = df_with_having.to_sql(dialect="duckdb")
        expected_sql = "SELECT x.department, x.location, AVG(x.salary) AS avg_salary, COUNT(x.id) AS emp_count\nFROM employees x\nGROUP BY x.department, x.location\nHAVING avg_salary > 90000 AND x.location = 'New York'"
        self.assertEqual(sql.strip(), expected_sql.strip())
        
        result = self.conn.execute(sql).fetchall()
        self.assertEqual(len(result), 2)  # Should have 2 rows (Engineering/New York, Marketing/New York)
        
        for row in result:
            self.assertEqual(row[1], 'New York')
            
        departments = [row[0] for row in result]
        self.assertIn('Engineering', departments)
        self.assertIn('Marketing', departments)
    
    def test_having_with_complex_condition(self):
        """Test having() with complex condition using multiple operators."""
        df_with_having = self.df.select(
            lambda x: x.department,
            lambda x: (min_salary := min(x.salary)),
            lambda x: (max_salary := max(x.salary)),
            lambda x: (salary_range := max(x.salary) - min(x.salary))
        ).group_by(
            lambda x: x.department
        ).having(
            lambda df: (df.min_salary > 80000) and (df.salary_range < 30000)
        )
        
        sql = df_with_having.to_sql(dialect="duckdb")
        expected_sql = "SELECT x.department, MIN(x.salary) AS min_salary, MAX(x.salary) AS max_salary, (MAX(x.salary) - MIN(x.salary)) AS salary_range\nFROM employees x\nGROUP BY x.department\nHAVING min_salary > 80000 AND salary_range < 30000"
        self.assertEqual(sql.strip(), expected_sql.strip())
        
        result = self.conn.execute(sql).fetchall()
        
        for row in result:
            min_sal = row[1]
            max_sal = row[2]
            sal_range = row[3]
            
            self.assertTrue(min_sal > 80000)
            self.assertTrue(sal_range < 30000)
    
    def test_having_with_mixed_aggregate_functions(self):
        """Test having() with mixed df and x parameters where both use aggregate functions."""
        df_with_having = self.df.select(
            lambda x: x.department,
            lambda x: (avg_salary := avg(x.salary)),
            lambda x: (emp_count := count(x.id))
        ).group_by(
            lambda x: x.department
        ).having(
            lambda df, x: (df.avg_salary > 90000) and (avg(x.salary) > 100000)
        )
        
        sql = df_with_having.to_sql(dialect="duckdb")
        expected_sql = "SELECT x.department, AVG(x.salary) AS avg_salary, COUNT(x.id) AS emp_count\nFROM employees x\nGROUP BY x.department\nHAVING avg_salary > 90000 AND AVG(x.salary) > 100000"
        self.assertEqual(sql.strip(), expected_sql.strip())
        
        result = self.conn.execute(sql).fetchall()
        self.assertEqual(len(result), 1)  # Should have 1 department (Engineering)
        
        self.assertEqual(result[0][0], 'Engineering')


if __name__ == "__main__":
    unittest.main()
