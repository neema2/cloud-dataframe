"""
Integration tests for scalar functions in the DataFrame DSL.

This module contains tests that verify the SQL generation and execution
of scalar functions in the cloud-dataframe DSL.
"""
import unittest
import os
import duckdb
from cloud_dataframe.core.dataframe import DataFrame
from cloud_dataframe.type_system.column import col, literal, count, avg, sum
from cloud_dataframe.functions.registry import FunctionRegistry


class TestScalarFunctionsIntegration(unittest.TestCase):
    """Test cases for scalar functions integration."""
    
    @classmethod
    def setUpClass(cls):
        """Set up the test database."""
        cls.db_path = "test_scalar_functions.db"
        if os.path.exists(cls.db_path):
            os.remove(cls.db_path)
        
        cls.conn = duckdb.connect(cls.db_path)
        
        cls.conn.execute("""
        CREATE TABLE employees (
            id INTEGER,
            name VARCHAR,
            department_id INTEGER,
            salary DOUBLE,
            hire_date DATE,
            end_date DATE
        )
        """)
        
        cls.conn.execute("""
        INSERT INTO employees VALUES
            (1, 'Alice', 1, 75000, '2020-01-15', '2023-05-20'),
            (2, 'Bob', 1, 65000, '2019-03-10', '2023-06-30'),
            (3, 'Charlie', 2, 85000, '2021-02-05', '2024-01-15'),
            (4, 'Diana', 2, 78000, '2018-11-20', '2023-12-31'),
            (5, 'Eve', 3, 95000, '2017-07-01', '2023-10-15')
        """)
    
    @classmethod
    def tearDownClass(cls):
        """Clean up the test database."""
        cls.conn.close()
        if os.path.exists(cls.db_path):
            os.remove(cls.db_path)
    
    def test_string_functions_with_literals(self):
        """Test string functions with literal values."""
        df = DataFrame.from_("employees", alias="e")
        
        upper_df = df.select(
            lambda e: e.id,
            lambda e: e.name,
            lambda e: (upper_name := upper(e.name))
        )
        
        sql = upper_df.to_sql(dialect="duckdb")
        expected_sql = """SELECT e.id, e.name, upper(e.name) AS upper_name
FROM employees e"""
        
        self.assertEqual(sql.strip(), expected_sql.strip())
        
        result = self.conn.execute(sql).fetchall()
        self.assertEqual(len(result), 5)
        
        for row in result:
            self.assertEqual(row[2], row[1].upper())
    
    def test_string_functions_with_expressions(self):
        """Test string functions with complex expressions."""
        df = DataFrame.from_("employees", alias="e")
        
        concat_df = df.select(
            lambda e: e.id,
            lambda e: (full_info := concat(e.name, " (ID: ", e.id, ", Dept: ", e.department_id, ")"))
        )
        
        sql = concat_df.to_sql(dialect="duckdb")
        expected_sql = """SELECT e.id, concat(e.name, " (ID: ", e.id, ", Dept: ", e.department_id, ")") AS full_info
FROM employees e"""
        
        self.assertEqual(sql.strip(), expected_sql.strip())
        
        result = self.conn.execute(sql).fetchall()
        self.assertEqual(len(result), 5)
        
        for row in result:
            expected = f"{row[0]}"  # This is the ID
            self.assertIn(expected, row[1])  # Check that ID is in the concatenated string
    
    def test_date_functions_with_literals(self):
        """Test date functions with literal values."""
        df = DataFrame.from_("employees", alias="e")
        
        date_diff_df = df.select(
            lambda e: e.id,
            lambda e: e.name,
            lambda e: (days_employed := date_diff("day", e.hire_date, e.end_date))
        )
        
        sql = date_diff_df.to_sql(dialect="duckdb")
        expected_sql = """SELECT e.id, e.name, date_diff("day", e.hire_date, e.end_date) AS days_employed
FROM employees e"""
        
        self.assertEqual(sql.strip(), expected_sql.strip())
        
        result = self.conn.execute(sql).fetchall()
        self.assertEqual(len(result), 5)
        
        for row in result:
            self.assertGreater(row[2], 0)
    
    def test_date_functions_with_expressions(self):
        """Test date functions with complex expressions."""
        df = DataFrame.from_("employees", alias="e")
        
        date_add_df = df.select(
            lambda e: e.id,
            lambda e: e.name,
            lambda e: (extended_date := date_add("month", 6, e.end_date))
        )
        
        sql = date_add_df.to_sql(dialect="duckdb")
        expected_sql = """SELECT e.id, e.name, date_add("month", 6, e.end_date) AS extended_date
FROM employees e"""
        
        self.assertEqual(sql.strip(), expected_sql.strip())
        
        result = self.conn.execute(sql).fetchall()
        self.assertEqual(len(result), 5)
    
    def test_numeric_functions_with_literals(self):
        """Test numeric functions with literal values."""
        df = DataFrame.from_("employees", alias="e")
        
        round_df = df.select(
            lambda e: e.id,
            lambda e: e.name,
            lambda e: (rounded_salary := round(e.salary / 1000, 1))
        )
        
        sql = round_df.to_sql(dialect="duckdb")
        expected_sql = """SELECT e.id, e.name, round((e.salary / 1000), 1) AS rounded_salary
FROM employees e"""
        
        self.assertEqual(sql.strip(), expected_sql.strip())
        
        result = self.conn.execute(sql).fetchall()
        self.assertEqual(len(result), 5)
    
    def test_numeric_functions_with_expressions(self):
        """Test numeric functions with complex expressions."""
        df = DataFrame.from_("employees", alias="e")
        
        abs_df = df.select(
            lambda e: e.id,
            lambda e: e.name,
            lambda e: (salary_diff := abs(e.salary - 80000))
        )
        
        sql = abs_df.to_sql(dialect="duckdb")
        expected_sql = """SELECT e.id, e.name, abs((e.salary - 80000)) AS salary_diff
FROM employees e"""
        
        self.assertEqual(sql.strip(), expected_sql.strip())
        
        result = self.conn.execute(sql).fetchall()
        self.assertEqual(len(result), 5)
        
        for row in result:
            self.assertGreaterEqual(row[2], 0)
    
    def test_scalar_function_in_filter(self):
        """Test using scalar functions in filter conditions."""
        df = DataFrame.from_("employees", alias="e")
        
        filtered_df = df.filter(lambda e: date_diff("day", e.hire_date, e.end_date) > 365)
        
        sql = filtered_df.to_sql(dialect="duckdb")
        expected_sql = """SELECT *
FROM employees e
WHERE date_diff("day", e.hire_date, e.end_date) > 365"""
        
        self.assertEqual(sql.strip(), expected_sql.strip())
        
        result = self.conn.execute(sql).fetchall()
        
        self.assertEqual(len(result), 5)
    
    def test_multiple_scalar_functions(self):
        """Test using multiple scalar functions together."""
        df = DataFrame.from_("employees", alias="e")
        
        complex_df = df.select(
            lambda e: e.id,
            lambda e: (upper_name := upper(e.name)),
            lambda e: (years_employed := round((date_diff("day", e.hire_date, e.end_date) / 365), 1)),
            lambda e: (salary_k := concat(round(e.salary / 1000, 0), "K"))
        )
        
        sql = complex_df.to_sql(dialect="duckdb")
        expected_sql = """SELECT e.id, upper(e.name) AS upper_name, round((date_diff("day", e.hire_date, e.end_date) / 365), 1) AS years_employed, concat(round((e.salary / 1000), 0), "K") AS salary_k
FROM employees e"""
        
        self.assertEqual(sql.strip(), expected_sql.strip())
        
        result = self.conn.execute(sql).fetchall()
        self.assertEqual(len(result), 5)
        
        for row in result:
            self.assertTrue(row[1].isupper())
            self.assertGreater(row[2], 0)  # years_employed should be positive
            self.assertTrue(row[3].endswith('K'))  # salary_k should end with 'K'


if __name__ == "__main__":
    unittest.main()
