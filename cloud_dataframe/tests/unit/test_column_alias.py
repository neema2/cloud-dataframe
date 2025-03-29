"""
Unit tests for column aliasing using walrus operator.

This module contains tests for verifying column aliasing using the
walrus operator (:=) syntax in the cloud-dataframe library.
"""
import unittest
from typing import Optional, Dict

from cloud_dataframe.core.dataframe import DataFrame
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.type_system.column import sum, avg, count
from cloud_dataframe.utils.lambda_parser import LambdaParser


class TestColumnAlias(unittest.TestCase):
    """Test cases for column aliasing using walrus operator."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.employee_schema = TableSchema(
            name="Employee",
            columns={
                "id": int,
                "name": str,
                "department": str,
                "salary": float,
                "age": int,
                "is_manager": bool,
            }
        )
    
    def test_simple_column_alias(self):
        """Test simple column aliasing using walrus operator."""
        expr = LambdaParser.parse_lambda(
            lambda e: (employee_id := e.id),
            self.employee_schema
        )
        
        self.assertIsNotNone(expr)
        
        from cloud_dataframe.backends.duckdb.sql_generator import _generate_expression
        sql = _generate_expression(expr)
        expected_sql = "e.id AS employee_id"
        self.assertEqual(sql, expected_sql)
    
    def test_expression_alias(self):
        """Test expression aliasing using walrus operator."""
        expr = LambdaParser.parse_lambda(
            lambda e: (salary_bonus := e.salary * 1.1),
            self.employee_schema
        )
        
        self.assertIsNotNone(expr)
        
        from cloud_dataframe.backends.duckdb.sql_generator import _generate_expression
        sql = _generate_expression(expr)
        expected_sql = "(e.salary * 1.1) AS salary_bonus"
        self.assertEqual(sql, expected_sql)
    
    def test_function_call_alias(self):
        """Test function call aliasing using walrus operator."""
        expr = LambdaParser.parse_lambda(
            lambda e: (total_salary := sum(e.salary)),
            self.employee_schema
        )
        
        self.assertIsNotNone(expr)
        
        from cloud_dataframe.backends.duckdb.sql_generator import _generate_expression
        sql = _generate_expression(expr)
        expected_sql = "SUM(e.salary) AS total_salary"
        self.assertEqual(sql, expected_sql)
    
    def test_multiple_aliases_in_dataframe(self):
        """Test multiple aliases in a DataFrame select."""
        df = DataFrame.from_("employees", alias="e").select(
            lambda e: (employee_id := e.id),
            lambda e: (employee_name := e.name),
            lambda e: (department := e.department),
            lambda e: (annual_salary := e.salary * 12)
        )
        
        sql = df.to_sql(dialect="duckdb")
        expected_sql = "SELECT e.id AS employee_id, e.name AS employee_name, e.department AS department, (e.salary * 12) AS annual_salary\nFROM employees e"
        self.assertEqual(sql.strip(), expected_sql)

if __name__ == "__main__":
    unittest.main()
