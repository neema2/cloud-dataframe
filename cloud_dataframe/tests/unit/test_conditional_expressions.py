"""
Unit tests for conditional expressions in lambda parser.

This module contains tests for evaluating conditional expressions
in the lambda parser of the cloud-dataframe library.
"""
import unittest
from typing import Optional, Dict

from cloud_dataframe.core.dataframe import DataFrame
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.type_system.column import sum, avg, count
from cloud_dataframe.utils.lambda_parser import LambdaParser
from cloud_dataframe.backends.duckdb.sql_generator import _generate_expression

class TestConditionalExpressions(unittest.TestCase):
    """Test cases for conditional expressions in lambda parser."""
    
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
    
    def test_simple_if_else(self):
        """Test simple if-else statement."""
        expr = LambdaParser.parse_lambda(
            lambda e: e.salary if e.is_manager else e.salary * 0.8,
            self.employee_schema
        )
        
        self.assertIsNotNone(expr)
        
        sql = _generate_expression(expr)
        expected_sql = "CASE WHEN e.is_manager THEN e.salary ELSE (e.salary * 0.8) END"
        self.assertEqual(sql, expected_sql)
    
    def test_nested_if_else(self):
        """Test nested if-else conditions."""
        expr = LambdaParser.parse_lambda(
            lambda e: e.salary * 1.2 if e.is_manager else (e.salary * 1.1 if e.age > 40 else e.salary),
            self.employee_schema
        )
        
        self.assertIsNotNone(expr)
        
        sql = _generate_expression(expr)
        expected_sql = "CASE WHEN e.is_manager THEN (e.salary * 1.2) ELSE CASE WHEN e.age > 40 THEN (e.salary * 1.1) ELSE e.salary END END"
        self.assertEqual(sql, expected_sql)
    
    def test_if_else_with_column_references(self):
        """Test if-else with column references."""
        expr = LambdaParser.parse_lambda(
            lambda e: e.department if e.salary > 50000 else "Other",
            self.employee_schema
        )
        
        self.assertIsNotNone(expr)
        
        sql = _generate_expression(expr)
        expected_sql = "CASE WHEN e.salary > 50000 THEN e.department ELSE 'Other' END"
        self.assertEqual(sql, expected_sql)
    
    def test_if_else_with_calculations(self):
        """Test if-else with calculations."""
        expr = LambdaParser.parse_lambda(
            lambda e: (e.salary * 1.1) if (e.department == "Engineering" and e.age > 30) else e.salary,
            self.employee_schema
        )
        
        self.assertIsNotNone(expr)
        
        sql = _generate_expression(expr)
        expected_sql = "CASE WHEN e.department = 'Engineering' AND e.age > 30 THEN (e.salary * 1.1) ELSE e.salary END"
        self.assertEqual(sql, expected_sql)

if __name__ == "__main__":
    unittest.main()
