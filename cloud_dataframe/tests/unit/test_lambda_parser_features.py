"""
Unit tests for lambda parser features.

This module contains tests for various features of the lambda parser
in the cloud-dataframe library.
"""
import unittest
from typing import Optional, Dict

from cloud_dataframe.core.dataframe import DataFrame, BinaryOperation
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.type_system.column import (
    Expression, ColumnReference, LiteralExpression,
    sum, avg, count
)
from cloud_dataframe.utils.lambda_parser import LambdaParser
from cloud_dataframe.backends.duckdb.sql_generator import _generate_expression

class TestLambdaParserFeatures(unittest.TestCase):
    """Test cases for lambda parser features."""
    
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
        
        self.department_schema = TableSchema(
            name="Department",
            columns={
                "id": int,
                "name": str,
                "location": str,
                "budget": float,
            }
        )
    
    def test_single_column_reference(self):
        """Test parsing of single column references."""
        expr = LambdaParser.parse_lambda(
            lambda e: e.salary,
            self.employee_schema
        )
        
        self.assertIsInstance(expr, ColumnReference)
        self.assertEqual(expr.name, "salary")
        self.assertEqual(expr.table_alias, "e")
        
        sql = _generate_expression(expr)
        expected_sql = "e.salary"
        self.assertEqual(sql, expected_sql)
    
    def test_binary_operation(self):
        """Test parsing of binary operations."""
        expr = LambdaParser.parse_lambda(
            lambda e: e.salary + e.age,
            self.employee_schema
        )
        
        self.assertIsInstance(expr, BinaryOperation)
        self.assertEqual(expr.operator, "+")
        
        self.assertIsInstance(expr.left, ColumnReference)
        self.assertEqual(expr.left.name, "salary")
        self.assertEqual(expr.left.table_alias, "e")
        
        self.assertIsInstance(expr.right, ColumnReference)
        self.assertEqual(expr.right.name, "age")
        self.assertEqual(expr.right.table_alias, "e")
        
        sql = _generate_expression(expr)
        expected_sql = "(e.salary + e.age)"
        self.assertEqual(sql, expected_sql)
    
    def test_boolean_expression(self):
        """Test parsing of boolean expressions."""
        expr = LambdaParser.parse_lambda(
            lambda e: e.salary > 50000,
            self.employee_schema
        )
        
        self.assertIsInstance(expr, BinaryOperation)
        self.assertEqual(expr.operator, ">")
        
        self.assertIsInstance(expr.left, ColumnReference)
        self.assertEqual(expr.left.name, "salary")
        self.assertEqual(expr.left.table_alias, "e")
        
        self.assertIsInstance(expr.right, LiteralExpression)
        self.assertEqual(expr.right.value, 50000)
        
        sql = _generate_expression(expr)
        expected_sql = "e.salary > 50000"
        self.assertEqual(sql, expected_sql)
    
    def test_multiple_table_references(self):
        """Test parsing of multiple table references."""
        expr = LambdaParser.parse_lambda(
            lambda e: e.department == "Engineering",
            self.employee_schema
        )
        
        self.assertIsInstance(expr, BinaryOperation)
        self.assertEqual(expr.operator, "=")
        
        self.assertIsInstance(expr.left, ColumnReference)
        self.assertEqual(expr.left.name, "department")
        self.assertEqual(expr.left.table_alias, "e")
        
        self.assertIsInstance(expr.right, LiteralExpression)
        self.assertEqual(expr.right.value, "Engineering")
        
        sql = _generate_expression(expr)
        expected_sql = "e.department = 'Engineering'"
        self.assertEqual(sql, expected_sql)
    
    def test_complex_boolean_expression(self):
        """Test parsing of complex boolean expressions."""
        expr = LambdaParser.parse_lambda(
            lambda e: e.department == "Engineering" and e.salary > 80000,
            self.employee_schema
        )
        
        self.assertIsNotNone(expr)
        
        sql = _generate_expression(expr)
        expected_sql = "e.department = 'Engineering' AND e.salary > 80000"
        self.assertEqual(sql, expected_sql)

if __name__ == "__main__":
    unittest.main()
