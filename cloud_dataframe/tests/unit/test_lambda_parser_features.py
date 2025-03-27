"""
Unit tests for lambda parser features.
"""
import unittest
from typing import Optional

from cloud_dataframe.core.dataframe import DataFrame
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.utils.lambda_parser import LambdaParser
from cloud_dataframe.type_system.expressions import (
    ColumnReference, BinaryOperation, LiteralExpression
)


class TestLambdaParserFeatures(unittest.TestCase):
    """Test cases for lambda parser features."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.schema = TableSchema(
            name="Employee",
            columns={
                "id": int,
                "name": str,
                "department": str,
                "salary": float,
                "bonus": float,
                "is_manager": bool,
                "age": int
            }
        )
    
    def test_single_column_reference(self):
        """Test parsing single column reference."""
        lambda_func = lambda x: x.name
        expr = LambdaParser.parse_lambda(lambda_func, self.schema)
        
        self.assertIsInstance(expr, ColumnReference)
        self.assertEqual(expr.name, "name")
        self.assertEqual(expr.table_alias, "x")
    
    def test_binary_operation(self):
        """Test parsing binary operation."""
        lambda_func = lambda x: x.salary > 50000
        expr = LambdaParser.parse_lambda(lambda_func, self.schema)
        
        self.assertIsInstance(expr, BinaryOperation)
        self.assertEqual(expr.operator, ">")
        self.assertIsInstance(expr.left, ColumnReference)
        self.assertEqual(expr.left.name, "salary")
        self.assertEqual(expr.left.table_alias, "x")
        self.assertIsInstance(expr.right, LiteralExpression)
        self.assertEqual(expr.right.value, 50000)
    
    def test_complex_binary_operation(self):
        """Test parsing complex binary operation."""
        lambda_func = lambda x: x.salary + x.bonus
        expr = LambdaParser.parse_lambda(lambda_func, self.schema)
        
        self.assertIsInstance(expr, BinaryOperation)
        self.assertEqual(expr.operator, "+")
        self.assertIsInstance(expr.left, ColumnReference)
        self.assertEqual(expr.left.name, "salary")
        self.assertEqual(expr.left.table_alias, "x")
        self.assertIsInstance(expr.right, ColumnReference)
        self.assertEqual(expr.right.name, "bonus")
        self.assertEqual(expr.right.table_alias, "x")
    
    def test_boolean_expression(self):
        """Test parsing boolean expression."""
        lambda_func = lambda x: x.is_manager == True and x.age > 40
        expr = LambdaParser.parse_lambda(lambda_func, self.schema)
        
        self.assertIsInstance(expr, BinaryOperation)
        self.assertEqual(expr.operator, "AND")
        
        self.assertIsInstance(expr.left, BinaryOperation)
        self.assertEqual(expr.left.operator, "==")
        self.assertIsInstance(expr.left.left, ColumnReference)
        self.assertEqual(expr.left.left.name, "is_manager")
        self.assertEqual(expr.left.left.table_alias, "x")
        self.assertIsInstance(expr.left.right, LiteralExpression)
        self.assertEqual(expr.left.right.value, True)
        
        self.assertIsInstance(expr.right, BinaryOperation)
        self.assertEqual(expr.right.operator, ">")
        self.assertIsInstance(expr.right.left, ColumnReference)
        self.assertEqual(expr.right.left.name, "age")
        self.assertEqual(expr.right.left.table_alias, "x")
        self.assertIsInstance(expr.right.right, LiteralExpression)
        self.assertEqual(expr.right.right.value, 40)
    
    def test_multiple_table_references(self):
        """Test parsing lambda with multiple table references."""
        lambda_func = lambda a, b: a.id == b.employee_id
        
        schema_b = TableSchema(
            name="Department",
            columns={
                "id": int,
                "name": str,
                "employee_id": int
            }
        )
        
        expr = LambdaParser.parse_lambda(lambda_func, [self.schema, schema_b])
        
        self.assertIsInstance(expr, BinaryOperation)
        self.assertEqual(expr.operator, "==")
        
        self.assertIsInstance(expr.left, ColumnReference)
        self.assertEqual(expr.left.name, "id")
        self.assertEqual(expr.left.table_alias, "a")
        
        self.assertIsInstance(expr.right, ColumnReference)
        self.assertEqual(expr.right.name, "employee_id")
        self.assertEqual(expr.right.table_alias, "b")


if __name__ == "__main__":
    unittest.main()
