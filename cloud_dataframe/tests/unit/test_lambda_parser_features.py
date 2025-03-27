"""Unit tests for lambda parser features."""
import unittest
from cloud_dataframe.utils.lambda_parser import LambdaParser
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.type_system.expressions import ColumnReference, BinaryOperation, LiteralExpression

class TestLambdaParserFeatures(unittest.TestCase):
    def setUp(self):
        self.schema = TableSchema(name="Employee", columns={"id": int, "name": str, "salary": float})
    
    def test_single_column_reference(self):
        lambda_func = lambda x: x.name
        expr = LambdaParser.parse_lambda(lambda_func, self.schema)
        self.assertIsInstance(expr, ColumnReference)
        self.assertEqual(expr.name, "name")
        self.assertEqual(expr.table_alias, "x")
