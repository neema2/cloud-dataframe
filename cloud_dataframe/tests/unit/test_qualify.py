"""
Unit tests for qualify function.

This module contains unit tests for the qualify method in the DataFrame class.
"""
import unittest
from unittest.mock import patch

from cloud_dataframe.core.dataframe import DataFrame, FilterCondition
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.type_system.column import (
    row_number, rank, dense_rank, window
)


class TestQualify(unittest.TestCase):
    """Test cases for qualify method."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.schema = TableSchema(
            name="Employees",
            columns={
                "id": int,
                "name": str,
                "department": str,
                "salary": int
            }
        )
        
        self.df = DataFrame.from_table_schema("employees", self.schema, alias="x")
    
    def test_qualify_lambda(self):
        """Test qualify with lambda function."""
        df_with_qualify = self.df.select(
            lambda x: x.id,
            lambda x: x.name,
            lambda x: x.department,
            lambda x: x.salary,
            lambda x: (row_num := window(func=row_number(), partition=x.department, order_by=x.salary))
        ).qualify(
            lambda x: x.row_num <= 2
        )
        
        self.assertIsNotNone(df_with_qualify.qualify_condition)
        self.assertTrue(isinstance(df_with_qualify.qualify_condition, FilterCondition))
        
        sql = df_with_qualify.to_sql(dialect="duckdb")
        self.assertIn("QUALIFY", sql)
        self.assertIn("row_num <= 2", sql)
    
    def test_qualify_with_complex_condition(self):
        """Test qualify with complex condition."""
        df_with_qualify = self.df.select(
            lambda x: x.id,
            lambda x: x.name,
            lambda x: x.department,
            lambda x: x.salary,
            lambda x: (row_num := window(func=row_number(), partition=x.department, order_by=x.salary)),
            lambda x: (rank_val := window(func=rank(), partition=x.department, order_by=x.salary))
        ).qualify(
            lambda x: (x.row_num <= 2) & (x.rank_val == 1)
        )
        
        self.assertIsNotNone(df_with_qualify.qualify_condition)
        
        sql = df_with_qualify.to_sql(dialect="duckdb")
        self.assertIn("QUALIFY", sql)
        self.assertIn("row_num <= 2", sql)
        self.assertIn("rank_val = 1", sql)
    
    def test_multiple_qualify_calls(self):
        """Test that multiple qualify calls overwrite the previous one."""
        df_with_qualify1 = self.df.qualify(lambda x: x.salary > 50000)
        
        df_with_qualify2 = df_with_qualify1.qualify(lambda x: x.salary < 100000)
        
        sql = df_with_qualify2.to_sql(dialect="duckdb")
        self.assertIn("QUALIFY", sql)
        self.assertIn("salary < 100000", sql)
        self.assertNotIn("x.salary > 50000", sql)
    
    def test_qualify_with_error(self):
        """Test that qualify raises an error with invalid lambda."""
        table_schema = self.schema
        
        from cloud_dataframe.utils.lambda_parser import LambdaParser
        original_parse_lambda = LambdaParser.parse_lambda
        
        def mock_parse_lambda(lambda_func, schema=None):
            return original_parse_lambda(lambda_func, table_schema)
        
        LambdaParser.parse_lambda = mock_parse_lambda
        
        try:
            with self.assertRaises(Exception):
                self.df.qualify(lambda x: x.non_existent_column > 10)
        finally:
            LambdaParser.parse_lambda = original_parse_lambda


if __name__ == "__main__":
    unittest.main()
