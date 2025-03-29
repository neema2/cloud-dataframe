"""
Unit tests to verify that window() function in column.py returns the same results
as parse_lambda(lambda x: window()) in lambda_parser.py.
"""
import unittest
from typing import List, Optional, Union

from cloud_dataframe.core.dataframe import DataFrame, OrderByClause, Sort
from cloud_dataframe.type_system.column import (
    window, rank, sum, ColumnReference, WindowFunction, Window, Frame,
    row, unbounded, range
)
from cloud_dataframe.utils.lambda_parser import LambdaParser
from cloud_dataframe.type_system.schema import TableSchema


class TestWindowFunctionComparison(unittest.TestCase):
    """
    Test cases to verify that window() function in column.py returns the same results
    as parse_lambda(lambda x: window()) in lambda_parser.py.
    """
    
    def setUp(self):
        """Set up test data."""
        self.df = DataFrame.from_("employees")
        self.table_schema = TableSchema(
            name="employees",
            columns=[
                ("id", "INTEGER"),
                ("name", "VARCHAR"),
                ("department", "VARCHAR"),
                ("salary", "DECIMAL"),
                ("col1", "INTEGER"),
                ("col2", "VARCHAR"),
                ("col3", "VARCHAR"),
                ("col4", "INTEGER")
            ]
        )
    
    def test_window_with_rank_function_comparison(self):
        """
        Test that window() with rank function returns the same result as
        parse_lambda(lambda x: window(rank(), partition=x.department, order_by=x.salary))
        """
        department_ref = ColumnReference(name="department", table_alias="x")
        salary_ref = ColumnReference(name="salary", table_alias="x")
        
        direct_window = window(
            func=rank(),
            partition=department_ref,
            order_by=salary_ref
        )
        
        lambda_window = LambdaParser.parse_lambda(
            lambda x: window(func=rank(), partition=x.department, order_by=x.salary)
        )
        
        self.assertEqual(direct_window.function_name, lambda_window.function_name)
        self.assertEqual(len(direct_window.window.partition_by), len(lambda_window.window.partition_by))
        self.assertEqual(direct_window.window.partition_by[0].name, lambda_window.window.partition_by[0].name)
        self.assertEqual(len(direct_window.window.order_by), len(lambda_window.window.order_by))
        self.assertEqual(direct_window.window.order_by[0].expression.name, lambda_window.window.order_by[0].expression.name)
    
    def test_window_with_sum_function_comparison(self):
        """
        Test that window() with sum function returns the same result as
        parse_lambda(lambda x: window(sum(x.salary), partition=x.department, order_by=x.salary))
        """
        department_ref = ColumnReference(name="department", table_alias="x")
        salary_ref = ColumnReference(name="salary", table_alias="x")
        salary_sum = sum(salary_ref)
        
        direct_window = window(
            func=salary_sum,
            partition=department_ref,
            order_by=salary_ref
        )
        
        lambda_window = LambdaParser.parse_lambda(
            lambda x: window(func=sum(x.salary), partition=x.department, order_by=x.salary)
        )
        
        self.assertEqual(direct_window.function_name, lambda_window.function_name)
        self.assertEqual(len(direct_window.parameters), len(lambda_window.parameters))
        self.assertEqual(direct_window.parameters[0].name, lambda_window.parameters[0].name)
        self.assertEqual(len(direct_window.window.partition_by), len(lambda_window.window.partition_by))
        self.assertEqual(direct_window.window.partition_by[0].name, lambda_window.window.partition_by[0].name)
        self.assertEqual(len(direct_window.window.order_by), len(lambda_window.window.order_by))
        self.assertEqual(direct_window.window.order_by[0].expression.name, lambda_window.window.order_by[0].expression.name)
    
    def test_window_with_frame_comparison(self):
        """
        Test that window() with frame specification returns the same result as
        parse_lambda(lambda x: window(frame=row(unbounded(), 0)))
        """
        frame_spec = row(unbounded(), 0)
        
        direct_window = window(frame=frame_spec)
        
        lambda_window = LambdaParser.parse_lambda(
            lambda x: window(frame=row(unbounded(), 0))
        )
        
        self.assertEqual(direct_window.function_name, lambda_window.function_name)
        self.assertEqual(direct_window.window.frame.type, lambda_window.window.frame.type)
        self.assertEqual(direct_window.window.frame.start, lambda_window.window.frame.start)
        self.assertEqual(direct_window.window.frame.end, lambda_window.window.frame.end)
    
    def test_window_with_only_partition_comparison(self):
        """
        Test that window() with only partition returns the same result as
        parse_lambda(lambda x: window(partition=x.department))
        """
        department_ref = ColumnReference(name="department", table_alias="x")
        
        direct_window = window(partition=department_ref)
        
        lambda_window = LambdaParser.parse_lambda(
            lambda x: window(partition=x.department)
        )
        
        self.assertEqual(direct_window.function_name, lambda_window.function_name)
        self.assertEqual(len(direct_window.window.partition_by), len(lambda_window.window.partition_by))
        self.assertEqual(direct_window.window.partition_by[0].name, lambda_window.window.partition_by[0].name)
    
    def test_window_with_only_order_by_comparison(self):
        """
        Test that window() with only order_by returns the same result as
        parse_lambda(lambda x: window(order_by=x.salary))
        """
        salary_ref = ColumnReference(name="salary", table_alias="x")
        
        direct_window = window(order_by=salary_ref)
        
        lambda_window = LambdaParser.parse_lambda(
            lambda x: window(order_by=x.salary)
        )
        
        self.assertEqual(direct_window.function_name, lambda_window.function_name)
        self.assertEqual(len(direct_window.window.order_by), len(lambda_window.window.order_by))
        self.assertEqual(direct_window.window.order_by[0].expression.name, lambda_window.window.order_by[0].expression.name)
        self.assertEqual(direct_window.window.order_by[0].direction, lambda_window.window.order_by[0].direction)
    
    def test_window_with_no_arguments_comparison(self):
        """
        Test that window() with no arguments returns the same result as
        parse_lambda(lambda x: window())
        """
        direct_window = window()
        
        lambda_window = LambdaParser.parse_lambda(
            lambda x: window(),
            self.table_schema
        )
        
        self.assertEqual(direct_window.function_name, lambda_window.function_name)
        self.assertEqual(len(direct_window.window.partition_by), len(lambda_window.window.partition_by))
        self.assertEqual(len(direct_window.window.order_by), len(lambda_window.window.order_by))
        self.assertIsNone(direct_window.window.frame)
        self.assertIsNone(lambda_window.window.frame)
    
    def test_window_with_complex_example(self):
        """
        Test the complex example provided by the user:
        window(sum(x.col1), partition=[x.col2, x.col3], order_by=x.col4, frame=row(2,0))
        """
        col1_ref = ColumnReference(name="col1", table_alias="x")
        col2_ref = ColumnReference(name="col2", table_alias="x")
        col3_ref = ColumnReference(name="col3", table_alias="x")
        col4_ref = ColumnReference(name="col4", table_alias="x")
        
        frame_spec = row(2, 0)
        
        direct_window = window(
            func=sum(col1_ref),
            partition=[col2_ref, col3_ref],
            order_by=col4_ref,
            frame=frame_spec
        )
        
        lambda_window = LambdaParser.parse_lambda(
            lambda x: window(sum(x.col1), partition=[x.col2, x.col3], order_by=x.col4, frame=row(2, 0))
        )
        
        self.assertEqual(direct_window.function_name, lambda_window.function_name)
        self.assertEqual(len(direct_window.parameters), len(lambda_window.parameters))
        self.assertEqual(direct_window.parameters[0].name, lambda_window.parameters[0].name)
        
        self.assertEqual(len(direct_window.window.partition_by), len(lambda_window.window.partition_by))
        self.assertEqual(direct_window.window.partition_by[0].name, lambda_window.window.partition_by[0].name)
        self.assertEqual(direct_window.window.partition_by[1].name, lambda_window.window.partition_by[1].name)
        
        self.assertEqual(len(direct_window.window.order_by), len(lambda_window.window.order_by))
        self.assertEqual(direct_window.window.order_by[0].expression.name, lambda_window.window.order_by[0].expression.name)
        
        self.assertEqual(direct_window.window.frame.type, lambda_window.window.frame.type)
        self.assertEqual(direct_window.window.frame.start, lambda_window.window.frame.start)
        self.assertEqual(direct_window.window.frame.end, lambda_window.window.frame.end)


if __name__ == "__main__":
    unittest.main()
