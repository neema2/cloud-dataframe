"""
Integration tests for window function parsing in lambda expressions.

These tests verify that the lambda parser correctly handles window() function calls
with different combinations of optional arguments.
"""
import unittest
from typing import List

from cloud_dataframe.core.dataframe import DataFrame, OrderByClause, Sort
from cloud_dataframe.type_system.column import (
    window, rank, sum, ColumnReference, WindowFunction, Window, Frame,
    row, unbounded
)
from cloud_dataframe.utils.lambda_parser import parse_lambda


class TestWindowFunctionParsing(unittest.TestCase):
    """Test cases for window function parsing in lambda expressions."""
    
    def setUp(self):
        """Set up test data."""
        self.df = DataFrame.from_("employees")
    
    def test_window_with_rank_function(self):
        """Test window() with rank function and partition by department."""
        expr = parse_lambda(lambda x: window(rank(), partition=x.department, order_by=x.salary))
        
        self.assertIsInstance(expr, WindowFunction)
        self.assertEqual(expr.function_name, "RANK")
        self.assertEqual(len(expr.window.partition_by), 1)
        self.assertEqual(expr.window.partition_by[0].name, "department")
        self.assertEqual(len(expr.window.order_by), 1)
        self.assertEqual(expr.window.order_by[0].expression.name, "salary")
    
    def test_window_with_sum_function(self):
        """Test window() with sum function, partition, order_by, and frame."""
        frame_spec = row(unbounded(), 0)
        
        expr = parse_lambda(lambda x: window(
            func=sum(x.salary),
            partition=x.department,
            order_by=x.salary,
            frame=frame_spec
        ))
        
        self.assertIsInstance(expr, WindowFunction)
        self.assertEqual(expr.function_name, "SUM")
        self.assertEqual(len(expr.parameters), 1)
        self.assertEqual(expr.parameters[0].name, "salary")
        self.assertEqual(len(expr.window.partition_by), 1)
        self.assertEqual(expr.window.partition_by[0].name, "department")
        self.assertEqual(len(expr.window.order_by), 1)
        self.assertEqual(expr.window.order_by[0].expression.name, "salary")
        self.assertIsInstance(expr.window.frame, Frame)
        self.assertEqual(expr.window.frame.type, "ROWS")
        self.assertEqual(expr.window.frame.start, "UNBOUNDED")
        self.assertEqual(expr.window.frame.end, 0)
    
    def test_window_with_multiple_functions(self):
        """Test multiple window functions in a single query."""
        frame_spec = row(unbounded(), 0)
        
        expr1 = parse_lambda(lambda x: window(rank(), partition=x.department, order_by=x.salary))
        
        expr2 = parse_lambda(lambda x: window(
            func=sum(x.salary),
            partition=x.department,
            order_by=x.salary,
            frame=frame_spec
        ))
        
        self.assertIsInstance(expr1, WindowFunction)
        self.assertEqual(expr1.function_name, "RANK")
        self.assertEqual(len(expr1.window.partition_by), 1)
        self.assertEqual(expr1.window.partition_by[0].name, "department")
        
        self.assertIsInstance(expr2, WindowFunction)
        self.assertEqual(expr2.function_name, "SUM")
        self.assertEqual(len(expr2.parameters), 1)
        self.assertEqual(expr2.parameters[0].name, "salary")
    
    def test_window_with_only_partition(self):
        """Test window() with only partition argument."""
        expr = parse_lambda(lambda x: window(partition=x.department))
        
        self.assertIsInstance(expr, WindowFunction)
        self.assertEqual(expr.function_name, "WINDOW")
        self.assertEqual(len(expr.window.partition_by), 1)
        self.assertEqual(expr.window.partition_by[0].name, "department")
        self.assertEqual(len(expr.window.order_by), 0)
        self.assertIsNone(expr.window.frame)
    
    def test_window_with_only_order_by(self):
        """Test window() with only order_by argument."""
        expr = parse_lambda(lambda x: window(order_by=x.salary))
        
        self.assertIsInstance(expr, WindowFunction)
        self.assertEqual(expr.function_name, "WINDOW")
        self.assertEqual(len(expr.window.partition_by), 0)
        self.assertEqual(len(expr.window.order_by), 1)
        self.assertEqual(expr.window.order_by[0].expression.name, "salary")
        self.assertEqual(expr.window.order_by[0].direction, Sort.ASC)
        self.assertIsNone(expr.window.frame)
    
    def test_window_with_only_frame(self):
        """Test window() with only frame argument."""
        frame_spec = row(unbounded(), 0)
        
        expr = parse_lambda(lambda x: window(frame=frame_spec))
        
        self.assertIsInstance(expr, WindowFunction)
        self.assertEqual(expr.function_name, "WINDOW")
        self.assertEqual(len(expr.window.partition_by), 0)
        self.assertEqual(len(expr.window.order_by), 0)
        self.assertIsInstance(expr.window.frame, Frame)
        self.assertEqual(expr.window.frame.type, "ROWS")
        self.assertEqual(expr.window.frame.start, "UNBOUNDED")
        self.assertEqual(expr.window.frame.end, 0)
    
    def test_window_with_only_func(self):
        """Test window() with only func argument."""
        expr = parse_lambda(lambda x: window(func=rank()))
        
        self.assertIsInstance(expr, WindowFunction)
        self.assertEqual(expr.function_name, "RANK")
        self.assertEqual(len(expr.window.partition_by), 0)
        self.assertEqual(len(expr.window.order_by), 0)
        self.assertIsNone(expr.window.frame)
    
    def test_window_with_no_arguments(self):
        """Test window() with no arguments."""
        expr = parse_lambda(lambda x: window())
        
        self.assertIsInstance(expr, WindowFunction)
        self.assertEqual(expr.function_name, "WINDOW")
        self.assertEqual(len(expr.window.partition_by), 0)
        self.assertEqual(len(expr.window.order_by), 0)
        self.assertIsNone(expr.window.frame)
    
    def test_window_with_list_partition(self):
        """Test window() with a list of partition columns."""
        expr = parse_lambda(lambda x: window(partition=[x.department, x.name]))
        
        self.assertIsInstance(expr, WindowFunction)
        self.assertEqual(expr.function_name, "WINDOW")
        self.assertEqual(len(expr.window.partition_by), 2)
        self.assertEqual(expr.window.partition_by[0].name, "department")
        self.assertEqual(expr.window.partition_by[1].name, "name")
        self.assertEqual(len(expr.window.order_by), 0)
        self.assertIsNone(expr.window.frame)
    
    def test_window_with_list_order_by(self):
        """Test window() with a list of order_by columns."""
        expr = parse_lambda(lambda x: window(order_by=[x.salary, x.id]))
        
        self.assertIsInstance(expr, WindowFunction)
        self.assertEqual(expr.function_name, "WINDOW")
        self.assertEqual(len(expr.window.partition_by), 0)
        self.assertEqual(len(expr.window.order_by), 2)
        self.assertEqual(expr.window.order_by[0].expression.name, "salary")
        self.assertEqual(expr.window.order_by[0].direction, Sort.ASC)
        self.assertEqual(expr.window.order_by[1].expression.name, "id")
        self.assertEqual(expr.window.order_by[1].direction, Sort.ASC)
        self.assertIsNone(expr.window.frame)
    
    def _normalize_sql(self, sql: str) -> str:
        """Normalize SQL string for comparison by removing extra whitespace."""
        import re
        sql = re.sub(r'--.*?\n', ' ', sql)
        sql = re.sub(r'\s+', ' ', sql)
        return sql.strip()


if __name__ == "__main__":
    unittest.main()
