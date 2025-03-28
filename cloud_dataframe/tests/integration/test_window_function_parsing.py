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
        dept_col = ColumnReference(name="department", table_alias="x")
        salary_col = ColumnReference(name="salary", table_alias="x")
        window_expr = window(rank(), partition=dept_col, order_by=salary_col)
        
        self.assertIsInstance(window_expr, WindowFunction)
        self.assertEqual(window_expr.function_name, "RANK")
        self.assertEqual(len(window_expr.window.partition_by), 1)
        self.assertEqual(window_expr.window.partition_by[0].name, "department")
        self.assertEqual(len(window_expr.window.order_by), 1)
        self.assertEqual(window_expr.window.order_by[0].expression.name, "salary")
    
    def test_window_with_sum_function(self):
        """Test window() with sum function, partition, order_by, and frame."""
        salary_col = ColumnReference(name="salary", table_alias="x")
        dept_col = ColumnReference(name="department", table_alias="x")
        frame_spec = row(unbounded(), 0)
        
        window_expr = window(
            func=sum(salary_col),
            partition=dept_col,
            order_by=salary_col,
            frame=frame_spec
        )
        
        self.assertIsInstance(window_expr, WindowFunction)
        self.assertEqual(window_expr.function_name, "SUM")
        self.assertEqual(len(window_expr.parameters), 1)
        self.assertEqual(window_expr.parameters[0].name, "salary")
        self.assertEqual(len(window_expr.window.partition_by), 1)
        self.assertEqual(window_expr.window.partition_by[0].name, "department")
        self.assertEqual(len(window_expr.window.order_by), 1)
        self.assertEqual(window_expr.window.order_by[0].expression.name, "salary")
        self.assertIsInstance(window_expr.window.frame, Frame)
        self.assertEqual(window_expr.window.frame.type, "ROWS")
        self.assertEqual(window_expr.window.frame.start, "UNBOUNDED")
        self.assertEqual(window_expr.window.frame.end, 0)
    
    def test_window_with_multiple_functions(self):
        """Test multiple window functions in a single query."""
        dept_col = ColumnReference(name="department", table_alias="x")
        salary_col = ColumnReference(name="salary", table_alias="x")
        frame_spec = row(unbounded(), 0)
        
        window_expr1 = window(rank(), partition=dept_col, order_by=salary_col)
        
        window_expr2 = window(
            func=sum(salary_col),
            partition=dept_col,
            order_by=salary_col,
            frame=frame_spec
        )
        
        self.assertIsInstance(window_expr1, WindowFunction)
        self.assertEqual(window_expr1.function_name, "RANK")
        self.assertEqual(len(window_expr1.window.partition_by), 1)
        self.assertEqual(window_expr1.window.partition_by[0].name, "department")
        
        self.assertIsInstance(window_expr2, WindowFunction)
        self.assertEqual(window_expr2.function_name, "SUM")
        self.assertEqual(len(window_expr2.parameters), 1)
        self.assertEqual(window_expr2.parameters[0].name, "salary")
    
    def test_window_with_only_partition(self):
        """Test window() with only partition argument."""
        window_expr = window(partition=ColumnReference(name="department", table_alias="x"))
        
        self.assertIsInstance(window_expr, WindowFunction)
        self.assertEqual(window_expr.function_name, "WINDOW")
        self.assertEqual(len(window_expr.window.partition_by), 1)
        self.assertEqual(window_expr.window.partition_by[0].name, "department")
        self.assertEqual(len(window_expr.window.order_by), 0)
        self.assertIsNone(window_expr.window.frame)
    
    def test_window_with_only_order_by(self):
        """Test window() with only order_by argument."""
        window_expr = window(order_by=ColumnReference(name="salary", table_alias="x"))
        
        self.assertIsInstance(window_expr, WindowFunction)
        self.assertEqual(window_expr.function_name, "WINDOW")
        self.assertEqual(len(window_expr.window.partition_by), 0)
        self.assertEqual(len(window_expr.window.order_by), 1)
        self.assertEqual(window_expr.window.order_by[0].expression.name, "salary")
        self.assertEqual(window_expr.window.order_by[0].direction, Sort.ASC)
        self.assertIsNone(window_expr.window.frame)
    
    def test_window_with_only_frame(self):
        """Test window() with only frame argument."""
        frame_spec = row(unbounded(), 0)
        window_expr = window(frame=frame_spec)
        
        self.assertIsInstance(window_expr, WindowFunction)
        self.assertEqual(window_expr.function_name, "WINDOW")
        self.assertEqual(len(window_expr.window.partition_by), 0)
        self.assertEqual(len(window_expr.window.order_by), 0)
        self.assertIsInstance(window_expr.window.frame, Frame)
        self.assertEqual(window_expr.window.frame.type, "ROWS")
        self.assertEqual(window_expr.window.frame.start, "UNBOUNDED")
        self.assertEqual(window_expr.window.frame.end, 0)
    
    def test_window_with_only_func(self):
        """Test window() with only func argument."""
        window_expr = window(func=rank())
        
        self.assertIsInstance(window_expr, WindowFunction)
        self.assertEqual(window_expr.function_name, "RANK")
        self.assertEqual(len(window_expr.window.partition_by), 0)
        self.assertEqual(len(window_expr.window.order_by), 0)
        self.assertIsNone(window_expr.window.frame)
    
    def test_window_with_no_arguments(self):
        """Test window() with no arguments."""
        window_expr = window()
        
        self.assertIsInstance(window_expr, WindowFunction)
        self.assertEqual(window_expr.function_name, "WINDOW")
        self.assertEqual(len(window_expr.window.partition_by), 0)
        self.assertEqual(len(window_expr.window.order_by), 0)
        self.assertIsNone(window_expr.window.frame)
    
    def test_window_with_list_partition(self):
        """Test window() with a list of partition columns."""
        partition_cols = [
            ColumnReference(name="department", table_alias="x"),
            ColumnReference(name="name", table_alias="x")
        ]
        window_expr = window(partition=partition_cols)
        
        self.assertIsInstance(window_expr, WindowFunction)
        self.assertEqual(window_expr.function_name, "WINDOW")
        self.assertEqual(len(window_expr.window.partition_by), 2)
        self.assertEqual(window_expr.window.partition_by[0].name, "department")
        self.assertEqual(window_expr.window.partition_by[1].name, "name")
        self.assertEqual(len(window_expr.window.order_by), 0)
        self.assertIsNone(window_expr.window.frame)
    
    def test_window_with_list_order_by(self):
        """Test window() with a list of order_by columns."""
        order_by_cols = [
            ColumnReference(name="salary", table_alias="x"),
            ColumnReference(name="id", table_alias="x")
        ]
        window_expr = window(order_by=order_by_cols)
        
        self.assertIsInstance(window_expr, WindowFunction)
        self.assertEqual(window_expr.function_name, "WINDOW")
        self.assertEqual(len(window_expr.window.partition_by), 0)
        self.assertEqual(len(window_expr.window.order_by), 2)
        self.assertEqual(window_expr.window.order_by[0].expression.name, "salary")
        self.assertEqual(window_expr.window.order_by[0].direction, Sort.ASC)
        self.assertEqual(window_expr.window.order_by[1].expression.name, "id")
        self.assertEqual(window_expr.window.order_by[1].direction, Sort.ASC)
        self.assertIsNone(window_expr.window.frame)
    
    def _normalize_sql(self, sql: str) -> str:
        """Normalize SQL string for comparison by removing extra whitespace."""
        import re
        sql = re.sub(r'--.*?\n', ' ', sql)
        sql = re.sub(r'\s+', ' ', sql)
        return sql.strip()


if __name__ == "__main__":
    unittest.main()
