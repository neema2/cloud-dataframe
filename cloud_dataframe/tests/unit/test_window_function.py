"""
Unit tests for window() function implementation.

This module contains tests for the window() function implementation with
different combinations of optional arguments.
"""
import unittest
from typing import Optional

from cloud_dataframe.type_system.column import (
    WindowFunction, Window, ColumnReference, sum, rank, row, unbounded, window,
    FunctionExpression, LiteralExpression
)
from cloud_dataframe.core.dataframe import OrderByClause, Sort


class TestWindowFunction(unittest.TestCase):
    """Test cases for window() function implementation."""
    
    def test_window_with_no_args(self):
        """Test window() with no arguments."""
        expr = window()
        
        self.assertIsInstance(expr, WindowFunction)
        self.assertEqual(expr.function_name, "WINDOW")
        self.assertIsInstance(expr.window, Window)
        self.assertEqual(len(expr.window.partition_by), 0)
        self.assertEqual(len(expr.window.order_by), 0)
        self.assertIsNone(expr.window.frame)
    
    def test_window_with_func_arg(self):
        """Test window() with only func argument."""
        sum_func = sum(ColumnReference(name="salary"))
        expr = window(func=sum_func)
        
        self.assertIsInstance(expr, WindowFunction)
        self.assertEqual(expr.function_name, "SUM")
        self.assertIsInstance(expr.window, Window)
        self.assertEqual(len(expr.window.partition_by), 0)
        self.assertEqual(len(expr.window.order_by), 0)
        self.assertIsNone(expr.window.frame)
        
        self.assertEqual(len(expr.parameters), 1)
        self.assertIsInstance(expr.parameters[0], ColumnReference)
        self.assertEqual(expr.parameters[0].name, "salary")
    
    def test_window_with_partition_arg(self):
        """Test window() with only partition argument."""
        dept_col = ColumnReference(name="department")
        expr = window(partition=dept_col)
        
        self.assertIsInstance(expr, WindowFunction)
        self.assertEqual(expr.function_name, "WINDOW")
        self.assertIsInstance(expr.window, Window)
        
        self.assertEqual(len(expr.window.partition_by), 1)
        self.assertIsInstance(expr.window.partition_by[0], ColumnReference)
        self.assertEqual(expr.window.partition_by[0].name, "department")
        
        self.assertEqual(len(expr.window.order_by), 0)
        self.assertIsNone(expr.window.frame)
    
    def test_window_with_order_by_arg(self):
        """Test window() with only order_by argument."""
        salary_col = ColumnReference(name="salary")
        expr = window(order_by=salary_col)
        
        self.assertIsInstance(expr, WindowFunction)
        self.assertEqual(expr.function_name, "WINDOW")
        self.assertIsInstance(expr.window, Window)
        
        self.assertEqual(len(expr.window.order_by), 1)
        self.assertEqual(expr.window.order_by[0].expression.name, "salary")
        
        self.assertEqual(len(expr.window.partition_by), 0)
        self.assertIsNone(expr.window.frame)
    
    def test_window_with_frame_arg(self):
        """Test window() with only frame argument."""
        frame = row(unbounded(), 0)  # unbounded preceding to current row
        expr = window(frame=frame)
        
        self.assertIsInstance(expr, WindowFunction)
        self.assertEqual(expr.function_name, "WINDOW")
        self.assertIsInstance(expr.window, Window)
        
        self.assertIsNotNone(expr.window.frame)
        self.assertEqual(expr.window.frame.type, "ROWS")
        self.assertTrue(expr.window.frame.is_unbounded_start)
        self.assertFalse(expr.window.frame.is_unbounded_end)
        
        self.assertEqual(len(expr.window.partition_by), 0)
        self.assertEqual(len(expr.window.order_by), 0)
    
    def test_window_with_multiple_args(self):
        """Test window() with multiple arguments."""
        dept_col = ColumnReference(name="department")
        salary_col = ColumnReference(name="salary")
        rank_func = rank()
        
        expr = window(
            func=rank_func,
            partition=dept_col,
            order_by=salary_col
        )
        
        self.assertIsInstance(expr, WindowFunction)
        self.assertEqual(expr.function_name, "RANK")
        self.assertIsInstance(expr.window, Window)
        
        self.assertEqual(len(expr.window.partition_by), 1)
        self.assertIsInstance(expr.window.partition_by[0], ColumnReference)
        self.assertEqual(expr.window.partition_by[0].name, "department")
        
        self.assertEqual(len(expr.window.order_by), 1)
        self.assertEqual(expr.window.order_by[0].expression.name, "salary")
        
        self.assertIsNone(expr.window.frame)
    
    def test_window_with_all_args(self):
        """Test window() with all arguments."""
        dept_col = ColumnReference(name="department")
        salary_col = ColumnReference(name="salary")
        rank_func = rank()
        frame = row(unbounded(), 0)  # unbounded preceding to current row
        
        expr = window(
            func=rank_func,
            partition=dept_col,
            order_by=salary_col,
            frame=frame
        )
        
        self.assertIsInstance(expr, WindowFunction)
        self.assertEqual(expr.function_name, "RANK")
        self.assertIsInstance(expr.window, Window)
        
        self.assertEqual(len(expr.window.partition_by), 1)
        self.assertIsInstance(expr.window.partition_by[0], ColumnReference)
        self.assertEqual(expr.window.partition_by[0].name, "department")
        
        self.assertEqual(len(expr.window.order_by), 1)
        self.assertEqual(expr.window.order_by[0].expression.name, "salary")
        
        self.assertIsNotNone(expr.window.frame)
        self.assertEqual(expr.window.frame.type, "ROWS")
        self.assertTrue(expr.window.frame.is_unbounded_start)
        self.assertFalse(expr.window.frame.is_unbounded_end)
    
    def test_window_with_array_partition(self):
        """Test window() with array partition_by."""
        dept_col = ColumnReference(name="department")
        id_col = ColumnReference(name="id")
        rank_func = rank()
        
        expr = window(
            func=rank_func,
            partition=[dept_col, id_col]
        )
        
        self.assertIsInstance(expr, WindowFunction)
        self.assertEqual(expr.function_name, "RANK")
        self.assertIsInstance(expr.window, Window)
        
        self.assertEqual(len(expr.window.partition_by), 2)
        self.assertEqual(expr.window.partition_by[0].name, "department")
        self.assertEqual(expr.window.partition_by[1].name, "id")
    
    def test_window_with_array_order_by(self):
        """Test window() with array order_by."""
        salary_col = ColumnReference(name="salary")
        id_col = ColumnReference(name="id")
        rank_func = rank()
        
        expr = window(
            func=rank_func,
            order_by=[salary_col, id_col]
        )
        
        self.assertIsInstance(expr, WindowFunction)
        self.assertEqual(expr.function_name, "RANK")
        self.assertIsInstance(expr.window, Window)
        
        self.assertEqual(len(expr.window.order_by), 2)
        self.assertEqual(expr.window.order_by[0].expression.name, "salary")
        self.assertEqual(expr.window.order_by[1].expression.name, "id")
    
    def test_window_with_order_by_direction(self):
        """Test window() with order_by direction specified."""
        salary_col = ColumnReference(name="salary")
        id_col = ColumnReference(name="id")
        rank_func = rank()
        
        salary_desc = OrderByClause(expression=salary_col, direction=Sort.DESC)
        id_asc = OrderByClause(expression=id_col, direction=Sort.ASC)
        
        expr = window(
            func=rank_func,
            order_by=[salary_desc, id_asc]
        )
        
        self.assertIsInstance(expr, WindowFunction)
        self.assertEqual(expr.function_name, "RANK")
        self.assertIsInstance(expr.window, Window)
        
        self.assertEqual(len(expr.window.order_by), 2)
        self.assertEqual(expr.window.order_by[0].expression.name, "salary")
        self.assertEqual(expr.window.order_by[0].direction, Sort.DESC)
        self.assertEqual(expr.window.order_by[1].expression.name, "id")
        self.assertEqual(expr.window.order_by[1].direction, Sort.ASC)


if __name__ == "__main__":
    unittest.main()
