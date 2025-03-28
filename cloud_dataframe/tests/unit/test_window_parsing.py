"""
Unit tests for parsing window() function expressions.

This module contains tests for parsing window() function expressions with
different combinations of optional arguments.
"""
import unittest
from typing import Optional

from cloud_dataframe.utils.lambda_parser import LambdaParser, parse_lambda
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.type_system.column import (
    WindowFunction, Window, ColumnReference, sum, rank, row, unbounded, window
)


class TestWindowParsing(unittest.TestCase):
    """Test cases for parsing window() function expressions."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.schema = TableSchema(
            name="Employee",
            columns={
                "id": int,
                "name": str,
                "department": str,
                "salary": float,
            }
        )
    
    def test_window_with_no_args(self):
        """Test parsing window() with no arguments."""
        window_func = window()
        
        self.assertIsInstance(window_func, WindowFunction)
        self.assertEqual(window_func.function_name, "WINDOW")
        self.assertIsInstance(window_func.window, Window)
        self.assertEqual(len(window_func.window.partition_by), 0)
        self.assertEqual(len(window_func.window.order_by), 0)
        self.assertIsNone(window_func.window.frame)
    
    def test_window_with_func_arg(self):
        """Test parsing window() with only func argument."""
        salary_col = ColumnReference(name="salary")
        sum_func = sum(salary_col)
        
        window_func = window(func=sum_func)
        
        self.assertIsInstance(window_func, WindowFunction)
        self.assertEqual(window_func.function_name, "SUM")
        self.assertIsInstance(window_func.window, Window)
        self.assertEqual(len(window_func.window.partition_by), 0)
        self.assertEqual(len(window_func.window.order_by), 0)
        self.assertIsNone(window_func.window.frame)
        
        self.assertEqual(len(window_func.parameters), 1)
        self.assertIsInstance(window_func.parameters[0], ColumnReference)
        self.assertEqual(window_func.parameters[0].name, "salary")
    
    def test_window_with_partition_arg(self):
        """Test parsing window() with only partition argument."""
        dept_col = ColumnReference(name="department")
        
        window_func = window(partition=dept_col)
        
        self.assertIsInstance(window_func, WindowFunction)
        self.assertEqual(window_func.function_name, "WINDOW")
        self.assertIsInstance(window_func.window, Window)
        
        self.assertEqual(len(window_func.window.partition_by), 1)
        self.assertIsInstance(window_func.window.partition_by[0], ColumnReference)
        self.assertEqual(window_func.window.partition_by[0].name, "department")
        
        self.assertEqual(len(window_func.window.order_by), 0)
        self.assertIsNone(window_func.window.frame)
    
    def test_window_with_order_by_arg(self):
        """Test parsing window() with only order_by argument."""
        salary_col = ColumnReference(name="salary")
        
        window_func = window(order_by=salary_col)
        
        self.assertIsInstance(window_func, WindowFunction)
        self.assertEqual(window_func.function_name, "WINDOW")
        self.assertIsInstance(window_func.window, Window)
        
        self.assertEqual(len(window_func.window.order_by), 1)
        self.assertEqual(window_func.window.order_by[0].expression.name, "salary")
        
        self.assertEqual(len(window_func.window.partition_by), 0)
        self.assertIsNone(window_func.window.frame)
    
    def test_window_with_frame_arg(self):
        """Test parsing window() with only frame argument."""
        frame = row(unbounded(), 0)  # unbounded preceding to current row
        
        window_func = window(frame=frame)
        
        self.assertIsInstance(window_func, WindowFunction)
        self.assertEqual(window_func.function_name, "WINDOW")
        self.assertIsInstance(window_func.window, Window)
        
        self.assertIsNotNone(window_func.window.frame)
        self.assertEqual(window_func.window.frame.type, "ROWS")
        self.assertTrue(window_func.window.frame.is_unbounded_start)
        self.assertFalse(window_func.window.frame.is_unbounded_end)
        
        self.assertEqual(len(window_func.window.partition_by), 0)
        self.assertEqual(len(window_func.window.order_by), 0)
    
    def test_window_with_multiple_args(self):
        """Test parsing window() with multiple arguments."""
        dept_col = ColumnReference(name="department")
        salary_col = ColumnReference(name="salary")
        rank_func = rank()
        
        window_func = window(
            func=rank_func,
            partition=dept_col,
            order_by=salary_col
        )
        
        self.assertIsInstance(window_func, WindowFunction)
        self.assertEqual(window_func.function_name, "RANK")
        self.assertIsInstance(window_func.window, Window)
        
        self.assertEqual(len(window_func.window.partition_by), 1)
        self.assertIsInstance(window_func.window.partition_by[0], ColumnReference)
        self.assertEqual(window_func.window.partition_by[0].name, "department")
        
        self.assertEqual(len(window_func.window.order_by), 1)
        self.assertEqual(window_func.window.order_by[0].expression.name, "salary")
        
        self.assertIsNone(window_func.window.frame)
    
    def test_window_with_all_args(self):
        """Test parsing window() with all arguments."""
        dept_col = ColumnReference(name="department")
        salary_col = ColumnReference(name="salary")
        rank_func = rank()
        frame = row(unbounded(), 0)  # unbounded preceding to current row
        
        window_func = window(
            func=rank_func,
            partition=dept_col,
            order_by=salary_col,
            frame=frame
        )
        
        self.assertIsInstance(window_func, WindowFunction)
        self.assertEqual(window_func.function_name, "RANK")
        self.assertIsInstance(window_func.window, Window)
        
        self.assertEqual(len(window_func.window.partition_by), 1)
        self.assertIsInstance(window_func.window.partition_by[0], ColumnReference)
        self.assertEqual(window_func.window.partition_by[0].name, "department")
        
        self.assertEqual(len(window_func.window.order_by), 1)
        self.assertEqual(window_func.window.order_by[0].expression.name, "salary")
        
        self.assertIsNotNone(window_func.window.frame)
        self.assertEqual(window_func.window.frame.type, "ROWS")
        self.assertTrue(window_func.window.frame.is_unbounded_start)
        self.assertFalse(window_func.window.frame.is_unbounded_end)
    
    def test_window_with_array_partition(self):
        """Test parsing window() with array partition_by."""
        dept_col = ColumnReference(name="department")
        id_col = ColumnReference(name="id")
        rank_func = rank()
        
        window_func = window(
            func=rank_func,
            partition=[dept_col, id_col]
        )
        
        self.assertIsInstance(window_func, WindowFunction)
        self.assertEqual(window_func.function_name, "RANK")
        self.assertIsInstance(window_func.window, Window)
        
        self.assertEqual(len(window_func.window.partition_by), 2)
        self.assertEqual(window_func.window.partition_by[0].name, "department")
        self.assertEqual(window_func.window.partition_by[1].name, "id")
    
    def test_window_with_array_order_by(self):
        """Test parsing window() with array order_by."""
        salary_col = ColumnReference(name="salary")
        id_col = ColumnReference(name="id")
        rank_func = rank()
        
        window_func = window(
            func=rank_func,
            order_by=[salary_col, id_col]
        )
        
        self.assertIsInstance(window_func, WindowFunction)
        self.assertEqual(window_func.function_name, "RANK")
        self.assertIsInstance(window_func.window, Window)
        
        self.assertEqual(len(window_func.window.order_by), 2)
        self.assertEqual(window_func.window.order_by[0].expression.name, "salary")
        self.assertEqual(window_func.window.order_by[1].expression.name, "id")


if __name__ == "__main__":
    unittest.main()
