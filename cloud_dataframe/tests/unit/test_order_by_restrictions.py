"""
Unit tests for order_by() function restrictions.

This module contains tests to verify that unsupported formats
for the order_by() function raise appropriate errors.
"""
import unittest
from typing import Optional

from cloud_dataframe.core.dataframe import DataFrame, Sort, OrderByClause
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.type_system.column import col


class TestOrderByRestrictions(unittest.TestCase):
    """Test cases for order_by() function restrictions."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.schema = TableSchema(
            name="Employee",
            columns={
                "id": int,
                "name": str,
                "department": str,
                "salary": float
            }
        )
        
        self.df = DataFrame.from_table_schema("employees", self.schema)
    
    def test_multiple_lambda_functions_not_supported(self):
        """Test that multiple lambda functions are not supported."""
        with self.assertRaises(TypeError):
            self.df.order_by(
                lambda x: x.department,
                lambda x: x.salary
            )
    
    def test_orderby_clause_not_supported(self):
        """Test that OrderByClause objects are not supported."""
        with self.assertRaises(ValueError):
            self.df.order_by(OrderByClause(
                expression=col("department"),
                direction=Sort.ASC
            ))
    
    def test_expression_objects_not_supported(self):
        """Test that Expression objects are not supported."""
        with self.assertRaises(ValueError):
            self.df.order_by(col("department"))
    
    def test_desc_parameter_not_supported(self):
        """Test that the desc parameter is not supported."""
        with self.assertRaises(TypeError):
            self.df.order_by(lambda x: x.department, desc=True)


if __name__ == "__main__":
    unittest.main()
