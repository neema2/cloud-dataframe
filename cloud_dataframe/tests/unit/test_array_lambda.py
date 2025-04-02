"""
Unit tests for array returns in lambda functions.

This module contains tests for using lambda functions that return arrays
in dataframe operations.
"""
import unittest
from typing import Optional

from cloud_dataframe.core.dataframe import DataFrame
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.type_system.column import avg


class TestArrayLambda(unittest.TestCase):
    """Test cases for array returns in lambda functions."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.schema = TableSchema(
            name="Employee",
            columns={
                "id": int,
                "name": str,
                "department": str,
                "location": str,
                "salary": float,
                "is_manager": bool,
                "manager_id": Optional[int]
            }
        )
        
        # Create a DataFrame with typed properties
        self.df = DataFrame.from_table_schema("employees", self.schema)
    
    def test_select_with_array_lambda(self):
        """Test selecting with array lambda."""
        # Test select with array lambda
        selected_df = self.df.select(
            lambda x: [x.name, x.department, x.salary]
        )
        
        # Check the SQL generation
        sql = selected_df.to_sql(dialect="duckdb")
        expected_sql = "SELECT x.name, x.department, x.salary\nFROM employees AS x"
        self.assertEqual(sql.strip(), expected_sql.strip())
    
    def test_group_by_with_array_lambda(self):
        """Test grouping with array lambda."""
        # Test group_by with array lambda
        grouped_df = self.df.group_by(
            lambda x: [x.department, x.location]
        ).select(
            lambda x: x.department,
            lambda x: x.location,
            lambda x: (avg_salary := avg(x.salary))
        )
        
        # Check the SQL generation
        sql = grouped_df.to_sql(dialect="duckdb")
        expected_sql = "SELECT x.department, x.location, AVG(x.salary) AS avg_salary\nFROM employees AS x\nGROUP BY x.department, x.location"
        self.assertEqual(sql.strip(), expected_sql.strip())
    
    def test_order_by_with_array_lambda(self):
        """Test ordering with array lambda."""
        # Test order_by with array lambda
        from cloud_dataframe.core.dataframe import Sort
        ordered_df = self.df.order_by(lambda x: [(x.department, Sort.DESC), (x.salary, Sort.DESC)])
        
        # Check the SQL generation
        sql = ordered_df.to_sql(dialect="duckdb")
        expected_sql = "SELECT *\nFROM employees AS x\nORDER BY x.department DESC, x.salary DESC"
        self.assertEqual(sql.strip(), expected_sql.strip())
    
    def test_mixed_array_and_single_lambdas(self):
        """Test mixing array and single lambdas."""
        # Test mixing array and single lambdas in select
        selected_df = self.df.select(
            lambda x: [x.name, x.department],
            lambda x: x.salary
        )
        
        # Check the SQL generation
        sql = selected_df.to_sql(dialect="duckdb")
        expected_sql = "SELECT x.name, x.department, x.salary\nFROM employees AS x"
        self.assertEqual(sql.strip(), expected_sql.strip())
        
        # Test mixing array and single lambdas in group_by
        grouped_df = self.df.group_by(
            lambda x: [x.department, x.location],
            lambda x: x.is_manager
        ).select(
            lambda x: x.department,
            lambda x: x.location,
            lambda x: x.is_manager,
            lambda x: (avg_salary := avg(x.salary))
        )
        
        # Check the SQL generation
        sql = grouped_df.to_sql(dialect="duckdb")
        expected_sql = "SELECT x.department, x.location, x.is_manager, AVG(x.salary) AS avg_salary\nFROM employees AS x\nGROUP BY x.department, x.location, x.is_manager"
        self.assertEqual(sql.strip(), expected_sql.strip())
        
        from cloud_dataframe.core.dataframe import Sort
        ordered_df = self.df.order_by(
            lambda x: [x.department, x.location, (x.salary, Sort.DESC)]
        )
        
        # Check the SQL generation
        sql = ordered_df.to_sql(dialect="duckdb")
        expected_sql = "SELECT x.name, x.department, x.salary\nFROM employees AS x\nORDER BY x.department ASC, x.location ASC, x.salary DESC"
        self.assertEqual(sql.strip(), expected_sql.strip())


if __name__ == "__main__":
    unittest.main()
