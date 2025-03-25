"""
Integration tests for array returns in lambda functions with DuckDB.

This module contains tests for using lambda functions that return arrays
in dataframe operations with a real DuckDB database.
"""
import unittest
import os
import duckdb

from cloud_dataframe.core.dataframe import DataFrame
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.type_system.column import as_column, avg, sum


class TestArrayLambdaDuckDB(unittest.TestCase):
    """Test cases for array returns in lambda functions with DuckDB."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Create a test database
        self.db_path = ":memory:"
        self.conn = duckdb.connect(self.db_path)
        
        # Create test tables
        self.conn.execute("""
            CREATE TABLE employees (
                id INTEGER,
                name VARCHAR,
                department VARCHAR,
                location VARCHAR,
                salary FLOAT,
                is_manager BOOLEAN
            )
        """)
        
        # Insert test data
        self.conn.execute("""
            INSERT INTO employees VALUES
            (1, 'John', 'Engineering', 'New York', 85000, true),
            (2, 'Alice', 'Engineering', 'San Francisco', 92000, false),
            (3, 'Bob', 'Sales', 'Chicago', 72000, true),
            (4, 'Carol', 'Sales', 'Chicago', 68000, false),
            (5, 'Dave', 'Marketing', 'New York', 78000, true),
            (6, 'Eve', 'Marketing', 'San Francisco', 82000, false)
        """)
        
        # Create schema
        self.schema = TableSchema(
            name="Employee",
            columns={
                "id": int,
                "name": str,
                "department": str,
                "location": str,
                "salary": float,
                "is_manager": bool
            }
        )
        
        # Create DataFrame
        self.df = DataFrame.from_table_schema("employees", self.schema)
    
    def tearDown(self):
        """Tear down test fixtures."""
        self.conn.close()
    
    def test_select_with_array_lambda(self):
        """Test selecting with array lambda using DuckDB."""
        # Test select with array lambda
        selected_df = self.df.select(
            lambda x: [x.name, x.department, x.salary]
        )
        
        # Execute the query
        result = self.conn.execute(selected_df.to_sql()).fetchall()
        
        # Check the results
        self.assertEqual(len(result), 6)
        self.assertEqual(len(result[0]), 3)  # Three columns selected
        
        # Check column values for first row
        self.assertEqual(result[0][0], 'John')
        self.assertEqual(result[0][1], 'Engineering')
        self.assertEqual(result[0][2], 85000)
    
    def test_group_by_with_array_lambda(self):
        """Test grouping with array lambda using DuckDB."""
        # Test group_by with array lambda
        # Use separate lambdas for each column to ensure they're properly included in GROUP BY
        grouped_df = self.df.group_by(
            lambda x: x.department,
            lambda x: x.location
        ).select(
            lambda x: x.department,
            lambda x: x.location,
            as_column(avg(lambda x: x.salary), "avg_salary")
        )
        
        # Execute the query
        result = self.conn.execute(grouped_df.to_sql()).fetchall()
        
        # Check the results
        # Should have 5 groups: Engineering/NY, Engineering/SF, Sales/Chicago, Marketing/NY, Marketing/SF
        self.assertEqual(len(result), 5)
        
        # Check that we have the right columns
        self.assertEqual(len(result[0]), 3)  # department, location, avg_salary
        
        # Find the Sales/Chicago group and check its average salary
        sales_chicago = None
        for row in result:
            if row[0] == 'Sales' and row[1] == 'Chicago':
                sales_chicago = row
                break
        
        self.assertIsNotNone(sales_chicago)
        if sales_chicago:  # Only check if sales_chicago is not None
            self.assertAlmostEqual(sales_chicago[2], 70000, delta=1)  # Average of 72000 and 68000
    
    def test_order_by_with_array_lambda(self):
        """Test ordering with array lambda using DuckDB."""
        # Test order_by with array lambda
        ordered_df = self.df.order_by(lambda x: [x.department, x.salary], desc=True)
        
        # Execute the query
        result = self.conn.execute(ordered_df.to_sql()).fetchall()
        
        # Check the results
        self.assertEqual(len(result), 6)
        
        # Check that the results are ordered correctly
        # First by department (desc): Sales, Marketing, Engineering
        # Then by salary (desc) within each department
        
        # First two rows should be Sales department
        self.assertEqual(result[0][2], 'Sales')
        self.assertEqual(result[1][2], 'Sales')
        
        # First row should have higher salary than second row
        self.assertGreater(result[0][4], result[1][4])
        
        # Next two rows should be Marketing department
        self.assertEqual(result[2][2], 'Marketing')
        self.assertEqual(result[3][2], 'Marketing')
        
        # Marketing row with higher salary should come first
        self.assertGreater(result[2][4], result[3][4])
    
    def test_mixed_array_and_single_lambdas(self):
        """Test mixing array and single lambdas with DuckDB."""
        # Test mixing array and single lambdas in select
        selected_df = self.df.select(
            lambda x: [x.name, x.department],
            lambda x: x.salary
        )
        
        # Execute the query
        result = self.conn.execute(selected_df.to_sql()).fetchall()
        
        # Check the results
        self.assertEqual(len(result), 6)
        self.assertEqual(len(result[0]), 3)  # Three columns selected
        
        # Test mixing array and single lambdas in group_by
        grouped_df = self.df.group_by(
            lambda x: [x.department, x.location],
            lambda x: x.is_manager
        ).select(
            lambda x: x.department,
            lambda x: x.location,
            lambda x: x.is_manager,
            as_column(sum(lambda x: x.salary), "total_salary")
        )
        
        # Execute the query
        result = self.conn.execute(grouped_df.to_sql()).fetchall()
        
        # Check the results
        # Should have groups for each department/location/is_manager combination
        self.assertTrue(len(result) > 4)  # More groups than just department/location
        
        # Check that we have the right columns
        self.assertEqual(len(result[0]), 4)  # department, location, is_manager, total_salary


if __name__ == "__main__":
    unittest.main()
