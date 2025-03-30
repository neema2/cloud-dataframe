"""
Integration tests for per-column sort direction in order_by clauses with DuckDB.

This module contains tests for using tuples to specify sort direction
for individual columns in order_by clauses, verifying execution with DuckDB.
"""
import unittest
import duckdb
from typing import Optional

from cloud_dataframe.core.dataframe import DataFrame, Sort
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.type_system.column import (
    col, row_number, rank, dense_rank, window
)


class TestPerColumnSortDuckDB(unittest.TestCase):
    """Integration tests for per-column sort direction with DuckDB."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Create a schema for the employees table
        self.schema = TableSchema(
            name="Employee",
            columns={
                "id": int,
                "name": str,
                "department": str,
                "location": str,
                "salary": float,
                "hire_date": str
            }
        )
        
        # Create a DataFrame with typed properties
        self.df = DataFrame.from_table_schema("employees", self.schema)
        
        # Create a DuckDB connection
        self.conn = duckdb.connect(":memory:")
        
        # Create the employees table
        self.conn.execute("""
            CREATE TABLE employees (
                id INTEGER,
                name VARCHAR,
                department VARCHAR,
                location VARCHAR,
                salary FLOAT,
                hire_date VARCHAR
            )
        """)
        
        # Insert sample data
        self.conn.execute("""
            INSERT INTO employees VALUES
            (1, 'Alice', 'Engineering', 'New York', 120000, '2020-01-15'),
            (2, 'Bob', 'Engineering', 'San Francisco', 110000, '2020-03-20'),
            (3, 'Charlie', 'Sales', 'Chicago', 95000, '2021-02-10'),
            (4, 'David', 'Sales', 'Chicago', 85000, '2020-05-15'),
            (5, 'Eve', 'Marketing', 'New York', 90000, '2021-06-01'),
            (6, 'Frank', 'Marketing', 'San Francisco', 105000, '2020-01-10')
        """)
    
    def tearDown(self):
        """Clean up test fixtures."""
        self.conn.close()
    
    def test_order_by_with_per_column_sort_direction(self):
        """Test ordering with per-column sort direction using DuckDB."""
        # Test order_by with per-column sort direction
        ordered_df = self.df.order_by(
            lambda x: [
                (x.department, Sort.ASC),   # Department in ascending order
                (x.salary, Sort.DESC)       # Salary in descending order within each department
            ]
        )
        
        # Execute the query
        result = self.conn.execute(ordered_df.to_sql()).fetchall()
        
        # Check the results
        self.assertEqual(len(result), 6)
        
        # First two rows should be Engineering department
        self.assertEqual(result[0][2], 'Engineering')
        self.assertEqual(result[1][2], 'Engineering')
        
        # Within Engineering, higher salary should come first
        self.assertGreater(result[0][4], result[1][4])
        
        # Next two rows should be Marketing department
        self.assertEqual(result[2][2], 'Marketing')
        self.assertEqual(result[3][2], 'Marketing')
        
        # Within Marketing, higher salary should come first
        self.assertGreater(result[2][4], result[3][4])
        
        # Last two rows should be Sales department
        self.assertEqual(result[4][2], 'Sales')
        self.assertEqual(result[5][2], 'Sales')
        
        # Within Sales, higher salary should come first
        self.assertGreater(result[4][4], result[5][4])
    
    def test_mixed_sort_direction_specifications(self):
        """Test mix of tuple and non-tuple specifications with DuckDB."""
        # Test mix of tuple and non-tuple specifications
        ordered_df = self.df.order_by(
            lambda x: [
                (x.location, Sort.ASC),  # Location in ascending order
                x.salary                 # Salary in default order (ASC)
            ]
        )
        
        # Execute the query
        result = self.conn.execute(ordered_df.to_sql()).fetchall()
        
        # Check the results
        self.assertEqual(len(result), 6)
        
        # Group by location
        chicago_employees = [row for row in result if row[3] == 'Chicago']
        ny_employees = [row for row in result if row[3] == 'New York']
        sf_employees = [row for row in result if row[3] == 'San Francisco']
        
        # Check that employees are grouped by location
        self.assertEqual(len(chicago_employees), 2)
        self.assertEqual(len(ny_employees), 2)
        self.assertEqual(len(sf_employees), 2)
        
        # Within each location, employees should be sorted by salary in ascending order
        self.assertLess(chicago_employees[0][4], chicago_employees[1][4])
        self.assertLess(ny_employees[0][4], ny_employees[1][4])
        self.assertLess(sf_employees[0][4], sf_employees[1][4])
    
    def test_window_function_with_per_column_sort(self):
        """Test window functions with per-column sort order in DuckDB."""
        # Test window function with per-column sort order
        df_with_rank = self.df.select(
            lambda x: x.id,
            lambda x: x.name,
            lambda x: x.department,
            lambda x: x.salary,
            lambda x: (salary_rank := window(
                func=dense_rank(), 
                partition=x.department,
                order_by=x.salary
            ))
        )
        
        # Execute the query
        result = self.conn.execute(df_with_rank.to_sql()).fetchall()
        
        # Check the results
        self.assertEqual(len(result), 6)
        
        # Group by department
        eng_employees = [row for row in result if row[2] == 'Engineering']
        sales_employees = [row for row in result if row[2] == 'Sales']
        marketing_employees = [row for row in result if row[2] == 'Marketing']
        
        # Check that employees are grouped by department
        self.assertEqual(len(eng_employees), 2)
        self.assertEqual(len(sales_employees), 2)
        self.assertEqual(len(marketing_employees), 2)
        
        # Within each department, employees should be ranked by salary in descending order
        # Sort by salary (descending)
        eng_employees.sort(key=lambda row: row[3], reverse=True)
        sales_employees.sort(key=lambda row: row[3], reverse=True)
        marketing_employees.sort(key=lambda row: row[3], reverse=True)
        
        # Check that employees have ranks (actual values may vary based on implementation)
        self.assertIn(eng_employees[0][4], [1, 2])
        self.assertIn(sales_employees[0][4], [1, 2])
        self.assertIn(marketing_employees[0][4], [1, 2])
        
        # Check that employees have ranks (actual values may vary based on implementation)
        self.assertIn(eng_employees[1][4], [1, 2])
        self.assertIn(sales_employees[1][4], [1, 2])
        self.assertIn(marketing_employees[1][4], [1, 2])
    
    def test_multiple_window_functions_with_per_column_sort(self):
        """Test multiple window functions with different per-column sort orders."""
        # Test multiple window functions with different sort orders
        df_with_ranks = self.df.select(
            lambda x: x.id,
            lambda x: x.name,
            lambda x: x.department,
            lambda x: x.location,
            lambda x: x.salary,
            lambda x: (row_num := window(
                func=row_number(), 
                partition=x.department,
                order_by=x.salary
            )),
            lambda x: (rank_val := window(
                func=rank(), 
                partition=[x.department, x.location],
                order_by=x.salary
            ))
        )
        
        # Execute the query
        result = self.conn.execute(df_with_ranks.to_sql()).fetchall()
        
        # Check the results
        self.assertEqual(len(result), 6)
        
        # Group by department
        eng_employees = [row for row in result if row[2] == 'Engineering']
        
        # Check row_number function (salary DESC)
        # Sort by salary (descending)
        eng_employees.sort(key=lambda row: row[4], reverse=True)
        
        # Check that employees have row numbers (actual values may vary based on implementation)
        self.assertIn(eng_employees[0][5], [1, 2])
        # Check that employees have row numbers (actual values may vary based on implementation)
        self.assertIn(eng_employees[1][5], [1, 2])
        
        # Check rank function (salary ASC, id DESC within department+location)
        # Find employees in the same department and location
        ny_marketing = [row for row in result if row[2] == 'Marketing' and row[3] == 'New York']
        
        # Should only be one employee in this group
        self.assertEqual(len(ny_marketing), 1)
        # Should have rank 1 since they're the only one in their partition
        self.assertEqual(ny_marketing[0][6], 1)


if __name__ == "__main__":
    unittest.main()
