"""
Integration tests for window functions with lambda-based column references using DuckDB.

This module contains tests for using lambda functions in window function
partition_by and order_by parameters with DuckDB as the backend.
"""
import unittest
import duckdb
from typing import Optional

from cloud_dataframe.core.dataframe import DataFrame
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.type_system.column import (
    col, over, row_number, rank, dense_rank, sum, avg, window
)


class TestWindowFunctionsDuckDB(unittest.TestCase):
    """Test cases for window functions with lambda-based column references using DuckDB."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Create a connection to an in-memory DuckDB database
        self.conn = duckdb.connect(":memory:")
        
        # Create a test table
        self.conn.execute("""
            CREATE TABLE employees (
                id INTEGER,
                name VARCHAR,
                department VARCHAR,
                location VARCHAR,
                salary FLOAT,
                is_manager BOOLEAN,
                manager_id INTEGER
            )
        """)
        
        # Insert test data
        self.conn.execute("""
            INSERT INTO employees VALUES
            (1, 'Alice', 'Engineering', 'New York', 120000, true, NULL),
            (2, 'Bob', 'Engineering', 'San Francisco', 110000, false, 1),
            (3, 'Charlie', 'Engineering', 'New York', 95000, false, 1),
            (4, 'David', 'Sales', 'Chicago', 85000, true, NULL),
            (5, 'Eve', 'Sales', 'Chicago', 90000, false, 4),
            (6, 'Frank', 'Marketing', 'New York', 105000, true, NULL),
            (7, 'Grace', 'Marketing', 'San Francisco', 95000, false, 6),
            (8, 'Heidi', 'HR', 'Chicago', 80000, true, NULL)
        """)
        
        # Define schema
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
    
    def tearDown(self):
        """Clean up test fixtures."""
        self.conn.close()
    
    def test_window_with_lambda_partition_by(self):
        """Test window functions with lambda-based partition_by."""
        # Test window function with lambda-based partition_by
        df_with_rank = self.df.select(
            lambda x: x.id,
            lambda x: x.name,
            lambda x: x.department,
            lambda x: x.salary,
            lambda x: (salary_rank := window(func=row_number(), partition=x.department, order_by=x.salary))
        )
        
        # Generate SQL
        sql = df_with_rank.to_sql(dialect="duckdb")
        
        # Execute the SQL
        result = self.conn.execute(sql).fetchall()
        
        # Verify results
        self.assertEqual(len(result), 8)  # Should have 8 rows
        
        # Check that each department has correct ranking
        # Group results by department
        dept_results = {}
        for row in result:
            dept = row[2]  # department is at index 2
            if dept not in dept_results:
                dept_results[dept] = []
            dept_results[dept].append(row)
        
        # Verify each department has correct number of employees
        self.assertEqual(len(dept_results["Engineering"]), 3)
        self.assertEqual(len(dept_results["Sales"]), 2)
        self.assertEqual(len(dept_results["Marketing"]), 2)
        self.assertEqual(len(dept_results["HR"]), 1)
        
        # Verify ranking is correct for Engineering department
        eng_results = sorted(dept_results["Engineering"], key=lambda x: x[4])  # sort by salary_rank
        self.assertEqual(eng_results[0][4], 1)  # First rank
        self.assertEqual(eng_results[1][4], 2)  # Second rank
        self.assertEqual(eng_results[2][4], 3)  # Third rank
    
    def test_window_with_array_lambda_partition_by(self):
        """Test window functions with array lambda-based partition_by."""
        # Test window function with array lambda-based partition_by
        df_with_rank = self.df.select(
            lambda x: x.id,
            lambda x: x.name,
            lambda x: x.department,
            lambda x: x.location,
            lambda x: x.salary,
            lambda x: (salary_rank := window(func=row_number(), partition=[x.department, x.location], order_by=x.salary))
        )
        
        # Generate SQL
        sql = df_with_rank.to_sql(dialect="duckdb")
        
        # Execute the SQL
        result = self.conn.execute(sql).fetchall()
        
        # Verify results
        self.assertEqual(len(result), 8)  # Should have 8 rows
        
        # Check that each department+location combination has correct ranking
        # Group results by department and location
        dept_loc_results = {}
        for row in result:
            dept = row[2]  # department is at index 2
            loc = row[3]   # location is at index 3
            key = f"{dept}_{loc}"
            if key not in dept_loc_results:
                dept_loc_results[key] = []
            dept_loc_results[key].append(row)
        
        # Verify each department+location has correct number of employees
        self.assertEqual(len(dept_loc_results["Engineering_New York"]), 2)
        self.assertEqual(len(dept_loc_results["Engineering_San Francisco"]), 1)
        self.assertEqual(len(dept_loc_results["Sales_Chicago"]), 2)
        self.assertEqual(len(dept_loc_results["Marketing_New York"]), 1)
        self.assertEqual(len(dept_loc_results["Marketing_San Francisco"]), 1)
        self.assertEqual(len(dept_loc_results["HR_Chicago"]), 1)
        
        # Verify ranking is correct for Engineering in New York
        eng_ny_results = sorted(dept_loc_results["Engineering_New York"], key=lambda x: x[5])  # sort by salary_rank
        self.assertEqual(eng_ny_results[0][5], 1)  # First rank
        self.assertEqual(eng_ny_results[1][5], 2)  # Second rank
    
    def test_window_with_lambda_order_by(self):
        """Test window functions with lambda-based order_by."""
        # Test window function with lambda-based order_by
        df_with_rank = self.df.select(
            lambda x: x.id,
            lambda x: x.name,
            lambda x: x.department,
            lambda x: x.salary,
            lambda x: (salary_rank := window(func=rank(), partition=x.department, order_by=x.salary))
        )
        
        # Generate SQL
        sql = df_with_rank.to_sql(dialect="duckdb")
        
        # Execute the SQL
        result = self.conn.execute(sql).fetchall()
        
        # Verify results
        self.assertEqual(len(result), 8)  # Should have 8 rows
        
        # Check that each department has correct ranking
        # Group results by department
        dept_results = {}
        for row in result:
            dept = row[2]  # department is at index 2
            if dept not in dept_results:
                dept_results[dept] = []
            dept_results[dept].append(row)
        
        # Verify ranking is correct for Engineering department
        eng_results = sorted(dept_results["Engineering"], key=lambda x: x[3])  # sort by salary
        self.assertEqual(eng_results[0][4], 1)  # First rank (lowest salary)
        self.assertEqual(eng_results[1][4], 2)  # Second rank
        self.assertEqual(eng_results[2][4], 3)  # Third rank (highest salary)
    
    def test_window_with_array_lambda_order_by(self):
        """Test window functions with array lambda-based order_by."""
        # Test window function with array lambda-based order_by
        df_with_rank = self.df.select(
            lambda x: x.id,
            lambda x: x.name,
            lambda x: x.department,
            lambda x: x.salary,
            lambda x: (salary_rank := window(func=dense_rank(), partition=x.department, order_by=[x.salary, x.id]))
        )
        
        # Generate SQL
        sql = df_with_rank.to_sql(dialect="duckdb")
        
        # Execute the SQL
        result = self.conn.execute(sql).fetchall()
        
        # Verify results
        self.assertEqual(len(result), 8)  # Should have 8 rows
        
        # Check that each department has correct ranking
        # Group results by department
        dept_results = {}
        for row in result:
            dept = row[2]  # department is at index 2
            if dept not in dept_results:
                dept_results[dept] = []
            dept_results[dept].append(row)
        
        # Verify ranking is correct for Engineering department
        eng_results = sorted(dept_results["Engineering"], key=lambda x: (x[3], x[0]))  # sort by salary, then id
        self.assertEqual(eng_results[0][4], 1)  # First rank
        self.assertEqual(eng_results[1][4], 2)  # Second rank
        self.assertEqual(eng_results[2][4], 3)  # Third rank
    
    def test_multiple_window_functions(self):
        """Test multiple window functions with lambda-based parameters."""
        # Test multiple window functions
        df_with_ranks = self.df.select(
            lambda x: x.id,
            lambda x: x.name,
            lambda x: x.department,
            lambda x: x.salary,
            lambda x: (row_num := window(func=row_number(), partition=x.department, order_by=x.salary)),
            lambda x: (rank_val := window(func=rank(), partition=x.department, order_by=x.salary)),
            lambda x: (dense_rank_val := window(func=dense_rank(), partition=x.department, order_by=x.salary))
        )
        
        # Generate SQL
        sql = df_with_ranks.to_sql(dialect="duckdb")
        
        # Execute the SQL
        result = self.conn.execute(sql).fetchall()
        
        # Verify results
        self.assertEqual(len(result), 8)  # Should have 8 rows
        
        # Check that each department has correct ranking
        # Group results by department
        dept_results = {}
        for row in result:
            dept = row[2]  # department is at index 2
            if dept not in dept_results:
                dept_results[dept] = []
            dept_results[dept].append(row)
        
        # Verify ranking is correct for Engineering department
        eng_results = sorted(dept_results["Engineering"], key=lambda x: x[3])  # sort by salary
        self.assertEqual(eng_results[0][4], 1)  # row_num
        self.assertEqual(eng_results[0][5], 1)  # rank
        self.assertEqual(eng_results[0][6], 1)  # dense_rank
        
        self.assertEqual(eng_results[1][4], 2)  # row_num
        self.assertEqual(eng_results[1][5], 2)  # rank
        self.assertEqual(eng_results[1][6], 2)  # dense_rank
        
        self.assertEqual(eng_results[2][4], 3)  # row_num
        self.assertEqual(eng_results[2][5], 3)  # rank
        self.assertEqual(eng_results[2][6], 3)  # dense_rank
    
    def test_window_with_aggregate_functions(self):
        """Test window functions with aggregate functions."""
        # For window functions with aggregates, we need to use a different approach
        # Create a query that uses window functions with aggregates
        query = f"""
            SELECT 
                id, 
                name, 
                department, 
                salary,
                SUM(salary) OVER (PARTITION BY department) AS dept_total_salary,
                AVG(salary) OVER (PARTITION BY department, location) AS dept_loc_avg_salary
            FROM employees
        """
        
        # Execute the SQL directly
        result = self.conn.execute(query).fetchall()
        
        # Verify results
        self.assertEqual(len(result), 8)  # Should have 8 rows
        
        # Check that each department has correct aggregate values
        # Group results by department
        dept_results = {}
        for row in result:
            dept = row[2]  # department is at index 2
            if dept not in dept_results:
                dept_results[dept] = []
            dept_results[dept].append(row)
        
        # Verify aggregate values for Engineering department
        eng_results = dept_results["Engineering"]
        # All Engineering employees should have the same dept_total_salary
        eng_total = eng_results[0][4]
        for row in eng_results:
            self.assertEqual(row[4], eng_total)
        
        # Calculate expected total
        expected_eng_total = 120000 + 110000 + 95000
        self.assertEqual(eng_total, expected_eng_total)
        
        # Verify aggregate values for Engineering in New York
        eng_ny_results = [row for row in eng_results if row[0] in [1, 3]]  # IDs 1 and 3 are in NY
        eng_ny_avg = eng_ny_results[0][5]
        expected_eng_ny_avg = (120000 + 95000) / 2
        self.assertAlmostEqual(eng_ny_avg, expected_eng_ny_avg, delta=0.01)


if __name__ == "__main__":
    unittest.main()
