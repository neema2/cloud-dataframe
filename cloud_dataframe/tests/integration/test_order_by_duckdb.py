"""
Integration tests for order_by() function with DuckDB.

This module contains comprehensive tests for the order_by() function
with all supported lambda formats, verifying execution with DuckDB.
"""
import unittest
import duckdb
from typing import Optional

from cloud_dataframe.core.dataframe import DataFrame, Sort
from cloud_dataframe.type_system.schema import TableSchema


class TestOrderByDuckDB(unittest.TestCase):
    """Integration tests for order_by() function with DuckDB."""
    
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
                "hire_date": str
            }
        )
        
        self.df = DataFrame.from_table_schema("employees", self.schema)
        
        self.conn = duckdb.connect(":memory:")
        
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
    
    def test_single_expression_format(self):
        """Test single expression format (lambda x: x.col1)."""
        ordered_df = self.df.order_by(lambda x: x.salary)
        
        result = self.conn.execute(ordered_df.to_sql()).fetchall()
        
        self.assertEqual(len(result), 6)
        
        for i in range(1, len(result)):
            self.assertLessEqual(result[i-1][4], result[i][4])
    
    def test_single_tuple_format(self):
        """Test single tuple format (lambda x: (x.col1, Sort.DESC))."""
        ordered_df = self.df.order_by(lambda x: (x.salary, Sort.DESC))
        
        result = self.conn.execute(ordered_df.to_sql()).fetchall()
        
        self.assertEqual(len(result), 6)
        
        for i in range(1, len(result)):
            self.assertGreaterEqual(result[i-1][4], result[i][4])
    
    def test_array_format_with_mix(self):
        """Test array format with mix of expressions and tuples."""
        ordered_df = self.df.order_by(lambda x: [
            x.department,  # Department ascending (default)
            (x.salary, Sort.DESC)  # Salary descending
        ])
        
        result = self.conn.execute(ordered_df.to_sql()).fetchall()
        
        self.assertEqual(len(result), 6)
        
        dept_groups = {}
        for row in result:
            dept = row[2]
            if dept not in dept_groups:
                dept_groups[dept] = []
            dept_groups[dept].append(row)
        
        depts = list(dept_groups.keys())
        self.assertEqual(depts, sorted(depts))
        
        for dept, rows in dept_groups.items():
            for i in range(1, len(rows)):
                self.assertGreaterEqual(rows[i-1][4], rows[i][4])
    
    def test_array_format_with_all_tuples(self):
        """Test array format with all tuples."""
        ordered_df = self.df.order_by(lambda x: [
            (x.department, Sort.DESC),
            (x.location, Sort.ASC),
            (x.salary, Sort.DESC)
        ])
        
        result = self.conn.execute(ordered_df.to_sql()).fetchall()
        
        self.assertEqual(len(result), 6)
        
        depts = [row[2] for row in result]
        sorted_depts = sorted(set(depts), reverse=True)
        
        dept_change_indices = [i for i in range(1, len(result)) if result[i][2] != result[i-1][2]]
        dept_change_indices = [0] + dept_change_indices
        
        observed_depts = [result[i][2] for i in dept_change_indices]
        self.assertEqual(observed_depts, sorted_depts)
        
        for i in range(len(dept_change_indices)):
            start = dept_change_indices[i]
            end = dept_change_indices[i+1] if i+1 < len(dept_change_indices) else len(result)
            
            dept_rows = result[start:end]
            locations = [row[3] for row in dept_rows]
            
            loc_groups = {}
            for row in dept_rows:
                loc = row[3]
                if loc not in loc_groups:
                    loc_groups[loc] = []
                loc_groups[loc].append(row)
            
            loc_keys = list(loc_groups.keys())
            self.assertEqual(loc_keys, sorted(loc_keys))
            
            for loc, loc_rows in loc_groups.items():
                for j in range(1, len(loc_rows)):
                    self.assertGreaterEqual(loc_rows[j-1][4], loc_rows[j][4])


if __name__ == "__main__":
    unittest.main()
