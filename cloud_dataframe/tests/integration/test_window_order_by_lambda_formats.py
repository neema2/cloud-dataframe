"""
Integration tests for window function order_by lambda formats with DuckDB.

This module contains tests for using different lambda formats in window function
order_by parameters with DuckDB as the backend:
1. Single expression: lambda x: x.col1
2. Single tuple with Sort enum: lambda x: (x.col1, Sort.DESC)
3. Array of expressions and tuples: lambda x: [x.col1, (x.col2, Sort.DESC), x.col3]
"""
import unittest
import duckdb
from typing import Optional

from cloud_dataframe.core.dataframe import DataFrame, Sort
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.type_system.column import (
    row_number, rank, dense_rank, sum, avg, window
)


class TestWindowOrderByLambdaFormatsDuckDB(unittest.TestCase):
    """Test cases for window function order_by lambda formats with DuckDB."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.conn = duckdb.connect(":memory:")
        
        self.conn.execute("""
            CREATE TABLE employees (
                id INTEGER,
                name VARCHAR,
                department VARCHAR,
                location VARCHAR,
                salary FLOAT,
                hire_date DATE,
                is_manager BOOLEAN,
                manager_id INTEGER
            )
        """)
        
        self.conn.execute("""
            INSERT INTO employees VALUES
            (1, 'Alice', 'Engineering', 'New York', 120000, '2020-01-15', true, NULL),
            (2, 'Bob', 'Engineering', 'San Francisco', 110000, '2021-03-10', false, 1),
            (3, 'Charlie', 'Engineering', 'New York', 95000, '2022-05-20', false, 1),
            (4, 'David', 'Sales', 'Chicago', 85000, '2019-11-05', true, NULL),
            (5, 'Eve', 'Sales', 'Chicago', 90000, '2020-08-12', false, 4),
            (6, 'Frank', 'Marketing', 'New York', 105000, '2021-02-28', true, NULL),
            (7, 'Grace', 'Marketing', 'San Francisco', 95000, '2022-01-10', false, 6),
            (8, 'Heidi', 'HR', 'Chicago', 80000, '2019-06-15', true, NULL)
        """)
        
        self.schema = TableSchema(
            name="Employee",
            columns={
                "id": int,
                "name": str,
                "department": str,
                "location": str,
                "salary": float,
                "hire_date": str,
                "is_manager": bool,
                "manager_id": Optional[int]
            }
        )
        
        self.df = DataFrame.from_table_schema("employees", self.schema)
    
    def tearDown(self):
        """Clean up test fixtures."""
        self.conn.close()
    
    def test_window_with_single_expression_order_by(self):
        """Test window function with single expression order_by (lambda x: x.col1)."""
        df_with_rank = self.df.select(
            lambda x: x.id,
            lambda x: x.name,
            lambda x: x.department,
            lambda x: x.salary,
            lambda x: (salary_rank := window(func=rank(), partition=x.department, order_by=x.salary))
        )
        
        sql = df_with_rank.to_sql(dialect="duckdb")
        expected_sql = "SELECT x.id, x.name, x.department, x.salary, RANK() OVER (PARTITION BY x.department ORDER BY x.salary ASC) AS salary_rank\nFROM employees x"
        self.assertEqual(sql.strip(), expected_sql.strip())
        
        result = self.conn.execute(sql).fetchall()
        
        self.assertEqual(len(result), 8)  # Should have 8 rows
        
        dept_results = {}
        for row in result:
            dept = row[2]  # department is at index 2
            if dept not in dept_results:
                dept_results[dept] = []
            dept_results[dept].append(row)
        
        eng_results = sorted(dept_results["Engineering"], key=lambda x: x[3])  # sort by salary
        self.assertEqual(eng_results[0][4], 1)  # First rank (lowest salary)
        self.assertEqual(eng_results[1][4], 2)  # Second rank
        self.assertEqual(eng_results[2][4], 3)  # Third rank (highest salary)
    
    def test_window_with_tuple_sort_desc_order_by(self):
        """Test window function with tuple and Sort.DESC order_by (lambda x: (x.col1, Sort.DESC))."""
        df_with_rank = self.df.select(
            lambda x: x.id,
            lambda x: x.name,
            lambda x: x.department,
            lambda x: x.salary,
            lambda x: (salary_rank := window(func=rank(), partition=x.department, order_by=(x.salary, Sort.DESC)))
        )
        
        sql = df_with_rank.to_sql(dialect="duckdb")
        expected_sql = "SELECT x.id, x.name, x.department, x.salary, RANK() OVER (PARTITION BY x.department ORDER BY x.salary DESC) AS salary_rank\nFROM employees x"
        self.assertEqual(sql.strip(), expected_sql.strip())
        
        result = self.conn.execute(sql).fetchall()
        
        self.assertEqual(len(result), 8)  # Should have 8 rows
        
        dept_results = {}
        for row in result:
            dept = row[2]  # department is at index 2
            if dept not in dept_results:
                dept_results[dept] = []
            dept_results[dept].append(row)
        
        eng_results = sorted(dept_results["Engineering"], key=lambda x: -x[3])  # sort by -salary (descending)
        self.assertEqual(eng_results[0][4], 1)  # First rank (highest salary)
        self.assertEqual(eng_results[1][4], 2)  # Second rank
        self.assertEqual(eng_results[2][4], 3)  # Third rank (lowest salary)
    
    def test_window_with_array_mixed_order_by(self):
        """Test window function with array of mixed expressions order_by (lambda x: [x.col1, (x.col2, Sort.DESC), x.col3])."""
        df_with_rank = self.df.select(
            lambda x: x.id,
            lambda x: x.name,
            lambda x: x.department,
            lambda x: x.location,
            lambda x: x.salary,
            lambda x: (rank_val := window(
                func=dense_rank(), 
                partition=x.department, 
                order_by=[x.location, (x.salary, Sort.DESC), x.id]
            ))
        )
        
        sql = df_with_rank.to_sql(dialect="duckdb")
        expected_sql = "SELECT x.id, x.name, x.department, x.location, x.salary, DENSE_RANK() OVER (PARTITION BY x.department ORDER BY x.location ASC, x.salary DESC, x.id ASC) AS rank_val\nFROM employees x"
        self.assertEqual(sql.strip(), expected_sql.strip())
        
        result = self.conn.execute(sql).fetchall()
        
        self.assertEqual(len(result), 8)  # Should have 8 rows
        
        dept_results = {}
        for row in result:
            dept = row[2]  # department is at index 2
            if dept not in dept_results:
                dept_results[dept] = []
            dept_results[dept].append(row)
        
        for row in dept_results["Engineering"]:
            print(f"ID: {row[0]}, Name: {row[1]}, Dept: {row[2]}, Location: {row[3]}, Salary: {row[4]}, Rank: {row[5]}")
            
        for row in dept_results["Engineering"]:
            self.assertTrue(row[5] > 0, f"Expected positive rank, got {row[5]}")
            
        self.assertEqual(len(dept_results["Engineering"]), 3)
    
    def test_window_with_multiple_functions_and_different_order_by_formats(self):
        """Test multiple window functions with different order_by formats."""
        df_with_ranks = self.df.select(
            lambda x: x.id,
            lambda x: x.name,
            lambda x: x.department,
            lambda x: x.salary,
            lambda x: (row_num := window(
                func=row_number(), 
                partition=x.department, 
                order_by=x.salary
            )),
            lambda x: (rank_desc := window(
                func=rank(), 
                partition=x.department, 
                order_by=(x.salary, Sort.DESC)
            )),
            lambda x: (dense_rank_mixed := window(
                func=dense_rank(), 
                partition=x.department, 
                order_by=[x.location, (x.salary, Sort.DESC)]
            ))
        )
        
        sql = df_with_ranks.to_sql(dialect="duckdb")
        expected_sql_parts = [
            "SELECT x.id, x.name, x.department, x.salary,",
            "ROW_NUMBER() OVER (PARTITION BY x.department ORDER BY x.salary ASC) AS row_num,",
            "RANK() OVER (PARTITION BY x.department ORDER BY x.salary DESC) AS rank_desc,",
            "DENSE_RANK() OVER (PARTITION BY x.department ORDER BY x.location ASC, x.salary DESC) AS dense_rank_mixed",
            "FROM employees x"
        ]
        
        for part in expected_sql_parts:
            self.assertIn(part.strip(), sql.replace("\n", " "))
        
        result = self.conn.execute(sql).fetchall()
        
        self.assertEqual(len(result), 8)  # Should have 8 rows
        
        dept_results = {}
        for row in result:
            dept = row[2]  # department is at index 2
            if dept not in dept_results:
                dept_results[dept] = []
            dept_results[dept].append(row)
        
        self.assertEqual(len(dept_results["Engineering"]), 3)
        
        eng_results_by_salary_asc = sorted(dept_results["Engineering"], key=lambda x: x[3])
        self.assertEqual(eng_results_by_salary_asc[0][4], 1)  # row_num
        self.assertEqual(eng_results_by_salary_asc[1][4], 2)  # row_num
        self.assertEqual(eng_results_by_salary_asc[2][4], 3)  # row_num
        
        eng_results_by_salary_desc = sorted(dept_results["Engineering"], key=lambda x: -x[3])
        self.assertEqual(eng_results_by_salary_desc[0][5], 1)  # rank_desc
        self.assertEqual(eng_results_by_salary_desc[1][5], 2)  # rank_desc
        self.assertEqual(eng_results_by_salary_desc[2][5], 3)  # rank_desc


if __name__ == "__main__":
    unittest.main()
