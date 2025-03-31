"""
Integration tests for array-based lambda syntax in select() as shown in README examples.

This module verifies that the array-based lambda syntax works correctly with DuckDB
for all examples that appear in the README.
"""
import unittest
import duckdb

from cloud_dataframe.core.dataframe import DataFrame, Sort
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.type_system.column import sum, avg, count


class TestReadmeArrayLambdaExamples(unittest.TestCase):
    """Test cases for array-based lambda syntax in select() as shown in README examples."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.db_path = ":memory:"
        self.conn = duckdb.connect(self.db_path)
        
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
        
        self.conn.execute("""
            INSERT INTO employees VALUES
            (1, 'John', 'Engineering', 'New York', 85000, true),
            (2, 'Alice', 'Engineering', 'San Francisco', 92000, false),
            (3, 'Bob', 'Sales', 'Chicago', 72000, true),
            (4, 'Carol', 'Sales', 'Chicago', 68000, false),
            (5, 'Dave', 'Marketing', 'New York', 78000, true),
            (6, 'Eve', 'Marketing', 'San Francisco', 82000, false)
        """)
        
        self.employee_schema = TableSchema(
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
        
        self.df = DataFrame.from_table_schema("employees", self.employee_schema)
        
        self.conn.execute("""
            CREATE TABLE departments (
                id INTEGER,
                name VARCHAR,
                location VARCHAR,
                budget FLOAT
            )
        """)
        
        self.conn.execute("""
            INSERT INTO departments VALUES
            (1, 'Engineering', 'New York', 500000),
            (2, 'Engineering', 'San Francisco', 600000),
            (3, 'Sales', 'Chicago', 400000),
            (4, 'Marketing', 'New York', 300000),
            (5, 'Marketing', 'San Francisco', 350000)
        """)
        
        self.department_schema = TableSchema(
            name="Department",
            columns={
                "id": int,
                "name": str,
                "location": str,
                "budget": float
            }
        )
        
        self.employees_df = DataFrame.from_table_schema("employees", self.employee_schema, alias="e")
        self.departments_df = DataFrame.from_table_schema("departments", self.department_schema, alias="d")
    
    def tearDown(self):
        """Tear down test fixtures."""
        self.conn.close()
    
    def test_basic_select_with_array_lambda(self):
        """Test basic select with array-based lambda syntax."""
        
        selected_df = self.df.select(
            lambda x: [x.id, x.name, (annual_salary := x.salary * 12)]
        )
        
        sql = selected_df.to_sql(dialect="duckdb")
        expected_sql = "SELECT x.id, x.name, (x.salary * 12) AS annual_salary\nFROM employees x"
        self.assertEqual(sql.strip(), expected_sql.strip())
        
        result = self.conn.execute(sql).fetchall()
        self.assertEqual(len(result), 6)  # Six employees
        self.assertEqual(len(result[0]), 3)  # Three columns
        
        self.assertEqual(result[0][2], 85000 * 12)
    
    def test_join_with_array_lambda(self):
        """Test join operations with array-based lambda syntax."""
        
        self.conn.execute("DROP TABLE employees")
        self.conn.execute("""
            CREATE TABLE employees (
                id INTEGER,
                name VARCHAR,
                department_id INTEGER,
                salary FLOAT
            )
        """)
        
        self.conn.execute("""
            INSERT INTO employees VALUES
            (1, 'John', 1, 85000),
            (2, 'Alice', 2, 92000),
            (3, 'Bob', 3, 72000),
            (4, 'Carol', 3, 68000),
            (5, 'Dave', 4, 78000),
            (6, 'Eve', 5, 82000)
        """)
        
        employee_join_schema = TableSchema(
            name="Employee",
            columns={
                "id": int,
                "name": str,
                "department_id": int,
                "salary": float
            }
        )
        
        employees_join_df = DataFrame.from_table_schema("employees", employee_join_schema, alias="e")
        
        joined_df = employees_join_df.join(
            self.departments_df,
            lambda e, d: e.department_id == d.id
        ).select(
            lambda e, d: [
                (employee_id := e.id),
                (employee_name := e.name),
                (department_name := d.name),
                (department_location := d.location),
                (employee_salary := e.salary)
            ]
        )
        
        sql = joined_df.to_sql(dialect="duckdb")
        result = self.conn.execute(sql).fetchall()
        
        self.assertTrue(len(result) > 0)
        self.assertEqual(len(result[0]), 5)  # Five columns selected
    
    def test_lambda_with_assignment_in_array(self):
        """Test array lambda with assignment expressions."""
        selected_df = self.df.select(
            lambda x: [
                x.id, 
                x.name, 
                (annual_salary := x.salary * 12)
            ]
        )
        
        result = self.conn.execute(selected_df.to_sql()).fetchall()
        self.assertEqual(len(result), 6)  # Six employees
        self.assertEqual(len(result[0]), 3)  # Three columns
        
        self.assertEqual(result[0][2], 85000 * 12)
    
    def test_aggregate_functions_with_array_lambda(self):
        """Test aggregate functions with array-based lambda syntax."""
        
        summary_df = self.df.select(
            lambda x: [
                (total_salary := sum(x.salary)),
                (avg_salary := avg(x.salary)),
                (employee_count := count(x.id))
            ]
        )
        
        sql = summary_df.to_sql(dialect="duckdb")
        expected_sql = "SELECT SUM(x.salary) AS total_salary, AVG(x.salary) AS avg_salary, COUNT(x.id) AS employee_count\nFROM employees x"
        self.assertEqual(sql.strip(), expected_sql.strip())
        
        result = self.conn.execute(sql).fetchall()
        self.assertEqual(len(result), 1)  # One row for aggregation
        self.assertEqual(len(result[0]), 3)  # Three columns
        
        self.assertTrue(result[0][0] is not None)
    
    def test_group_by_with_array_lambda(self):
        """Test group by with array-based lambda syntax."""
        
        grouped_df = self.df.group_by(lambda x: x.department).select(
            lambda x: x.department,
            lambda x: (total_salary := sum(x.salary))
        )
        
        sql = grouped_df.to_sql(dialect="duckdb")
        print(f"Generated SQL: {sql}")  # Print SQL for debugging
        
        result = self.conn.execute(sql).fetchall()
        self.assertEqual(len(result), 3)  # Three departments
        self.assertEqual(len(result[0]), 2)  # Two columns


if __name__ == "__main__":
    unittest.main()
