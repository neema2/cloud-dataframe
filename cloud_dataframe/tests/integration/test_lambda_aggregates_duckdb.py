"""
Integration tests for lambda-based aggregate functions with DuckDB.
"""
import unittest
import duckdb

from cloud_dataframe.core.dataframe import DataFrame
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.type_system.column import as_column, sum, avg, count, min, max


class TestLambdaAggregatesDuckDB(unittest.TestCase):
    """Test cases for lambda-based aggregate functions with DuckDB."""
    
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
                salary FLOAT,
                bonus FLOAT,
                tax_rate FLOAT
            )
        """)
        
        # Insert test data
        self.conn.execute("""
            INSERT INTO employees VALUES
            (1, 'John', 80000, 10000, 0.25),
            (2, 'Alice', 90000, 15000, 0.28),
            (3, 'Bob', 70000, 7000, 0.22),
            (4, 'Carol', 85000, 12000, 0.26),
            (5, 'Dave', 75000, 8000, 0.24)
        """)
        
        # Create schema
        self.schema = TableSchema(
            name="Employee",
            columns={
                "id": int,
                "name": str,
                "salary": float,
                "bonus": float,
                "tax_rate": float
            }
        )
        
        # Create DataFrame
        self.df = DataFrame.from_table_schema("employees", self.schema)
    
    def tearDown(self):
        """Tear down test fixtures."""
        self.conn.close()
    
    def test_simple_lambda_aggregates(self):
        """Test simple lambda aggregates with DuckDB."""
        query = self.df.select(
            as_column(sum(lambda x: x.salary), "total_salary"),
            as_column(avg(lambda x: x.salary), "avg_salary"),
            as_column(count(lambda x: x.id), "employee_count")
        )
        
        # Execute the query
        result = self.conn.execute(query.to_sql()).fetchall()
        
        # Check the results
        self.assertEqual(len(result), 1)
        self.assertEqual(len(result[0]), 3)
        
        # Total salary should be sum of all salaries
        self.assertAlmostEqual(result[0][0], 400000)  # 80000 + 90000 + 70000 + 85000 + 75000
        
        # Average salary
        self.assertAlmostEqual(result[0][1], 80000)  # 400000 / 5
        
        # Employee count
        self.assertEqual(result[0][2], 5)
    
    def test_complex_lambda_aggregates(self):
        """Test complex lambda aggregates with binary operations in DuckDB."""
        query = self.df.select(
            as_column(sum(lambda x: x.salary + x.bonus), "total_compensation"),
            as_column(avg(lambda x: x.salary * (1 - x.tax_rate)), "avg_net_salary")
        )
        
        # Execute the query
        result = self.conn.execute(query.to_sql()).fetchall()
        
        # Check the results
        self.assertEqual(len(result), 1)
        self.assertEqual(len(result[0]), 2)
        
        # Total compensation (sum of salary + bonus for all employees)
        self.assertAlmostEqual(result[0][0], 452000)  # Sum of (salary + bonus) for all employees
        
        # Average net salary (avg of salary * (1 - tax_rate) for all employees)
        expected_net_avg = (80000 * 0.75 + 90000 * 0.72 + 70000 * 0.78 + 85000 * 0.74 + 75000 * 0.76) / 5
        self.assertAlmostEqual(result[0][1], expected_net_avg, delta=0.1)
    
    def test_group_by_with_lambda_aggregates(self):
        """Test group by with lambda aggregates."""
        # Add department data
        self.conn.execute("""
            ALTER TABLE employees ADD COLUMN department VARCHAR
        """)
        
        self.conn.execute("""
            UPDATE employees SET department = 'Engineering' WHERE id IN (1, 3)
        """)
        
        self.conn.execute("""
            UPDATE employees SET department = 'Finance' WHERE id IN (2, 4, 5)
        """)
        
        # Update schema
        self.schema = TableSchema(
            name="Employee",
            columns={
                "id": int,
                "name": str,
                "salary": float,
                "bonus": float,
                "tax_rate": float,
                "department": str
            }
        )
        
        # Create DataFrame
        self.df = DataFrame.from_table_schema("employees", self.schema)
        
        # Test group by with lambda aggregates
        query = self.df.group_by(lambda x: x.department).select(
            lambda x: x.department,
            as_column(sum(lambda x: x.salary), "total_salary"),
            as_column(avg(lambda x: x.salary), "avg_salary"),
            as_column(count(lambda x: x.id), "employee_count")
        )
        
        # Execute the query
        result = self.conn.execute(query.to_sql()).fetchall()
        
        # Check the results
        self.assertEqual(len(result), 2)  # Two departments
        
        # Sort results by department for consistent testing
        result.sort(key=lambda x: x[0])
        
        # Engineering department
        self.assertEqual(result[0][0], "Engineering")
        self.assertAlmostEqual(result[0][1], 150000)  # 80000 + 70000
        self.assertAlmostEqual(result[0][2], 75000)   # 150000 / 2
        self.assertEqual(result[0][3], 2)
        
        # Finance department
        self.assertEqual(result[1][0], "Finance")
        self.assertAlmostEqual(result[1][1], 250000)  # 90000 + 85000 + 75000
        self.assertAlmostEqual(result[1][2], 83333.33, delta=0.1)  # 250000 / 3
        self.assertEqual(result[1][3], 3)


if __name__ == "__main__":
    unittest.main()
