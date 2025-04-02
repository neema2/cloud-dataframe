"""
Unit tests for lambda-based aggregate functions.
"""
import unittest
from typing import Optional

from cloud_dataframe.core.dataframe import DataFrame
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.type_system.column import sum, avg, count, min, max


class TestLambdaAggregates(unittest.TestCase):
    """Test cases for lambda-based aggregate functions."""
    
    def setUp(self):
        """Set up test fixtures."""
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
        
        # Create DataFrame with schema
        self.df = DataFrame.from_table_schema("employees", self.schema, alias="x")
    
    def test_simple_lambda_aggregates(self):
        """Test simple lambda aggregates with single column references."""
        # Test with simple column references
        query = self.df.select(
            lambda x: x.name,
            lambda x: (total_salary := sum(x.salary)),
            lambda x: (avg_salary := avg(x.salary)),
            lambda x: (employee_count := count(x.id)),
            lambda x: (min_salary := min(x.salary)),
            lambda x: (max_salary := max(x.salary))
        )
        
        sql = query.to_sql(dialect="duckdb")
        expected = "SELECT x.name, SUM(x.salary) AS total_salary, AVG(x.salary) AS avg_salary, COUNT(x.id) AS employee_count, MIN(x.salary) AS min_salary, MAX(x.salary) AS max_salary\nFROM employees AS x"
        self.assertEqual(sql.strip(), expected)
    
    def test_complex_lambda_aggregates(self):
        """Test complex lambda aggregates with binary operations."""
        # Test with binary operations
        query = self.df.select(
            lambda x: x.name,
            lambda x: (total_compensation := sum(x.salary + x.bonus)),
            lambda x: (avg_net_salary := avg(x.salary * (1 - x.tax_rate)))
        )
        
        sql = query.to_sql(dialect="duckdb")
        expected = "SELECT x.name, SUM((x.salary + x.bonus)) AS total_compensation, AVG((x.salary * (1 - x.tax_rate))) AS avg_net_salary\nFROM employees AS x"
        self.assertEqual(sql.strip(), expected)
    
    def test_count_distinct(self):
        """Test count distinct with lambda expressions."""
        query = self.df.select(
            lambda x: (unique_names := count(x.name, distinct=True))
        )
        
        sql = query.to_sql(dialect="duckdb")
        expected = "SELECT COUNT(DISTINCT x.name) AS unique_names\nFROM employees AS x"
        self.assertEqual(sql.strip(), expected)
    
    def test_aggregate_in_group_by(self):
        """Test aggregates with group by using lambda expressions."""
        query = self.df.group_by(lambda x: x.name).select(
            lambda x: x.name,
            lambda x: (total_salary := sum(x.salary)),
            lambda x: (avg_bonus := avg(x.bonus))
        )
        
        sql = query.to_sql(dialect="duckdb")
        expected = "SELECT x.name, SUM(x.salary) AS total_salary, AVG(x.bonus) AS avg_bonus\nFROM employees AS x\nGROUP BY x.name"
        self.assertEqual(sql.strip(), expected)


if __name__ == "__main__":
    unittest.main()
