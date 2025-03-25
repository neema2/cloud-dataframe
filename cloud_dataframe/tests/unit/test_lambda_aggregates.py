"""
Unit tests for lambda-based aggregate functions.
"""
import unittest
from typing import Optional

from cloud_dataframe.core.dataframe import DataFrame
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.type_system.column import as_column, sum, avg, count, min, max


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
        self.df = DataFrame.from_table_schema("employees", self.schema)
    
    def test_simple_lambda_aggregates(self):
        """Test simple lambda aggregates with single column references."""
        # Test with simple column references
        query = self.df.select(
            lambda x: x.name,
            as_column(sum(lambda x: x.salary), "total_salary"),
            as_column(avg(lambda x: x.salary), "avg_salary"),
            as_column(count(lambda x: x.id), "employee_count"),
            as_column(min(lambda x: x.salary), "min_salary"),
            as_column(max(lambda x: x.salary), "max_salary")
        )
        
        sql = query.to_sql(dialect="duckdb")
        expected = "SELECT name, SUM(salary) AS total_salary, AVG(salary) AS avg_salary, COUNT(id) AS employee_count, MIN(salary) AS min_salary, MAX(salary) AS max_salary\nFROM employees"
        self.assertEqual(sql.strip(), expected)
    
    def test_complex_lambda_aggregates(self):
        """Test complex lambda aggregates with binary operations."""
        # Test with binary operations
        query = self.df.select(
            lambda x: x.name,
            as_column(sum(lambda x: x.salary + x.bonus), "total_compensation"),
            as_column(avg(lambda x: x.salary * (1 - x.tax_rate)), "avg_net_salary")
        )
        
        sql = query.to_sql(dialect="duckdb")
        expected = "SELECT name, SUM((salary + bonus)) AS total_compensation, AVG((salary * (1 - tax_rate))) AS avg_net_salary\nFROM employees"
        self.assertEqual(sql.strip(), expected)
    
    def test_count_distinct(self):
        """Test count distinct with lambda expressions."""
        query = self.df.select(
            as_column(count(lambda x: x.name, distinct=True), "unique_names")
        )
        
        sql = query.to_sql(dialect="duckdb")
        expected = "SELECT COUNT(DISTINCT name) AS unique_names\nFROM employees"
        self.assertEqual(sql.strip(), expected)
    
    def test_aggregate_in_group_by(self):
        """Test aggregates with group by using lambda expressions."""
        query = self.df.group_by(lambda x: x.name).select(
            lambda x: x.name,
            as_column(sum(lambda x: x.salary), "total_salary"),
            as_column(avg(lambda x: x.bonus), "avg_bonus")
        )
        
        sql = query.to_sql(dialect="duckdb")
        expected = "SELECT name, SUM(salary) AS total_salary, AVG(bonus) AS avg_bonus\nFROM employees\nGROUP BY name"
        self.assertEqual(sql.strip(), expected)


if __name__ == "__main__":
    unittest.main()
