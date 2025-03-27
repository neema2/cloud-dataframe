"""
Unit tests for column aliasing using the := syntax.
"""
import unittest
from typing import Optional

from cloud_dataframe.core.dataframe import DataFrame
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.type_system.column import sum, avg


class TestColumnAlias(unittest.TestCase):
    """Test cases for column aliasing with := syntax."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.schema = TableSchema(
            name="Employee",
            columns={
                "id": int,
                "name": str,
                "department": str,
                "salary": float,
                "bonus": float
            }
        )
        
        self.df = DataFrame.from_table_schema("employees", self.schema, alias="x")
    
    def test_simple_column_alias(self):
        """Test simple column aliasing with := syntax."""
        query = self.df.select(
            lambda x: (employee_name := x.name),
            lambda x: (employee_salary := x.salary)
        )
        
        sql = query.to_sql(dialect="duckdb")
        expected = "SELECT x.name AS employee_name, x.salary AS employee_salary\nFROM employees x"
        self.assertEqual(sql.strip(), expected)
    
    def test_expression_alias(self):
        """Test expression aliasing with := syntax."""
        query = self.df.select(
            lambda x: (employee_name := x.name),
            lambda x: (total_compensation := x.salary + x.bonus)
        )
        
        sql = query.to_sql(dialect="duckdb")
        expected = "SELECT x.name AS employee_name, (x.salary + x.bonus) AS total_compensation\nFROM employees x"
        self.assertEqual(sql.strip(), expected)
    
    def test_function_alias(self):
        """Test function call aliasing with := syntax."""
        query = self.df.group_by(lambda x: x.department).select(
            lambda x: x.department,
            lambda x: (total_salary := sum(x.salary)),
            lambda x: (avg_bonus := avg(x.bonus))
        )
        
        sql = query.to_sql(dialect="duckdb")
        expected = "SELECT x.department, SUM(x.salary) AS total_salary, AVG(x.bonus) AS avg_bonus\nFROM employees x\nGROUP BY x.department"
        self.assertEqual(sql.strip(), expected)


if __name__ == "__main__":
    unittest.main()
