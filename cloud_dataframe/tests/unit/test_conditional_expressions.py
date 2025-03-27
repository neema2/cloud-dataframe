"""Unit tests for conditional expressions in lambda parser."""
import unittest
from cloud_dataframe.core.dataframe import DataFrame
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.type_system.column import sum, avg

class TestConditionalExpressions(unittest.TestCase):
    def setUp(self):
        self.schema = TableSchema(
            name="Employee",
            columns={
                "id": int, "name": str, "department": str, "salary": float,
                "bonus": float, "is_manager": bool, "age": int
            }
        )
        self.df = DataFrame.from_table_schema("employees", self.schema, alias="x")
    
    def test_simple_if_else(self):
        query = self.df.select(
            lambda x: x.name,
            lambda x: x.department,
            lambda x: "High" if x.salary > 80000 else "Low"
        )
        sql = query.to_sql(dialect="duckdb")
        expected = "SELECT x.name, x.department, CASE WHEN x.salary > 80000 THEN 'High' ELSE 'Low' END\nFROM employees x"
        self.assertEqual(sql.strip(), expected)
