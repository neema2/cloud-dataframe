"""Unit tests for column aliasing using the := syntax."""
import unittest
from cloud_dataframe.core.dataframe import DataFrame
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.type_system.column import sum, avg

class TestColumnAlias(unittest.TestCase):
    def setUp(self):
        self.schema = TableSchema(name="Employee", columns={"id": int, "name": str, "salary": float})
        self.df = DataFrame.from_table_schema("employees", self.schema, alias="x")
    
    def test_simple_column_alias(self):
        query = self.df.select(lambda x: (employee_name := x.name))
        sql = query.to_sql(dialect="duckdb")
        expected = "SELECT x.name AS employee_name\nFROM employees x"
        self.assertEqual(sql.strip(), expected)
