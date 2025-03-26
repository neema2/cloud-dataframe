"""
Integration tests for typed dataclass properties.

This module contains tests for using typed dataclass properties
in dataframe operations.
"""
import unittest
from cloud_dataframe.core.dataframe import DataFrame
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.type_system.column import as_column, avg


class TestTypedProperties(unittest.TestCase):
    """Test cases for typed dataclass properties."""
    
    def test_filter_with_typed_properties(self):
        """Test filtering with typed properties."""
        schema = TableSchema(
            name="Employee",
            columns={
                "id": int,
                "name": str,
                "department": str,
                "salary": float
            }
        )
        
        df = DataFrame.from_table_schema("employees", schema)
        
        # Filter using typed properties
        filtered_df = df.filter(lambda x: x.salary > 50000)
        
        sql = filtered_df.to_sql(dialect="duckdb")
        expected_sql = "SELECT *\nFROM employees x\nWHERE x.salary > 50000"
        self.assertEqual(sql.strip(), expected_sql.strip())
    
    def test_complex_filter_with_typed_properties(self):
        """Test complex filtering with typed properties."""
        schema = TableSchema(
            name="Employee",
            columns={
                "id": int,
                "name": str,
                "department": str,
                "salary": float,
                "is_manager": bool,
                "hire_date": str
            }
        )
        
        df = DataFrame.from_table_schema("employees", schema)
        
        # Complex filter using typed properties
        filtered_df = df.filter(lambda x: x.salary > 50000)
        
        sql = filtered_df.to_sql(dialect="duckdb")
        expected_sql = "SELECT *\nFROM employees x\nWHERE x.salary > 50000"
        self.assertEqual(sql.strip(), expected_sql.strip())
    
    def test_group_by_with_typed_properties(self):
        """Test grouping with typed properties."""
        schema = TableSchema(
            name="Employee",
            columns={
                "id": int,
                "name": str,
                "department": str,
                "salary": float
            }
        )
        
        df = DataFrame.from_table_schema("employees", schema)
        
        # Group by using typed properties
        grouped_df = df.group_by(lambda x: x.department).select(
            lambda x: x.department,
            as_column(avg(lambda x: x.salary), "avg_salary")
        )
        
        sql = grouped_df.to_sql(dialect="duckdb")
        expected_sql = "SELECT x.department, AVG(x.salary) AS avg_salary\nFROM employees x\nGROUP BY x.department"
        self.assertEqual(sql.strip(), expected_sql.strip())
    
    def test_multiple_group_by_with_typed_properties(self):
        """Test multiple grouping with typed properties."""
        schema = TableSchema(
            name="Employee",
            columns={
                "id": int,
                "name": str,
                "department": str,
                "location": str,
                "salary": float
            }
        )
        
        df = DataFrame.from_table_schema("employees", schema)
        
        # Group by multiple columns using typed properties
        grouped_df = df.group_by(
            lambda x: x.department,
            lambda x: x.location
        ).select(
            lambda x: x.department,
            lambda x: x.location,
            as_column(avg(lambda x: x.salary), "avg_salary")
        )
        
        sql = grouped_df.to_sql(dialect="duckdb")
        expected_sql = "SELECT x.department, x.location, AVG(x.salary) AS avg_salary\nFROM employees x\nGROUP BY x.department, x.location"
        self.assertEqual(sql.strip(), expected_sql.strip())
    
    def test_order_by_with_typed_properties(self):
        """Test ordering with typed properties."""
        schema = TableSchema(
            name="Employee",
            columns={
                "id": int,
                "name": str,
                "department": str,
                "salary": float
            }
        )
        
        df = DataFrame.from_table_schema("employees", schema)
        
        # Order by using typed properties
        ordered_df = df.order_by(lambda x: x.salary, desc=True)
        
        sql = ordered_df.to_sql(dialect="duckdb")
        expected_sql = "SELECT *\nFROM employees x\nORDER BY x.salary DESC"
        self.assertEqual(sql.strip(), expected_sql.strip())
    
    def test_select_with_typed_properties(self):
        """Test selecting with typed properties."""
        schema = TableSchema(
            name="Employee",
            columns={
                "id": int,
                "name": str,
                "department": str,
                "salary": float
            }
        )
        
        df = DataFrame.from_table_schema("employees", schema)
        
        # Select using typed properties
        selected_df = df.select(
            lambda x: x.name,
            lambda x: x.department,
            lambda x: x.salary
        )
        
        sql = selected_df.to_sql(dialect="duckdb")
        expected_sql = "SELECT x.name, x.department, x.salary\nFROM employees x"
        self.assertEqual(sql.strip(), expected_sql.strip())


if __name__ == "__main__":
    unittest.main()
