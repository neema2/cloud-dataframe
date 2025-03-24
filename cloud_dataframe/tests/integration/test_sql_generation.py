"""
Integration tests for SQL generation.

This module contains tests for generating SQL from DataFrame objects.
"""
import unittest
from dataclasses import dataclass
from typing import Optional

from cloud_dataframe.core.dataframe import DataFrame, BinaryOperation, JoinType, TableReference
from cloud_dataframe.type_system.column import (
    col, literal, as_column, count, sum, avg, min, max
)
from cloud_dataframe.type_system.schema import TableSchema, ColSpec
from cloud_dataframe.type_system.decorators import dataclass_to_schema


@dataclass_to_schema()
class Employee:
    """Employee dataclass for testing type-safe operations."""
    id: int
    name: str
    department: str
    salary: float
    manager_id: Optional[int] = None


@dataclass_to_schema()
class Department:
    """Department dataclass for testing type-safe operations."""
    id: int
    name: str
    location: str


class TestDuckDBSQLGeneration(unittest.TestCase):
    """Test cases for DuckDB SQL generation."""
    
    def test_simple_select(self):
        """Test generating SQL for a simple SELECT query."""
        df = DataFrame.from_table("employees")
        sql = df.to_sql(dialect="duckdb")
        
        print(f"Generated SQL: {sql}")
        expected_sql = "SELECT *\nFROM employees"
        self.assertEqual(sql.strip(), expected_sql)
    
    def test_select_columns(self):
        """Test generating SQL for a SELECT query with specific columns."""
        df = DataFrame.create_select(
            as_column(col("id"), "id"),
            as_column(col("name"), "name")
        ).from_table("employees")
        
        sql = df.to_sql(dialect="duckdb")
        expected_sql = "SELECT id AS id, name AS name\nFROM employees"
        self.assertEqual(sql.strip(), expected_sql)
    
    def test_filter(self):
        """Test generating SQL for a filtered query."""
        df = DataFrame.from_table("employees").filter(
            BinaryOperation(
                left=col("salary"),
                operator=">",
                right=literal(50000)
            )
        )
        
        sql = df.to_sql(dialect="duckdb")
        expected_sql = "SELECT *\nFROM employees\nWHERE salary > 50000"
        self.assertEqual(sql.strip(), expected_sql)
    
    def test_group_by(self):
        """Test generating SQL for a GROUP BY query."""
        df = DataFrame.from_table("employees") \
            .group_by_columns("department") \
            .select(
                as_column(col("department"), "department"),
                as_column(count("*"), "employee_count"),
                as_column(avg("salary"), "avg_salary")
            )
        
        sql = df.to_sql(dialect="duckdb")
        expected_sql = "SELECT department AS department, COUNT(*) AS employee_count, AVG(salary) AS avg_salary\nFROM employees\nGROUP BY department"
        self.assertEqual(sql.strip(), expected_sql)
    
    def test_order_by(self):
        """Test generating SQL for an ORDER BY query."""
        df = DataFrame.from_table("employees") \
            .order_by_columns("salary", desc=True)
        
        sql = df.to_sql(dialect="duckdb")
        expected_sql = "SELECT *\nFROM employees\nORDER BY salary DESC"
        self.assertEqual(sql.strip(), expected_sql)
    
    def test_limit_offset(self):
        """Test generating SQL for a query with LIMIT and OFFSET."""
        df = DataFrame.from_table("employees") \
            .limit(10) \
            .offset(5)
        
        sql = df.to_sql(dialect="duckdb")
        expected_sql = "SELECT *\nFROM employees\nLIMIT 10 OFFSET 5"
        self.assertEqual(sql.strip(), expected_sql)
    
    def test_distinct(self):
        """Test generating SQL for a DISTINCT query."""
        df = DataFrame.from_table("employees") \
            .distinct_rows() \
            .select(
                as_column(col("department"), "department")
            )
        
        sql = df.to_sql(dialect="duckdb")
        expected_sql = "SELECT DISTINCT department AS department\nFROM employees"
        self.assertEqual(sql.strip(), expected_sql)
    
    def test_join(self):
        """Test generating SQL for a JOIN query."""
        employees = DataFrame.from_table("employees", alias="e")
        departments = DataFrame.from_table("departments", alias="d")
        
        joined_df = employees.join(
            departments,
            BinaryOperation(
                left=col("department_id", "e"),
                operator="=",
                right=col("id", "d")
            )
        )
        
        sql = joined_df.to_sql(dialect="duckdb")
        expected_sql = "SELECT *\nFROM employees AS e INNER JOIN departments AS d ON e.department_id = d.id"
        self.assertEqual(sql.strip(), expected_sql)
    
    def test_left_join(self):
        """Test generating SQL for a LEFT JOIN query."""
        employees = DataFrame.from_table("employees", alias="e")
        departments = DataFrame.from_table("departments", alias="d")
        
        joined_df = employees.left_join(
            departments,
            BinaryOperation(
                left=col("department_id", "e"),
                operator="=",
                right=col("id", "d")
            )
        )
        
        sql = joined_df.to_sql(dialect="duckdb")
        expected_sql = "SELECT *\nFROM employees AS e LEFT JOIN departments AS d ON e.department_id = d.id"
        self.assertEqual(sql.strip(), expected_sql)
    
    def test_with_cte(self):
        """Test generating SQL for a query with a CTE."""
        dept_counts = DataFrame.from_table("employees") \
            .group_by_columns("department_id") \
            .select(
                as_column(col("department_id"), "department_id"),
                as_column(count("*"), "employee_count")
            )
        
        df = DataFrame.from_table("departments", alias="d") \
            .with_cte("dept_counts", dept_counts) \
            .join(
                TableReference(table_name="dept_counts", alias="dc"),
                BinaryOperation(
                    left=col("id", "d"),
                    operator="=",
                    right=col("department_id", "dc")
                )
            )
        
        sql = df.to_sql(dialect="duckdb")
        expected_sql = "WITH dept_counts AS (\nSELECT department_id AS department_id, COUNT(*) AS employee_count\nFROM employees\nGROUP BY department_id\n)\nSELECT *\nFROM departments AS d INNER JOIN dept_counts AS dc ON d.id = dc.department_id"
        self.assertEqual(sql.strip(), expected_sql)
    
    def test_type_safe_operations(self):
        """Test generating SQL for type-safe operations."""
        # Create a schema manually since the decorator might not have applied yet
        schema = TableSchema(name="Employee", columns={
            "id": int,
            "name": str,
            "department": str,
            "salary": float,
            "manager_id": Optional[int]
        })
        
        # Create a DataFrame with the schema
        df = DataFrame.from_table_schema("employees", schema, alias="e")
        
        # Filter using the schema
        filtered_df = df.filter(
            BinaryOperation(
                left=col("salary"),
                operator=">",
                right=literal(50000)
            )
        )
        
        sql = filtered_df.to_sql(dialect="duckdb")
        expected_sql = "SELECT *\nFROM employees AS e\nWHERE salary > 50000"
        self.assertEqual(sql.strip(), expected_sql)


if __name__ == "__main__":
    unittest.main()
