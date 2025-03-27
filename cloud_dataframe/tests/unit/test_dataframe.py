"""
Unit tests for the DataFrame class.

This module contains tests for the core DataFrame operations.
"""
import unittest
from dataclasses import dataclass
from typing import Optional, cast

from cloud_dataframe.core.dataframe import DataFrame, BinaryOperation, JoinType, TableReference
from cloud_dataframe.type_system.column import (
    col, literal, as_column, count, sum, avg, min, max
)
from cloud_dataframe.type_system.schema import TableSchema, ColSpec
from cloud_dataframe.type_system.decorators import dataclass_to_schema


class TestDataFrame(unittest.TestCase):
    """Test cases for the DataFrame class."""
    
    def test_select(self):
        """Test the select method."""
        df = DataFrame().select(
            as_column(col("id"), "id"),
            as_column(col("name"), "name")
        )
        
        self.assertEqual(len(df.columns), 2)
        self.assertEqual(df.columns[0].name, "id")
        self.assertEqual(df.columns[1].name, "name")
    
    def test_from_table(self):
        """Test the from_ method."""
        df = DataFrame.from_("employees", schema="public", alias="e")
        
        self.assertIsNotNone(df.source)
        self.assertIsInstance(df.source, TableReference)
        # Cast to TableReference to access specific attributes
        table_ref = cast(TableReference, df.source)
        self.assertEqual(table_ref.table_name, "employees")
        self.assertEqual(table_ref.schema, "public")
        self.assertEqual(table_ref.alias, "e")
    
    def test_filter(self):
        """Test the filter method."""
        df = DataFrame.from_("employees", alias="e")
        filtered_df = df.filter(
            lambda e: e.salary > 50000
        )
        
        self.assertIsNotNone(filtered_df.filter_condition)
    
    def test_group_by(self):
        """Test the group_by method."""
        df = DataFrame.from_("employees")
        grouped_df = df.group_by(lambda x: x.department)
        
        self.assertIsNotNone(grouped_df.group_by_clauses)
        # Check that group_by is properly initialized
        self.assertIsInstance(grouped_df.group_by_clauses, list)
        self.assertEqual(len(grouped_df.group_by_clauses), 1)  # Verify one column in group by
    
    def test_order_by(self):
        """Test the order_by method."""
        df = DataFrame.from_("employees")
        ordered_df = df.order_by(lambda x: x.salary, desc=True)
        
        self.assertEqual(len(ordered_df.order_by_clauses), 1)
    
    def test_limit(self):
        """Test the limit method."""
        df = DataFrame.from_("employees")
        limited_df = df.limit(10)
        
        self.assertEqual(limited_df.limit_value, 10)
    
    def test_offset(self):
        """Test the offset method."""
        df = DataFrame.from_("employees")
        offset_df = df.offset(5)
        
        self.assertEqual(offset_df.offset_value, 5)
    
    def test_distinct(self):
        """Test the distinct_rows method."""
        df = DataFrame.from_("employees")
        distinct_df = df.distinct_rows()
        
        self.assertTrue(distinct_df.distinct)
    
    def test_join(self):
        """Test the join method."""
        employees = DataFrame.from_("employees", alias="e")
        departments = DataFrame.from_("departments", alias="d")
        
        joined_df = employees.join(
            departments,
            lambda e, d: e.department_id == d.id
        )
        
        self.assertIsNotNone(joined_df.source)
    
    def test_left_join(self):
        """Test the left_join method."""
        employees = DataFrame.from_("employees", alias="e")
        departments = DataFrame.from_("departments", alias="d")
        
        joined_df = employees.left_join(
            departments,
            lambda e, d: e.department_id == d.id
        )
        
        self.assertIsNotNone(joined_df.source)
    
    def test_with_cte(self):
        """Test the with_cte method."""
        dept_counts = DataFrame.from_("employees") \
            .group_by(lambda x: x.department_id) \
            .select(
                lambda x: x.department_id,
                as_column(count(lambda x: x.id), "employee_count")
            )
        
        df = DataFrame.from_("departments") \
            .with_cte("dept_counts", dept_counts)
        
        self.assertEqual(len(df.ctes), 1)
        self.assertEqual(df.ctes[0].name, "dept_counts")


@dataclass_to_schema()
class Employee:
    """Employee dataclass for testing type-safe operations."""
    id: int
    name: str
    department: str
    salary: float
    manager_id: Optional[int] = None


class TestTypeSafeOperations(unittest.TestCase):
    """Test cases for type-safe DataFrame operations."""
    
    def test_from_table_schema(self):
        """Test creating a DataFrame with a schema."""
        # Create a schema manually since the decorator might not have applied yet
        schema = TableSchema(name="Employee", columns={
            "id": int,
            "name": str,
            "department": str,
            "salary": float,
            "manager_id": Optional[int]
        })
        
        df = DataFrame.from_table_schema("employees", schema)
        
        self.assertIsNotNone(df.source)
        self.assertIsInstance(df.source, TableReference)
        table_ref = cast(TableReference, df.source)
        self.assertEqual(table_ref.table_name, "employees")
    
    def test_col_spec(self):
        """Test creating a ColSpec from a dataclass field."""
        # Create a schema manually since the decorator might not have applied yet
        schema = TableSchema(name="Employee", columns={
            "id": int,
            "name": str,
            "department": str,
            "salary": float,
            "manager_id": Optional[int]
        })
        
        col_spec = ColSpec(name="salary", table_schema=schema)
        
        self.assertEqual(col_spec.name, "salary")
        self.assertEqual(col_spec.table_schema, schema)


if __name__ == "__main__":
    unittest.main()
