"""
Integration tests for Pure Relation language generation.

This module contains tests for generating Pure Relation language from DataFrame objects.
"""
import unittest
from dataclasses import dataclass
from typing import Optional

from cloud_dataframe.core.dataframe import DataFrame, TableReference
from cloud_dataframe.type_system.column import (
    col, literal, count, sum, avg, min, max
)
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.type_system.decorators import dataclass_to_schema


@dataclass
@dataclass_to_schema()
class Employee:
    """Employee dataclass for testing type-safe operations."""
    id: int
    name: str
    department: str
    salary: float
    manager_id: Optional[int] = None


@dataclass
@dataclass_to_schema()
class Department:
    """Department dataclass for testing type-safe operations."""
    id: int
    name: str
    location: str


class TestPureRelationGeneration(unittest.TestCase):
    """Test cases for Pure Relation language generation."""
    
    def test_simple_select(self):
        """Test generating Pure Relation for a simple SELECT query."""
        df = DataFrame.from_("employees", alias="x")
        relation = df.to_sql(dialect="pure_relation")
        
        print(f"Generated Pure Relation: {relation}")
        expected_relation = "employees->project()"
        self.assertEqual(relation.strip(), expected_relation)
    
    def test_select_columns(self):
        """Test generating Pure Relation for a SELECT query with specific columns."""
        df = DataFrame.from_("employees", alias="e").select(
            lambda e: (id := e.id),
            lambda e: (name := e.name)
        )
        
        relation = df.to_sql(dialect="pure_relation")
        expected_relation = "employees->project([x|$x.id, x|$x.name])"
        self.assertEqual(relation.strip(), expected_relation)
    
    def test_filter(self):
        """Test generating Pure Relation for a filtered query."""
        df = DataFrame.from_("employees", alias="x").filter(
            lambda x: x.salary > 50000
        )
        
        relation = df.to_sql(dialect="pure_relation")
        expected_relation = "employees->filter(x|$x.salary > 50000)"
        self.assertEqual(relation.strip(), expected_relation)
    
    def test_group_by(self):
        """Test generating Pure Relation for a GROUP BY query."""
        df = DataFrame.from_("employees", alias="x").group_by(lambda x: x.department).select(
            lambda x: x.department, 
            lambda x: (employee_count := count(x.id)), 
            lambda x: (avg_salary := avg(x.salary))
        )
        
        relation = df.to_sql(dialect="pure_relation")
        self.assertIn("employees->groupBy", relation)
        self.assertIn("count", relation)
        self.assertIn("avg", relation)
    
    def test_order_by(self):
        """Test generating Pure Relation for an ORDER BY query."""
        from cloud_dataframe.core.dataframe import Sort
        df = DataFrame.from_("employees", alias="x").order_by(lambda x: (x.salary, Sort.DESC))
        
        relation = df.to_sql(dialect="pure_relation")
        expected_relation = "employees->sort(~x.salary->descending())"
        self.assertEqual(relation.strip(), expected_relation)
    
    def test_limit_offset(self):
        """Test generating Pure Relation for a query with LIMIT and OFFSET."""
        df = DataFrame.from_("employees", alias="x") \
            .limit(10) \
            .offset(5)
        
        relation = df.to_sql(dialect="pure_relation")
        expected_relation = "employees->project()->slice(5, 10)"
        self.assertEqual(relation.strip(), expected_relation)
    
    def test_distinct(self):
        """Test generating Pure Relation for a DISTINCT query."""
        df = DataFrame.from_("employees", alias="x") \
            .distinct_rows() \
            .select(
                lambda x: (department := col("department"))
            )
        
        relation = df.to_sql(dialect="pure_relation")
        self.assertIn("distinct", relation)
    
    def test_join(self):
        """Test generating Pure Relation for a JOIN query."""
        employees = DataFrame.from_("employees", alias="e")
        departments = DataFrame.from_("departments", alias="d")
        
        joined_df = employees.join(
            departments,
            lambda e, d: e.department_id == d.id
        )
        
        relation = joined_df.to_sql(dialect="pure_relation")
        expected_relation = "employees->join(departments, x|$x.department_id == $x.id)"
        self.assertEqual(relation.strip(), expected_relation)
    
    def test_left_join(self):
        """Test generating Pure Relation for a LEFT JOIN query."""
        employees = DataFrame.from_("employees", alias="e")
        departments = DataFrame.from_("departments", alias="d")
        
        joined_df = employees.left_join(
            departments,
            lambda e, d: e.department_id == d.id
        )
        
        relation = joined_df.to_sql(dialect="pure_relation")
        expected_relation = "employees->leftJoin(departments, x|$x.department_id == $x.id)"
        self.assertEqual(relation.strip(), expected_relation)
    
    def test_with_cte(self):
        """Test generating Pure Relation for a query with a CTE."""
        dept_counts = DataFrame.from_("employees", alias="x").group_by(lambda x: x.department_id).select(
            lambda x: x.department_id, 
            lambda x: (employee_count := count(x.id))
        )
        
        df = DataFrame.from_("departments", alias="d").with_cte("dept_counts", dept_counts).join(TableReference(table_name="dept_counts", alias="dc"), lambda d, dc: d.id == dc.department_id)
        
        relation = df.to_sql(dialect="pure_relation")
        self.assertIn("departments->join", relation)
    
    def test_type_safe_operations(self):
        """Test generating Pure Relation for type-safe operations."""
        schema = TableSchema(name="Employee", columns={
            "id": int,
            "name": str,
            "department": str,
            "salary": float,
            "manager_id": Optional[int]
        })
        
        df = DataFrame.from_table_schema("employees", schema, alias="e")
        
        filtered_df = df.filter(
            lambda e: e.salary > 50000
        )
        
        relation = filtered_df.to_sql(dialect="pure_relation")
        expected_relation = "employees->filter(x|$x.salary > 50000)"
        self.assertEqual(relation.strip(), expected_relation)


if __name__ == "__main__":
    unittest.main()
