"""
Integration tests for complex filter conditions.

This module contains tests for complex filter conditions using lambda expressions.
"""
import unittest
from dataclasses import dataclass
from typing import Optional

from cloud_dataframe.core.dataframe import DataFrame
from cloud_dataframe.type_system.column import col, literal
from cloud_dataframe.type_system.decorators import dataclass_to_schema


@dataclass
@dataclass_to_schema()
class Employee:
    """Employee dataclass for testing complex filter conditions."""
    id: int
    name: str
    department: str
    salary: float
    age: int
    is_manager: bool
    hire_date: str
    manager_id: Optional[int] = None


class TestComplexFilterConditions(unittest.TestCase):
    """Test cases for complex filter conditions."""
    
    def test_simple_comparison(self):
        """Test simple comparison filter."""
        df = DataFrame.from_("employees", alias="e").filter(
            lambda e: e.salary > 50000
        )
        
        sql = df.to_sql(dialect="duckdb")
        expected_sql = "SELECT *\nFROM employees AS e\nWHERE e.salary > 50000"
        self.assertEqual(sql.strip(), expected_sql)
    
    def test_boolean_and_condition(self):
        """Test filter with AND boolean condition."""
        df = DataFrame.from_("employees", alias="e").filter(
            lambda e: e.salary > 50000 and e.department == "Engineering"
        )
        
        sql = df.to_sql(dialect="duckdb")
        expected_sql = "SELECT *\nFROM employees AS e\nWHERE e.salary > 50000 AND e.department = 'Engineering'"
        self.assertEqual(sql.strip(), expected_sql)
    
    def test_boolean_or_condition(self):
        """Test filter with OR boolean condition."""
        df = DataFrame.from_("employees", alias="e").filter(
            lambda e: e.department == "Engineering" or e.department == "Sales"
        )
        
        sql = df.to_sql(dialect="duckdb")
        expected_sql = "SELECT *\nFROM employees AS e\nWHERE (e.department = 'Engineering' OR e.department = 'Sales')"
        self.assertEqual(sql.strip(), expected_sql)
    
    def test_complex_boolean_condition(self):
        """Test filter with complex boolean condition (AND + OR)."""
        df = DataFrame.from_("employees", alias="e").filter(
            lambda e: (e.department == "Engineering" or e.department == "Sales") and e.salary > 60000
        )
        
        sql = df.to_sql(dialect="duckdb")
        expected_sql = "SELECT *\nFROM employees AS e\nWHERE (e.department = 'Engineering' OR e.department = 'Sales') AND e.salary > 60000"
        self.assertEqual(sql.strip(), expected_sql)
    
    def test_multiple_and_conditions(self):
        """Test filter with multiple AND conditions."""
        df = DataFrame.from_("employees", alias="e").filter(
            lambda e: e.salary > 50000 and e.age > 30 and e.is_manager == True
        )
        
        sql = df.to_sql(dialect="duckdb")
        expected_sql = "SELECT *\nFROM employees AS e\nWHERE e.salary > 50000 AND e.age > 30 AND e.is_manager = TRUE"
        self.assertEqual(sql.strip(), expected_sql)
    
    def test_multiple_or_conditions(self):
        """Test filter with multiple OR conditions."""
        df = DataFrame.from_("employees", alias="e").filter(
            lambda e: e.department == "Engineering" or e.department == "Sales" or e.department == "Marketing"
        )
        
        sql = df.to_sql(dialect="duckdb")
        expected_sql = "SELECT *\nFROM employees AS e\nWHERE e.department = 'Engineering' OR e.department = 'Sales' OR e.department = 'Marketing'"
        self.assertEqual(sql.strip(), expected_sql)
    
    def test_not_equal_condition(self):
        """Test filter with not equal condition."""
        df = DataFrame.from_("employees", alias="e").filter(
            lambda e: e.department != "HR"
        )
        
        sql = df.to_sql(dialect="duckdb")
        expected_sql = "SELECT *\nFROM employees AS e\nWHERE e.department != 'HR'"
        self.assertEqual(sql.strip(), expected_sql)
    
    def test_less_than_equal_condition(self):
        """Test filter with less than or equal condition."""
        df = DataFrame.from_("employees", alias="e").filter(
            lambda e: e.age <= 40
        )
        
        sql = df.to_sql(dialect="duckdb")
        expected_sql = "SELECT *\nFROM employees AS e\nWHERE e.age <= 40"
        self.assertEqual(sql.strip(), expected_sql)
    
    def test_greater_than_equal_condition(self):
        """Test filter with greater than or equal condition."""
        df = DataFrame.from_("employees", alias="e").filter(
            lambda e: e.salary >= 75000
        )
        
        sql = df.to_sql(dialect="duckdb")
        expected_sql = "SELECT *\nFROM employees AS e\nWHERE e.salary >= 75000"
        self.assertEqual(sql.strip(), expected_sql)
    
    def test_boolean_equality(self):
        """Test filter with boolean equality."""
        df = DataFrame.from_("employees", alias="e").filter(
            lambda e: e.is_manager == True
        )
        
        sql = df.to_sql(dialect="duckdb")
        expected_sql = "SELECT *\nFROM employees AS e\nWHERE e.is_manager = TRUE"
        self.assertEqual(sql.strip(), expected_sql)
    
    def test_complex_nested_condition(self):
        """Test filter with complex nested condition."""
        df = DataFrame.from_("employees", alias="x").filter(
            lambda x: ((x.department == "Engineering" and x.salary > 80000) or (x.department == "Sales" and x.salary > 60000) or (x.is_manager == True and x.age > 40))
        )
        
        sql = df.to_sql(dialect="duckdb")
        expected_sql = "SELECT *\nFROM employees AS x\nWHERE x.department = 'Engineering' AND x.salary > 80000 OR x.department = 'Sales' AND x.salary > 60000 OR x.is_manager = TRUE AND x.age > 40"
        self.assertEqual(sql.strip(), expected_sql)
    
    def test_chained_filters(self):
        """Test chaining multiple filters."""
        df = DataFrame.from_("employees", alias="e").filter(
            lambda e: (e.salary > 50000) and (e.department == "Engineering") and (e.age > 30)
        )
        
        sql = df.to_sql(dialect="duckdb")
        expected_sql = "SELECT *\nFROM employees AS e\nWHERE e.salary > 50000 AND e.department = 'Engineering' AND e.age > 30"
        self.assertEqual(sql.strip(), expected_sql)


if __name__ == "__main__":
    unittest.main()
