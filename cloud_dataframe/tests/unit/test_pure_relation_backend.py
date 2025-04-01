"""
Unit tests for Pure Relation backend.

This module contains tests that verify the Pure Relation code generation
from cloud-dataframe DataFrame operations.
"""
import unittest
from cloud_dataframe.core.dataframe import DataFrame, Sort
from cloud_dataframe.type_system.column import col, literal, count, avg, sum


class TestPureRelationBackend(unittest.TestCase):
    """Test cases for Pure Relation backend."""
    
    def test_simple_filter(self):
        """Test a simple filter query."""
        df = DataFrame.from_("employees", alias="e")
        filtered_df = df.filter(lambda e: e.salary > 70000)
        code = filtered_df.to_sql(dialect="pure_relation")
        
        self.assertIn("->filter", code)
        self.assertIn("$e.salary", code)
        self.assertIn("> 70000", code)
    
    def test_join_with_lambda(self):
        """Test a join with lambda expression."""
        employees = DataFrame.from_("employees", alias="e")
        departments = DataFrame.from_("departments", alias="d")
        
        joined_df = employees.join(
            departments,
            lambda e, d: e.department_id == d.id
        )
        
        code = joined_df.to_sql(dialect="pure_relation")
        
        self.assertIn("->join", code)
        self.assertIn("JoinKind.INNER", code)
        self.assertIn("department_id", code)
        self.assertIn("id", code)
    
    def test_group_by_with_aggregation(self):
        """Test a group by with aggregation."""
        df = DataFrame.from_("employees")
        grouped_df = df.group_by(lambda x: x.department_id).select(
            lambda x: x.department_id,
            lambda x: (employee_count := count(x.id)),
            lambda x: (avg_salary := avg(x.salary))
        )
        
        code = grouped_df.to_sql(dialect="pure_relation")
        
        self.assertIn("->groupBy", code)
        self.assertIn("department_id", code)
        self.assertIn("employee_count", code)
        self.assertIn("avg_salary", code)
    
    def test_order_by(self):
        """Test an order by operation."""
        df = DataFrame.from_("employees", alias="e")
        ordered_df = df.order_by(
            lambda e: [(e.salary, Sort.DESC)]
        )
        
        code = ordered_df.to_sql(dialect="pure_relation")
        
        self.assertIn("->sort", code)
        self.assertIn("descending", code)
        self.assertIn("salary", code)
    
    def test_select_columns(self):
        """Test selecting specific columns."""
        df = DataFrame.from_("employees", alias="e")
        selected_df = df.select(
            lambda e: e.id,
            lambda e: e.name,
            lambda e: e.department_id
        )
        
        code = selected_df.to_sql(dialect="pure_relation")
        
        self.assertIn("->select", code)
        self.assertIn("id", code)
        self.assertIn("name", code)
        self.assertIn("department_id", code)
    
    def test_limit(self):
        """Test limit operation."""
        df = DataFrame.from_("employees")
        limited_df = df.limit(10)
        
        code = limited_df.to_sql(dialect="pure_relation")
        
        self.assertIn("->limit", code)
        self.assertIn("10", code)


if __name__ == "__main__":
    unittest.main()
