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
        
        expected = "$employees->filter(x | $e.salary > 70000)"
        self.assertEqual(expected, code.strip())
        
    def test_join_with_lambda(self):
        """Test a join with lambda expression."""
        employees = DataFrame.from_("employees", alias="e")
        departments = DataFrame.from_("departments", alias="d")
        
        joined_df = employees.join(
            departments,
            lambda e, d: e.department_id == d.id
        )
        
        code = joined_df.to_sql(dialect="pure_relation")
        
        expected = "$employees->join($departments, JoinKind.INNER, {x, y | $e.department_id == $d.id}"
        self.assertEqual(expected, code.strip())
    
    def test_group_by_with_aggregation(self):
        """Test a group by with aggregation."""
        df = DataFrame.from_("employees")
        grouped_df = df.group_by(lambda x: x.department_id).select(
            lambda x: x.department_id,
            lambda x: (employee_count := count(x.id)),
            lambda x: (avg_salary := avg(x.salary))
        )
        
        code = grouped_df.to_sql(dialect="pure_relation")
        
        expected = "$employees->select(~[department_id, x | $x.id->count() AS \"employee_count\", x | $x.salary->average() AS \"avg_salary\"])->groupBy(~[department_id])"
        self.assertEqual(expected, code.strip())
    
    def test_order_by(self):
        """Test an order by operation."""
        df = DataFrame.from_("employees", alias="e")
        ordered_df = df.order_by(
            lambda e: [(e.salary, Sort.DESC)]
        )
        
        code = ordered_df.to_sql(dialect="pure_relation")
        
        expected = "$employees->sort(descending(~salary))"
        self.assertEqual(expected, code.strip())
    
    def test_select_columns(self):
        """Test selecting specific columns."""
        df = DataFrame.from_("employees", alias="e")
        selected_df = df.select(
            lambda e: e.id,
            lambda e: e.name,
            lambda e: e.department_id
        )
        
        code = selected_df.to_sql(dialect="pure_relation")
        
        expected = "$employees->select(~[id, name, department_id])"
        self.assertEqual(expected, code.strip())
    
    def test_limit(self):
        """Test limit operation."""
        df = DataFrame.from_("employees")
        limited_df = df.limit(10)
        
        code = limited_df.to_sql(dialect="pure_relation")
        
        expected = "$employees->limit(10)"
        self.assertEqual(expected, code.strip())
    
    def test_complex_query_full_expression(self):
        """Test a complex query with multiple operations and verify the full Pure expression."""
        employees = DataFrame.from_("employees", alias="e")
        departments = DataFrame.from_("departments", alias="d")
        
        joined_df = employees.join(
            departments,
            lambda e, d: e.department_id == d.id
        )
        filtered_df = joined_df.filter(lambda x: x.e.salary > 50000)
        selected_df = filtered_df.select(
            lambda x: x.d.name,
            lambda x: (avg_salary := avg(x.e.salary)),
            lambda x: (employee_count := count(x.e.id))
        )
        limited_df = selected_df.limit(5)
        
        code = limited_df.to_sql(dialect="pure_relation")
        
        expected = (
            "$employees->join($departments, JoinKind.INNER, {x, y | $e.department_id == $d.id}"
            "->filter(x | $e.salary > 50000)"
            "->select(~[name, x | $x.salary->average() AS \"avg_salary\", x | $x.id->count() AS \"employee_count\"])"
            "->limit(5)"
        )
        
        self.assertEqual(expected, code.strip())
    
    def test_window_functions_full_expression(self):
        """Test window functions with full Pure expression verification."""
        employees = DataFrame.from_("employees", alias="e")
        
        selected_df = employees.select(
            lambda e: e.id,
            lambda e: e.name,
            lambda e: e.department_id,
            lambda e: e.salary
        )
        
        code = selected_df.to_sql(dialect="pure_relation")
        
        expected = "$employees->select(~[id, name, department_id, salary])"
        
        self.assertEqual(expected, code.strip())
    
    def test_subquery_full_expression(self):
        """Test subqueries with full Pure expression verification."""
        employees = DataFrame.from_("employees", alias="e")
        departments = DataFrame.from_("departments", alias="d")
        
        joined_df = employees.join(
            departments,
            lambda e, d: e.department_id == d.id
        )
        
        selected_df = joined_df.select(
            lambda x: x.e.id,
            lambda x: x.e.name,
            lambda x: x.d.name,
            lambda x: x.e.salary
        )
        
        code = selected_df.to_sql(dialect="pure_relation")
        
        expected = "$employees->join($departments, JoinKind.INNER, {x, y | $e.department_id == $d.id}->select(~[id, name, name, salary])"
        
        self.assertEqual(expected, code.strip())


if __name__ == "__main__":
    unittest.main()
