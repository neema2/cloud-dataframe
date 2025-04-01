"""
Integration tests for Pure Relation language generation with full expressions.

This module contains tests that show the complete Pure Relation expressions
generated from DataFrame objects for each operation type.
"""
import unittest
from dataclasses import dataclass
from typing import Optional

from cloud_dataframe.core.dataframe import DataFrame, TableReference, Sort
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


class TestPureRelationFullExpressions(unittest.TestCase):
    """Test cases showing full Pure Relation expressions for each operation."""
    
    def setUp(self):
        """Set up test cases."""
        print("\n" + "="*80)
        print(f"TEST: {self._testMethodName}")
        print("="*80)
    
    def test_simple_select(self):
        """Show full Pure Relation for a simple SELECT query."""
        print("\nDataFrame DSL:")
        print("DataFrame.from_(\"employees\", alias=\"e\")")
        
        df = DataFrame.from_("employees", alias="e")
        relation = df.to_sql(dialect="pure_relation")
        
        print("\nGenerated Pure Relation:")
        print(relation)
        
        print("\nExplanation:")
        print("The simple select is translated to a project() operation in Pure Relation")
        print("which returns all columns from the employees table.")
    
    def test_select_columns(self):
        """Show full Pure Relation for a SELECT query with specific columns."""
        print("\nDataFrame DSL:")
        print("""
DataFrame.from_("employees", alias="e").select(
    lambda e: (id := e.id),
    lambda e: (name := e.name),
    lambda e: (salary := e.salary)
)
        """)
        
        df = DataFrame.from_("employees", alias="e").select(
            lambda e: (id := e.id),
            lambda e: (name := e.name),
            lambda e: (salary := e.salary)
        )
        relation = df.to_sql(dialect="pure_relation")
        
        print("\nGenerated Pure Relation:")
        print(relation)
        
        print("\nExplanation:")
        print("The select with specific columns is translated to a project() operation")
        print("with an array of column expressions in Pure Relation.")
    
    def test_filter(self):
        """Show full Pure Relation for a filtered query."""
        print("\nDataFrame DSL:")
        print("""
DataFrame.from_("employees", alias="e").filter(
    lambda e: e.salary > 50000
)
        """)
        
        df = DataFrame.from_("employees", alias="e").filter(
            lambda e: e.salary > 50000
        )
        relation = df.to_sql(dialect="pure_relation")
        
        print("\nGenerated Pure Relation:")
        print(relation)
        
        print("\nExplanation:")
        print("The filter operation is translated to a filter() function in Pure Relation")
        print("with a lambda expression that defines the filter condition.")
    
    def test_group_by(self):
        """Show full Pure Relation for a GROUP BY query."""
        print("\nDataFrame DSL:")
        print("""
DataFrame.from_("employees", alias="e").group_by(
    lambda e: e.department
).select(
    lambda e: e.department,
    lambda e: (employee_count := count(e.id)),
    lambda e: (avg_salary := avg(e.salary))
)
        """)
        
        df = DataFrame.from_("employees", alias="e").group_by(
            lambda e: e.department
        ).select(
            lambda e: e.department,
            lambda e: (employee_count := count(e.id)),
            lambda e: (avg_salary := avg(e.salary))
        )
        relation = df.to_sql(dialect="pure_relation")
        
        print("\nGenerated Pure Relation:")
        print(relation)
        
        print("\nExplanation:")
        print("The group_by operation is translated to a groupBy() function in Pure Relation")
        print("with the grouping key and aggregate expressions.")
    
    def test_order_by(self):
        """Show full Pure Relation for an ORDER BY query."""
        print("\nDataFrame DSL:")
        print("""
DataFrame.from_("employees", alias="e").order_by(
    lambda e: (e.salary, Sort.DESC)
)
        """)
        
        df = DataFrame.from_("employees", alias="e").order_by(
            lambda e: (e.salary, Sort.DESC)
        )
        relation = df.to_sql(dialect="pure_relation")
        
        print("\nGenerated Pure Relation:")
        print(relation)
        
        print("\nExplanation:")
        print("The order_by operation is translated to a sort() function in Pure Relation")
        print("with a descending sort direction for the salary column.")
    
    def test_limit_offset(self):
        """Show full Pure Relation for a query with LIMIT and OFFSET."""
        print("\nDataFrame DSL:")
        print("""
DataFrame.from_("employees", alias="e").limit(10).offset(5)
        """)
        
        df = DataFrame.from_("employees", alias="e").limit(10).offset(5)
        relation = df.to_sql(dialect="pure_relation")
        
        print("\nGenerated Pure Relation:")
        print(relation)
        
        print("\nExplanation:")
        print("The limit and offset operations are translated to a slice() function in Pure Relation")
        print("which takes the starting index and the number of elements to return.")
    
    def test_distinct(self):
        """Show full Pure Relation for a DISTINCT query."""
        print("\nDataFrame DSL:")
        print("""
DataFrame.from_("employees", alias="e").distinct_rows().select(
    lambda e: (department := e.department)
)
        """)
        
        df = DataFrame.from_("employees", alias="e").distinct_rows().select(
            lambda e: (department := e.department)
        )
        relation = df.to_sql(dialect="pure_relation")
        
        print("\nGenerated Pure Relation:")
        print(relation)
        
        print("\nExplanation:")
        print("The distinct_rows operation is translated to a distinct() function in Pure Relation")
        print("which removes duplicate rows from the result set.")
    
    def test_join(self):
        """Show full Pure Relation for a JOIN query."""
        print("\nDataFrame DSL:")
        print("""
employees = DataFrame.from_("employees", alias="e")
departments = DataFrame.from_("departments", alias="d")
joined_df = employees.join(
    departments,
    lambda e, d: e.department_id == d.id
)
        """)
        
        employees = DataFrame.from_("employees", alias="e")
        departments = DataFrame.from_("departments", alias="d")
        joined_df = employees.join(
            departments,
            lambda e, d: e.department_id == d.id
        )
        relation = joined_df.to_sql(dialect="pure_relation")
        
        print("\nGenerated Pure Relation:")
        print(relation)
        
        print("\nExplanation:")
        print("The join operation is translated to a join() function in Pure Relation")
        print("with the right table and a lambda expression for the join condition.")
    
    def test_left_join(self):
        """Show full Pure Relation for a LEFT JOIN query."""
        print("\nDataFrame DSL:")
        print("""
employees = DataFrame.from_("employees", alias="e")
departments = DataFrame.from_("departments", alias="d")
joined_df = employees.left_join(
    departments,
    lambda e, d: e.department_id == d.id
)
        """)
        
        employees = DataFrame.from_("employees", alias="e")
        departments = DataFrame.from_("departments", alias="d")
        joined_df = employees.left_join(
            departments,
            lambda e, d: e.department_id == d.id
        )
        relation = joined_df.to_sql(dialect="pure_relation")
        
        print("\nGenerated Pure Relation:")
        print(relation)
        
        print("\nExplanation:")
        print("The left_join operation is translated to a leftJoin() function in Pure Relation")
        print("which keeps all rows from the left table even if there's no match in the right table.")
    
    def test_with_cte(self):
        """Show full Pure Relation for a query with a CTE."""
        print("\nDataFrame DSL:")
        print("""
dept_counts = DataFrame.from_("employees", alias="e").group_by(
    lambda e: e.department_id
).select(
    lambda e: e.department_id,
    lambda e: (employee_count := count(e.id))
)

df = DataFrame.from_("departments", alias="d").with_cte(
    "dept_counts", dept_counts
).join(
    TableReference(table_name="dept_counts", alias="dc"),
    lambda d, dc: d.id == dc.department_id
)
        """)
        
        dept_counts = DataFrame.from_("employees", alias="e").group_by(
            lambda e: e.department_id
        ).select(
            lambda e: e.department_id,
            lambda e: (employee_count := count(e.id))
        )
        
        df = DataFrame.from_("departments", alias="d").with_cte(
            "dept_counts", dept_counts
        ).join(
            TableReference(table_name="dept_counts", alias="dc"),
            lambda d, dc: d.id == dc.department_id
        )
        
        relation = df.to_sql(dialect="pure_relation")
        
        print("\nGenerated Pure Relation:")
        print(relation)
        
        print("\nExplanation:")
        print("The with_cte operation is translated to a let expression in Pure Relation")
        print("which defines a temporary relation that can be referenced in the main query.")
    
    def test_complex_query(self):
        """Show full Pure Relation for a complex query with multiple operations."""
        print("\nDataFrame DSL:")
        print("""
employees_df = DataFrame.from_("employees", alias="e")
filtered_df = employees_df.filter(lambda e: e.salary > 50000)
grouped_df = filtered_df.group_by(lambda e: e.department)
selected_df = grouped_df.select(
    lambda e: e.department,
    lambda e: (avg_salary := avg(e.salary)),
    lambda e: (max_salary := max(e.salary)),
    lambda e: (employee_count := count(e.id))
)
ordered_df = selected_df.order_by(lambda e: (e.avg_salary, Sort.DESC))
complex_df = ordered_df.limit(5)
        """)
        
        employees_df = DataFrame.from_("employees", alias="e")
        filtered_df = employees_df.filter(lambda e: e.salary > 50000)
        grouped_df = filtered_df.group_by(lambda e: e.department)
        selected_df = grouped_df.select(
            lambda e: e.department,
            lambda e: (avg_salary := avg(e.salary)),
            lambda e: (max_salary := max(e.salary)),
            lambda e: (employee_count := count(e.id))
        )
        ordered_df = selected_df.order_by(lambda e: (e.avg_salary, Sort.DESC))
        complex_df = ordered_df.limit(5)
        
        relation = complex_df.to_sql(dialect="pure_relation")
        
        print("\nGenerated Pure Relation:")
        print(relation)
        
        print("\nExplanation:")
        print("This complex query combines multiple operations:")
        print("1. filter() - Filters employees with salary > 50000")
        print("2. groupBy() - Groups by department")
        print("3. project() - Selects department and aggregate functions")
        print("4. sort() - Orders by avg_salary in descending order")
        print("5. limit() - Limits the result to 5 rows")


if __name__ == "__main__":
    unittest.main()
