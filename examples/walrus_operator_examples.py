"""
Example queries demonstrating column renaming with the walrus operator in select() statements.

This file shows different ways to use the walrus operator (:=) for column aliasing 
in cloud-dataframe when working with DuckDB.
"""
import pandas as pd
import duckdb
from typing import Optional

from cloud_dataframe.core.dataframe import DataFrame
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.type_system.column import (
    as_column, count, sum, avg, min, max
)

def setup_sample_data():
    """Set up sample data in DuckDB for the examples."""
    conn = duckdb.connect(":memory:")
    
    employees_data = pd.DataFrame({
        "id": [1, 2, 3, 4, 5, 6],
        "name": ["Alice", "Bob", "Charlie", "David", "Eve", "Frank"],
        "department": ["Engineering", "Engineering", "Sales", "Sales", "Marketing", "Marketing"],
        "salary": [80000.0, 90000.0, 70000.0, 75000.0, 65000.0, 60000.0],
        "hire_date": ["2020-01-15", "2020-03-20", "2021-02-10", "2020-05-15", "2021-06-01", "2020-01-10"],
        "manager_id": [None, 1, 1, 3, 4, None]
    })
    
    departments_data = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Engineering", "Sales", "Marketing"],
        "budget": [1000000.0, 800000.0, 600000.0],
        "location": ["New York", "Chicago", "San Francisco"]
    })
    
    conn.register("employees", employees_data)
    conn.register("departments", departments_data)
    
    employee_schema = TableSchema(
        name="Employee",
        columns={
            "id": int,
            "name": str,
            "department": str,
            "salary": float,
            "hire_date": str,
            "manager_id": Optional[int]
        }
    )
    
    department_schema = TableSchema(
        name="Department",
        columns={
            "id": int,
            "name": str,
            "budget": float,
            "location": str
        }
    )
    
    return conn, employee_schema, department_schema

def example_1_basic_column_aliasing():
    """
    Example 1: Basic column aliasing with the walrus operator.
    
    This demonstrates the simplest form of column renaming using the walrus operator,
    where columns are directly referenced from the DataFrame.
    """
    print("\n=== Example 1: Basic Column Aliasing ===")
    conn, employee_schema, _ = setup_sample_data()
    
    df = DataFrame.from_table_schema("employees", employee_schema)
    
    query = df.select(
        emp_id := lambda x: x.id,          # Simple renaming
        full_name := lambda x: x.name,     # Changing column name for clarity
        dept := lambda x: x.department,    # Shortening column name
        annual_income := lambda x: x.salary  # More descriptive column name
    )
    
    sql = query.to_sql(dialect="duckdb")
    print("SQL:")
    print(sql)
    
    result = conn.execute(sql).fetchdf()
    print("\nResult:")
    print(result)
    
def example_2_calculated_columns():
    """
    Example 2: Using the walrus operator with calculated columns.
    
    This example shows how to use the walrus operator to create new columns
    that are calculated from existing columns.
    """
    print("\n=== Example 2: Calculated Columns ===")
    conn, employee_schema, _ = setup_sample_data()
    
    df = DataFrame.from_table_schema("employees", employee_schema)
    
    query = df.select(
        employee_name := lambda x: x.name,
        department := lambda x: x.department,
        salary := lambda x: x.salary,
        monthly_salary := lambda x: x.salary / 12,  # Calculate monthly salary
        salary_with_bonus := lambda x: x.salary * 1.15,  # Calculate salary with 15% bonus
        tax_amount := lambda x: x.salary * 0.3  # Calculate tax amount (30% of salary)
    )
    
    sql = query.to_sql(dialect="duckdb")
    print("SQL:")
    print(sql)
    
    result = conn.execute(sql).fetchdf()
    print("\nResult:")
    print(result)

def example_3_conditional_expressions():
    """
    Example 3: Using the walrus operator with conditional expressions.
    
    This example demonstrates how to use the walrus operator with conditional
    expressions to create columns with values based on conditions.
    """
    print("\n=== Example 3: Conditional Expressions ===")
    conn, employee_schema, _ = setup_sample_data()
    
    df = DataFrame.from_table_schema("employees", employee_schema)
    
    query = df.select(
        employee_name := lambda x: x.name,
        department := lambda x: x.department,
        salary := lambda x: x.salary,
        performance_category := lambda x: "High" if x.salary > 80000 else "Medium" if x.salary > 70000 else "Standard",
        bonus_eligible := lambda x: x.salary > 75000,
        bonus_amount := lambda x: x.salary * 0.2 if x.salary > 80000 else x.salary * 0.1 if x.salary > 70000 else 0
    )
    
    sql = query.to_sql(dialect="duckdb")
    print("SQL:")
    print(sql)
    
    result = conn.execute(sql).fetchdf()
    print("\nResult:")
    print(result)

def example_4_aggregations():
    """
    Example 4: Using the walrus operator with aggregation functions.
    
    This example shows how to use the walrus operator with aggregation
    functions in a group by query.
    """
    print("\n=== Example 4: Aggregation Functions ===")
    conn, employee_schema, _ = setup_sample_data()
    
    df = DataFrame.from_table_schema("employees", employee_schema)
    
    query = df.group_by(
        lambda x: x.department
    ).select(
        department := lambda x: x.department,
        employee_count := lambda x: count(x.id),  # Count employees per department
        total_salary := lambda x: sum(x.salary),  # Sum of salaries per department
        avg_salary := lambda x: avg(x.salary),  # Average salary per department
        min_salary := lambda x: min(x.salary),  # Minimum salary per department
        max_salary := lambda x: max(x.salary)   # Maximum salary per department
    )
    
    sql = query.to_sql(dialect="duckdb")
    print("SQL:")
    print(sql)
    
    result = conn.execute(sql).fetchdf()
    print("\nResult:")
    print(result)

def example_5_joining_tables():
    """
    Example 5: Using the walrus operator with joined tables.
    
    This example demonstrates how to use the walrus operator to rename
    columns when working with joined tables.
    """
    print("\n=== Example 5: Joining Tables ===")
    conn, employee_schema, department_schema = setup_sample_data()
    
    employees_df = DataFrame.from_table_schema("employees", employee_schema, alias="e")
    departments_df = DataFrame.from_table_schema("departments", department_schema, alias="d")
    
    joined_df = employees_df.join(
        departments_df,
        lambda e, d: e.department == d.name
    )
    
    query = joined_df.select(
        employee_id := lambda x: x.e.id,
        employee_name := lambda x: x.e.name,
        department_name := lambda x: x.d.name,
        location := lambda x: x.d.location,
        annual_salary := lambda x: x.e.salary,
        department_budget := lambda x: x.d.budget,
        budget_percentage := lambda x: (x.e.salary / x.d.budget) * 100
    )
    
    sql = query.to_sql(dialect="duckdb")
    print("SQL:")
    print(sql)
    
    result = conn.execute(sql).fetchdf()
    print("\nResult:")
    print(result)

def example_6_combined_approach():
    """
    Example 6: Combining different approaches with the walrus operator.
    
    This example shows a more complex query that combines multiple techniques:
    - Direct column renaming
    - Calculated columns
    - Filtering
    - Grouping
    - Aggregations
    - Ordering
    """
    print("\n=== Example 6: Combined Approach ===")
    conn, employee_schema, department_schema = setup_sample_data()
    
    employees_df = DataFrame.from_table_schema("employees", employee_schema, alias="e")
    departments_df = DataFrame.from_table_schema("departments", department_schema, alias="d")
    
    joined_df = employees_df.join(
        departments_df,
        lambda e, d: e.department == d.name
    )
    
    filtered_df = joined_df.filter(
        lambda x: x.e.salary > 65000
    )
    
    query = filtered_df.group_by(
        lambda x: x.d.name,
        lambda x: x.d.location,
        lambda x: x.d.budget
    ).select(
        department := lambda x: x.d.name,
        location := lambda x: x.d.location,
        employee_count := lambda x: count(x.e.id),
        total_salary := lambda x: sum(x.e.salary),
        avg_salary := lambda x: avg(x.e.salary),
        budget_utilization := lambda x: (sum(x.e.salary) / x.d.budget) * 100
    ).order_by(
        lambda x: x.d.name
    )
    
    sql = query.to_sql(dialect="duckdb")
    print("SQL:")
    print(sql)
    
    result = conn.execute(sql).fetchdf()
    print("\nResult:")
    print(result)

def run_all_examples():
    """Run all examples demonstrating walrus operator usage."""
    example_1_basic_column_aliasing()
    example_2_calculated_columns()
    example_3_conditional_expressions()
    example_4_aggregations()
    example_5_joining_tables()
    example_6_combined_approach()

if __name__ == "__main__":
    run_all_examples()
