"""
Debug script for testing nested function calls in lambda expressions.

This script demonstrates how to use nested function calls in lambda expressions
with the cloud-dataframe library.
"""
import pandas as pd
import duckdb
from typing import Optional
import inspect

from cloud_dataframe.core.dataframe import DataFrame
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.type_system.column import sum, avg, count, min, max, date_diff
from cloud_dataframe.utils.lambda_parser import LambdaParser


def main():
    """Main function to demonstrate nested function calls."""
    # Create a DuckDB connection
    conn = duckdb.connect(":memory:")
    
    # Create test data
    employees_data = pd.DataFrame({
        "id": [1, 2, 3, 4, 5, 6],
        "name": ["Alice", "Bob", "Charlie", "David", "Eve", "Frank"],
        "department": ["Engineering", "Engineering", "Sales", "Sales", "Marketing", "Marketing"],
        "salary": [80000.0, 90000.0, 70000.0, 75000.0, 65000.0, 60000.0],
        "bonus": [10000.0, 15000.0, 8000.0, 7500.0, 6000.0, 5000.0],
        "is_manager": [True, False, True, False, True, False],
        "manager_id": [None, 1, None, 3, None, 5],
        "start_date": ["2020-01-01", "2020-02-15", "2019-11-01", "2021-03-10", "2018-07-01", "2022-01-15"],
        "end_date": ["2023-12-31", "2023-12-31", "2023-12-31", "2023-12-31", "2023-12-31", "2023-12-31"]
    })
    
    # Create the employees table in DuckDB
    conn.register("employees", employees_data)
    
    # Create a schema for the employees table
    schema = TableSchema(
        name="Employee",
        columns={
            "id": int,
            "name": str,
            "department": str,
            "salary": float,
            "bonus": float,
            "is_manager": bool,
            "manager_id": Optional[int],
            "start_date": str,
            "end_date": str
        }
    )
    
    # Create a DataFrame with typed properties
    df = DataFrame.from_table_schema("employees", schema)
    
    # Debug lambda parsing
    print("\n=== Debug Lambda Parsing ===")
    
    # Example 1: Simple column reference
    lambda_func = lambda x: x.salary
    print(f"Lambda source: {inspect.getsource(lambda_func).strip()}")
    parsed = LambdaParser.parse_lambda(lambda_func)
    print(f"Parsed result: {parsed}")
    
    # Example 2: Binary operation
    lambda_func = lambda x: x.salary + x.bonus
    print(f"\nLambda source: {inspect.getsource(lambda_func).strip()}")
    parsed = LambdaParser.parse_lambda(lambda_func)
    print(f"Parsed result: {parsed}")
    
    # Example 3: Aggregate function with binary operation - direct style
    lambda_func = lambda x: sum(x.salary + x.bonus)
    print(f"\nLambda source: {inspect.getsource(lambda_func).strip()}")
    parsed = LambdaParser.parse_lambda(lambda_func)
    print(f"Parsed result: {parsed}")
    
    # Example 4: Scalar function - direct style
    lambda_func = lambda x: date_diff(x.start_date, x.end_date)
    print(f"\nLambda source: {inspect.getsource(lambda_func).strip()}")
    parsed = LambdaParser.parse_lambda(lambda_func)
    print(f"Parsed result: {parsed}")
    
    # Example 5: Complex expression with multiple aggregates - direct style
    lambda_func = lambda x: sum(x.salary) > avg(x.bonus)
    print(f"\nLambda source: {inspect.getsource(lambda_func).strip()}")
    parsed = LambdaParser.parse_lambda(lambda_func)
    print(f"Parsed result: {parsed}")
    
    # SQL Generation Examples
    print("\n=== SQL Generation Examples ===")
    
    # Example 1: Sum with binary operation - direct style
    print("\n=== Example 1: Sum with binary operation (direct style) ===")
    example1 = df.group_by(lambda x: x.department).select(
        lambda x: x.department,
        as_column(lambda x: sum(x.salary + x.bonus), "total_compensation")
    )
    
    sql1 = example1.to_sql(dialect="duckdb")
    print(f"SQL: {sql1}")
    
    try:
        result1 = conn.execute(sql1).fetchdf()
        print("Result:")
        print(result1)
    except Exception as e:
        print(f"Error executing SQL: {e}")
    
    # Example 2: Multiple aggregates with expressions - direct style
    print("\n=== Example 2: Multiple aggregates with expressions (direct style) ===")
    example2 = df.group_by(lambda x: x.department).select(
        lambda x: x.department,
        as_column(lambda x: sum(x.salary), "total_salary"),
        as_column(lambda x: avg(x.salary / 12), "avg_monthly_salary"),
        as_column(lambda x: max(x.salary + x.bonus), "max_total_comp")
    )
    
    sql2 = example2.to_sql(dialect="duckdb")
    print(f"SQL: {sql2}")
    
    try:
        result2 = conn.execute(sql2).fetchdf()
        print("Result:")
        print(result2)
    except Exception as e:
        print(f"Error executing SQL: {e}")
    
    # Example 3: Having with aggregate expression - direct style
    print("\n=== Example 3: Having with aggregate expression (direct style) ===")
    # Create a simpler version for testing
    example3 = df.group_by(lambda x: x.department).select(
        lambda x: x.department,
        as_column(lambda x: count(x.id), "employee_count")
    )
    
    sql3 = example3.to_sql(dialect="duckdb")
    print(f"SQL: {sql3}")
    
    try:
        result3 = conn.execute(sql3).fetchdf()
        print("Result:")
        print(result3)
    except Exception as e:
        print(f"Error executing SQL: {e}")
    
    # Example 4: Filter with complex expression
    print("\n=== Example 4: Filter with complex expression ===")
    example4 = df.filter(
        lambda x: (x.salary > 50000) and (x.bonus / x.salary > 0.1)
    )
    
    sql4 = example4.to_sql(dialect="duckdb")
    print(f"SQL: {sql4}")
    
    try:
        result4 = conn.execute(sql4).fetchdf()
        print("Result:")
        print(result4)
    except Exception as e:
        print(f"Error executing SQL: {e}")
    
    # Example 5: Scalar function date_diff - direct style
    print("\n=== Example 5: Scalar function date_diff (direct style) ===")
    example5 = df.select(
        lambda x: x.name,
        lambda x: x.department,
        as_column(lambda x: date_diff(x.start_date, x.end_date), "days_employed")
    )
    
    # Create a new table with proper date types
    conn.execute("""
        CREATE OR REPLACE TABLE employees_with_dates AS
        SELECT 
            id, name, department, salary, bonus, is_manager, manager_id,
            CAST(start_date AS DATE) AS start_date,
            CAST(end_date AS DATE) AS end_date
        FROM employees
    """)
    
    # Update the example to use the new table
    example5 = DataFrame.from_table_schema("employees_with_dates", schema).select(
        lambda x: x.name,
        lambda x: x.department,
        as_column(lambda x: date_diff(x.start_date, x.end_date), "days_employed")
    )
    
    sql5 = example5.to_sql(dialect="duckdb")
    print(f"SQL: {sql5}")
    
    try:
        result5 = conn.execute(sql5).fetchdf()
        print("Result:")
        print(result5)
    except Exception as e:
        print(f"Error executing SQL: {e}")
    
    # Close the connection
    conn.close()


if __name__ == "__main__":
    main()
