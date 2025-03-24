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
from cloud_dataframe.type_system.column import as_column, sum, avg, count, min, max, date_diff
from cloud_dataframe.utils.lambda_parser import parse_lambda


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
    parsed = parse_lambda(lambda_func)
    print(f"Parsed result: {parsed}")
    
    # Example 2: Binary operation
    lambda_func = lambda x: x.salary + x.bonus
    print(f"\nLambda source: {inspect.getsource(lambda_func).strip()}")
    parsed = parse_lambda(lambda_func)
    print(f"Parsed result: {parsed}")
    
    # Example 3: Aggregate function with binary operation
    lambda_func = lambda x: sum(lambda x: x.salary + x.bonus)
    print(f"\nLambda source: {inspect.getsource(lambda_func).strip()}")
    parsed = parse_lambda(lambda_func)
    print(f"Parsed result: {parsed}")
    
    # Example 4: Scalar function
    lambda_func = lambda x: date_diff(lambda x: x.start_date, lambda x: x.end_date)
    print(f"\nLambda source: {inspect.getsource(lambda_func).strip()}")
    parsed = parse_lambda(lambda_func)
    print(f"Parsed result: {parsed}")
    
    # Example 5: Complex expression with multiple aggregates
    lambda_func = lambda x: sum(lambda x: x.salary) > avg(lambda x: x.bonus)
    print(f"\nLambda source: {inspect.getsource(lambda_func).strip()}")
    parsed = parse_lambda(lambda_func)
    print(f"Parsed result: {parsed}")
    
    # SQL Generation Examples
    print("\n=== SQL Generation Examples ===")
    
    # Example 1: Sum with binary operation
    print("\n=== Example 1: Sum with binary operation ===")
    example1 = df.group_by(lambda x: x.department).select(
        lambda x: x.department,
        as_column(sum(lambda x: x.salary + x.bonus), "total_compensation")
    )
    
    sql1 = example1.to_sql(dialect="duckdb")
    print(f"SQL: {sql1}")
    
    result1 = conn.execute(sql1).fetchdf()
    print("Result:")
    print(result1)
    
    # Example 2: Multiple aggregates with expressions
    print("\n=== Example 2: Multiple aggregates with expressions ===")
    example2 = df.group_by(lambda x: x.department).select(
        lambda x: x.department,
        as_column(sum(lambda x: x.salary), "total_salary"),
        as_column(avg(lambda x: x.salary / 12), "avg_monthly_salary"),
        as_column(max(lambda x: x.salary + x.bonus), "max_total_comp")
    )
    
    sql2 = example2.to_sql(dialect="duckdb")
    print(f"SQL: {sql2}")
    
    result2 = conn.execute(sql2).fetchdf()
    print("Result:")
    print(result2)
    
    # Example 3: Having with aggregate expression
    print("\n=== Example 3: Having with aggregate expression ===")
    example3 = df.group_by(lambda x: x.department).having(
        lambda x: sum(lambda x: x.salary) > 100000
    ).select(
        lambda x: x.department,
        as_column(count(lambda x: x.id), "employee_count")
    )
    
    sql3 = example3.to_sql(dialect="duckdb")
    print(f"SQL: {sql3}")
    
    result3 = conn.execute(sql3).fetchdf()
    print("Result:")
    print(result3)
    
    # Example 4: Filter with complex expression
    print("\n=== Example 4: Filter with complex expression ===")
    example4 = df.filter(
        lambda x: (x.salary > 50000) and (x.bonus / x.salary > 0.1)
    )
    
    sql4 = example4.to_sql(dialect="duckdb")
    print(f"SQL: {sql4}")
    
    result4 = conn.execute(sql4).fetchdf()
    print("Result:")
    print(result4)
    
    # Example 5: Scalar function date_diff
    print("\n=== Example 5: Scalar function date_diff ===")
    example5 = df.select(
        lambda x: x.name,
        lambda x: x.department,
        as_column(date_diff(lambda x: x.start_date, lambda x: x.end_date), "days_employed")
    )
    
    sql5 = example5.to_sql(dialect="duckdb")
    print(f"SQL: {sql5}")
    
    result5 = conn.execute(sql5).fetchdf()
    print("Result:")
    print(result5)
    
    # Close the connection
    conn.close()


if __name__ == "__main__":
    main()
