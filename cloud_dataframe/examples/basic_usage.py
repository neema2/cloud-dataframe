"""
Basic usage examples for the cloud-dataframe DSL.

This module demonstrates how to use the cloud-dataframe DSL for common
dataframe operations and SQL generation.
"""
from dataclasses import dataclass
from typing import Optional

import duckdb

from cloud_dataframe.core.dataframe import DataFrame, BinaryOperation
from cloud_dataframe.type_system.column import (
    col, literal, as_column, count, sum, avg, min, max
)
from cloud_dataframe.type_system.decorators import dataclass_to_schema


# Define a schema using dataclasses
@dataclass_to_schema()
class Employee:
    """Employee schema."""
    id: int
    name: str
    department: str
    salary: float
    manager_id: Optional[int] = None


@dataclass_to_schema()
class Department:
    """Department schema."""
    id: int
    name: str
    location: str


def basic_operations():
    """Demonstrate basic dataframe operations."""
    # Create a DataFrame from a table
    df = DataFrame.from_table("employees")
    
    # Select specific columns
    df_select = df.select(
        as_column(col("id"), "id"),
        as_column(col("name"), "name"),
        as_column(col("salary"), "salary")
    )
    
    # Filter rows
    df_filter = df.filter(lambda x: x.salary > 50000)
    
    # Group by and aggregate
    df_group = df.group_by_columns("department") \
        .select(
            as_column(col("department"), "department"),
            as_column(count("*"), "employee_count"),
            as_column(avg("salary"), "avg_salary")
        )
    
    # Order by
    df_order = df.order_by_columns("salary", desc=True)
    
    # Limit and offset
    df_limit = df.limit(10).offset(5)
    
    # Print the generated SQL
    print("SELECT:")
    print(df_select.to_sql(dialect="duckdb"))
    print("\nFILTER:")
    print(df_filter.to_sql(dialect="duckdb"))
    print("\nGROUP BY:")
    print(df_group.to_sql(dialect="duckdb"))
    print("\nORDER BY:")
    print(df_order.to_sql(dialect="duckdb"))
    print("\nLIMIT/OFFSET:")
    print(df_limit.to_sql(dialect="duckdb"))


def join_operations():
    """Demonstrate join operations."""
    # Create DataFrames from tables
    employees = DataFrame.from_table("employees", alias="e")
    departments = DataFrame.from_table("departments", alias="d")
    
    # Inner join
    inner_join = employees.join(
        departments,
        BinaryOperation(
            left=col("department_id", "e"),
            operator="=",
            right=col("id", "d")
        )
    )
    
    # Left join
    left_join = employees.left_join(
        departments,
        BinaryOperation(
            left=col("department_id", "e"),
            operator="=",
            right=col("id", "d")
        )
    )
    
    # Print the generated SQL
    print("INNER JOIN:")
    print(inner_join.to_sql(dialect="duckdb"))
    print("\nLEFT JOIN:")
    print(left_join.to_sql(dialect="duckdb"))


def type_safe_operations():
    """Demonstrate type-safe operations."""
    # Create a schema manually since the decorator might not have applied yet
    from cloud_dataframe.type_system.schema import TableSchema
    
    schema = TableSchema(name="Employee", columns={
        "id": int,
        "name": str,
        "department": str,
        "salary": float,
        "manager_id": Optional[int]
    })
    
    # Create a DataFrame with the schema
    df = DataFrame.from_table_schema("employees", schema)
    
    # Type-safe column references
    dept_col = col("department")
    salary_col = col("salary")
    
    # Type-safe operations
    df_filter = df.filter(lambda x: x.salary > 50000 and x.department == "Engineering")
    
    # Print the generated SQL
    print("TYPE-SAFE FILTER:")
    print(df_filter.to_sql(dialect="duckdb"))


def execute_query():
    """Execute a query using DuckDB."""
    # Create a connection to DuckDB
    conn = duckdb.connect(":memory:")
    
    # Create sample data
    conn.execute("""
        CREATE TABLE employees (
            id INTEGER,
            name VARCHAR,
            department VARCHAR,
            salary DOUBLE,
            manager_id INTEGER
        )
    """)
    
    conn.execute("""
        INSERT INTO employees VALUES
        (1, 'Alice', 'Engineering', 80000, NULL),
        (2, 'Bob', 'Engineering', 70000, 1),
        (3, 'Charlie', 'Sales', 60000, NULL),
        (4, 'Dave', 'Sales', 50000, 3),
        (5, 'Eve', 'Marketing', 65000, NULL)
    """)
    
    # Create a DataFrame
    df = DataFrame.from_table("employees") \
        .group_by_columns("department") \
        .select(
            as_column(col("department"), "department"),
            as_column(count("*"), "employee_count"),
            as_column(avg("salary"), "avg_salary")
        )
    
    # Generate SQL
    sql = df.to_sql(dialect="duckdb")
    print("SQL:")
    print(sql)
    
    # Execute the query
    result = conn.execute(sql).fetchall()
    print("\nRESULT:")
    for row in result:
        print(row)
    
    # Close the connection
    conn.close()


if __name__ == "__main__":
    print("=== Basic Operations ===")
    basic_operations()
    
    print("\n=== Join Operations ===")
    join_operations()
    
    print("\n=== Type-Safe Operations ===")
    type_safe_operations()
    
    print("\n=== Execute Query ===")
    execute_query()
