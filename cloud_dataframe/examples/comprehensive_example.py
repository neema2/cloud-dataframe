"""
Comprehensive example showing all SQL functions supported by cloud-dataframe.

This example demonstrates all supported aggregate and window functions 
in a single query with the new type-safe column references.
"""
import duckdb
from dataclasses import dataclass
from typing import Optional

from cloud_dataframe.core.dataframe import DataFrame, Sort
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.type_system.column import (
    col, literal,
    # Aggregate functions
    count, sum, avg, min, max,
    # Window functions
    row_number, rank, dense_rank, over, window
)

# Define schemas using TableSchema
employee_schema = TableSchema(
    name="Employee",
    columns={
        "id": int,
        "name": str,
        "department": str,
        "location": str,
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

def create_sample_data():
    """Create sample data in DuckDB for the example."""
    conn = duckdb.connect(":memory:")
    
    # Create employee table
    conn.execute("""
        CREATE TABLE employees (
            id INTEGER,
            name VARCHAR,
            department VARCHAR,
            location VARCHAR,
            salary FLOAT,
            hire_date VARCHAR,
            manager_id INTEGER
        )
    """)
    
    # Insert sample employee data
    conn.execute("""
        INSERT INTO employees VALUES
        (1, 'Alice', 'Engineering', 'New York', 120000, '2020-01-15', NULL),
        (2, 'Bob', 'Engineering', 'San Francisco', 110000, '2020-03-20', 1),
        (3, 'Charlie', 'Engineering', 'New York', 95000, '2021-02-10', 1),
        (4, 'David', 'Sales', 'Chicago', 85000, '2020-05-15', NULL),
        (5, 'Eve', 'Sales', 'Chicago', 90000, '2021-06-01', 4),
        (6, 'Frank', 'Marketing', 'New York', 105000, '2020-01-10', NULL),
        (7, 'Grace', 'Marketing', 'San Francisco', 95000, '2021-04-15', 6),
        (8, 'Heidi', 'HR', 'Chicago', 80000, '2020-08-01', NULL)
    """)
    
    # Create department table
    conn.execute("""
        CREATE TABLE departments (
            id INTEGER,
            name VARCHAR,
            budget FLOAT,
            location VARCHAR
        )
    """)
    
    # Insert sample department data
    conn.execute("""
        INSERT INTO departments VALUES
        (1, 'Engineering', 1000000, 'New York'),
        (2, 'Engineering', 800000, 'San Francisco'),
        (3, 'Sales', 600000, 'Chicago'),
        (4, 'Marketing', 500000, 'New York'),
        (5, 'Marketing', 400000, 'San Francisco'),
        (6, 'HR', 300000, 'Chicago')
    """)
    
    return conn

def comprehensive_query_example():
    """
    Build a comprehensive query that shows all supported SQL functions.
    """
    # Create DataFrames with type-safe schemas
    employees = DataFrame.from_table_schema("employees", employee_schema, alias="e")
    departments = DataFrame.from_table_schema("departments", department_schema, alias="d")
    
    # Create a query that demonstrates window functions
    window_query = employees.select(
        # Basic columns
        lambda x: (id := col("id", "e")),
        lambda x: (name := col("name", "e")),
        lambda x: (department := col("department", "e")),
        lambda x: (location := col("location", "e")),
        lambda x: (salary := col("salary", "e")),
        
        # Window functions with lambda-based partition_by and order_by
        lambda x: (salary_rank_in_dept := window(func=row_number(), partition=x.e.department, order_by=x.e.salary)),
        
        lambda x: (salary_rank_with_ties := window(func=rank(), partition=x.e.department, order_by=x.e.salary)),
        
        lambda x: (dense_salary_rank := window(func=dense_rank(), partition=x.e.department, order_by=x.e.salary))
    ).filter(
        lambda x: x.e.salary > 90000
    ).order_by(
        lambda x: [x.e.department, x.e.salary],
        desc=True
    ).limit(10)
    
    # Generate and print the SQL for window functions
    window_sql = window_query.to_sql(dialect="duckdb")
    print("\n=== WINDOW FUNCTIONS QUERY ===")
    print(window_sql)
    
    # Create a query that demonstrates aggregate functions
    agg_query = employees.group_by(
        lambda x: x.e.department,
        lambda x: x.e.location
    ).select(
        lambda x: x.e.department,
        lambda x: x.e.location,
        lambda x: (employee_count := count(x.e.id)),
        lambda x: (avg_salary := avg(x.e.salary)),
        lambda x: (total_salary := sum(x.e.salary)),
        lambda x: (min_salary := min(x.e.salary)),
        lambda x: (max_salary := max(x.e.salary))
    ).order_by(
        lambda x: x.e.department,
        desc=True
    )
    
    # Generate and print the SQL for aggregate functions
    agg_sql = agg_query.to_sql(dialect="duckdb")
    print("\n=== AGGREGATE FUNCTIONS QUERY ===")
    print(agg_sql)
    
    # Execute the queries and show results
    conn = create_sample_data()
    print("\n=== WINDOW FUNCTIONS QUERY RESULTS ===")
    try:
        window_results = conn.execute(window_sql).fetchall()
        for row in window_results:
            print(row)
    except Exception as e:
        print(f"Error executing window query: {e}")
    
    print("\n=== AGGREGATE FUNCTIONS QUERY RESULTS ===")
    try:
        agg_results = conn.execute(agg_sql).fetchall()
        for row in agg_results:
            print(row)
    except Exception as e:
        print(f"Error executing aggregate query: {e}")
    finally:
        conn.close()
    
    # No additional execution needed as we've already run both queries

def comprehensive_query_with_array_lambdas():
    """
    Build a comprehensive query that shows all supported SQL functions,
    using the new array lambda syntax for multi-column operations.
    """
    # Create DataFrames with type-safe schemas
    employees = DataFrame.from_table_schema("employees", employee_schema, alias="e")
    departments = DataFrame.from_table_schema("departments", department_schema, alias="d")
    
    # Create a query that demonstrates window functions with array lambdas
    window_query = employees.select(
        # Basic columns using array lambda
        lambda x: [col("id", "e"), col("name", "e"), col("department", "e"), 
                  col("location", "e"), col("salary", "e")],
        
        # Window functions with array lambda-based partition_by and order_by
        lambda x: (salary_rank_in_dept := window(func=row_number(), partition=x.e.department, order_by=[(x.e.salary, Sort.DESC)])),
        
        lambda x: (salary_rank_with_ties := window(func=rank(), partition=[x.e.department, x.e.location], order_by=[(x.e.salary, Sort.ASC), (x.e.id, Sort.DESC)])),
        
        lambda x: (dense_salary_rank := window(func=dense_rank(), partition=x.e.department, order_by=[(x.e.salary, Sort.DESC), (x.e.id, Sort.ASC)]))
    ).filter(
        lambda x: x.e.salary > 90000
    ).order_by(
        lambda x: [x.e.department, x.e.salary],
        desc=True
    ).limit(10)
    
    # Generate and print the SQL for window functions with array lambdas
    window_sql = window_query.to_sql(dialect="duckdb")
    print("\n=== WINDOW FUNCTIONS WITH ARRAY LAMBDA SYNTAX ===")
    print(window_sql)
    
    # Create a query that demonstrates aggregate functions with array lambdas
    agg_query = employees.group_by(
        lambda x: [x.e.department, x.e.location]
    ).select(
        lambda x: [x.e.department, x.e.location],
        lambda x: (employee_count := count(x.e.id)),
        lambda x: (avg_salary := avg(x.e.salary)),
        lambda x: (total_salary := sum(x.e.salary)),
        lambda x: (min_salary := min(x.e.salary)),
        lambda x: (max_salary := max(x.e.salary))
    ).order_by(
        lambda x: x.e.department,
        desc=True
    )
    
    # Generate and print the SQL for aggregate functions with array lambdas
    agg_sql = agg_query.to_sql(dialect="duckdb")
    print("\n=== AGGREGATE FUNCTIONS WITH ARRAY LAMBDA SYNTAX ===")
    print(agg_sql)
    
    # Execute the queries and show results
    conn = create_sample_data()
    print("\n=== WINDOW FUNCTIONS WITH ARRAY LAMBDA RESULTS ===")
    try:
        window_results = conn.execute(window_sql).fetchall()
        for row in window_results:
            print(row)
    except Exception as e:
        print(f"Error executing window query: {e}")
    
    print("\n=== AGGREGATE FUNCTIONS WITH ARRAY LAMBDA RESULTS ===")
    try:
        agg_results = conn.execute(agg_sql).fetchall()
        for row in agg_results:
            print(row)
    except Exception as e:
        print(f"Error executing aggregate query: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    comprehensive_query_example()
    comprehensive_query_with_array_lambdas()
