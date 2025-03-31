# Cloud DataFrame

A Python DSL for type-safe dataframe operations that generates SQL for database execution.

## Overview

Cloud DataFrame is a Python library that provides a type-safe domain-specific language (DSL) for modeling SQL operations. It allows you to build SQL queries using a fluent interface with type checking, and then execute those queries against different database backends.

The initial version supports DuckDB as the execution backend, with plans to add support for other databases in the future. This project is inspired by the neema2/legend-dataframe project but implemented in Python with a focus on type safety and extensibility.

## Features

- Type-safe dataframe operations using Python's type hints
- Fluent interface for building SQL queries
- Support for common SQL operations:
  - SELECT, FROM, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT, OFFSET
  - Joins (INNER, LEFT, RIGHT, FULL, CROSS)
  - Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
  - Window functions (ROW_NUMBER, RANK, DENSE_RANK)
  - Common Table Expressions (CTEs)
- SQL generation for DuckDB execution
- Extensible architecture for adding support for other database backends

## Installation

```bash
pip install cloud-dataframe
```

## Usage

### Basic Operations

```python
from cloud_dataframe.core.dataframe import DataFrame
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.type_system.column import sum, avg, count

# Create a schema for the employees table
schema = TableSchema(
    name="Employee",
    columns={
        "id": int,
        "name": str,
        "department": str,
        "location": str,
        "salary": float,
        "is_manager": bool
    }
)

# Create a DataFrame with typed properties
df = DataFrame.from_table_schema("employees", schema)

# Filter rows
filtered_df = df.filter(lambda x: x.salary > 50000)

# Select specific columns with lambda expressions
selected_df = df.select(
    lambda x: [
        x.id,
        x.name,
        (annual_salary := x.salary * 12)
    ]
)

# Generate SQL for DuckDB
sql = selected_df.to_sql(dialect="duckdb")
print(sql)
```

### Sorting with Structured Lambda Syntax

```python
from cloud_dataframe.core.dataframe import DataFrame, Sort
from cloud_dataframe.type_system.schema import TableSchema

# Create a schema for the employees table
schema = TableSchema(
    name="Employee",
    columns={
        "id": int,
        "name": str,
        "department": str,
        "location": str,
        "salary": float
    }
)

# Create a DataFrame with typed properties
df = DataFrame.from_table_schema("employees", schema)

# Single expression format
ordered_df = df.order_by(lambda x: x.salary)

# Single tuple format with Sort enum
ordered_df = df.order_by(lambda x: (x.salary, Sort.DESC))

# Array format with mix of expressions and tuples
ordered_df = df.order_by(lambda x: [
    x.department,  # Department ascending (default)
    (x.salary, Sort.DESC)  # Salary descending
])

# Array format with all tuples
ordered_df = df.order_by(lambda x: [
    (x.department, Sort.DESC),
    (x.location, Sort.ASC),
    (x.salary, Sort.DESC)
])
```

### Join Operations with Lambda Expressions

```python
from cloud_dataframe.core.dataframe import DataFrame
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.type_system.column import count, sum, avg

# Create schemas for employees and departments
employee_schema = TableSchema(
    name="Employee",
    columns={
        "id": int,
        "name": str,
        "department_id": int,
        "salary": float
    }
)

department_schema = TableSchema(
    name="Department",
    columns={
        "id": int,
        "name": str,
        "location": str,
        "budget": float
    }
)

# Create DataFrames with typed properties
employees_df = DataFrame.from_table_schema("employees", employee_schema, alias="e")
departments_df = DataFrame.from_table_schema("departments", department_schema, alias="d")

# Inner join with lambda expression
joined_df = employees_df.join(
    departments_df,
    lambda e, d: e.department_id == d.id
).select(
    lambda e, d: [
        (employee_id := e.id),
        (employee_name := e.name),
        (department_name := d.name),
        (department_location := d.location),
        (employee_salary := e.salary)
    ]
)

# Left join with lambda expression
left_joined_df = employees_df.left_join(
    departments_df,
    lambda e, d: e.department_id == d.id
).select(
    lambda e, d: [
        e.id,
        e.name,
        (department_name := d.name),
        d.location,
        e.salary
    ]
)

# Join with aggregation
aggregated_df = employees_df.join(
    departments_df,
    lambda e, d: e.department_id == d.id
).group_by(
    lambda d: d.name
).select(
    lambda d, e: [
        (department_name := d.name),
        (employee_count := count(e.id)),
        (total_salary := sum(e.salary)),
        (avg_salary := avg(e.salary))
    ]
).order_by(
    lambda d: d.name
)
```

### Aggregate Functions with Lambda Expressions

```python
from cloud_dataframe.core.dataframe import DataFrame
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.type_system.column import sum, avg, count, min, max

# Create a schema for the employees table
schema = TableSchema(
    name="Employee",
    columns={
        "id": int,
        "name": str,
        "department": str,
        "salary": float,
        "bonus": float,
        "tax_rate": float
    }
)

# Create a DataFrame with typed properties
df = DataFrame.from_table_schema("employees", schema)

# Simple aggregate functions
summary_df = df.select(
    lambda x: [
        (total_salary := sum(x.salary)),
        (avg_salary := avg(x.salary)),
        (employee_count := count(x.id))
    ]
)

# Complex aggregate functions with expressions
complex_df = df.select(
    lambda x: [
        (total_compensation := sum(x.salary + x.bonus)),
        (avg_net_salary := avg(x.salary * (1 - x.tax_rate)))
    ]
)

# Group by with aggregate functions
grouped_df = df.group_by(lambda x: x.department).select(
    lambda x: [
        x.department,
        (total_salary := sum(x.salary)),
        (avg_salary := avg(x.salary)),
        (employee_count := count(x.id))
    ]
)
```

### Window Functions and Advanced Aggregations

```python
from cloud_dataframe.core.dataframe import DataFrame
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.type_system.column import (
    sum, avg, row_number, rank, dense_rank,
    row, range, unbounded, window
)

# Create a schema for the sales table
schema = TableSchema(
    name="Sales",
    columns={
        "product_id": int,
        "date": str,
        "region": str,
        "sales": int
    }
)

# Create a DataFrame with typed properties
df = DataFrame.from_table_schema("sales", schema, alias="x")

# Running total with unbounded preceding frame
running_total_df = df.select(
    lambda x: [
        x.product_id,
        x.date,
        x.sales,
        (running_total := window(
            func=sum(x.sales), 
            partition=x.product_id, 
            order_by=x.date, 
            frame=row(unbounded(), 0)
        ))
    ]
).order_by(
    lambda x: [x.product_id, x.date]
)

# Moving average with preceding and following rows
moving_avg_df = df.select(
    lambda x: [
        x.product_id,
        x.date,
        x.sales,
        (moving_avg := window(
            func=avg(x.sales), 
            partition=x.product_id, 
            order_by=x.date, 
            frame=row(1, 1)
        ))
    ]
).order_by(
    lambda x: [x.product_id, x.date]
)

# Complex expression in window function
complex_window_df = df.select(
    lambda x: [
        x.product_id,
        x.region,
        x.sales,
        (adjusted_total := window(
            func=sum(x.sales + 10), 
            partition=x.region, 
            frame=range(unbounded(), 0)
        ))
    ]
).order_by(
    lambda x: [x.region, x.product_id]
)
```

### Conditional Expressions and Nested Functions

```python
from cloud_dataframe.core.dataframe import DataFrame
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.type_system.column import date_diff

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
        "start_date": str,
        "end_date": str
    }
)

# Create a DataFrame with typed properties
df = DataFrame.from_table_schema("employees", schema)

# Conditional expressions
result_df = df.select(
    lambda x: [
        x.id,
        x.name,
        x.salary,
        (bonus := x.salary * 0.2 if x.is_manager else x.salary * 0.1)
    ]
)

# Nested functions with binary operations
nested_df = df.group_by(lambda x: x.department).select(
    lambda x: [
        x.department,
        (total_compensation := sum(x.salary + x.bonus)),
        (avg_monthly_salary := avg(x.salary / 12)),
        (max_total_comp := max(x.salary + x.bonus))
    ]
)

# Using scalar functions
date_df = df.select(
    lambda x: [
        x.name,
        x.department,
        (days_employed := date_diff(x.start_date, x.end_date))
    ]
)

# Having clause with aggregate expression
filtered_df = df.group_by(lambda x: x.department).having(
    lambda x: sum(x.salary) > 100000
).select(
    lambda x: [
        x.department,
        (employee_count := count())
    ]
)
```

### Type-Safe Operations with Dataclasses

```python
from dataclasses import dataclass
from typing import Optional
from cloud_dataframe.core.dataframe import DataFrame, Sort
from cloud_dataframe.type_system.decorators import dataclass_to_schema
from cloud_dataframe.type_system.schema import TableSchema

# Define a dataclass for employees
@dataclass_to_schema()
class Employee:
    id: int
    name: str
    department: str
    salary: float
    location: str
    is_manager: bool
    manager_id: Optional[int] = None

# Create a DataFrame with type information from the dataclass
df = DataFrame.from_table_schema("employees", Employee.__table_schema__)

# Type-safe operations with lambda expressions
filtered_df = df.filter(
    lambda x: x.salary > 50000 and x.department == "Engineering"
)

# Select with column aliases
result_df = filtered_df.select(
    lambda x: [
        (employee_id := x.id),
        (employee_name := x.name),
        (dept := x.department),
        (annual_salary := x.salary * 12)
    ]
)

# Order by with multiple columns and sort directions
ordered_df = result_df.order_by(lambda x: [
    (x.dept, Sort.ASC),
    (x.annual_salary, Sort.DESC)
])
```

### Extending with New Database Backends

To add support for a new database backend, you need to:

1. Create a new module in the `cloud_dataframe.backends` package
2. Implement a SQL generator function for the new backend
3. Register the SQL generator with the backend registry

```python
from cloud_dataframe.backends import register_sql_generator
from cloud_dataframe.core.dataframe import DataFrame

def generate_postgres_sql(df: DataFrame) -> str:
    # Implement PostgreSQL-specific SQL generation
    pass

# Register the SQL generator
register_sql_generator("postgres", generate_postgres_sql)

# Use the new backend
sql = df.to_sql(dialect="postgres")
```

## Architecture

The Cloud DataFrame library is designed with extensibility in mind, following a modular architecture:

1. **Core DataFrame API**: Provides the fluent interface for building SQL queries
2. **Type System**: Ensures type safety through Python's type hints and dataclass integration
3. **Backend Registry**: Allows for easy registration of new database backends
4. **SQL Generators**: Database-specific modules for generating SQL

This architecture makes it easy to add support for new database backends without modifying the core API.

## License

Apache License 2.0
