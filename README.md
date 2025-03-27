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
from cloud_dataframe.type_system.column import sum, avg, count

# Create a DataFrame from a table
df = DataFrame.from_table("employees")

# Filter rows
filtered_df = df.filter(lambda x: x.salary > 50000)

# Select specific columns with walrus operator
selected_df = DataFrame.select(
    id := df.id,
    name := df.name,
    salary := df.salary
)

# Group by and aggregate with walrus operator
summary_df = df.group_by_columns("department") \
    .select(
        department := df.department,
        avg_salary := lambda x: avg(x.salary),
        employee_count := lambda x: count(x.id)
    )

# Generate SQL for DuckDB
sql = summary_df.to_sql()
print(sql)
```

### Type-Safe Operations with Dataclasses

```python
from dataclasses import dataclass
from typing import Optional, int, str, float
from cloud_dataframe.type_system.decorators import dataclass_to_schema
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.core.dataframe import DataFrame, Sort

@dataclass_to_schema()
class Employee:
    id: int
    name: str
    department: str
    salary: float
    manager_id: Optional[int] = None

# Create a DataFrame with type information
df = DataFrame.from_table_schema("employees", Employee.__table_schema__)

# Type-safe operations with walrus operator
filtered_df = df.filter(lambda x: x.salary > 50000 and x.department == "Engineering")

# Select with column aliases using walrus operator
result_df = filtered_df.select(
    employee_id := df.id,
    employee_name := df.name,
    dept := df.department,
    annual_salary := lambda x: x.salary * 12
)

# Order by with multiple columns and sort directions
ordered_df = result_df.order_by(lambda x: [
    (x.dept, Sort.ASC),
    (x.annual_salary, Sort.DESC)
])
```

### Join Operations with Lambda Expressions

```python
from cloud_dataframe.core.dataframe import DataFrame

# Create DataFrames for employees and departments
employees = DataFrame.from_table("employees", alias="e")
departments = DataFrame.from_table("departments", alias="d")

# Simple join with single condition
joined_df = employees.join(
    departments,
    lambda e, d: e.department_id == d.id
)

# Join with multiple conditions
complex_join_df = employees.join(
    departments,
    lambda e, d: (e.department_id == d.id) and 
                 (e.salary > 50000) and 
                 (d.location == "New York")
)

# Left join with condition
left_joined_df = employees.left_join(
    departments,
    lambda e, d: e.department_id == d.id
)

# Select columns from joined tables with walrus operator
result_df = joined_df.select(
    employee_id := employees.id,
    employee_name := employees.name,
    department_name := departments.name,
    location := departments.location
)
```

### Conditional Expressions

```python
from cloud_dataframe.core.dataframe import DataFrame

df = DataFrame.from_table("employees")

# Simple if-else with walrus operator
result_df = df.select(
    id := df.id,
    name := df.name,
    bonus_eligible := lambda x: x.salary > 50000
)

# CASE WHEN expression with calculations
result_df = df.select(
    id := df.id,
    name := df.name,
    salary := df.salary,
    bonus := lambda x: x.salary * 1.2 if x.is_manager else x.salary * 1.1 if x.age > 40 else x.salary
)
```

### Window Functions and Advanced Aggregations

```python
from cloud_dataframe.core.dataframe import DataFrame
from cloud_dataframe.type_system.column import sum, avg, row_number, rank, dense_rank

df = DataFrame.from_table("employees")

# Window functions with PARTITION BY and ORDER BY
result_df = df.select(
    id := df.id,
    name := df.name,
    department := df.department,
    salary := df.salary,
    dept_rank := lambda x: rank().over(
        partition_by=[x.department],
        order_by=[(x.salary, "DESC")]
    )
)

# Group by with multiple aggregations
summary_df = df.group_by_columns("department", "location") \
    .select(
        department := df.department,
        location := df.location,
        avg_salary := lambda x: avg(x.salary),
        total_salary := lambda x: sum(x.salary),
        employee_count := lambda x: count(x.id)
    )
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
