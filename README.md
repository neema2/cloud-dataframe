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
from cloud_dataframe.type_system.column import col, literal, as_column, sum, avg, count

# Create a DataFrame from a table
df = DataFrame.from_table("employees")

# Filter rows
filtered_df = df.filter(lambda x: x.salary > 50000)

# Select specific columns
selected_df = DataFrame.select(
    as_column(col("id"), "id"),
    as_column(col("name"), "name"),
    as_column(col("salary"), "salary")
)

# Group by and aggregate
summary_df = df.group_by_columns("department") \
    .select(
        as_column(col("department"), "department"),
        as_column(avg("salary"), "avg_salary"),
        as_column(count("*"), "employee_count")
    )

# Generate SQL for DuckDB
sql = summary_df.to_sql()
print(sql)
```

### Type-Safe Operations with Dataclasses

```python
from dataclasses import dataclass
from typing import Optional, int, str, float
from cloud_dataframe.type_system.decorators import dataclass_to_schema, col
from cloud_dataframe.type_system.schema import TableSchema

@dataclass_to_schema()
class Employee:
    id: int
    name: str
    department: str
    salary: float
    manager_id: Optional[int] = None

# Create a DataFrame with type information
df = DataFrame.from_table_schema("employees", Employee.__table_schema__)

# Type-safe column references
dept_col = col("department")(Employee)
salary_col = col("salary")(Employee)

# Type-safe operations
filtered_df = df.filter(lambda x: x.salary > 50000 and x.department == "Engineering")
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
