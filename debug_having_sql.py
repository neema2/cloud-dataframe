"""
Debug script for testing SQL generation for having clauses with aggregate functions.

This script demonstrates how to use the having method with lambda expressions
that contain aggregate functions and prints the generated SQL.
"""
import pandas as pd
import duckdb
from typing import Optional

from cloud_dataframe.core.dataframe import DataFrame
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.type_system.column import as_column, sum, count

def main():
    """Main function to debug having SQL generation."""
    # Create a schema for the employees table
    schema = TableSchema(
        name="Employee",
        columns={
            "id": int,
            "name": str,
            "department": str,
            "salary": float,
        }
    )
    
    # Create a DataFrame with typed properties
    df = DataFrame.from_table_schema("employees", schema)
    
    # Test having method with lambda expression
    print("=== Testing having method with lambda expression ===")
    try:
        # Create a query with having clause
        query = df.group_by(lambda x: x.department).having(
            lambda x: sum(x.salary) > 100000
        ).select(
            lambda x: x.department,
            as_column(lambda x: count(x.id), "employee_count")
        )
        
        # Generate SQL
        sql = query.to_sql(dialect="duckdb")
        print(f"Generated SQL: {sql}")
        
        # Print the having_condition object
        print(f"\nHaving condition: {query.having_condition}")
        
        # Print the group_by_clauses
        print(f"\nGroup by clauses: {query.group_by_clauses}")
        
        print("\nTest passed!")
    except Exception as e:
        print(f"Error: {e}")
        print("\nTest failed!")

if __name__ == "__main__":
    main()
