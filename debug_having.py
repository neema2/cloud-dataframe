"""
Debug script for testing the having method with lambda expressions.

This script demonstrates how to use the having method with lambda expressions
that contain aggregate functions.
"""
import pandas as pd
import duckdb
from typing import Optional

from cloud_dataframe.core.dataframe import DataFrame
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.type_system.column import as_column, sum, count

def main():
    """Main function to debug having method."""
    # Create a DuckDB connection
    conn = duckdb.connect(":memory:")
    
    # Create test data
    employees_data = pd.DataFrame({
        "id": [1, 2, 3, 4, 5, 6],
        "name": ["Alice", "Bob", "Charlie", "David", "Eve", "Frank"],
        "department": ["Engineering", "Engineering", "Sales", "Sales", "Marketing", "Marketing"],
        "salary": [80000.0, 90000.0, 70000.0, 75000.0, 65000.0, 60000.0],
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
        
        # Execute the query
        result = conn.execute(sql).fetchdf()
        print("Query result:")
        print(result)
        
        print("\nTest passed!")
    except Exception as e:
        print(f"Error: {e}")
        print("\nTest failed!")
    
    # Close the connection
    conn.close()

if __name__ == "__main__":
    main()
