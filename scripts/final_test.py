"""
Final test script to verify SQL generation with all clauses.
"""
from cloud_dataframe.core.dataframe import DataFrame, BinaryOperation
from cloud_dataframe.type_system.column import col, literal, count, avg

def main():
    """Run a complete test of SQL generation."""
    # Create a base DataFrame with a source table
    df = DataFrame.from_("employees", alias="x")
    
    # Apply filter
    filtered_df = df.filter(
        lambda x: x.salary > 50000
    )
    
    # Apply group by
    grouped_df = filtered_df.group_by("department")
    
    # Apply select
    result_df = grouped_df.select(
        lambda x: x.department,
        as_column(count(lambda x: x.id), "employee_count"),
        as_column(avg(lambda x: x.salary), "avg_salary")
    )
    
    # Generate SQL for DuckDB
    sql = result_df.to_sql(dialect="duckdb")
    print("Generated SQL for DuckDB:")
    print(sql)

if __name__ == "__main__":
    main()
