import logging
import sys
from cloud_dataframe.core.dataframe import DataFrame
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.type_system.column import ColumnReference
from typing import Optional

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def debug_test_scalar_function_date_diff():
    """Debug the date_diff test to see what SQL is actually being generated."""
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
    
    df = DataFrame.from_table_schema("employees", schema)
    start_date_col = ColumnReference(name="start_date")
    end_date_col = ColumnReference(name="end_date")
    
    test_df = df.select(
        lambda x: x.name,
        lambda x: x.department,
        lambda x: (days_employed := x.date_diff('day', start_date_col, end_date_col))
    )
    
    actual_sql = test_df.to_sql(dialect="duckdb")
    expected_sql = "SELECT x.name, x.department, DATEDIFF('day', CAST(x.start_date_col AS DATE), CAST(x.end_date_col AS DATE)) AS days_employed\nFROM employees x"
    
    print("ACTUAL SQL:")
    print(actual_sql)
    print("\nEXPECTED SQL:")
    print(expected_sql)
    
    if actual_sql.strip() == expected_sql.strip():
        print("\nSQLs MATCH!")
    else:
        print("\nSQLs DO NOT MATCH!")
        print("\nCharacter comparison:")
        for i, (a, e) in enumerate(zip(actual_sql, expected_sql)):
            if a != e:
                print(f"Position {i}: '{a}' != '{e}'")
                start = max(0, i - 10)
                end = min(len(actual_sql), i + 10)
                print(f"Context: '{actual_sql[start:end]}'")
                break

if __name__ == "__main__":
    print("Debugging test_scalar_function_date_diff...")
    debug_test_scalar_function_date_diff()
