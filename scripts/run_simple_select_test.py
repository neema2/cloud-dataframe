"""
Script to run a simple select test and show all outputs.

This script:
1. Creates test data
2. Builds a DataFrame with column selection
3. Generates SQL and Pure code
4. Shows the Pure query for REPL
5. Shows the expected REPL SQL without modification
"""
import os
import csv
import tempfile
import sys
sys.path.append('/home/ubuntu/repos/cloud-dataframe')
from cloud_dataframe.core.dataframe import DataFrame

def main():
    """Run the simple select test and show all outputs."""
    with tempfile.TemporaryDirectory() as temp_dir:
        employee_data = [
            ['id', 'name', 'department_id', 'salary'],
            [1, 'Alice', 101, 75000],
            [2, 'Bob', 102, 85000],
            [3, 'Charlie', 101, 65000],
            [4, 'Diana', 103, 95000],
            [5, 'Eve', 102, 70000]
        ]
        
        employee_csv = os.path.join(temp_dir, 'employees.csv')
        with open(employee_csv, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(employee_data)
        
        print(f'Created test data at: {employee_csv}')
        
        df = DataFrame.from_('employees', alias='e')
        selected_df = df.select(
            lambda e: e.id,
            lambda e: e.name,
            lambda e: e.salary
        )
        
        sql_code = selected_df.to_sql(dialect='duckdb')
        pure_code = selected_df.to_sql(dialect='pure_relation')
        
        pure_query = '#>{local::DuckDuckDatabase.employees}#->select(~[id, name, salary])'
        
        print('\n=== DataFrame Generated SQL ===')
        print(sql_code.strip())
        
        print('\n=== DataFrame Generated Pure ===')
        print(pure_code.strip())
        
        print('\n=== Pure Code for REPL ===')
        print(pure_query)
        
        print('\n=== Expected REPL SQL (without modification) ===')
        print('SELECT e.id, e.name, e.salary FROM employees AS e')

if __name__ == '__main__':
    main()
