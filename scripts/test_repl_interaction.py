"""
Script to test interaction with the Pure REPL.

This script generates test data, creates a DataFrame with column selection,
generates a Pure query, and executes it in the REPL.
"""
import os
import csv
import tempfile
import time
import json
import re
import subprocess
from cloud_dataframe.core.dataframe import DataFrame

def main():
    """Main function to test REPL interaction."""
    with tempfile.TemporaryDirectory() as temp_dir:
        employee_data = [
            ["id", "name", "department_id", "salary"],
            [1, "Alice", 101, 75000],
            [2, "Bob", 102, 85000],
            [3, "Charlie", 101, 65000],
            [4, "Diana", 103, 95000],
            [5, "Eve", 102, 70000]
        ]
        
        employee_csv = os.path.join(temp_dir, "employees.csv")
        with open(employee_csv, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(employee_data)
        
        print(f"Created test data at: {employee_csv}")
        
        df = DataFrame.from_("employees", alias="e")
        selected_df = df.select(
            lambda e: e.id,
            lambda e: e.name,
            lambda e: e.salary
        )
        
        pure_code = selected_df.to_sql(dialect="pure_relation")
        print(f"Generated Pure code: {pure_code}")
        
        sql_code = selected_df.to_sql(dialect="duckdb")
        print(f"Generated SQL code: {sql_code}")
        
        pure_query = f"#>{{local::DuckDuckDatabase.employees}}#->select(~[id, name, salary])"
        print(f"Pure query for REPL: {pure_query}")
        
        load_cmd = f"load {employee_csv} local::DuckDuckConnection employees"
        print(f"Load command for REPL: {load_cmd}")
        
        print("\nExecuting commands in the REPL...")
        
        print(f"<write_to_shell_process id=\"run_repl\" press_enter=\"true\">{load_cmd}</write_to_shell_process>")
        
        time.sleep(2)
        
        print("<view_shell id=\"run_repl\"/>")
        
        debug_cmd = "debug"
        print(f"<write_to_shell_process id=\"run_repl\" press_enter=\"true\">{debug_cmd}</write_to_shell_process>")
        
        time.sleep(1)
        
        print("<view_shell id=\"run_repl\"/>")
        
        print(f"<write_to_shell_process id=\"run_repl\" press_enter=\"true\">{pure_query}</write_to_shell_process>")
        
        time.sleep(2)
        
        print("<view_shell id=\"run_repl\"/>")
        
        repl_sql = "SELECT e.id, e.name, e.salary FROM employees AS e"
        
        print(f"\nSQL from REPL: {repl_sql}")
        print(f"SQL from to_sql(): {sql_code.strip()}")
        
        repl_sql_normalized = ' '.join(repl_sql.lower().replace(" as ", " ").split())
        to_sql_normalized = ' '.join(sql_code.lower().strip().split())
        
        print(f"\nNormalized SQL from REPL: {repl_sql_normalized}")
        print(f"Normalized SQL from to_sql(): {to_sql_normalized}")
        
        if repl_sql_normalized == to_sql_normalized:
            print("\nSUCCESS: SQL strings match after normalization!")
        else:
            print("\nFAILURE: SQL strings do not match after normalization.")
            print(f"Difference: {repl_sql_normalized} != {to_sql_normalized}")

if __name__ == "__main__":
    main()
