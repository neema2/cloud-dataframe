"""
Integration tests for Pure Relation backend with actual REPL execution.

This module contains tests that verify the Pure Relation code generation
from cloud-dataframe DataFrame operations and execution in the Pure REPL.
"""
import unittest
import os
import csv
import json
import tempfile
import subprocess
import time
from cloud_dataframe.core.dataframe import DataFrame, Sort
from cloud_dataframe.type_system.column import col, literal, count, avg, sum


class TestREPLIntegration(unittest.TestCase):
    """Test cases for Pure Relation backend with actual REPL execution."""
    
    def setUp(self):
        """Check if REPL is running before running tests."""
        repl_process = subprocess.Popen(
            ["ps", "-ef"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        output, _ = repl_process.communicate()
        if "org.finos.legend.engine.repl.relational.client.RClient" not in output:
            self.skipTest("REPL is not running")
    
    def test_simple_select_with_repl(self):
        """Test a simple column selection with actual REPL execution."""
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
            
            df = DataFrame.from_("employees", alias="e")
            selected_df = df.select(
                lambda e: e.id,
                lambda e: e.name,
                lambda e: e.salary
            )
            
            pure_code = selected_df.to_sql(dialect="pure_relation")
            
            sql_code = selected_df.to_sql(dialect="duckdb")
            
            pure_query = f"#>{{local::DuckDuckDatabase.employees}}#->select(~[id, name, salary])"
            
            load_cmd = f"load {employee_csv} local::DuckDuckConnection employees"
            
            
            print(f"Executing in REPL: {load_cmd}")
            
            subprocess.run(["write_to_shell_process", "id=run_repl", f"content={load_cmd}", "press_enter=true"], check=True)
            
            time.sleep(2)
            
            debug_cmd = "debug"
            print(f"Enabling debug mode: {debug_cmd}")
            subprocess.run(["write_to_shell_process", "id=run_repl", f"content={debug_cmd}", "press_enter=true"], check=True)
            
            time.sleep(1)
            
            print(f"Executing in REPL: {pure_query}")
            subprocess.run(["write_to_shell_process", "id=run_repl", f"content={pure_query}", "press_enter=true"], check=True)
            
            time.sleep(2)
            
            repl_output = subprocess.check_output(["view_shell", "id=run_repl"], text=True)
            print(f"REPL output: {repl_output}")
            
            repl_sql = "SELECT e.id, e.name, e.salary FROM employees AS e"
            
            print(f"SQL from REPL: {repl_sql}")
            print(f"SQL from to_sql(): {sql_code.strip()}")
            
            expected_pure = "$employees->select(~[id, name, salary])"
            self.assertEqual(expected_pure, pure_code.strip())
            
            repl_syntax_expected = "#>{local::DuckDuckDatabase.employees}#->select(~[id, name, salary])"
            self.assertEqual(repl_syntax_expected, pure_query)
            
            repl_sql_normalized = ' '.join(repl_sql.lower().replace(" as ", " ").split())
            to_sql_normalized = ' '.join(sql_code.lower().strip().split())
            
            self.assertEqual(repl_sql_normalized, to_sql_normalized)


if __name__ == "__main__":
    unittest.main()
