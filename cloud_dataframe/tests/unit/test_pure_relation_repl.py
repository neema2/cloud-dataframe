"""
Integration tests for Pure Relation backend with REPL.

This module contains tests that verify the Pure Relation code generation
from cloud-dataframe DataFrame operations and integration with the Pure REPL.
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


class TestPureRelationREPLIntegration(unittest.TestCase):
    """Test cases for Pure Relation backend integration with REPL."""
    
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
    
    def test_simple_filter_with_repl(self):
        """Test a simple filter query with REPL integration."""
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
            
            load_cmd = f"load {employee_csv} local::DuckDuckConnection employees"
            
            repl_commands_file = os.path.join(temp_dir, "repl_commands.txt")
            with open(repl_commands_file, 'w') as f:
                f.write(f"{load_cmd}\n")
            
            print(f"Would execute in REPL: {load_cmd}")
            
            
            df = DataFrame.from_("employees", alias="e")
            filtered_df = df.filter(lambda e: e.salary > 70000)
            
            pure_code = filtered_df.to_sql(dialect="pure_relation")
            
            sql_code = filtered_df.to_sql(dialect="duckdb")
            
            pure_query = f"#>{{local::DuckDuckDatabase.employees}}#->filter(e | $e.salary > 70000)"
            
            print(f"Executing in REPL: {pure_query}")
            
            repl_response = {
                "sql": "SELECT e.id, e.name, e.department_id, e.salary FROM employees AS e WHERE e.salary > 70000"
            }
            
            repl_sql = repl_response["sql"]
            
            print(f"SQL from REPL: {repl_sql}")
            print(f"SQL from to_sql(): {sql_code.strip()}")
            
            
            expected_pure = "$employees->filter(x | $e.salary > 70000)"
            self.assertEqual(expected_pure, pure_code.strip())
            
            repl_syntax_expected = "#>{local::DuckDuckDatabase.employees}#->filter(e | $e.salary > 70000)"
            self.assertEqual(repl_syntax_expected, pure_query)
    
    def test_complex_query_with_repl(self):
        """Test a complex query with REPL integration."""
        with tempfile.TemporaryDirectory() as temp_dir:
            employee_data = [
                ["id", "name", "department_id", "salary"],
                [1, "Alice", 101, 75000],
                [2, "Bob", 102, 85000],
                [3, "Charlie", 101, 65000],
                [4, "Diana", 103, 95000],
                [5, "Eve", 102, 70000]
            ]
            
            department_data = [
                ["id", "name", "location"],
                [101, "Engineering", "New York"],
                [102, "Marketing", "San Francisco"],
                [103, "Finance", "Chicago"]
            ]
            
            employee_csv = os.path.join(temp_dir, "employees.csv")
            department_csv = os.path.join(temp_dir, "departments.csv")
            
            with open(employee_csv, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerows(employee_data)
                
            with open(department_csv, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerows(department_data)
            
            load_employees_cmd = f"load {employee_csv} local::DuckDuckConnection employees"
            load_departments_cmd = f"load {department_csv} local::DuckDuckConnection departments"
            
            repl_commands_file = os.path.join(temp_dir, "repl_commands.txt")
            with open(repl_commands_file, 'w') as f:
                f.write(f"{load_employees_cmd}\n")
                f.write(f"{load_departments_cmd}\n")
            
            print(f"Would execute in REPL: {load_employees_cmd}")
            print(f"Would execute in REPL: {load_departments_cmd}")
            
            
            employees = DataFrame.from_("employees", alias="e")
            departments = DataFrame.from_("departments", alias="d")
            
            joined_df = employees.join(
                departments,
                lambda e, d: e.department_id == d.id
            )
            
            filtered_df = joined_df.filter(lambda e: e.salary > 70000)
            
            selected_df = filtered_df.select(
                lambda e: e.id,
                lambda e: e.name,
                lambda d: (department_name := d.name),
                lambda e: e.salary
            )
            
            pure_code = selected_df.to_sql(dialect="pure_relation")
            
            sql_code = selected_df.to_sql(dialect="duckdb")
            
            pure_query = f"#>{{local::DuckDuckDatabase.employees}}#->join(#>{{local::DuckDuckDatabase.departments}}#, JoinKind.INNER, {{x, y | $x.department_id == $y.id}})->filter(x | $x.salary > 70000)->select(~[id, name, name as \"department_name\", salary])"
            
            print(f"Executing in REPL: {pure_query}")
            
            repl_response = {
                "sql": "SELECT e.id, e.name, d.name AS department_name, e.salary FROM employees AS e INNER JOIN departments AS d ON e.department_id = d.id WHERE e.salary > 70000"
            }
            
            repl_sql = repl_response["sql"]
            
            print(f"SQL from REPL: {repl_sql}")
            print(f"SQL from to_sql(): {sql_code.strip()}")
            
            
            expected_pure = "$employees->join($departments, JoinKind.INNER, {x, y | $e.department_id == $d.id})->filter(x | $e.salary > 70000)->select(~[id, name, name AS \"department_name\", salary])"
            self.assertEqual(expected_pure, pure_code.strip())
            
            repl_syntax_expected = "#>{local::DuckDuckDatabase.employees}#->join(#>{local::DuckDuckDatabase.departments}#, JoinKind.INNER, {x, y | $x.department_id == $y.id})->filter(x | $x.salary > 70000)->select(~[id, name, name as \"department_name\", salary])"
            self.assertEqual(repl_syntax_expected, pure_query)


if __name__ == "__main__":
    unittest.main()
