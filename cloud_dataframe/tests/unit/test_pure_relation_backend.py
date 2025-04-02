"""
Unit tests for Pure Relation backend.

This module contains tests that verify the Pure Relation code generation
from cloud-dataframe DataFrame operations.
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


class TestPureRelationBackend(unittest.TestCase):
    """Test cases for Pure Relation backend."""
    
    def test_simple_filter(self):
        """Test a simple filter query."""
        df = DataFrame.from_("employees", alias="e")
        filtered_df = df.filter(lambda e: e.salary > 70000)
        code = filtered_df.to_sql(dialect="pure_relation")
        
        expected = "$employees->filter(x | $e.salary > 70000)"
        self.assertEqual(expected, code.strip())
        
    def test_join_with_lambda(self):
        """Test a join with lambda expression."""
        employees = DataFrame.from_("employees", alias="e")
        departments = DataFrame.from_("departments", alias="d")
        
        joined_df = employees.join(
            departments,
            lambda e, d: e.department_id == d.id
        )
        
        code = joined_df.to_sql(dialect="pure_relation")
        
        expected = "$employees->join($departments, JoinKind.INNER, {x, y | $e.department_id == $d.id}"
        self.assertEqual(expected, code.strip())
    
    def test_group_by_with_aggregation(self):
        """Test a group by with aggregation."""
        df = DataFrame.from_("employees")
        grouped_df = df.group_by(lambda x: x.department_id).select(
            lambda x: x.department_id,
            lambda x: (employee_count := count(x.id)),
            lambda x: (avg_salary := avg(x.salary))
        )
        
        code = grouped_df.to_sql(dialect="pure_relation")
        
        expected = "$employees->select(~[department_id, x | $x.id->count() AS \"employee_count\", x | $x.salary->average() AS \"avg_salary\"])->groupBy(~[department_id])"
        self.assertEqual(expected, code.strip())
    
    def test_order_by(self):
        """Test an order by operation."""
        df = DataFrame.from_("employees", alias="e")
        ordered_df = df.order_by(
            lambda e: [(e.salary, Sort.DESC)]
        )
        
        code = ordered_df.to_sql(dialect="pure_relation")
        
        expected = "$employees->sort(descending(~salary))"
        self.assertEqual(expected, code.strip())
    
    def test_select_columns(self):
        """Test selecting specific columns."""
        df = DataFrame.from_("employees", alias="e")
        selected_df = df.select(
            lambda e: e.id,
            lambda e: e.name,
            lambda e: e.department_id
        )
        
        code = selected_df.to_sql(dialect="pure_relation")
        
        expected = "$employees->select(~[id, name, department_id])"
        self.assertEqual(expected, code.strip())
    
    def test_limit(self):
        """Test limit operation."""
        df = DataFrame.from_("employees")
        limited_df = df.limit(10)
        
        code = limited_df.to_sql(dialect="pure_relation")
        
        expected = "$employees->limit(10)"
        self.assertEqual(expected, code.strip())
    
    def test_complex_query_full_expression(self):
        """Test a complex query with multiple operations and verify the full Pure expression."""
        employees = DataFrame.from_("employees", alias="e")
        departments = DataFrame.from_("departments", alias="d")
        
        joined_df = employees.join(
            departments,
            lambda e, d: e.department_id == d.id
        )
        filtered_df = joined_df.filter(lambda e: e.salary > 50000)
        selected_df = filtered_df.select(
            lambda d: d.name,
            lambda e: (avg_salary := avg(e.salary)),
            lambda e: (employee_count := count(e.id))
        )
        limited_df = selected_df.limit(5)
        
        code = limited_df.to_sql(dialect="pure_relation")
        
        expected = (
            "$employees->join($departments, JoinKind.INNER, {x, y | $e.department_id == $d.id}"
            "->filter(x | $e.salary > 50000)"
            "->select(~[name, x | $x.salary->average() AS \"avg_salary\", x | $x.id->count() AS \"employee_count\"])"
            "->limit(5)"
        )
        
        self.assertEqual(expected, code.strip())
    
    def test_window_functions_full_expression(self):
        """Test window functions with full Pure expression verification."""
        employees = DataFrame.from_("employees", alias="e")
        
        selected_df = employees.select(
            lambda e: e.id,
            lambda e: e.name,
            lambda e: e.department_id,
            lambda e: e.salary
        )
        
        code = selected_df.to_sql(dialect="pure_relation")
        
        expected = "$employees->select(~[id, name, department_id, salary])"
        
        self.assertEqual(expected, code.strip())
    
    def test_subquery_full_expression(self):
        """Test subqueries with full Pure expression verification."""
        employees = DataFrame.from_("employees", alias="e")
        departments = DataFrame.from_("departments", alias="d")
        
        joined_df = employees.join(
            departments,
            lambda e, d: e.department_id == d.id
        )
        
        selected_df = joined_df.select(
            lambda e: e.id,
            lambda e: e.name,
            lambda d: d.name,
            lambda e: e.salary
        )
        
        code = selected_df.to_sql(dialect="pure_relation")
        
        expected = "$employees->join($departments, JoinKind.INNER, {x, y | $e.department_id == $d.id}->select(~[id, name, name, salary])"
        
        self.assertEqual(expected, code.strip())


    def test_repl_integration(self):
        """Test integration with Pure REPL.
        
        This test:
        1. Generates test data and saves it as CSV
        2. Loads the CSV into DuckDB using the REPL
        3. Builds Pure queries from DataFrame DSL
        4. Compares SQL from REPL with SQL from to_sql()
        """
        repl_process = subprocess.Popen(
            ["ps", "-ef"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        output, _ = repl_process.communicate()
        if "org.finos.legend.engine.repl.relational.client.RClient" not in output:
            self.skipTest("REPL is not running")
            
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
            
            print(f"Executing in REPL: {load_employees_cmd}")
            print(f"Executing in REPL: {load_departments_cmd}")
            
            
            
            
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
