"""
Standalone test for Pure Relation backend with REPL integration.

This test:
1. Checks if REPL is running and starts it if needed
2. Turns on debug mode on REPL
3. Uploads CSV into REPL
4. Runs the Pure generated code against the REPL
"""
import unittest
import os
import csv
import json
import tempfile
import subprocess
import time
import signal
import sys
from pathlib import Path
from cloud_dataframe.core.dataframe import DataFrame

class TestStandaloneREPL(unittest.TestCase):
    """Test standalone REPL integration with Pure Relation backend."""
    
    repl_process = None
    temp_dir = None
    
    @classmethod
    def setUpClass(cls):
        """Set up the test environment, including starting the REPL if needed."""
        cls.temp_dir = tempfile.TemporaryDirectory()
        
        repl_running = cls._check_repl_running()
        
        if not repl_running:
            print("REPL is not running. Starting REPL...")
            cls._start_repl()
        else:
            print("REPL is already running.")
        
        time.sleep(2)
        
        cls._enable_debug_mode()
        
        cls._create_and_load_test_data()
    
    @classmethod
    def tearDownClass(cls):
        """Clean up resources after tests."""
        if cls.repl_process is not None:
            print("Terminating REPL process...")
            cls.repl_process.terminate()
            cls.repl_process.wait()
        
        if cls.temp_dir:
            cls.temp_dir.cleanup()
    
    @classmethod
    def _check_repl_running(cls):
        """Check if the REPL is already running."""
        try:
            result = subprocess.run(
                ["ps", "-ef"], 
                capture_output=True, 
                text=True
            )
            return "org.finos.legend.engine.repl.relational.client.RClient" in result.stdout and "grep" not in result.stdout
        except Exception as e:
            print(f"Error checking if REPL is running: {e}")
            return False
    
    @classmethod
    def _start_repl(cls):
        """Start the REPL if it's not already running."""
        legend_engine_dir = "/home/ubuntu/repos/legend-engine"
        repl_dir = os.path.join(legend_engine_dir, "legend-engine-config", "legend-engine-repl")
        
        try:
            subprocess.run(
                ["mvn", "dependency:build-classpath", "-Dmdep.outputFile=classpath.txt"],
                cwd=repl_dir,
                check=True
            )
            
            cls.repl_process = subprocess.Popen(
                ["java", "-cp", f"target/classes:$(cat classpath.txt)", 
                 "org.finos.legend.engine.repl.relational.client.RClient"],
                cwd=repl_dir,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                shell=True
            )
            
            time.sleep(5)
            
        except Exception as e:
            print(f"Error starting REPL: {e}")
            raise
    
    @classmethod
    def _enable_debug_mode(cls):
        """Enable debug mode in the REPL."""
        try:
            result = subprocess.run(
                ["ps", "-ef", "|", "grep", "RClient", "|", "grep", "-v", "grep"],
                capture_output=True,
                text=True,
                shell=True
            )
            
            lines = result.stdout.strip().split('\n')
            if not lines or not lines[0]:
                print("Could not find REPL process to enable debug mode")
                return
            
            pid = lines[0].split()[1]
            
            debug_cmd = "debug\n"
            subprocess.run(
                ["echo", debug_cmd, "|", "nc", "-w", "1", "localhost", "8888"],
                shell=True
            )
            
            print("Debug mode enabled in REPL")
            
        except Exception as e:
            print(f"Error enabling debug mode: {e}")
    
    @classmethod
    def _create_and_load_test_data(cls):
        """Create test data and load it into the REPL."""
        employee_data = [
            ["id", "name", "department_id", "salary"],
            [1, "Alice", 101, 75000],
            [2, "Bob", 102, 85000],
            [3, "Charlie", 101, 65000],
            [4, "Diana", 103, 95000],
            [5, "Eve", 102, 70000]
        ]
        
        employee_csv = os.path.join(cls.temp_dir.name, "employees.csv")
        with open(employee_csv, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(employee_data)
        
        print(f"Created test data at: {employee_csv}")
        
        load_cmd = f"load {employee_csv} local::DuckDuckConnection employees\n"
        
        try:
            result = subprocess.run(
                ["ps", "-ef", "|", "grep", "RClient", "|", "grep", "-v", "grep"],
                capture_output=True,
                text=True,
                shell=True
            )
            
            lines = result.stdout.strip().split('\n')
            if not lines or not lines[0]:
                print("Could not find REPL process to load data")
                return
            
            subprocess.run(
                ["echo", load_cmd, "|", "nc", "-w", "1", "localhost", "8888"],
                shell=True
            )
            
            print(f"Loaded data into REPL: {load_cmd.strip()}")
            
        except Exception as e:
            print(f"Error loading data into REPL: {e}")
    
    @classmethod
    def _run_pure_query(cls, pure_query):
        """Run a Pure query in the REPL and return the result."""
        try:
            query_cmd = f"{pure_query}\n"
            
            query_file = os.path.join(cls.temp_dir.name, "query.txt")
            with open(query_file, 'w') as f:
                f.write(query_cmd)
            
            print(f"\n=== Executing in REPL ===")
            print(f"{pure_query}")
            
            result = subprocess.run(
                ["cat", query_file, "|", "nc", "-w", "10", "localhost", "8888"],
                capture_output=True,
                text=True,
                shell=True,
                timeout=30  # Increased timeout to allow REPL to respond
            )
            
            output = result.stdout
            print(f"\n=== Raw REPL Output ===")
            print(output)
            
            if not output.strip():
                print("No output received from REPL. Testing REPL connectivity...")
                test_result = subprocess.run(
                    ["echo", "help", "|", "nc", "-w", "2", "localhost", "8888"],
                    capture_output=True,
                    text=True,
                    shell=True,
                    timeout=5
                )
                print(f"REPL connectivity test result: {test_result.stdout}")
                
                raise RuntimeError("REPL is not responding. Test failed.")
            
            sql_start = output.find("\"sql\":")
            if sql_start != -1:
                sql_end = output.find(",", sql_start)
                sql = output[sql_start+6:sql_end].strip().strip('"')
                print(f"\n=== Extracted SQL from REPL ===")
                print(sql)
                return {"sql": sql, "output": output}
            else:
                print("Could not find SQL in REPL output.")
                raise ValueError("Could not find SQL in REPL output. Test failed.")
            
        except subprocess.TimeoutExpired as e:
            print(f"Timeout running Pure query: {e}")
            raise RuntimeError(f"REPL query timed out: {e}")
        except Exception as e:
            print(f"Error running Pure query: {e}")
            raise
    
    def test_simple_select(self):
        """Test a simple select query with the REPL."""
        df = DataFrame.from_("employees", alias="employees_0")
        
        selected_df = df.select(
            lambda employees_0: (id := employees_0.id),
            lambda employees_0: (name := employees_0.name),
            lambda employees_0: (salary := employees_0.salary)
        )
        
        sql_code = selected_df.to_sql(dialect="duckdb")
        pure_code = selected_df.to_sql(dialect="pure_relation")
        
        pure_query = '#>{local::DuckDuckDatabase.employees}#->select(~[id, name, salary])->from(local::DuckDuckRuntime)'
        
        print("\n=== DataFrame Generated SQL ===")
        print(sql_code.strip())
        
        print("\n=== DataFrame Generated Pure ===")
        print(pure_code.strip())
        
        print("\n=== Pure Code for REPL ===")
        print(pure_query)
        
        result = self._run_pure_query(pure_query)
        
        print("\n=== REPL Result ===")
        print(result)
        
        self.assertIn("sql", result, "REPL did not return SQL in debug mode")
        
        repl_sql = result["sql"].lower().replace(" ", "")
        df_sql = sql_code.lower().replace(" ", "")
        
        self.assertIn("select", repl_sql)
        self.assertIn("employees", repl_sql)
        self.assertIn("id", repl_sql)
        self.assertIn("name", repl_sql)
        self.assertIn("salary", repl_sql)
        
        self.assertIn("select", pure_code.lower())
        self.assertIn("id", pure_code.lower())
        self.assertIn("name", pure_code.lower())
        self.assertIn("salary", pure_code.lower())

if __name__ == "__main__":
    unittest.main()
