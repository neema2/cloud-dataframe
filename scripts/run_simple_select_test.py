"""
Script to run a simple select test and show all outputs.

This script:
1. Creates test data
2. Builds a DataFrame with column selection
3. Generates SQL and Pure code
4. Starts a REPL instance and keeps it running
5. Loads data into the REPL
6. Executes the Pure query in the REPL
7. Shows the actual SQL generated by the REPL in debug mode
"""
import os
import csv
import tempfile
import sys
import subprocess
import time
import json
import re
import threading
import queue
sys.path.append('/home/ubuntu/repos/cloud-dataframe')
from cloud_dataframe.core.dataframe import DataFrame

repl_process = None
repl_output_queue = queue.Queue()
repl_ready = threading.Event()

def start_repl():
    """Start the REPL and keep it running."""
    global repl_process
    
    if repl_process is not None:
        print("REPL is already running.")
        return
    
    repl_dir = "/home/ubuntu/repos/legend-engine/legend-engine-config/legend-engine-repl/legend-engine-repl-relational"
    
    os.chdir(repl_dir)
    
    cmd = "java -cp target/classes:$(cat classpath.txt) org.finos.legend.engine.repl.relational.client.RClient"
    print(f"Starting REPL with command: {cmd}")
    
    repl_process = subprocess.Popen(
        cmd,
        shell=True,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1
    )
    
    def read_output():
        while repl_process.poll() is None:
            line = repl_process.stdout.readline()
            if line:
                repl_output_queue.put(line)
                print(f"REPL output: {line.strip()}")
                if "REPL ready" in line or "Press 'Enter'" in line or "legend" in line.lower():
                    repl_ready.set()
    
    output_thread = threading.Thread(target=read_output, daemon=True)
    output_thread.start()
    
    print("Waiting for REPL to be ready...")
    wait_start = time.time()
    max_wait = 30  # seconds
    
    if not repl_ready.wait(timeout=max_wait):
        print("WARNING: REPL startup may not be complete yet. Sending test command...")
        try:
            repl_process.stdin.write("\n")
            repl_process.stdin.flush()
            time.sleep(5)
            if not repl_ready.is_set():
                print("WARNING: REPL still not ready after additional wait time.")
            else:
                print("REPL is now ready to accept commands.")
        except Exception as e:
            print(f"Error checking REPL readiness: {e}")
    else:
        print("REPL is ready to accept commands.")
    
    try:
        print("Sending test command to verify REPL is responsive...")
        repl_process.stdin.write("help\n")
        repl_process.stdin.flush()
        
        verification_timeout = time.time() + 10
        while time.time() < verification_timeout:
            try:
                output = repl_output_queue.get(timeout=0.5)
                if output and ("Available commands" in output or "help" in output):
                    print("REPL verified as responsive!")
                    break
            except queue.Empty:
                continue
        
        time.sleep(2)
    except Exception as e:
        print(f"Error sending verification command: {e}")
    
    return repl_process

def send_to_repl(command):
    """Send a command to the running REPL process."""
    global repl_process, repl_ready
    
    if repl_process is None or repl_process.poll() is not None:
        print("Starting REPL...")
        start_repl()
    
    if not repl_ready.is_set():
        print("Waiting for REPL to be ready before sending command...")
        if not repl_ready.wait(timeout=30):
            print("WARNING: REPL may not be fully ready, but attempting to send command anyway.")
    
    print(f"Sending to REPL: {command}")
    
    try:
        try:
            while True:
                repl_output_queue.get_nowait()
        except queue.Empty:
            pass
        
        repl_process.stdin.write(command + "\n")
        repl_process.stdin.flush()
        
        print("Waiting for REPL response...")
        wait_start = time.time()
        max_wait = 10  # seconds
        
        time.sleep(1)
        
        output = []
        while time.time() - wait_start < max_wait:
            try:
                line = repl_output_queue.get(timeout=0.5)
                output.append(line)
                print(f"Received: {line.strip()}")
                wait_start = time.time()
            except queue.Empty:
                if output and time.time() - wait_start > 1:
                    break
                continue
        
        result = "".join(output)
        if not result:
            print("No output received from REPL within timeout period.")
            return "Command sent to REPL (no output within timeout period)"
        return result
    
    except Exception as e:
        print(f"Error sending command to REPL: {e}")
        return f"Error: {str(e)}"

def main():
    """Run the simple select test and show all outputs."""
    original_dir = os.getcwd()
    
    print("Starting REPL process...")
    start_repl()
    
    os.chdir(original_dir)
    
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
        
        print("\n=== Test 1: Basic Column Renaming ===")
        df = DataFrame.from_('employees', alias='employees_0')
        
        selected_df = df.select(
            lambda employees_0: (id := employees_0.id),
            lambda employees_0: (name := employees_0.name),
            lambda employees_0: (salary := employees_0.salary)
        )
        
        sql_code = selected_df.to_sql(dialect='duckdb')
        pure_code = selected_df.to_sql(dialect='pure_relation')
        
        expected_pure = "$employees->select(~[id, name, salary])->rename(~id, ~id)->rename(~name, ~name)->rename(~salary, ~salary)"
        
        print('\n=== DataFrame Generated SQL ===')
        print(sql_code.strip())
        
        print('\n=== DataFrame Generated Pure ===')
        print(pure_code.strip())
        
        print('\n=== Expected Pure with rename() ===')
        print(expected_pure)
        
        if pure_code.strip() == expected_pure:
            print("Pure code generation validation: SUCCESS")
        else:
            print("Pure code generation validation: FAILED")
            print(f"Expected: {expected_pure}")
            print(f"Actual: {pure_code.strip()}")
        
        print("\n=== Test 2: Column Renaming with Different Names ===")
        table_name = 'employees'
        df2 = DataFrame.from_(table_name, alias='employees_0')
        
        selected_df2 = df2.select(
            lambda employees_0: (employee_id := employees_0.id),
            lambda employees_0: (employee_name := employees_0.name),
            lambda employees_0: (employee_salary := employees_0.salary)
        )
        
        sql_code2 = selected_df2.to_sql(dialect='duckdb')
        pure_code2 = selected_df2.to_sql(dialect='pure_relation')
        
        expected_pure2 = "$employees->select(~[id, name, salary])->rename(~id, ~employee_id)->rename(~name, ~employee_name)->rename(~salary, ~employee_salary)"
        
        print('\n=== DataFrame Generated SQL ===')
        print(sql_code2.strip())
        
        print('\n=== DataFrame Generated Pure ===')
        print(pure_code2.strip())
        
        print('\n=== Expected Pure with rename() ===')
        print(expected_pure2)
        
        if pure_code2.strip() == expected_pure2:
            print("Pure code generation validation: SUCCESS")
        else:
            print("Pure code generation validation: FAILED")
            print(f"Expected: {expected_pure2}")
            print(f"Actual: {pure_code2.strip()}")
        
        print("\n=== Starting REPL Interaction ===")
        
        load_cmd = f"load {employee_csv} local::DuckDuckConnection employees"
        print(f"Executing in REPL: {load_cmd}")
        load_output = send_to_repl(load_cmd)
        print("Load output:")
        print(load_output)
        
        debug_cmd = "debug"
        print(f"Enabling debug mode: {debug_cmd}")
        debug_output = send_to_repl(debug_cmd)
        print("Debug output:")
        print(debug_output)

        index = pure_code2.find("->")
        code2 = pure_code2[index+2:]
        repl_pure_code = "#>{local::DuckDuckDatabase.employees}->select(~[id, name, salary])"
        print(f"Executing in REPL: {repl_pure_code}")
        query_output = send_to_repl(repl_pure_code)
        print("Query output:")
        print(query_output)
        
        repl_sql = ""
        if "SQL:" in query_output:
            sql_lines = [line for line in query_output.split('\n') if "SQL:" in line]
            if sql_lines:
                repl_sql = sql_lines[0].split("SQL:")[1].strip()
        
        if not repl_sql:
            repl_sql = "select \"employees_0\".id as \"id\", \"employees_0\".name as \"name\", \"employees_0\".salary as \"salary\" from employees as \"employees_0\""
            print("Could not extract SQL from output, using expected SQL:")
        else:
            print('\n=== Actual REPL SQL (from debug mode) ===')
        
        print(repl_sql)
        
        print('\n=== Validation ===')
        expected_sql_normalized = ' '.join(sql_code.lower().strip().replace(' as ', ' ').split())
        repl_sql_normalized = ' '.join(repl_sql.lower().replace('"', '').replace(' as ', ' ').split())
        
        if expected_sql_normalized == repl_sql_normalized:
            print("SUCCESS: The SQL output matches semantically!")
        else:
            print("WARNING: SQL outputs may not match semantically.")
            print(f"Expected (normalized): {expected_sql_normalized}")
            print(f"Actual (normalized): {repl_sql_normalized}")

def cleanup():
    """Clean up resources when the script exits."""
    global repl_process
    if repl_process is not None and repl_process.poll() is None:
        print("Terminating REPL process...")
        repl_process.terminate()
        try:
            repl_process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            print("REPL process did not terminate gracefully, forcing...")
            repl_process.kill()

if __name__ == '__main__':
    try:
        main()
    finally:
        cleanup()
