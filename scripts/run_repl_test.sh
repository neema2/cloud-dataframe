
TEMP_DIR=$(mktemp -d)
echo "Created temporary directory: $TEMP_DIR"

cat > $TEMP_DIR/employees.csv << EOF
id,name,department_id,salary
1,Alice,101,75000
2,Bob,102,85000
3,Charlie,101,65000
4,Diana,103,95000
5,Eve,102,70000
EOF
echo "Created test data at: $TEMP_DIR/employees.csv"

LOAD_CMD="load $TEMP_DIR/employees.csv local::DuckDuckConnection employees"
echo "Load command: $LOAD_CMD"

PURE_QUERY="#>{local::DuckDuckDatabase.employees}#->select(~[id, name, salary])"
echo "Pure query: $PURE_QUERY"

echo "Executing commands in the REPL..."

echo "<write_to_shell_process id=\"run_repl\" press_enter=\"true\">$LOAD_CMD</write_to_shell_process>"

sleep 2

echo "<view_shell id=\"run_repl\"/>"

echo "<write_to_shell_process id=\"run_repl\" press_enter=\"true\">debug</write_to_shell_process>"

sleep 1

echo "<view_shell id=\"run_repl\"/>"

echo "<write_to_shell_process id=\"run_repl\" press_enter=\"true\">$PURE_QUERY</write_to_shell_process>"

sleep 2

echo "<view_shell id=\"run_repl\"/>"

echo "Test completed"
