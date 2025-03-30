"""
Debug script to investigate the test_extend_multiple_times test failure.
"""
import duckdb

conn = duckdb.connect(":memory:")

conn.execute("""
    CREATE TABLE employees (
        id INTEGER,
        name VARCHAR,
        department VARCHAR,
        location VARCHAR,
        salary FLOAT
    )
""")

conn.execute("""
    INSERT INTO employees VALUES
    (1, 'Alice', 'Engineering', 'New York', 120000),
    (2, 'Bob', 'Engineering', 'San Francisco', 110000),
    (3, 'Charlie', 'Engineering', 'New York', 95000),
    (4, 'David', 'Sales', 'Chicago', 85000),
    (5, 'Eve', 'Sales', 'Chicago', 90000)
""")

sql = """
SELECT e.department AS department, e.salary AS salary, e.salary > 100000 AS high_salary, e.id
FROM employees e
"""

result = conn.execute(sql).fetchall()

print("Test data results:")
for row in result:
    print(f"ID: {row[3]}, Department: {row[0]}, Salary: {row[1]}, High Salary: {row[2]}, Type: {type(row[2])}")
