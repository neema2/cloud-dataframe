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
SELECT e.id, e.department AS department, e.salary AS salary, e.salary > 100000 AS high_salary
FROM employees e
"""

result = conn.execute(sql).fetchall()

print("Test data results with explicit column order:")
for row in result:
    print(f"ID: {row[0]}, Department: {row[1]}, Salary: {row[2]}, High Salary: {row[3]}, Type: {type(row[3])}")
    
print("\nDetailed analysis of row with ID=3:")
row_3 = conn.execute("SELECT e.id, e.department AS department, e.salary AS salary, e.salary > 100000 AS high_salary FROM employees e WHERE e.id = 3").fetchone()
print(f"ID: {row_3[0]}, Department: {row_3[1]}, Salary: {row_3[2]}, High Salary: {row_3[3]}, Type: {type(row_3[3])}")
print(f"Expected condition (salary > 100000): {row_3[2] > 100000}")
