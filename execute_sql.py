"""Script to execute SQL queries in DuckDB for date_functions_with_literals test."""
import duckdb
import os

db_path = 'test_scalar_functions.db'
if os.path.exists(db_path):
    os.remove(db_path)

conn = duckdb.connect(db_path)

conn.execute('''
CREATE TABLE employees (
    id INTEGER,
    name VARCHAR,
    department_id INTEGER,
    salary DOUBLE,
    hire_date DATE,
    end_date DATE
)
''')

conn.execute('''
INSERT INTO employees VALUES
    (1, 'Alice', 1, 75000, '2020-01-15', '2023-05-20'),
    (2, 'Bob', 1, 65000, '2019-03-10', '2023-06-30'),
    (3, 'Charlie', 2, 85000, '2021-02-05', '2024-01-15'),
    (4, 'Diana', 2, 78000, '2018-11-20', '2023-12-31'),
    (5, 'Eve', 3, 95000, '2017-07-01', '2023-10-15')
''')

generated_sql = '''SELECT e.id, e.name, date_diff('day', e.hire_date, e.end_date) AS days_employed
FROM employees e'''

print('RESULTS FROM GENERATED SQL (without CAST):')
try:
    result = conn.execute(generated_sql).fetchall()
    print(f'Success! {len(result)} rows returned')
    for row in result:
        print(row)
except Exception as e:
    print(f'Error: {e}')

expected_sql = '''SELECT e.id, e.name, DATE_DIFF('day', CAST(e.hire_date AS DATE), CAST(e.end_date AS DATE)) AS days_employed
FROM employees e'''

print('\nRESULTS FROM EXPECTED SQL (with CAST):')
try:
    result = conn.execute(expected_sql).fetchall()
    print(f'Success! {len(result)} rows returned')
    for row in result:
        print(row)
except Exception as e:
    print(f'Error: {e}')

conn.close()
if os.path.exists(db_path):
    os.remove(db_path)
