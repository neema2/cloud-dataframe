"""Script to capture expected SQL for date_functions_with_literals test."""
expected_sql = """SELECT e.id, e.name, date_diff('day', e.hire_date, e.end_date) AS days_employed
FROM employees e"""

exec_sql = """SELECT e.id, e.name, DATE_DIFF('day', CAST(e.hire_date AS DATE), CAST(e.end_date AS DATE)) AS days_employed
FROM employees e"""

print('EXPECTED SQL (from test assertion):')
print(expected_sql)
print('\nEXECUTED SQL (in test):')
print(exec_sql)
