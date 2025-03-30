"""
Debug script to verify the select() function SQL generation fix.
"""
import duckdb
from cloud_dataframe.core.dataframe import DataFrame
from cloud_dataframe.type_system.schema import TableSchema

schema = TableSchema(
    name='Employee',
    columns={
        'id': int,
        'name': str,
        'department': str,
        'location': str,
        'salary': float
    }
)

df = DataFrame.from_table_schema('employees', schema, alias='e')

df_selected = df.select(lambda e: e.id, lambda e: e.name, lambda e: e.salary)
print('SQL after select:')
print(df_selected.to_sql(dialect='duckdb'))
print("Expected: SELECT e.id, e.name, e.salary")

df_aliased = df.select(
    lambda e: (employee_id := e.id),
    lambda e: (employee_name := e.name),
    lambda e: (employee_salary := e.salary)
)
print('\nSQL after select with aliases:')
print(df_aliased.to_sql(dialect='duckdb'))
print("Expected: SELECT e.id AS employee_id, e.name AS employee_name, e.salary AS employee_salary")

df_computed = df.select(
    lambda e: e.id,
    lambda e: e.name,
    lambda e: (bonus := e.salary * 0.1)
)
print('\nSQL after select with computed column:')
print(df_computed.to_sql(dialect='duckdb'))
print("Expected: SELECT e.id, e.name, (e.salary * 0.1) AS bonus")
