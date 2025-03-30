"""
Debug script to investigate the select() function and SQL generation.
"""
import duckdb
from cloud_dataframe.core.dataframe import DataFrame
from cloud_dataframe.type_system.schema import TableSchema

full_schema = TableSchema(
    name='Employee',
    columns={
        'id': int,
        'name': str,
        'department': str,
        'location': str,
        'salary': float
    }
)

partial_schema = TableSchema(
    name='Employee',
    columns={
        'id': int,
        'name': str,
        'salary': float
    }
)

print('=== Testing with full schema ===')
df_full = DataFrame.from_table_schema('employees', full_schema, alias='e')
df_selected_full = df_full.select(lambda e: e.id, lambda e: e.name, lambda e: e.salary)
print('SQL after select with full schema:')
print(df_selected_full.to_sql(dialect='duckdb'))

print('\n=== Testing with partial schema ===')
df_partial = DataFrame.from_table_schema('employees', partial_schema, alias='e')
df_selected_partial = df_partial.select(lambda e: e.id, lambda e: e.name, lambda e: e.salary)
print('SQL after select with partial schema:')
print(df_selected_partial.to_sql(dialect='duckdb'))

print('\n=== Testing extend with full schema ===')
df_extended_full = df_selected_full.extend(lambda e: (bonus := e.salary * 0.1))
print('SQL after extend with full schema:')
print(df_extended_full.to_sql(dialect='duckdb'))

print('\n=== Testing extend with partial schema ===')
df_extended_partial = df_selected_partial.extend(lambda e: (bonus := e.salary * 0.1))
print('SQL after extend with partial schema:')
print(df_extended_partial.to_sql(dialect='duckdb'))
