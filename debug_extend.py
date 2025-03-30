"""
Debug script to investigate the extend() function and SQL generation.
"""
import duckdb
from cloud_dataframe.core.dataframe import DataFrame
from cloud_dataframe.type_system.schema import TableSchema

schema = TableSchema(
    name='Employee',
    columns={
        'id': int,
        'name': str,
        'salary': float
    }
)

df = DataFrame.from_table_schema('employees', schema, alias='e')

df_selected = df.select(lambda e: e.id, lambda e: e.name, lambda e: e.salary)
print('SQL after select:')
print(df_selected.to_sql(dialect='duckdb'))

df_extended = df_selected.extend(lambda e: (bonus := e.salary * 0.1))
print('\nSQL after extend:')
print(df_extended.to_sql(dialect='duckdb'))

print('\nTesting multiple extends:')
df_multi = df.select(lambda e: e.id, lambda e: e.name)
print('After initial select:')
print(df_multi.to_sql(dialect='duckdb'))

df_multi = df_multi.extend(lambda e: (department := e.department))
print('\nAfter first extend:')
print(df_multi.to_sql(dialect='duckdb'))

df_multi = df_multi.extend(lambda e: (salary := e.salary))
print('\nAfter second extend:')
print(df_multi.to_sql(dialect='duckdb'))

df_multi = df_multi.extend(lambda e: (high_salary := e.salary > 100000))
print('\nAfter third extend:')
print(df_multi.to_sql(dialect='duckdb'))
