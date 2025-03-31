"""Script to capture generated SQL for date_functions_with_literals test."""
from cloud_dataframe.core.dataframe import DataFrame

df = DataFrame.from_('employees', alias='e')
date_diff_df = df.select(
    lambda e: e.id,
    lambda e: e.name,
    lambda e: (days_employed := e.date_diff('day', e.hire_date, e.end_date))
)

print('GENERATED SQL:')
print(date_diff_df.to_sql(dialect='duckdb'))
