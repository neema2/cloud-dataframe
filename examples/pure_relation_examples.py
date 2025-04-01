"""
Examples of generating Pure Relation language from DataFrame DSL.

This script demonstrates how to use the cloud-dataframe library to generate
Pure Relation language from DataFrame operations.
"""
from cloud_dataframe.core.dataframe import DataFrame, Sort
from cloud_dataframe.type_system.column import count, avg, sum, max, min

df1 = DataFrame.from_("employees", alias="e")
relation1 = df1.to_sql(dialect="pure_relation")
print("Simple select all:")
print(relation1)
print()

df2 = DataFrame.from_("employees", alias="e").select(
    lambda e: (id := e.id),
    lambda e: (name := e.name),
    lambda e: (salary := e.salary)
)
relation2 = df2.to_sql(dialect="pure_relation")
print("Select specific columns:")
print(relation2)
print()

df3 = DataFrame.from_("employees", alias="e").filter(
    lambda e: e.salary > 50000
)
relation3 = df3.to_sql(dialect="pure_relation")
print("Filter:")
print(relation3)
print()

df4 = DataFrame.from_("employees", alias="e").group_by(
    lambda e: e.department
).select(
    lambda e: e.department,
    lambda e: (avg_salary := avg(e.salary)),
    lambda e: (employee_count := count(e.id))
)
relation4 = df4.to_sql(dialect="pure_relation")
print("Group by with aggregates:")
print(relation4)
print()

df5 = DataFrame.from_("employees", alias="e").order_by(
    lambda e: (e.salary, Sort.DESC)
)
relation5 = df5.to_sql(dialect="pure_relation")
print("Order by:")
print(relation5)
print()

employees = DataFrame.from_("employees", alias="e")
departments = DataFrame.from_("departments", alias="d")
joined_df = employees.join(
    departments,
    lambda e, d: e.department_id == d.id
)
relation6 = joined_df.to_sql(dialect="pure_relation")
print("Join:")
print(relation6)
print()

employees_df = DataFrame.from_("employees", alias="e")
filtered_df = employees_df.filter(lambda e: e.salary > 50000)
grouped_df = filtered_df.group_by(lambda e: e.department)
selected_df = grouped_df.select(
    lambda e: e.department,
    lambda e: (avg_salary := avg(e.salary)),
    lambda e: (max_salary := max(e.salary)),
    lambda e: (employee_count := count(e.id))
)
ordered_df = selected_df.order_by(lambda e: (e.avg_salary, Sort.DESC))
complex_df = ordered_df.limit(5)

relation7 = complex_df.to_sql(dialect="pure_relation")
print("Complex query with multiple operations:")
print(relation7)
print()
