"""
Debug script for lambda-based aggregate functions.
"""
import inspect
from cloud_dataframe.utils.lambda_parser import LambdaParser
from cloud_dataframe.core.dataframe import DataFrame
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.type_system.column import as_column, sum, avg, count, min, max

# Create a test schema
schema = TableSchema(
    name="Employee",
    columns={
        "id": int,
        "name": str,
        "salary": float,
        "bonus": float,
        "tax_rate": float
    }
)

# Create a DataFrame with the schema
df = DataFrame.from_table_schema("employees", schema)

# Test with different lambda expressions
print("=== Simple column reference ===")
lambda_func = lambda x: x.salary
print(f"Lambda source: {inspect.getsource(lambda_func).strip()}")
parsed = LambdaParser.parse_lambda(lambda_func)
print(f"Parsed result: {parsed}")

print("\n=== Binary operation ===")
lambda_func = lambda x: x.salary + x.bonus
print(f"Lambda source: {inspect.getsource(lambda_func).strip()}")
parsed = LambdaParser.parse_lambda(lambda_func)
print(f"Parsed result: {parsed}")

print("\n=== Complex expression ===")
lambda_func = lambda x: x.salary * (1 - x.tax_rate)
print(f"Lambda source: {inspect.getsource(lambda_func).strip()}")
parsed = LambdaParser.parse_lambda(lambda_func)
print(f"Parsed result: {parsed}")

print("\n=== Aggregate function with simple column ===")
query = df.select(
    as_column(sum(lambda x: x.salary), "total_salary")
)
sql = query.to_sql(dialect="duckdb")
print(f"Generated SQL: {sql}")

print("\n=== Aggregate function with binary operation ===")
query = df.select(
    as_column(sum(lambda x: x.salary + x.bonus), "total_compensation")
)
sql = query.to_sql(dialect="duckdb")
print(f"Generated SQL: {sql}")

print("\n=== Multiple aggregate functions with complex expressions ===")
query = df.select(
    as_column(sum(lambda x: x.salary + x.bonus), "total_compensation"),
    as_column(avg(lambda x: x.salary * (1 - x.tax_rate)), "avg_net_salary")
)
sql = query.to_sql(dialect="duckdb")
print(f"Generated SQL: {sql}")

print("\n=== Count distinct with lambda ===")
query = df.select(
    as_column(count(lambda x: x.name, distinct=True), "unique_names")
)
sql = query.to_sql(dialect="duckdb")
print(f"Generated SQL: {sql}")

print("\n=== Group by with lambda aggregates ===")
query = df.group_by(lambda x: x.name).select(
    lambda x: x.name,
    as_column(sum(lambda x: x.salary), "total_salary"),
    as_column(avg(lambda x: x.bonus), "avg_bonus")
)
sql = query.to_sql(dialect="duckdb")
print(f"Generated SQL: {sql}")
