"""
Debug script for lambda-based aggregate functions.
"""
import inspect
from cloud_dataframe.utils.lambda_parser import LambdaParser
from cloud_dataframe.core.dataframe import DataFrame
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.type_system.column import sum, avg, count, min, max

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
    total_salary := lambda x: sum(x.salary)
)
sql = query.to_sql(dialect="duckdb")
print(f"Generated SQL: {sql}")

print("\n=== Aggregate function with binary operation ===")
query = df.select(
    total_compensation := lambda x: sum(x.salary + x.bonus)
)
sql = query.to_sql(dialect="duckdb")
print(f"Generated SQL: {sql}")

print("\n=== Multiple aggregate functions with complex expressions ===")
query = df.select(
    total_compensation := lambda x: sum(x.salary + x.bonus),
    avg_net_salary := lambda x: avg(x.salary * (1 - x.tax_rate))
)
sql = query.to_sql(dialect="duckdb")
print(f"Generated SQL: {sql}")

print("\n=== Count distinct with lambda ===")
query = df.select(
    unique_names := lambda x: count(x.name, distinct=True)
)
sql = query.to_sql(dialect="duckdb")
print(f"Generated SQL: {sql}")

print("\n=== Group by with lambda aggregates ===")
query = df.group_by(lambda x: x.name).select(
    lambda x: x.name,
    total_salary := lambda x: sum(x.salary),
    avg_bonus := lambda x: avg(x.bonus)
)
sql = query.to_sql(dialect="duckdb")
print(f"Generated SQL: {sql}")
