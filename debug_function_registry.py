"""Debug script to understand function registry parameter handling."""
from cloud_dataframe.functions.registry import FunctionRegistry
from cloud_dataframe.type_system.column import LiteralExpression, ColumnReference

day_literal = LiteralExpression(value='day')
month_literal = LiteralExpression(value='month')
start_date = ColumnReference(name='start_date')
end_date = ColumnReference(name='end_date')
interval = LiteralExpression(value=6)

print("Testing date_diff function creation:")
try:
    date_diff_func = FunctionRegistry.create_function("date_diff", [day_literal, start_date, end_date])
    print(f"SUCCESS: Created date_diff with 3 parameters: {date_diff_func}")
    print(f"Function name: {date_diff_func.function_name}")
    print(f"Parameters: {date_diff_func.parameters}")
except ValueError as e:
    print(f"ERROR: Failed to create date_diff with 3 parameters: {e}")

try:
    date_diff_func = FunctionRegistry.create_function("date_diff", [start_date, end_date])
    print(f"SUCCESS: Created date_diff with 2 parameters: {date_diff_func}")
    print(f"Function name: {date_diff_func.function_name}")
    print(f"Parameters: {date_diff_func.parameters}")
except ValueError as e:
    print(f"ERROR: Failed to create date_diff with 2 parameters: {e}")

print("\nTesting date_add function creation:")
try:
    date_add_func = FunctionRegistry.create_function("date_add", [month_literal, interval, start_date])
    print(f"SUCCESS: Created date_add with 3 parameters: {date_add_func}")
    print(f"Function name: {date_add_func.function_name}")
    print(f"Parameters: {date_add_func.parameters}")
except ValueError as e:
    print(f"ERROR: Failed to create date_add with 3 parameters: {e}")

try:
    date_add_func = FunctionRegistry.create_function("date_add", [interval, start_date])
    print(f"SUCCESS: Created date_add with 2 parameters: {date_add_func}")
    print(f"Function name: {date_add_func.function_name}")
    print(f"Parameters: {date_add_func.parameters}")
except ValueError as e:
    print(f"ERROR: Failed to create date_add with 2 parameters: {e}")

date_diff_class = FunctionRegistry.get_function_class("date_diff")
print(f"\ndate_diff parameter types: {date_diff_class.parameter_types}")

date_add_class = FunctionRegistry.get_function_class("date_add")
print(f"date_add parameter types: {date_add_class.parameter_types}")
