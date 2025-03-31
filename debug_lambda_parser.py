"""Debug script to understand lambda parser execution path."""
from cloud_dataframe.utils.lambda_parser import parse_lambda
from cloud_dataframe.functions.registry import FunctionRegistry
from cloud_dataframe.type_system.column import LiteralExpression, ColumnReference

# Test lambda function with method call
def test_method_call():
    print("Testing lambda with method call:")
    lambda_func = lambda x: x.date_diff('day', x.start_date, x.end_date)
    
    # Parse the lambda
    try:
        result = parse_lambda(lambda_func)
        print(f"Result type: {type(result)}")
        print(f"Result: {result}")
        if hasattr(result, 'function_name'):
            print(f"Function name: {result.function_name}")
        if hasattr(result, 'parameters'):
            print(f"Parameters: {result.parameters}")
            for i, param in enumerate(result.parameters):
                print(f"  Parameter {i}: {type(param)} - {param}")
    except Exception as e:
        print(f"Error parsing lambda: {e}")

# Test lambda function with direct function call
def test_direct_call():
    print("\nTesting lambda with direct function call:")
    lambda_func = lambda x: FunctionRegistry.get_function("date_diff")("day", x.start_date, x.end_date)
    
    # Parse the lambda
    try:
        result = parse_lambda(lambda_func)
        print(f"Result type: {type(result)}")
        print(f"Result: {result}")
        if hasattr(result, 'function_name'):
            print(f"Function name: {result.function_name}")
        if hasattr(result, 'parameters'):
            print(f"Parameters: {result.parameters}")
            for i, param in enumerate(result.parameters):
                print(f"  Parameter {i}: {type(param)} - {param}")
    except Exception as e:
        print(f"Error parsing lambda: {e}")

# Run the tests
if __name__ == "__main__":
    test_method_call()
    test_direct_call()
