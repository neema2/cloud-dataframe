"""
Debug script for lambda parser issues.
"""
import ast
import inspect
from cloud_dataframe.utils.lambda_parser import LambdaParser
from cloud_dataframe.core.dataframe import DataFrame
from cloud_dataframe.type_system.schema import TableSchema

def debug_lambda_source(lambda_func):
    """Debug lambda source code extraction."""
    try:
        source = inspect.getsource(lambda_func)
        print(f"Original source: {source.strip()}")
        
        if "\\" in source:
            source = source.replace("\\", "").strip()
            print(f"After removing backslashes: {source}")
        
        lambda_start = source.find("lambda")
        if lambda_start >= 0:
            source = source[lambda_start:]
            print(f"After finding lambda start: {source}")
            
            depth = 0
            lambda_end = len(source)
            
            colon_pos = source.find(":")
            if colon_pos > 0:
                print(f"Colon position: {colon_pos}")
                for i in range(colon_pos, len(source)):
                    if source[i] == "(":
                        depth += 1
                    elif source[i] == ")":
                        depth -= 1
                        if depth < 0:
                            lambda_end = i
                            break
            
            source = f"lambda{source[6:lambda_end]}"
            print(f"Final lambda source: {source}")
        
        tree = ast.parse(source.strip())
        print(f"AST dump: {ast.dump(tree, indent=2)}")
        
        lambda_node = None
        for node in ast.walk(tree):
            if isinstance(node, ast.Lambda):
                lambda_node = node
                break
        
        if not lambda_node:
            print("Could not find lambda expression in source code")
        else:
            print(f"Lambda node: {ast.dump(lambda_node, indent=2)}")
            
        return source
    except Exception as e:
        print(f"Error: {str(e)}")
        return None

def debug_complex_nested_condition():
    """Debug complex nested condition test case."""
    print("\n=== Testing complex nested condition ===")
    lambda_func = lambda x: (x.department == "Engineering" and x.salary > 80000) or \
                 (x.department == "Sales" and x.salary > 60000) or \
                 (x.is_manager == True and x.age > 40)
    
    debug_lambda_source(lambda_func)
    
    try:
        schema = TableSchema(
            name="Employee",
            columns={
                "id": int,
                "name": str,
                "department": str,
                "salary": float,
                "age": int,
                "is_manager": bool
            }
        )
        
        expr = LambdaParser.parse_lambda(lambda_func, schema)
        print(f"Parsed expression: {expr}")
    except Exception as e:
        print(f"Parser error: {str(e)}")

def debug_chained_filters():
    """Debug chained filters test case."""
    print("\n=== Testing chained filters ===")
    
    df = DataFrame.from_("employees", alias="x")
    
    filter1 = lambda x: x.salary > 50000
    filter2 = lambda x: x.department == "Engineering"
    filter3 = lambda x: x.age > 30
    
    print("Filter 1:")
    debug_lambda_source(filter1)
    
    print("\nFilter 2:")
    debug_lambda_source(filter2)
    
    print("\nFilter 3:")
    debug_lambda_source(filter3)
    
    try:
        schema = TableSchema(
            name="Employee",
            columns={
                "id": int,
                "name": str,
                "department": str,
                "salary": float,
                "age": int
            }
        )
        
        expr1 = LambdaParser.parse_lambda(filter1, schema)
        print(f"Parsed filter 1: {expr1}")
        
        expr2 = LambdaParser.parse_lambda(filter2, schema)
        print(f"Parsed filter 2: {expr2}")
        
        expr3 = LambdaParser.parse_lambda(filter3, schema)
        print(f"Parsed filter 3: {expr3}")
    except Exception as e:
        print(f"Parser error: {str(e)}")

if __name__ == "__main__":
    debug_complex_nested_condition()
    debug_chained_filters()
