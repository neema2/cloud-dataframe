"""Debug script for lambda parser in cloud-dataframe."""
import ast
import inspect
from cloud_dataframe.utils.lambda_parser import LambdaParser
from cloud_dataframe.type_system.schema import TableSchema

def extract_lambda_source(lambda_func):
    try:
        source = inspect.getsource(lambda_func)
        lambda_start = source.find("lambda")
        if lambda_start >= 0:
            source = source[lambda_start:]
        return source.strip()
    except:
        return "lambda: <body>"

def debug_lambda(lambda_func, schema=None):
    source = extract_lambda_source(lambda_func)
    print(f"Lambda source: {source}")
    try:
        tree = ast.parse(source.strip())
        if schema:
            expr = LambdaParser.parse_lambda(lambda_func, schema)
            print(expr)
    except SyntaxError as e:
        print(f"Syntax error: {e}")
