# Function Resolution in Lambda Expressions

This document explains how functions are resolved in lambda expressions in the cloud-dataframe library.

## Direct Function Calls

When a function is called directly (e.g., `date_diff('day', x.start_date, x.end_date)`), the lambda parser:

1. Checks if the function exists in the FunctionRegistry using `FunctionRegistry.get_function_class(node.func.id)`
2. If found, tries to create the function using `FunctionRegistry.create_function(node.func.id, args_list)`
3. If creation fails, falls back to a simple FunctionExpression

This means that for direct function calls, the function name must be registered in the FunctionRegistry, but it doesn't need to be imported or in scope in the Python code. This is why functions like `date_diff`, `upper`, and `concat` work in lambda expressions even though the linter shows them as undefined.

## Attribute Method Calls

When a function is called as an attribute (e.g., `x.date_diff('day', y, z)`), the lambda parser:

1. Creates a FunctionExpression with the attribute name as the function name
2. Does not check the FunctionRegistry

This means that for attribute method calls, the function does not need to be registered in the FunctionRegistry, but it also won't be resolved to the same function implementation as a direct call.

## Wrapper Functions

In some test files, wrapper functions are defined for scalar functions:

```python
def date_diff(unit, start_date, end_date):
    """Wrapper for DateDiffFunction to use in lambda expressions."""
    return FunctionRegistry.create_function("date_diff", [unit, start_date, end_date])
```

These wrapper functions are not strictly necessary when using direct function calls (e.g., `date_diff('day', x, y)`), as the lambda parser will resolve these names through the FunctionRegistry. However, they can be useful for:

1. Making the linter happy by providing a defined function
2. Historical reasons if the code previously used attribute calls (e.g., `x.date_diff`)
3. Providing explicit control over function creation parameters

## Usage in Tests

In test files like test_scalar_functions.py, functions like date_diff are called directly without being
imported. This works because the lambda parser resolves them through the FunctionRegistry.

In other test files like test_nested_functions_duckdb.py, we use wrapper functions that explicitly call
FunctionRegistry.create_function. This is an alternative approach when the function isn't imported.

## Usage Recommendations

For consistency, prefer direct function calls when the function is registered in the FunctionRegistry. This approach relies on the lambda parser's built-in resolution mechanism and is more concise.

If you need to use many functions that aren't directly imported, consider adding wrapper functions to satisfy the linter, but be aware that they are redundant at runtime.
