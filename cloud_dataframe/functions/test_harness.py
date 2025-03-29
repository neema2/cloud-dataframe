"""
Test harness for scalar functions in the DataFrame DSL.

This module provides utilities for testing scalar functions against
a DuckDB backend to ensure they work correctly.
"""
import duckdb
import pandas as pd
from typing import Any, Callable, Dict, List, Optional, Union

from cloud_dataframe.functions.base import ScalarFunction
from cloud_dataframe.functions.registry import FunctionRegistry


class DuckDBBackendContext:
    """
    Mock backend context for testing functions against DuckDB.
    """
    
    def __init__(self):
        self.backend = "default"


class FunctionTestHarness:
    """
    Test harness for scalar functions in the DataFrame DSL.
    
    This class provides utilities for testing scalar functions against
    a DuckDB backend to ensure they work correctly.
    """
    
    def __init__(self):
        """Initialize the test harness with a DuckDB connection."""
        self.conn = duckdb.connect(":memory:")
        self.backend_context = DuckDBBackendContext()
    
    def test_function(
        self,
        function_class: type,
        test_cases: List[Dict[str, Any]],
        verbose: bool = False
    ) -> bool:
        """
        Test a function against a set of test cases.
        
        Args:
            function_class: The function class to test
            test_cases: A list of test cases, each containing:
                - params: List of parameter values
                - expected: Expected result
            verbose: Whether to print detailed test results
            
        Returns:
            True if all tests pass, False otherwise
        """
        function_name = function_class.function_name
        if verbose:
            print(f"Testing function: {function_name}")
        
        all_passed = True
        
        for i, test_case in enumerate(test_cases):
            params = test_case["params"]
            expected = test_case["expected"]
            
            param_exprs = [MockExpression(p) for p in params]
            
            function = function_class(param_exprs)
            
            sql = function.to_sql(self.backend_context)
            
            try:
                result = self.execute_sql(sql)
                
                passed = self.compare_results(result, expected)
                
                if verbose:
                    status = "PASSED" if passed else "FAILED"
                    print(f"  Test {i+1}: {status}")
                    print(f"    SQL: {sql}")
                    print(f"    Expected: {expected}")
                    print(f"    Actual: {result}")
                
                all_passed = all_passed and passed
            
            except Exception as e:
                if verbose:
                    print(f"  Test {i+1}: FAILED (Error)")
                    print(f"    SQL: {sql}")
                    print(f"    Error: {str(e)}")
                
                all_passed = False
        
        if verbose:
            overall = "PASSED" if all_passed else "FAILED"
            print(f"Function {function_name}: {overall}")
        
        return all_passed
    
    def execute_sql(self, sql: str) -> Any:
        """
        Execute SQL against DuckDB and return the result.
        
        Args:
            sql: SQL expression to execute
            
        Returns:
            Result of the SQL execution
        """
        query = f"SELECT {sql} AS result"
        
        result = self.conn.execute(query).fetchone()[0]
        
        return result
    
    def compare_results(self, actual: Any, expected: Any) -> bool:
        """
        Compare actual and expected results.
        
        Args:
            actual: Actual result from SQL execution
            expected: Expected result
            
        Returns:
            True if results match, False otherwise
        """
        if pd.isna(actual) and pd.isna(expected):
            return True
        
        if isinstance(actual, (int, float)) and isinstance(expected, (int, float)):
            return abs(actual - expected) < 1e-6
        
        if isinstance(expected, str) and "date" in str(type(actual)).lower():
            return str(actual).startswith(expected)
        
        return str(actual) == str(expected)


class MockExpression:
    """
    Mock expression for testing functions.
    
    This class provides a simple implementation of the Expression interface
    that can be used for testing functions.
    """
    
    def __init__(self, value: Any):
        """
        Initialize a mock expression with a value.
        
        Args:
            value: The value to use in SQL generation
        """
        self.value = value
    
    def to_sql(self, backend_context) -> str:
        """
        Generate SQL for the mock expression.
        
        Args:
            backend_context: Context object containing backend-specific information
            
        Returns:
            SQL string representation of the value
        """
        if self.value is None:
            return "NULL"
        elif isinstance(self.value, str):
            escaped = self.value.replace("'", "''")
            return f"'{escaped}'"
        elif isinstance(self.value, bool):
            return "TRUE" if self.value else "FALSE"
        elif isinstance(self.value, (int, float)):
            return str(self.value)
        else:
            return f"'{str(self.value)}'"


def run_all_tests(verbose: bool = True) -> Dict[str, bool]:
    """
    Run tests for all registered functions.
    
    Args:
        verbose: Whether to print detailed test results
        
    Returns:
        Dictionary mapping function names to test results (True if passed, False if failed)
    """
    test_harness = FunctionTestHarness()
    results = {}
    
    test_cases = {
        "upper": [
            {"params": ["hello"], "expected": "HELLO"},
            {"params": ["WORLD"], "expected": "WORLD"},
            {"params": ["Hello World"], "expected": "HELLO WORLD"},
        ],
        "lower": [
            {"params": ["HELLO"], "expected": "hello"},
            {"params": ["world"], "expected": "world"},
            {"params": ["Hello World"], "expected": "hello world"},
        ],
        "concat": [
            {"params": ["Hello", " ", "World"], "expected": "Hello World"},
            {"params": ["abc", "123"], "expected": "abc123"},
        ],
        "substring": [
            {"params": ["Hello World", 1, 5], "expected": "Hello"},
            {"params": ["Hello World", 7, 5], "expected": "World"},
        ],
        "length": [
            {"params": ["Hello"], "expected": 5},
            {"params": [""], "expected": 0},
            {"params": ["Hello World"], "expected": 11},
        ],
        "replace": [
            {"params": ["Hello World", "World", "Universe"], "expected": "Hello Universe"},
            {"params": ["abcabc", "a", "x"], "expected": "xbcxbc"},
        ],
        
        "date_diff": [
            {"params": ["day", "2023-01-01", "2023-01-10"], "expected": 9},
            {"params": ["month", "2023-01-01", "2023-03-01"], "expected": 2},
        ],
        "date_part": [
            {"params": ["year", "2023-01-01"], "expected": 2023},
            {"params": ["month", "2023-01-01"], "expected": 1},
            {"params": ["day", "2023-01-01"], "expected": 1},
        ],
        "date_trunc": [
            {"params": ["month", "2023-01-15"], "expected": "2023-01-01"},
            {"params": ["year", "2023-06-15"], "expected": "2023-01-01"},
        ],
        "current_date": [
            {"params": [], "expected": None},  # Special case, will be handled in test
        ],
        "date_add": [
            {"params": ["day", 5, "2023-01-01"], "expected": "2023-01-06"},
            {"params": ["month", 2, "2023-01-01"], "expected": "2023-03-01"},
        ],
        "date_sub": [
            {"params": ["day", 5, "2023-01-10"], "expected": "2023-01-05"},
            {"params": ["month", 2, "2023-03-01"], "expected": "2023-01-01"},
        ],
        
        "abs": [
            {"params": [5], "expected": 5},
            {"params": [-5], "expected": 5},
            {"params": [0], "expected": 0},
        ],
        "round": [
            {"params": [5.5], "expected": 6},
            {"params": [5.4], "expected": 5},
            {"params": [5.55, 1], "expected": 5.6},
        ],
        "ceil": [
            {"params": [5.1], "expected": 6},
            {"params": [5.0], "expected": 5},
            {"params": [-5.1], "expected": -5},
        ],
        "floor": [
            {"params": [5.9], "expected": 5},
            {"params": [5.0], "expected": 5},
            {"params": [-5.1], "expected": -6},
        ],
        "power": [
            {"params": [2, 3], "expected": 8},
            {"params": [10, 2], "expected": 100},
            {"params": [2, 0.5], "expected": 1.4142135623730951},
        ],
        "sqrt": [
            {"params": [4], "expected": 2},
            {"params": [9], "expected": 3},
            {"params": [2], "expected": 1.4142135623730951},
        ],
        "mod": [
            {"params": [10, 3], "expected": 1},
            {"params": [10, 2], "expected": 0},
            {"params": [10, 10], "expected": 0},
        ],
    }
    
    current_date_tests = test_cases.pop("current_date")
    
    for function_name, function_test_cases in test_cases.items():
        function_class = FunctionRegistry.get_function_class(function_name)
        if function_class:
            results[function_name] = test_harness.test_function(
                function_class, function_test_cases, verbose
            )
    
    if verbose:
        print("Testing function: current_date")
    
    try:
        function_class = FunctionRegistry.get_function_class("current_date")
        function = function_class([])
        sql = function.to_sql(test_harness.backend_context)
        result = test_harness.execute_sql(sql)
        
        import re
        is_valid_date = bool(re.match(r'^\d{4}-\d{2}-\d{2}$', str(result)))
        
        if verbose:
            status = "PASSED" if is_valid_date else "FAILED"
            print(f"  Test 1: {status}")
            print(f"    SQL: {sql}")
            print(f"    Result: {result}")
            print(f"    Valid date: {is_valid_date}")
        
        results["current_date"] = is_valid_date
    
    except Exception as e:
        if verbose:
            print(f"  Test 1: FAILED (Error)")
            print(f"    Error: {str(e)}")
        
        results["current_date"] = False
    
    if verbose:
        passed = sum(1 for result in results.values() if result)
        total = len(results)
        print(f"\nOverall: {passed}/{total} functions passed")
    
    return results


if __name__ == "__main__":
    run_all_tests(verbose=True)
