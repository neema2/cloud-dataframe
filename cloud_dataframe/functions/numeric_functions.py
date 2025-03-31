"""
Numeric functions for the DataFrame DSL.

This module provides implementations of numeric manipulation functions
that can work across different SQL backends.
"""
from typing import List, Union

from cloud_dataframe.functions.base import ScalarFunction


class AbsFunction(ScalarFunction):
    """
    Returns the absolute value of a number.
    
    Example:
        lambda x: abs(x.value)
    """
    function_name = "abs"
    parameter_types = [("value", Union[int, float])]
    return_type = Union[int, float]
    
    def __init__(self, parameters: List):
        super().__init__(parameters)
    
    def to_sql_default(self, backend_context):
        """Default implementation (DuckDB)"""
        param_sql = self.param_sql_dict.get("value", "")
        return f"ABS({param_sql})"


class RoundFunction(ScalarFunction):
    """
    Rounds a number to the specified number of decimal places.
    
    Example:
        lambda x: round(x.value, 2)  # Round to 2 decimal places
    """
    function_name = "round"
    parameter_types = [("value", float), ("decimals", int)]
    return_type = float
    
    def __init__(self, parameters: List):
        if len(parameters) == 1:
            from cloud_dataframe.functions.test_harness import MockExpression
            parameters.append(MockExpression(0))  # Default to 0 decimal places
        super().__init__(parameters)
    
    def to_sql_default(self, backend_context):
        """Default implementation (DuckDB)"""
        value_sql = self.param_sql_dict.get("value", "")
        decimals_sql = self.param_sql_dict.get("decimals", "")
        return f"ROUND({value_sql}, {decimals_sql})"


class CeilFunction(ScalarFunction):
    """
    Returns the smallest integer greater than or equal to a number.
    
    Example:
        lambda x: ceil(x.value)
    """
    function_name = "ceil"
    parameter_types = [("value", float)]
    return_type = int
    
    def __init__(self, parameters: List):
        super().__init__(parameters)
    
    def to_sql_default(self, backend_context):
        """Default implementation (DuckDB)"""
        param_sql = self.param_sql_dict.get("value", "")
        return f"CEIL({param_sql})"
    
    def to_sql_postgres(self, backend_context):
        """PostgreSQL-specific implementation"""
        param_sql = self.param_sql_dict.get("value", "")
        return f"CEILING({param_sql})"


class FloorFunction(ScalarFunction):
    """
    Returns the largest integer less than or equal to a number.
    
    Example:
        lambda x: floor(x.value)
    """
    function_name = "floor"
    parameter_types = [("value", float)]
    return_type = int
    
    def __init__(self, parameters: List):
        super().__init__(parameters)
    
    def to_sql_default(self, backend_context):
        """Default implementation (DuckDB)"""
        param_sql = self.param_sql_dict.get("value", "")
        return f"FLOOR({param_sql})"


class PowerFunction(ScalarFunction):
    """
    Returns the value of a number raised to the power of another number.
    
    Example:
        lambda x: power(x.base, 2)  # Square the value
    """
    function_name = "power"
    parameter_types = [("base", Union[int, float]), ("exponent", Union[int, float])]
    return_type = float
    
    def __init__(self, parameters: List):
        super().__init__(parameters)
    
    def to_sql_default(self, backend_context):
        """Default implementation (DuckDB)"""
        base_sql = self.param_sql_dict.get("base", "")
        exponent_sql = self.param_sql_dict.get("exponent", "")
        return f"POWER({base_sql}, {exponent_sql})"


class SqrtFunction(ScalarFunction):
    """
    Returns the square root of a number.
    
    Example:
        lambda x: sqrt(x.value)
    """
    function_name = "sqrt"
    parameter_types = [("value", Union[int, float])]
    return_type = float
    
    def __init__(self, parameters: List):
        super().__init__(parameters)
    
    def to_sql_default(self, backend_context):
        """Default implementation (DuckDB)"""
        param_sql = self.param_sql_dict.get("value", "")
        return f"SQRT({param_sql})"


class ModFunction(ScalarFunction):
    """
    Returns the remainder of a division operation.
    
    Example:
        lambda x: mod(x.value, 3)  # Get remainder when divided by 3
    """
    function_name = "mod"
    parameter_types = [("dividend", int), ("divisor", int)]
    return_type = int
    
    def __init__(self, parameters: List):
        super().__init__(parameters)
    
    def to_sql_default(self, backend_context):
        """Default implementation (DuckDB)"""
        dividend_sql = self.param_sql_dict.get("dividend", "")
        divisor_sql = self.param_sql_dict.get("divisor", "")
        return f"MOD({dividend_sql}, {divisor_sql})"
    
    def to_sql_postgres(self, backend_context):
        """PostgreSQL-specific implementation"""
        dividend_sql = self.param_sql_dict.get("dividend", "")
        divisor_sql = self.param_sql_dict.get("divisor", "")
        return f"({dividend_sql} % {divisor_sql})"
