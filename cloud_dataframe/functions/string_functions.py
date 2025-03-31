"""
String functions for the DataFrame DSL.

This module provides implementations of string manipulation functions
that can work across different SQL backends.
"""
from typing import List

from cloud_dataframe.functions.base import ScalarFunction


class UpperFunction(ScalarFunction):
    """
    Converts a string to uppercase.
    
    Example:
        lambda x: upper(x.name)
    """
    function_name = "upper"
    parameter_types = [("text", str)]
    return_type = str
    
    def __init__(self, parameters: List):
        super().__init__(parameters)
    
    def to_sql_default(self, backend_context):
        """Default implementation (DuckDB)"""
        param_sql = self.param_sql_dict.get("text", "")
        return f"UPPER({param_sql})"
    


class LowerFunction(ScalarFunction):
    """
    Converts a string to lowercase.
    
    Example:
        lambda x: lower(x.name)
    """
    function_name = "lower"
    parameter_types = [("text", str)]
    return_type = str
    
    def __init__(self, parameters: List):
        super().__init__(parameters)
    
    def to_sql_default(self, backend_context):
        """Default implementation (DuckDB)"""
        param_sql = self.param_sql_dict.get("text", "")
        return f"LOWER({param_sql})"


class ConcatFunction(ScalarFunction):
    """
    Concatenates two or more strings.
    
    Example:
        lambda x: concat(x.first_name, ' ', x.last_name)
    """
    function_name = "concat"
    parameter_types = [("param1", str), ("param2", str)]  # Minimum required parameters
    return_type = str
    accepts_variable_args = True  # Allow variable number of arguments
    
    def __init__(self, parameters: List):
        super().__init__(parameters)
    
    def to_sql_default(self, backend_context):
        """Default implementation (DuckDB)"""
        params_sql = []
        for i in range(len(self.parameters)):
            param_key = f"param{i+1}"
            if param_key in self.param_sql_dict:
                params_sql.append(self.param_sql_dict[param_key])
            else:
                from ..backends.duckdb.sql_generator import _generate_expression
                params_sql.append(_generate_expression(self.parameters[i]))
        return " || ".join(params_sql)
    
    def to_sql_postgres(self, backend_context):
        """PostgreSQL-specific implementation"""
        params_sql = []
        for i in range(len(self.parameters)):
            param_key = f"param{i+1}"
            if param_key in self.param_sql_dict:
                params_sql.append(self.param_sql_dict[param_key])
            else:
                from ..backends.duckdb.sql_generator import _generate_expression
                params_sql.append(_generate_expression(self.parameters[i]))
        return f"CONCAT({', '.join(params_sql)})"


class SubstringFunction(ScalarFunction):
    """
    Extracts a substring from a string.
    
    Example:
        lambda x: substring(x.name, 1, 3)  # Extract 3 characters starting from position 1
    """
    function_name = "substring"
    parameter_types = [("text", str), ("start", int), ("length", int)]
    return_type = str
    
    def __init__(self, parameters: List):
        super().__init__(parameters)
    
    def to_sql_default(self, backend_context):
        """Default implementation (DuckDB)"""
        text_sql = self.param_sql_dict.get("text", "")
        start_sql = self.param_sql_dict.get("start", "")
        length_sql = self.param_sql_dict.get("length", "")
        return f"SUBSTRING({text_sql}, {start_sql}, {length_sql})"


class LengthFunction(ScalarFunction):
    """
    Returns the length of a string.
    
    Example:
        lambda x: length(x.name)
    """
    function_name = "length"
    parameter_types = [("text", str)]
    return_type = int
    
    def __init__(self, parameters: List):
        super().__init__(parameters)
    
    def to_sql_default(self, backend_context):
        """Default implementation (DuckDB)"""
        param_sql = self.param_sql_dict.get("text", "")
        return f"LENGTH({param_sql})"
    
    def to_sql_postgres(self, backend_context):
        """PostgreSQL-specific implementation"""
        param_sql = self.param_sql_dict.get("text", "")
        return f"CHAR_LENGTH({param_sql})"


class ReplaceFunction(ScalarFunction):
    """
    Replaces all occurrences of a substring with another substring.
    
    Example:
        lambda x: replace(x.text, 'old', 'new')
    """
    function_name = "replace"
    parameter_types = [("text", str), ("search", str), ("replacement", str)]
    return_type = str
    
    def __init__(self, parameters: List):
        super().__init__(parameters)
    
    def to_sql_default(self, backend_context):
        """Default implementation (DuckDB)"""
        text_sql = self.param_sql_dict.get("text", "")
        search_sql = self.param_sql_dict.get("search", "")
        replacement_sql = self.param_sql_dict.get("replacement", "")
        return f"REPLACE({text_sql}, {search_sql}, {replacement_sql})"
