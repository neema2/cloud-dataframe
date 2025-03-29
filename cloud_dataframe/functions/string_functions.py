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
        param_sql = self.parameters[0].to_sql(backend_context)
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
        param_sql = self.parameters[0].to_sql(backend_context)
        return f"LOWER({param_sql})"


class ConcatFunction(ScalarFunction):
    """
    Concatenates two or more strings.
    
    Example:
        lambda x: concat(x.first_name, ' ', x.last_name)
    """
    function_name = "concat"
    parameter_types = []  # Variable number of parameters
    return_type = str
    
    def __init__(self, parameters: List):
        from cloud_dataframe.type_system.column import Expression
        super(Expression, self).__init__()
        self.parameters = parameters
    
    def to_sql_default(self, backend_context):
        """Default implementation (DuckDB)"""
        params_sql = [p.to_sql(backend_context) for p in self.parameters]
        return " || ".join(params_sql)
    
    def to_sql_postgres(self, backend_context):
        """PostgreSQL-specific implementation"""
        params_sql = [p.to_sql(backend_context) for p in self.parameters]
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
        text_sql = self.parameters[0].to_sql(backend_context)
        start_sql = self.parameters[1].to_sql(backend_context)
        length_sql = self.parameters[2].to_sql(backend_context)
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
        param_sql = self.parameters[0].to_sql(backend_context)
        return f"LENGTH({param_sql})"
    
    def to_sql_postgres(self, backend_context):
        """PostgreSQL-specific implementation"""
        param_sql = self.parameters[0].to_sql(backend_context)
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
        text_sql = self.parameters[0].to_sql(backend_context)
        search_sql = self.parameters[1].to_sql(backend_context)
        replacement_sql = self.parameters[2].to_sql(backend_context)
        return f"REPLACE({text_sql}, {search_sql}, {replacement_sql})"
