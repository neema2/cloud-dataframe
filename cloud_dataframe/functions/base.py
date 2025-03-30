"""
Base classes for SQL scalar functions in the DataFrame DSL.

This module provides the foundation for implementing SQL scalar functions
that can work across different SQL backends.
"""
from typing import Any, Dict, List, Optional, Tuple, Type, Union

from cloud_dataframe.type_system.column import Expression


class ScalarFunction(Expression):
    """
    Base class for all scalar functions in the DataFrame DSL.
    
    This class provides the foundation for implementing SQL scalar functions
    that can work across different SQL backends. Each subclass should define:
    
    - function_name: The name of the function as it appears in SQL
    - parameter_types: A list of tuples (param_name, param_type) defining the expected parameters
    - return_type: The type returned by the function
    
    Subclasses should implement generate_sql_default for the default (DuckDB) implementation
    and can optionally implement backend-specific methods (generate_sql_postgres, etc.)
    """
    
    function_name = None
    parameter_types = []  # List of (param_name, param_type) tuples
    return_type = None
    
    def __init__(self, parameters: List[Expression]):
        """
        Initialize a scalar function with the given parameters.
        
        Args:
            parameters: List of Expression objects representing the function parameters
        """
        super().__init__()
        self.parameters = parameters
        
        if self.parameter_types and len(parameters) != len(self.parameter_types):
            expected_count = len(self.parameter_types)
            actual_count = len(parameters)
            raise ValueError(
                f"Function '{self.function_name}' expects {expected_count} parameters, "
                f"but {actual_count} were provided."
            )
        
        self._sql_cache = {}
        
        self.parameters_sql = {}
    
    def _generate_param_sql(self, param_index, backend_context):
        """
        Generate SQL for a function parameter using the backend's SQL generator.
        
        Args:
            param_index: Index of the parameter in self.parameters
            backend_context: Context object containing backend-specific information
            
        Returns:
            SQL string representation of the parameter
        """
        backend = getattr(backend_context, 'backend', 'default')
        param_key = f"{param_index}_{backend}"
        
        if param_key not in self.parameters_sql:
            from ..backends.duckdb.sql_generator import _generate_expression
            self.parameters_sql[param_key] = _generate_expression(self.parameters[param_index])
            
        return self.parameters_sql[param_key]
    
    def generate_sql_default(self, backend_context):
        """
        Generate default SQL implementation (DuckDB).
        
        This method should be implemented by subclasses to provide the default
        SQL implementation for the function.
        
        Args:
            backend_context: Context object containing backend-specific information
            
        Returns:
            SQL string representation of the function
        """
        raise NotImplementedError(
            f"Function '{self.function_name}' does not implement generate_sql_default"
        )
    
    def to_sql_default(self, backend_context):
        """
        Default SQL implementation (DuckDB).
        
        This method uses the cached SQL if available, otherwise generates it.
        
        Args:
            backend_context: Context object containing backend-specific information
            
        Returns:
            SQL string representation of the function
        """
        backend = 'default'
        if backend not in self._sql_cache:
            self._sql_cache[backend] = self.generate_sql_default(backend_context)
        return self._sql_cache[backend]
    
    def to_sql(self, backend_context):
        """
        Generate SQL for the function based on the target backend.
        
        This method dispatches to the appropriate backend-specific implementation
        based on the backend specified in the context. If no backend-specific
        implementation is available, it falls back to the default implementation.
        
        Args:
            backend_context: Context object containing backend-specific information
            
        Returns:
            SQL string representation of the function
        """
        backend = getattr(backend_context, 'backend', 'default')
        method_name = f"to_sql_{backend}"
        
        if hasattr(self, method_name):
            return getattr(self, method_name)(backend_context)
        
        return self.to_sql_default(backend_context)


class FunctionNotSupportedError(Exception):
    """
    Exception raised when a function is not supported by a specific backend.
    """
    
    def __init__(self, function_name, backend):
        """
        Initialize the exception.
        
        Args:
            function_name: Name of the function that is not supported
            backend: Name of the backend that does not support the function
        """
        super().__init__(
            f"Function '{function_name}' is not supported by the '{backend}' backend"
        )
        self.function_name = function_name
        self.backend = backend
