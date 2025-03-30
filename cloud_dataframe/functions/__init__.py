"""
Function registry for the cloud-dataframe DSL.

This module provides a registry for scalar functions across different database backends.
"""
from typing import Dict, Type, Any, Optional, Union, Callable

from ..type_system.column import ScalarFunction, Expression

FUNCTION_REGISTRY: Dict[str, Type[ScalarFunction]] = {}

def register_function(function_class: Type[ScalarFunction]) -> Type[ScalarFunction]:
    """
    Register a function class in the registry.
    
    Args:
        function_class: The function class to register
        
    Returns:
        The function class (for decorator usage)
    """
    function_name = function_class.__name__.upper()
    FUNCTION_REGISTRY[function_name] = function_class
    return function_class

def get_function_class(function_name: str) -> Optional[Type[ScalarFunction]]:
    """
    Get the function class for a function name.
    
    Args:
        function_name: The name of the function to get
        
    Returns:
        The function class, or None if not found
    """
    function_name = function_name.upper()
    return FUNCTION_REGISTRY.get(function_name)

from .date_functions import date_diff
from .string_functions import concat, upper, lower
from .math_functions import round, ceil, floor
from .time_functions import date_part, date_trunc

__all__ = [
    'register_function',
    'get_function_class',
    'date_diff',
    'concat',
    'upper',
    'lower',
    'round',
    'ceil',
    'floor',
    'date_part',
    'date_trunc',
]
