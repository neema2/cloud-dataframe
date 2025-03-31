"""
Function registry for the DataFrame DSL.

This module provides a registry for scalar functions that can be used
in the DataFrame DSL. The registry maps function names to their implementations
and provides methods for creating function instances.
"""
from typing import Dict, List, Type

from cloud_dataframe.functions.base import ScalarFunction
from cloud_dataframe.functions.string_functions import (
    UpperFunction,
    LowerFunction,
    ConcatFunction,
    SubstringFunction,
    LengthFunction,
    ReplaceFunction,
)
from cloud_dataframe.functions.date_functions import (
    DateDiffFunction,
    DatePartFunction,
    DateTruncFunction,
    CurrentDateFunction,
    DateAddFunction,
    DateSubFunction,
)
from cloud_dataframe.functions.numeric_functions import (
    AbsFunction,
    RoundFunction,
    CeilFunction,
    FloorFunction,
    PowerFunction,
    SqrtFunction,
    ModFunction,
)


class FunctionRegistry:
    """
    Registry for scalar functions in the DataFrame DSL.
    
    This class provides methods for registering and retrieving function
    implementations based on their names.
    """
    
    _functions: Dict[str, Type[ScalarFunction]] = {}
    
    @classmethod
    def register_function(cls, function_class: Type[ScalarFunction]) -> None:
        """
        Register a function class by its name.
        
        Args:
            function_class: The function class to register
        """
        if not function_class.function_name:
            raise ValueError("Function class must have a function_name attribute")
        
        cls._functions[function_class.function_name] = function_class
    
    @classmethod
    def get_function_class(cls, function_name: str) -> Type[ScalarFunction]:
        """
        Get the function class for a given name.
        
        Args:
            function_name: The name of the function to retrieve
            
        Returns:
            The function class for the given name, or None if not found
        """
        return cls._functions.get(function_name)
    
    @classmethod
    def create_function(cls, function_name: str, parameters: List) -> ScalarFunction:
        """
        Create a function instance for a given name and parameters.
        
        Args:
            function_name: The name of the function to create
            parameters: The parameters to pass to the function constructor
            
        Returns:
            A function instance, or None if the function is not registered
            
        Raises:
            ValueError: If the function is not registered
        """
        function_class = cls.get_function_class(function_name)
        if function_class:
            return function_class(parameters)
        
        raise ValueError(f"Function '{function_name}' is not registered")


def register_all_functions():
    """Register all available scalar functions with the registry."""
    FunctionRegistry.register_function(UpperFunction)
    FunctionRegistry.register_function(LowerFunction)
    FunctionRegistry.register_function(ConcatFunction)
    FunctionRegistry.register_function(SubstringFunction)
    FunctionRegistry.register_function(LengthFunction)
    FunctionRegistry.register_function(ReplaceFunction)
    
    FunctionRegistry.register_function(DateDiffFunction)
    FunctionRegistry.register_function(DatePartFunction)
    FunctionRegistry.register_function(DateTruncFunction)
    FunctionRegistry.register_function(CurrentDateFunction)
    FunctionRegistry.register_function(DateAddFunction)
    FunctionRegistry.register_function(DateSubFunction)
    
    FunctionRegistry.register_function(AbsFunction)
    FunctionRegistry.register_function(RoundFunction)
    FunctionRegistry.register_function(CeilFunction)
    FunctionRegistry.register_function(FloorFunction)
    FunctionRegistry.register_function(PowerFunction)
    FunctionRegistry.register_function(SqrtFunction)
    FunctionRegistry.register_function(ModFunction)


register_all_functions()
