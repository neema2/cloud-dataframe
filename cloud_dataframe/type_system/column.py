"""
Column and expression types for the cloud-dataframe type system.

This module defines the core column and expression classes used to build
type-safe dataframe operations.
"""
from __future__ import annotations
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar, Union, Generic, cast
from dataclasses import dataclass, field

T = TypeVar('T')
R = TypeVar('R')


@dataclass
class Expression:
    """Base class for all expressions in the DataFrame DSL."""
    pass


@dataclass
class LiteralExpression(Expression):
    """Represents a literal value in an expression."""
    value: Any


@dataclass
class ColumnReference(Expression):
    """Reference to a column in a table."""
    name: str
    table_alias: Optional[str] = None


@dataclass
class FunctionExpression(Expression):
    """Function call expression."""
    function_name: str
    parameters: List[Expression] = field(default_factory=list)


@dataclass
class AggregateFunction(FunctionExpression):
    """Base class for aggregate functions."""
    pass


@dataclass
class CountFunction(AggregateFunction):
    """COUNT aggregate function."""
    distinct: bool = False


@dataclass
class SumFunction(AggregateFunction):
    """SUM aggregate function."""
    pass


@dataclass
class AvgFunction(AggregateFunction):
    """AVG aggregate function."""
    pass


@dataclass
class MinFunction(AggregateFunction):
    """MIN aggregate function."""
    pass


@dataclass
class MaxFunction(AggregateFunction):
    """MAX aggregate function."""
    pass


@dataclass
class Window:
    """Window specification for window functions."""
    partition_by: List[Expression] = field(default_factory=list)
    order_by: List[Any] = field(default_factory=list)  # Will be OrderByClause
    frame: Optional[Any] = None  # Will be Frame


@dataclass
class WindowFunction(FunctionExpression):
    """Base class for window functions."""
    window: Window = field(default_factory=Window)


@dataclass
class RowNumberFunction(WindowFunction):
    """ROW_NUMBER window function."""
    pass


@dataclass
class RankFunction(WindowFunction):
    """RANK window function."""
    pass


@dataclass
class DenseRankFunction(WindowFunction):
    """DENSE_RANK window function."""
    pass


@dataclass
class Column:
    """
    Represents a column in a DataFrame.
    
    A column has a name, an expression that defines its value,
    and an optional alias.
    """
    name: str
    expression: Expression
    alias: Optional[str] = None


# Helper functions for creating expressions

def col(name: str, table_alias: Optional[str] = None) -> ColumnReference:
    """
    Create a column reference.
    
    Args:
        name: The name of the column
        table_alias: Optional table alias
        
    Returns:
        A ColumnReference expression
    """
    return ColumnReference(name=name, table_alias=table_alias)


def literal(value: Any) -> LiteralExpression:
    """
    Create a literal expression.
    
    Args:
        value: The literal value
        
    Returns:
        A LiteralExpression
    """
    return LiteralExpression(value=value)


def as_column(expr: Expression, alias: str) -> Column:
    """
    Create a column with an alias.
    
    Args:
        expr: The expression for the column
        alias: The alias for the column
        
    Returns:
        A Column with the specified alias
    """
    return Column(name=alias, expression=expr, alias=alias)


# Aggregate functions

def count(expr: Union[str, Expression], distinct: bool = False) -> CountFunction:
    """
    Create a COUNT aggregate function.
    
    Args:
        expr: The expression to count
        distinct: Whether to count distinct values
        
    Returns:
        A CountFunction expression
    """
    if isinstance(expr, str):
        expr = col(expr)
    
    return CountFunction(
        function_name="COUNT",
        parameters=[expr],
        distinct=distinct
    )


def sum(expr: Union[str, Expression]) -> SumFunction:
    """
    Create a SUM aggregate function.
    
    Args:
        expr: The expression to sum
        
    Returns:
        A SumFunction expression
    """
    if isinstance(expr, str):
        expr = col(expr)
    
    return SumFunction(
        function_name="SUM",
        parameters=[expr]
    )


def avg(expr: Union[str, Expression]) -> AvgFunction:
    """
    Create an AVG aggregate function.
    
    Args:
        expr: The expression to average
        
    Returns:
        An AvgFunction expression
    """
    if isinstance(expr, str):
        expr = col(expr)
    
    return AvgFunction(
        function_name="AVG",
        parameters=[expr]
    )


def min(expr: Union[str, Expression]) -> MinFunction:
    """
    Create a MIN aggregate function.
    
    Args:
        expr: The expression to find the minimum of
        
    Returns:
        A MinFunction expression
    """
    if isinstance(expr, str):
        expr = col(expr)
    
    return MinFunction(
        function_name="MIN",
        parameters=[expr]
    )


def max(expr: Union[str, Expression]) -> MaxFunction:
    """
    Create a MAX aggregate function.
    
    Args:
        expr: The expression to find the maximum of
        
    Returns:
        A MaxFunction expression
    """
    if isinstance(expr, str):
        expr = col(expr)
    
    return MaxFunction(
        function_name="MAX",
        parameters=[expr]
    )


# Window functions

def row_number() -> RowNumberFunction:
    """
    Create a ROW_NUMBER window function.
    
    Returns:
        A RowNumberFunction expression
    """
    return RowNumberFunction(function_name="ROW_NUMBER")


def rank() -> RankFunction:
    """
    Create a RANK window function.
    
    Returns:
        A RankFunction expression
    """
    return RankFunction(function_name="RANK")


def dense_rank() -> DenseRankFunction:
    """
    Create a DENSE_RANK window function.
    
    Returns:
        A DenseRankFunction expression
    """
    return DenseRankFunction(function_name="DENSE_RANK")


def over(func: WindowFunction, 
         partition_by: Optional[Union[List[Union[Expression, Callable]], Callable]] = None,
         order_by: Optional[Union[List[Union[Expression, Callable]], Callable]] = None) -> WindowFunction:
    """
    Apply a window specification to a window function.
    
    Args:
        func: The window function
        partition_by: Optional list of expressions or lambda function to partition by
        order_by: Optional list of expressions or lambda function to order by
        
    Returns:
        The window function with the window specification applied
    """
    from ..utils.lambda_parser import parse_lambda
    
    window = Window()
    
    if partition_by:
        if callable(partition_by):
            # Handle lambda function
            parsed_expressions = parse_lambda(partition_by)
            if isinstance(parsed_expressions, list):
                window.partition_by = parsed_expressions
            else:
                window.partition_by = [parsed_expressions]
        else:
            # Handle list of expressions
            window.partition_by = [
                p if not isinstance(p, str) else col(p)
                for p in partition_by
            ]
    
    if order_by:
        if callable(order_by):
            # Handle lambda function
            parsed_expressions = parse_lambda(order_by)
            if isinstance(parsed_expressions, list):
                window.order_by = parsed_expressions
            else:
                window.order_by = [parsed_expressions]
        else:
            # Handle list of expressions
            window.order_by = order_by
    
    func.window = window
    return func
