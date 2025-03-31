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
    

@dataclass
class LiteralExpression(Expression):
    """Represents a literal value in an expression."""
    value: Any


@dataclass
class ColumnReference(Expression):
    """Reference to a column in a table."""
    name: str
    table_alias: Optional[str] = None
    table_name: Optional[str] = None


@dataclass
class FunctionExpression(Expression):
    """Function call expression."""
    function_name: str
    parameters: List[Expression] = field(default_factory=list)


@dataclass
class ScalarFunction(FunctionExpression):
    """Base class for scalar functions."""
    pass



@dataclass
class AggregateFunction(FunctionExpression):
    """Base class for aggregate functions."""
    pass


@dataclass
class CountFunction(AggregateFunction):
    """COUNT aggregate function."""
    distinct: bool = False
    
    def __post_init__(self):
        # Convert string "*" to a LiteralExpression for COUNT(*)
        if self.parameters and self.parameters[0] == "*":
            self.parameters = [LiteralExpression(value="*")]


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
class Frame:
    """Frame specification for window functions."""
    type: str  # 'ROWS' or 'RANGE'
    start: Any = None  # Start boundary: number or UNBOUNDED
    end: Any = None  # End boundary: number or UNBOUNDED
    is_unbounded_start: bool = False
    is_unbounded_end: bool = False
    
    def __post_init__(self):
        # Validate frame type
        if self.type.upper() not in ('ROWS', 'RANGE'):
            raise ValueError(f"Frame type must be 'ROWS' or 'RANGE', got '{self.type}'")


@dataclass
class Window:
    """Window specification for window functions."""
    partition_by: List[Expression] = field(default_factory=list)
    order_by: List[Any] = field(default_factory=list)  # Will be OrderByClause
    frame: Optional[Any] = None  # Will be Frame
    
    def set_partition_by(self, expressions: List[Any]) -> None:
        """Set the partition by expressions."""
        self.partition_by = expressions
        
    def set_order_by(self, clauses: List[Any]) -> None:
        """Set the order by clauses."""
        self.order_by = clauses
        
    def set_frame(self, frame: Frame) -> None:
        """Set the frame specification."""
        self.frame = frame


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

def col(name: str, table_alias: Optional[str] = None, table_name: Optional[str] = None) -> ColumnReference:
    """
    Create a column reference.
    
    Args:
        name: The name of the column
        table_alias: Optional table alias
        table_name: Optional table name
        
    Returns:
        A ColumnReference expression
    """
    return ColumnReference(name=name, table_alias=table_alias, table_name=table_name)


def literal(value: Any) -> LiteralExpression:
    """
    Create a literal expression.
    
    Args:
        value: The literal value
        
    Returns:
        A LiteralExpression
    """
    return LiteralExpression(value=value)




# Aggregate functions

def count(expr: Union[Callable, Expression, None] = None, distinct: bool = False) -> CountFunction:
    """
    Create a COUNT aggregate function.
    
    Args:
        expr: Expression to count, can be a column reference or other expression
              If None, COUNT(1) will be used
              For backward compatibility, can also be a lambda function
        distinct: Whether to count distinct values
        
    Returns:
        A CountFunction expression
    """
    from ..utils.lambda_parser import parse_lambda
    
    # Handle COUNT(*) special case - convert to COUNT(1)
    if expr is None:
        # Create a special marker for COUNT(1)
        return CountFunction(
            function_name="COUNT",
            parameters=[LiteralExpression(value=1)],
            distinct=distinct
        )
    
    # For backward compatibility, handle lambda functions
    if callable(expr) and not isinstance(expr, Expression):
        parsed_expr = parse_lambda(expr)
        return CountFunction(
            function_name="COUNT",
            parameters=[parsed_expr],
            distinct=distinct
        )
    
    # Handle direct expression
    return CountFunction(
        function_name="COUNT",
        parameters=[expr],
        distinct=distinct
    )


def sum(expr: Union[Callable, Expression]) -> SumFunction:
    """
    Create a SUM aggregate function.
    
    Args:
        expr: Expression to sum
              Examples: x.salary, x.salary - x.tax
              For backward compatibility, can also be a lambda function
        
    Returns:
        A SumFunction expression
    """
    from ..utils.lambda_parser import parse_lambda
    
    # For backward compatibility, handle lambda functions
    if callable(expr) and not isinstance(expr, Expression):
        parsed_expr = parse_lambda(expr)
        return SumFunction(
            function_name="SUM",
            parameters=[parsed_expr]
        )
    
    # Handle direct expression
    return SumFunction(
        function_name="SUM",
        parameters=[expr]
    )


def avg(expr: Union[Callable, Expression]) -> AvgFunction:
    """
    Create an AVG aggregate function.
    
    Args:
        expr: Expression to average
              Examples: x.salary, x.revenue / x.count
              For backward compatibility, can also be a lambda function
        
    Returns:
        An AvgFunction expression
    """
    from ..utils.lambda_parser import parse_lambda
    
    # For backward compatibility, handle lambda functions
    if callable(expr) and not isinstance(expr, Expression):
        parsed_expr = parse_lambda(expr)
        return AvgFunction(
            function_name="AVG",
            parameters=[parsed_expr]
        )
    
    # Handle direct expression
    return AvgFunction(
        function_name="AVG",
        parameters=[expr]
    )


def min(expr: Union[Callable, Expression]) -> MinFunction:
    """
    Create a MIN aggregate function.
    
    Args:
        expr: Expression to find the minimum of
              Examples: x.salary, x.price - x.discount
              For backward compatibility, can also be a lambda function
        
    Returns:
        A MinFunction expression
    """
    from ..utils.lambda_parser import parse_lambda
    
    # For backward compatibility, handle lambda functions
    if callable(expr) and not isinstance(expr, Expression):
        parsed_expr = parse_lambda(expr)
        return MinFunction(
            function_name="MIN",
            parameters=[parsed_expr]
        )
    
    # Handle direct expression
    return MinFunction(
        function_name="MIN",
        parameters=[expr]
    )


def max(expr: Union[Callable, Expression]) -> MaxFunction:
    """
    Create a MAX aggregate function.
    
    Args:
        expr: Expression to find the maximum of
              Examples: x.salary, x.price * (1 + x.tax_rate)
              For backward compatibility, can also be a lambda function
        
    Returns:
        A MaxFunction expression
    """
    from ..utils.lambda_parser import parse_lambda
    
    # For backward compatibility, handle lambda functions
    if callable(expr) and not isinstance(expr, Expression):
        parsed_expr = parse_lambda(expr)
        return MaxFunction(
            function_name="MAX",
            parameters=[parsed_expr]
        )
    
    # Handle direct expression
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





def unbounded() -> str:
    """
    Create an UNBOUNDED boundary for window frames.
    
    Returns:
        A string representing UNBOUNDED
    """
    return "UNBOUNDED"


def row(start: Union[int, str] = 0, end: Union[int, str] = 0) -> Frame:
    """
    Create a ROWS frame specification for window functions.
    
    Args:
        start: Start boundary (number of rows preceding, 0 for current row, or unbounded())
        end: End boundary (number of rows following, 0 for current row, or unbounded())
        
    Returns:
        A Frame object with ROWS type
    """
    is_unbounded_start = isinstance(start, str) and start == "UNBOUNDED"
    is_unbounded_end = isinstance(end, str) and end == "UNBOUNDED"
    
    return Frame(type="ROWS", start=start, end=end, 
                is_unbounded_start=is_unbounded_start, 
                is_unbounded_end=is_unbounded_end)


def range(start: Union[int, str] = 0, end: Union[int, str] = 0) -> Frame:
    """
    Create a RANGE frame specification for window functions.
    
    Args:
        start: Start boundary (range value preceding, 0 for current row, or unbounded())
        end: End boundary (range value following, 0 for current row, or unbounded())
        
    Returns:
        A Frame object with RANGE type
    """
    is_unbounded_start = isinstance(start, str) and start == "UNBOUNDED"
    is_unbounded_end = isinstance(end, str) and end == "UNBOUNDED"
    
    return Frame(type="RANGE", start=start, end=end, 
                is_unbounded_start=is_unbounded_start, 
                is_unbounded_end=is_unbounded_end)


# Scalar functions




def window(func: Optional[FunctionExpression] = None,
           partition: Optional[Union[List[Expression], Expression]] = None,
           order_by: Optional[Union[List[Expression], Expression]] = None,
           frame: Optional[Frame] = None) -> WindowFunction:
    """
    Create a window function specification.
    
    Args:
        func: Optional window function to apply (must be an Expression, not a lambda)
        partition: Optional list of expressions to partition by (must be Expression objects, not lambdas)
        order_by: Optional list of expressions to order by (must be Expression objects, not lambdas)
        frame: Optional frame specification created with row() or range() functions
        
    Returns:
        A WindowFunction with the window specification applied
    """
    from ..core.dataframe import OrderByClause, Sort
    
    window_obj = Window()
    partition_by_list = []
    order_by_list = []
    window_func = None
    
    if func is not None:
        if isinstance(func, FunctionExpression):
            window_func = WindowFunction(function_name=func.function_name, parameters=func.parameters)
        else:
            raise ValueError(f"Not a FunctionExpression{str(func)} {str(type(func))}")
    else:
        window_func = WindowFunction(function_name="WINDOW")
    
    if partition is not None:
        if isinstance(partition, list):
            # Handle list of expressions (already Expression objects)
            partition_by_list = partition
        else:
            partition_by_list = [partition]
    
    if order_by is not None:
        if isinstance(order_by, list):
            for item in order_by:
                if isinstance(item, OrderByClause):
                    order_by_list.append(item)
                elif isinstance(item, tuple) and len(item) == 2:
                    col_expr, sort_dir = item
                    # Convert string sort direction to OrderByClause equivalent
                    dir_enum = Sort.DESC if sort_dir == Sort.DESC or (isinstance(sort_dir, str) and sort_dir.upper() == 'DESC') else Sort.ASC
                    order_by_list.append(OrderByClause(expression=col_expr, direction=dir_enum))
                else:
                    # Use default ASC ordering
                    order_by_list.append(OrderByClause(expression=item, direction=Sort.ASC))

        elif isinstance(order_by, tuple) and len(order_by) == 2:
            col_expr, sort_dir = order_by
            dir_enum = Sort.DESC if sort_dir == Sort.DESC or (isinstance(sort_dir, str) and sort_dir.upper() == 'DESC') else Sort.ASC
            order_by_list.append(OrderByClause(expression=col_expr, direction=dir_enum))
        else:
            order_by_list.append(OrderByClause(expression=order_by, direction=Sort.ASC))
    
    window_obj.set_partition_by(partition_by_list)
    window_obj.set_order_by(order_by_list)
    
    # Add frame handling
    if frame:
        window_obj.set_frame(frame)
    
    window_func.window = window_obj
    return window_func
