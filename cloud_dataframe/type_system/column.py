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
    
    def as_column(self, alias: str) -> 'Column':
        """
        Create a column with this expression and the given alias.
        
        Args:
            alias: The alias for the column
            
        Returns:
            A Column with this expression and the specified alias
        """
        return Column(name=alias, expression=self, alias=alias)


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
class ScalarFunction(FunctionExpression):
    """Base class for scalar functions."""
    pass


@dataclass
class DateDiffFunction(ScalarFunction):
    """DATE_DIFF scalar function."""
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


def as_column(expr: Union[Expression, Callable], alias: str) -> Column:
    """
    Create a column with an alias.
    
    Args:
        expr: The expression for the column. Can be:
            - An Expression object
            - A lambda function that returns an expression (e.g., lambda x: x.column_name)
            - A lambda function with nested function calls (e.g., lambda x: sum(x.salary + x.bonus))
        alias: The alias for the column
        
    Returns:
        A Column with the specified alias
    """
    from ..utils.lambda_parser import parse_lambda
    
    if callable(expr) and not isinstance(expr, Expression):
        # Parse lambda function to get the expression
        parsed_expr = parse_lambda(expr)
    else:
        parsed_expr = expr
        
    return Column(name=alias, expression=parsed_expr, alias=alias)


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


def over(func: WindowFunction, 
         partition_by: Optional[Union[List[Expression], Callable]] = None,
         order_by: Optional[Union[List[Expression], Callable]] = None) -> WindowFunction:
    """
    Apply a window specification to a window function.
    
    Args:
        func: The window function
        partition_by: Optional list of expressions or lambda function to partition by
            Can be a lambda that returns:
            - A single column reference (lambda x: x.column)
            - A list of column references (lambda x: [x.col1, x.col2])
        order_by: Optional list of expressions or lambda function to order by
            Can be a lambda that returns:
            - A single column reference (lambda x: x.column)
            - A list of column references (lambda x: [x.col1, x.col2])
            - A list with tuples specifying sort direction (lambda x: [(x.col1, 'DESC'), (x.col2, 'ASC')])
        
    Returns:
        The window function with the window specification applied
    """
    from ..utils.lambda_parser import parse_lambda
    from ..core.dataframe import OrderByClause, SortDirection
    
    window = Window()
    partition_by_list = []
    order_by_list = []
    
    if partition_by:
        if callable(partition_by):
            # Handle lambda function
            parsed_expressions = parse_lambda(partition_by)
            if isinstance(parsed_expressions, list):
                partition_by_list = parsed_expressions
            else:
                partition_by_list = [parsed_expressions]
        else:
            # Handle list of expressions (already Expression objects)
            partition_by_list = partition_by
    
    if order_by:
        if callable(order_by):
            # Handle lambda function
            parsed_expressions = parse_lambda(order_by)
            
            if isinstance(parsed_expressions, list):
                # Process array lambdas
                for item in parsed_expressions:
                    # Check if this is a tuple with sort direction
                    if isinstance(item, tuple) and len(item) == 2:
                        col_expr, sort_dir = item
                        # Convert string sort direction to OrderByClause equivalent
                        dir_enum = SortDirection.DESC if isinstance(sort_dir, str) and sort_dir.upper() == 'DESC' else SortDirection.ASC
                        order_by_list.append(OrderByClause(expression=col_expr, direction=dir_enum))
                    else:
                        # Use default ASC ordering
                        order_by_list.append(OrderByClause(expression=item, direction=SortDirection.ASC))
            else:
                # Single expression with default ASC ordering
                order_by_list.append(OrderByClause(expression=parsed_expressions, direction=SortDirection.ASC))
        else:
            # Handle list of expressions (already OrderByClause objects)
            order_by_list = order_by
    
    # Set the window properties using setter methods
    window.set_partition_by(partition_by_list)
    window.set_order_by(order_by_list)
    
    func.window = window
    return func


# Scalar functions

def date_diff(expr1: Union[Callable, Expression], expr2: Union[Callable, Expression]) -> DateDiffFunction:
    """
    Create a DATE_DIFF scalar function.
    
    Args:
        expr1: First date expression (lambda function or Expression)
              Example: lambda x: x.start_date
        expr2: Second date expression (lambda function or Expression)
              Example: lambda x: x.end_date
        
    Returns:
        A DateDiffFunction expression
    """
    from ..utils.lambda_parser import parse_lambda
    
    if callable(expr1) and not isinstance(expr1, Expression):
        parsed_expr1 = parse_lambda(expr1)
    else:
        parsed_expr1 = expr1
    
    if callable(expr2) and not isinstance(expr2, Expression):
        parsed_expr2 = parse_lambda(expr2)
    else:
        parsed_expr2 = expr2
    
    # Store column names for SQL generation
    col_names = []
    if isinstance(parsed_expr1, ColumnReference):
        col_names.append(parsed_expr1.name)
    if isinstance(parsed_expr2, ColumnReference):
        col_names.append(parsed_expr2.name)
    
    func = DateDiffFunction(
        function_name="DATE_DIFF",
        parameters=[parsed_expr1, parsed_expr2]
    )
    
    # Store column names as an attribute for SQL generation
    if col_names:
        func.column_names = col_names
    
    return func

@dataclass
class DateDiffFunction(ScalarFunction):
    """DATE_DIFF scalar function."""
    pass


def date_diff(expr1: Union[Callable, Expression], expr2: Union[Callable, Expression]) -> DateDiffFunction:
    """
    Create a DATE_DIFF scalar function.
    
    Args:
        expr1: First date expression
        expr2: Second date expression
        
    Returns:
        A DateDiffFunction expression
    """
    from ..utils.lambda_parser import parse_lambda
    
    if callable(expr1):
        parsed_expr1 = parse_lambda(expr1)
    else:
        parsed_expr1 = expr1
    
    if callable(expr2):
        parsed_expr2 = parse_lambda(expr2)
    else:
        parsed_expr2 = expr2
    
    return DateDiffFunction(
        function_name="DATE_DIFF",
        parameters=[parsed_expr1, parsed_expr2]
    )
