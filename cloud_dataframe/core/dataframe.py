"""
Core DataFrame module for cloud-dataframe.

This module defines the base DataFrame class and core operations that can be
translated to SQL for execution against different database backends.
"""
from __future__ import annotations
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar, Union, Generic, cast
from enum import Enum
import inspect
from dataclasses import dataclass, field

from ..type_system.column import Column, ColumnReference, Expression, LiteralExpression
from ..type_system.schema import TableSchema, ColSpec

T = TypeVar('T')
R = TypeVar('R')


class JoinType(Enum):
    """Join types supported by the DataFrame DSL."""
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    FULL = "FULL"
    CROSS = "CROSS"


class SortDirection(Enum):
    """Sort directions for ORDER BY clauses."""
    ASC = "ASC"
    DESC = "DESC"


@dataclass
class OrderByClause:
    """Represents an ORDER BY clause in a SQL query."""
    expression: Expression
    direction: SortDirection = SortDirection.ASC


@dataclass
class GroupByClause:
    """Represents a GROUP BY clause in a SQL query."""
    columns: List[Expression] = field(default_factory=list)


@dataclass
class FilterCondition(Expression):
    """Base class for filter conditions."""
    pass


@dataclass
class BinaryOperation(FilterCondition):
    """Binary operation (e.g., =, >, <, etc.)."""
    left: Expression
    operator: str
    right: Expression


@dataclass
class UnaryOperation(FilterCondition):
    """Unary operation (e.g., NOT)."""
    operator: str
    expression: Expression


@dataclass
class DataSource:
    """Base class for data sources."""
    pass


@dataclass
class TableReference(DataSource):
    """Reference to a database table."""
    table_name: str
    schema: Optional[str] = None
    alias: Optional[str] = None
    table_schema: Optional[TableSchema] = None


@dataclass
class SubquerySource(DataSource):
    """Subquery as a data source."""
    dataframe: 'DataFrame'
    alias: str


@dataclass
class JoinOperation(DataSource):
    """Join operation between two data sources."""
    left: DataSource
    right: DataSource
    join_type: JoinType
    condition: FilterCondition


@dataclass
class CommonTableExpression:
    """Common Table Expression (CTE) for WITH clause."""
    name: str
    query: Union['DataFrame', str]
    columns: List[str] = field(default_factory=list)
    is_recursive: bool = False


class DataFrame:
    """
    Core DataFrame class for modeling SQL operations.
    
    This class provides a fluent interface for building SQL queries
    that can be executed against different database backends.
    """
    
    def __init__(self):
        self.columns: List[Column] = []
        self.source: Optional[DataSource] = None
        self.filter_condition: Optional[FilterCondition] = None
        self.group_by_clause: Optional[GroupByClause] = None
        self.having: Optional[FilterCondition] = None
        self.order_by_clauses: List[OrderByClause] = []
        self.limit_value: Optional[int] = None
        self.offset_value: Optional[int] = None
        self.distinct: bool = False
        self.ctes: List[CommonTableExpression] = []
    
    @classmethod
    def create_select(cls, *columns: Column) -> 'DataFrame':
        """
        Create a new DataFrame with the specified columns.
        
        Args:
            *columns: The columns to select
            
        Returns:
            A new DataFrame instance
        """
        df = cls()
        df.columns = list(columns)
        return df
        
    def select(self, *columns: Column) -> 'DataFrame':
        """
        Select columns from this DataFrame.
        
        Args:
            *columns: The columns to select
            
        Returns:
            The DataFrame with the columns selected
        """
        self.columns = list(columns)
        return self
    
    @classmethod
    def from_(cls, table_name: str, schema: Optional[str] = None, alias: Optional[str] = None) -> 'DataFrame':
        """
        Create a new DataFrame from a database table.
        
        Args:
            table_name: The name of the table
            schema: Optional schema name
            alias: Optional table alias
            
        Returns:
            A new DataFrame instance
        """
        df = cls()
        df.source = TableReference(table_name=table_name, schema=schema, alias=alias)
        return df
    
    @classmethod
    def from_table_schema(cls, table_name: str, table_schema: TableSchema, alias: Optional[str] = None) -> 'DataFrame':
        """
        Create a new DataFrame from a table with a defined schema.
        
        Args:
            table_name: The name of the table
            table_schema: The schema definition for the table
            alias: Optional table alias
            
        Returns:
            A new DataFrame instance with type information
        """
        df = cls()
        df.source = TableReference(
            table_name=table_name, 
            table_schema=table_schema,
            alias=alias
        )
        return df
    
    def filter(self, condition: Union[FilterCondition, Callable[[Any], bool]]) -> 'DataFrame':
        """
        Filter the DataFrame based on a condition.
        
        Args:
            condition: A FilterCondition object or a lambda function
            
        Returns:
            The DataFrame with the filter applied
        """
        # If condition is a lambda, convert it to a FilterCondition
        if callable(condition) and not isinstance(condition, FilterCondition):
            # Extract the lambda's AST and convert to FilterCondition
            # This is a placeholder - actual implementation will be more complex
            self.filter_condition = self._lambda_to_filter_condition(condition)
        else:
            self.filter_condition = cast(FilterCondition, condition)
        
        return self
    
    def _lambda_to_filter_condition(self, lambda_func: Callable[[Any], bool]) -> FilterCondition:
        """
        Convert a lambda function to a FilterCondition.
        
        This is a complex operation that requires parsing the lambda's AST.
        For now, this is a placeholder that will be implemented later.
        
        Args:
            lambda_func: The lambda function to convert
            
        Returns:
            A FilterCondition representing the lambda
        """
        # This is a placeholder - actual implementation will be more complex
        # and will involve parsing the lambda's AST
        return BinaryOperation(
            left=LiteralExpression(value=True),
            operator="=",
            right=LiteralExpression(value=True)
        )
    
    def group_by(self, *columns: Union[str, Expression, ColSpec]) -> 'DataFrame':
        """
        Group the DataFrame by the specified columns.
        
        Args:
            *columns: The columns to group by
            
        Returns:
            The DataFrame with the grouping applied
        """
        expressions = []
        for col in columns:
            if isinstance(col, str):
                expressions.append(ColumnReference(col))
            elif isinstance(col, ColSpec):
                expressions.append(ColumnReference(col.name))
            else:
                expressions.append(col)
        
        self.group_by_clause = GroupByClause(columns=expressions)
        return self
    
    def order_by(self, *clauses: Union[OrderByClause, Expression, str, ColSpec], 
                 desc: bool = False) -> 'DataFrame':
        """
        Order the DataFrame by the specified columns.
        
        Args:
            *clauses: The columns or OrderByClauses to order by
            desc: Whether to sort in descending order (if not using OrderByClause)
            
        Returns:
            The DataFrame with the ordering applied
        """
        direction = SortDirection.DESC if desc else SortDirection.ASC
        
        for clause in clauses:
            if isinstance(clause, OrderByClause):
                self.order_by_clauses.append(clause)
            elif isinstance(clause, str):
                self.order_by_clauses.append(OrderByClause(
                    expression=ColumnReference(clause),
                    direction=direction
                ))
            elif isinstance(clause, ColSpec):
                self.order_by_clauses.append(OrderByClause(
                    expression=ColumnReference(clause.name),
                    direction=direction
                ))
            else:
                self.order_by_clauses.append(OrderByClause(
                    expression=clause,
                    direction=direction
                ))
        
        return self
    
    def limit(self, limit: int) -> 'DataFrame':
        """
        Limit the number of rows returned.
        
        Args:
            limit: The maximum number of rows to return
            
        Returns:
            The DataFrame with the limit applied
        """
        self.limit_value = limit
        return self
    
    def offset(self, offset: int) -> 'DataFrame':
        """
        Skip the specified number of rows.
        
        Args:
            offset: The number of rows to skip
            
        Returns:
            The DataFrame with the offset applied
        """
        self.offset_value = offset
        return self
    
    def distinct_rows(self) -> 'DataFrame':
        """
        Make the query return distinct rows.
        
        Returns:
            The DataFrame with DISTINCT applied
        """
        self.distinct = True
        return self
    
    def with_cte(self, name: str, query: Union['DataFrame', str], 
                 columns: Optional[List[str]] = None, is_recursive: bool = False) -> 'DataFrame':
        """
        Add a Common Table Expression (CTE) to the query.
        
        Args:
            name: The name of the CTE
            query: The DataFrame or SQL string for the CTE
            columns: Optional column names for the CTE
            is_recursive: Whether the CTE is recursive
            
        Returns:
            The DataFrame with the CTE added
        """
        self.ctes.append(CommonTableExpression(
            name=name,
            query=query,
            columns=columns or [],
            is_recursive=is_recursive
        ))
        return self
    
    def join(self, right: Union['DataFrame', TableReference], 
             condition: FilterCondition, join_type: JoinType = JoinType.INNER) -> 'DataFrame':
        """
        Join this DataFrame with another DataFrame or table.
        
        Args:
            right: The DataFrame or table to join with
            condition: The join condition
            join_type: The type of join to perform
            
        Returns:
            A new DataFrame representing the join
        """
        if self.source is None:
            raise ValueError("Cannot join a DataFrame without a source")
        
        right_source = right if isinstance(right, DataSource) else SubquerySource(
            dataframe=right,
            alias=f"subquery_{len(self.ctes)}"
        )
        
        result = DataFrame()
        result.source = JoinOperation(
            left=self.source,
            right=right_source,
            join_type=join_type,
            condition=condition
        )
        
        # Combine columns from both sides
        # This is a simplification - in reality, we'd need to handle column name conflicts
        result.columns = self.columns.copy()
        if isinstance(right, DataFrame):
            result.columns.extend(right.columns)
        
        return result
    
    def left_join(self, right: Union['DataFrame', TableReference], 
                  condition: FilterCondition) -> 'DataFrame':
        """
        Perform a LEFT JOIN with another DataFrame or table.
        
        Args:
            right: The DataFrame or table to join with
            condition: The join condition
            
        Returns:
            A new DataFrame representing the join
        """
        return self.join(right, condition, JoinType.LEFT)
    
    def right_join(self, right: Union['DataFrame', TableReference], 
                   condition: FilterCondition) -> 'DataFrame':
        """
        Perform a RIGHT JOIN with another DataFrame or table.
        
        Args:
            right: The DataFrame or table to join with
            condition: The join condition
            
        Returns:
            A new DataFrame representing the join
        """
        return self.join(right, condition, JoinType.RIGHT)
    
    def full_join(self, right: Union['DataFrame', TableReference], 
                  condition: FilterCondition) -> 'DataFrame':
        """
        Perform a FULL JOIN with another DataFrame or table.
        
        Args:
            right: The DataFrame or table to join with
            condition: The join condition
            
        Returns:
            A new DataFrame representing the join
        """
        return self.join(right, condition, JoinType.FULL)
    
    def cross_join(self, right: Union['DataFrame', TableReference]) -> 'DataFrame':
        """
        Perform a CROSS JOIN with another DataFrame or table.
        
        Args:
            right: The DataFrame or table to join with
            
        Returns:
            A new DataFrame representing the join
        """
        # For CROSS JOIN, we use a dummy condition that's always true
        condition = BinaryOperation(
            left=LiteralExpression(value=True),
            operator="=",
            right=LiteralExpression(value=True)
        )
        return self.join(right, condition, JoinType.CROSS)
    
    def to_sql(self, dialect: str = "duckdb") -> str:
        """
        Generate SQL for the specified dialect.
        
        Args:
            dialect: The SQL dialect to generate (default: "duckdb")
            
        Returns:
            The generated SQL string
        """
        # Use the backend registry to get the appropriate SQL generator
        from ..backends import get_sql_generator
        
        try:
            generator = get_sql_generator(dialect)
            return generator(self)
        except ValueError as e:
            raise ValueError(f"Unsupported SQL dialect: {dialect}") from e
