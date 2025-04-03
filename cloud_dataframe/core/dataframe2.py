"""
Alternative implementation of DataFrame using a node-based chain of operations.

This module defines the base Dataframe class that uses nodes to represent operations
rather than accumulating operations in lists.
"""
from __future__ import annotations
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar, Union, Generic, Type, cast
from enum import Enum
import inspect
from dataclasses import dataclass, field

from ..type_system.column import Column, ColumnReference, Expression, LiteralExpression
from ..type_system.schema import TableSchema, ColSpec, create_dynamic_dataclass_from_schema
from .dataframe import JoinType, Sort, OrderByClause, FilterCondition, BinaryOperation, DataFrame, TableReference


@dataclass
class Node:
    """Base class for all operation nodes in the chain."""
    parent: Optional[Node] = None
    
    def to_dataframe(self) -> DataFrame:
        """
        Convert this node chain to a DataFrame instance.
        
        This is used to convert the node chain to a DataFrame before generating SQL.
        
        Returns:
            A DataFrame instance representing this node chain
        """
        df = DataFrame()
        self._apply_to_dataframe(df)
        return df
    
    def _apply_to_dataframe(self, df: DataFrame) -> None:
        """
        Apply this node's operation to the DataFrame.
        
        This method should be implemented by subclasses to apply their specific
        operation to the provided DataFrame instance.
        
        Args:
            df: The DataFrame to apply the operation to
        """
        if self.parent:
            self.parent._apply_to_dataframe(df)


@dataclass
class FromNode(Node):
    """Node representing a FROM operation."""
    table_name: str = field(default="")
    schema: Optional[str] = None
    alias: Optional[str] = None
    
    def _apply_to_dataframe(self, df: DataFrame) -> None:
        if self.parent:
            self.parent._apply_to_dataframe(df)
        
        df.source = TableReference(
            table_name=self.table_name,
            schema=self.schema,
            alias=self.alias or "x"  # Use 'x' as the default table alias
        )


@dataclass
class FromTableSchemaNode(Node):
    """Node representing a FROM operation with a table schema."""
    table_name: str = field(default="")
    table_schema: TableSchema = field(default_factory=lambda: TableSchema(name="default", columns={}))
    alias: Optional[str] = None
    
    def _apply_to_dataframe(self, df: DataFrame) -> None:
        if self.parent:
            self.parent._apply_to_dataframe(df)
        
        df.source = TableReference(
            table_name=self.table_name,
            table_schema=self.table_schema,
            alias=self.alias or "x"  # Use 'x' as the default table alias
        )
        
        df._table_class = create_dynamic_dataclass_from_schema(self.table_name, self.table_schema)


@dataclass
class SelectNode(Node):
    """Node representing a SELECT operation."""
    columns: List[Union[Column, Callable[[Any], Any]]] = field(default_factory=list)
    
    def _apply_to_dataframe(self, df: DataFrame) -> None:
        if self.parent:
            self.parent._apply_to_dataframe(df)
        
        df.select(*self.columns)


@dataclass
class FilterNode(Node):
    """Node representing a FILTER operation."""
    condition: Callable[[Any], bool] = field(default=lambda x: True)
    
    def _apply_to_dataframe(self, df: DataFrame) -> None:
        if self.parent:
            self.parent._apply_to_dataframe(df)
        
        df.filter(self.condition)


@dataclass
class GroupByNode(Node):
    """Node representing a GROUP BY operation."""
    columns: List[Union[Expression, Callable[[Any], Any]]] = field(default_factory=list)
    
    def _apply_to_dataframe(self, df: DataFrame) -> None:
        if self.parent:
            self.parent._apply_to_dataframe(df)
        
        df.group_by(*self.columns)


@dataclass
class OrderByNode(Node):
    """Node representing an ORDER BY operation."""
    lambda_func: Callable[[Any], Any] = field(default=lambda x: x)
    
    def _apply_to_dataframe(self, df: DataFrame) -> None:
        if self.parent:
            self.parent._apply_to_dataframe(df)
        
        df.order_by(self.lambda_func)


@dataclass
class LimitNode(Node):
    """Node representing a LIMIT operation."""
    limit: int = field(default=100)
    
    def _apply_to_dataframe(self, df: DataFrame) -> None:
        if self.parent:
            self.parent._apply_to_dataframe(df)
        
        df.limit(self.limit)


@dataclass
class OffsetNode(Node):
    """Node representing an OFFSET operation."""
    offset: int = field(default=0)
    
    def _apply_to_dataframe(self, df: DataFrame) -> None:
        if self.parent:
            self.parent._apply_to_dataframe(df)
        
        df.offset(self.offset)


@dataclass
class DistinctRowsNode(Node):
    """Node representing a DISTINCT operation."""
    
    def _apply_to_dataframe(self, df: DataFrame) -> None:
        if self.parent:
            self.parent._apply_to_dataframe(df)
        
        df.distinct_rows()


@dataclass
class HavingNode(Node):
    """Node representing a HAVING operation."""
    condition: Union[Callable[[Any], bool], Callable[[Any, Any], bool], FilterCondition, Expression] = field(default=lambda x: True)
    
    def _apply_to_dataframe(self, df: DataFrame) -> None:
        if self.parent:
            self.parent._apply_to_dataframe(df)
        
        df.having(self.condition)


@dataclass
class QualifyNode(Node):
    """Node representing a QUALIFY operation."""
    condition: Union[Callable[[Any], bool], Callable[[Any, Any], bool], FilterCondition, Expression] = field(default=lambda x: True)
    
    def _apply_to_dataframe(self, df: DataFrame) -> None:
        if self.parent:
            self.parent._apply_to_dataframe(df)
        
        df.qualify(self.condition)


@dataclass
class WithCteNode(Node):
    """Node representing a WITH CTE operation."""
    name: str = field(default="")
    query: Union[DataFrame, str] = field(default="")
    columns: Optional[List[str]] = None
    is_recursive: bool = False
    
    def _apply_to_dataframe(self, df: DataFrame) -> None:
        if self.parent:
            self.parent._apply_to_dataframe(df)
        
        df.with_cte(
            name=self.name,
            query=self.query,
            columns=self.columns,
            is_recursive=self.is_recursive
        )


@dataclass
class JoinNode(Node):
    """Node representing a JOIN operation."""
    right: Any = field(default=None)  # Union[DataFrame, 'Dataframe']
    condition: Callable[[Any, Any], bool] = field(default=lambda x, y: True)
    join_type: JoinType = JoinType.INNER
    
    def _apply_to_dataframe(self, df: DataFrame) -> None:
        if self.parent:
            self.parent._apply_to_dataframe(df)
        
        right_df = self.right
        if hasattr(self.right, '_root_node') and hasattr(self.right._root_node, 'to_dataframe'):
            right_df = self.right._root_node.to_dataframe()
            
        df.join(right_df, self.condition, self.join_type)


class Dataframe:
    """
    Alternative implementation of DataFrame using a node-based chain of operations.
    
    This class provides a fluent interface for building SQL queries using a chain
    of operation nodes instead of accumulating operations in lists.
    """
    
    def __init__(self, root_node: Optional[Node] = None):
        """
        Initialize a new Dataframe instance.
        
        Args:
            root_node: The root node of the operation chain
        """
        self._root_node = root_node
        
    @classmethod
    def from_(cls, table_name: str, schema: Optional[str] = None, alias: Optional[str] = None) -> 'Dataframe':
        """
        Create a new Dataframe from a database table.
        
        Args:
            table_name: The name of the table
            schema: Optional schema name
            alias: Optional table alias. If not provided, table_name will be used as the alias.
            
        Returns:
            A new Dataframe instance
        """
        node = FromNode(
            parent=None,
            table_name=table_name,
            schema=schema,
            alias=alias
        )
        return cls(root_node=node)
    
    @classmethod
    def from_table_schema(cls, table_name: str, table_schema: TableSchema, alias: Optional[str] = None) -> 'Dataframe':
        """
        Create a new Dataframe from a table with a defined schema.
        
        Args:
            table_name: The name of the table
            table_schema: The schema definition for the table
            alias: Optional table alias
            
        Returns:
            A new Dataframe instance with type information
        """
        node = FromTableSchemaNode(
            parent=None,
            table_name=table_name,
            table_schema=table_schema,
            alias=alias
        )
        return cls(root_node=node)
        
    def select(self, *columns: Union[Column, Callable[[Any], Any]]) -> 'Dataframe':
        """
        Select columns from this Dataframe.
        
        Args:
            *columns: The columns to select. Can be:
                - Column objects
                - Lambda functions that access dataclass properties (e.g., lambda x: x.column_name)
                - Lambda functions that return arrays (e.g., lambda x: [x.name, x.age])
                - Lambda functions with aggregate functions (e.g., lambda x: count(x.id).as_column('count'))
            
        Returns:
            The Dataframe with the columns selected
        """
        if self._root_node is None:
            raise ValueError("Cannot select from a Dataframe without a source")
            
        node = SelectNode(
            parent=self._root_node,
            columns=list(columns)
        )
        return Dataframe(root_node=node)
        
    def filter(self, condition: Callable[[Any], bool]) -> 'Dataframe':
        """
        Filter the Dataframe based on a lambda function.
        
        Args:
            condition: A lambda function or generator expression
            
        Returns:
            The Dataframe with the filter applied
        """
        if self._root_node is None:
            raise ValueError("Cannot filter a Dataframe without a source")
            
        node = FilterNode(
            parent=self._root_node,
            condition=condition
        )
        return Dataframe(root_node=node)
        
    def group_by(self, *columns: Union[Expression, Callable[[Any], Any]]) -> 'Dataframe':
        """
        Group the Dataframe by the specified columns.
        
        Args:
            *columns: The columns to group by. Can be:
                - Expression objects
                - Lambda functions that access dataclass properties (e.g., lambda x: x.column_name)
                - Lambda functions that return arrays (e.g., lambda x: [x.department, x.location])
            
        Returns:
            The Dataframe with the grouping applied
        """
        if self._root_node is None:
            raise ValueError("Cannot group a Dataframe without a source")
            
        node = GroupByNode(
            parent=self._root_node,
            columns=list(columns)
        )
        return Dataframe(root_node=node)
        
    def order_by(self, lambda_func: Callable[[Any], Any]) -> 'Dataframe':
        """
        Order the Dataframe by the specified columns.
        
        Args:
            lambda_func: The lambda function to specify order by columns. Can be:
                - A lambda that returns a column reference (e.g., lambda x: x.column_name)
                - A lambda that returns a tuple with Sort enum (e.g., lambda x: (x.column_name, Sort.DESC))
                - A lambda that returns an array of column references and tuples (e.g., lambda x: 
                  [x.department, (x.salary, Sort.DESC), x.name])
                
        Returns:
            The Dataframe with the ordering applied
        """
        if self._root_node is None:
            raise ValueError("Cannot order a Dataframe without a source")
            
        node = OrderByNode(
            parent=self._root_node,
            lambda_func=lambda_func
        )
        return Dataframe(root_node=node)
        
    def limit(self, limit: int) -> 'Dataframe':
        """
        Limit the number of rows returned.
        
        Args:
            limit: The maximum number of rows to return
            
        Returns:
            The Dataframe with the limit applied
        """
        if self._root_node is None:
            raise ValueError("Cannot limit a Dataframe without a source")
            
        node = LimitNode(
            parent=self._root_node,
            limit=limit
        )
        return Dataframe(root_node=node)
        
    def offset(self, offset: int) -> 'Dataframe':
        """
        Skip the specified number of rows.
        
        Args:
            offset: The number of rows to skip
            
        Returns:
            The Dataframe with the offset applied
        """
        if self._root_node is None:
            raise ValueError("Cannot offset a Dataframe without a source")
            
        node = OffsetNode(
            parent=self._root_node,
            offset=offset
        )
        return Dataframe(root_node=node)
        
    def distinct_rows(self) -> 'Dataframe':
        """
        Make the query return distinct rows.
        
        Returns:
            The Dataframe with DISTINCT applied
        """
        if self._root_node is None:
            raise ValueError("Cannot make distinct a Dataframe without a source")
            
        node = DistinctRowsNode(
            parent=self._root_node
        )
        return Dataframe(root_node=node)
        
    def having(self, condition: Union[Callable[[Any], bool], Callable[[Any, Any], bool], FilterCondition, Expression]) -> 'Dataframe':
        """
        Add a HAVING clause to filter grouped results.
        
        Args:
            condition: A lambda function, FilterCondition, or Expression
                      that defines the HAVING condition. The lambda function can have
                      one parameter (df) for new columns or two parameters (df, x) for
                      both new and existing columns.
                
        Returns:
            The Dataframe with the HAVING clause applied
        """
        if self._root_node is None:
            raise ValueError("Cannot apply having to a Dataframe without a source")
            
        node = HavingNode(
            parent=self._root_node,
            condition=condition
        )
        return Dataframe(root_node=node)
        
    def qualify(self, condition: Union[Callable[[Any], bool], Callable[[Any, Any], bool], FilterCondition, Expression]) -> 'Dataframe':
        """
        Add a QUALIFY clause to filter results of window functions.
        
        Args:
            condition: A lambda function, FilterCondition, or Expression
                      that defines the QUALIFY condition. The lambda function can have
                      one parameter (df) for new columns or two parameters (df, x) for
                      both new and existing columns.
                      
        Returns:
            A new Dataframe with the QUALIFY condition applied
        """
        if self._root_node is None:
            raise ValueError("Cannot apply qualify to a Dataframe without a source")
            
        node = QualifyNode(
            parent=self._root_node,
            condition=condition
        )
        return Dataframe(root_node=node)
        
    def with_cte(self, name: str, query: Union[DataFrame, str], 
                 columns: Optional[List[str]] = None, is_recursive: bool = False) -> 'Dataframe':
        """
        Add a Common Table Expression (CTE) to the query.
        
        Args:
            name: The name of the CTE
            query: The DataFrame or SQL string for the CTE
            columns: Optional column names for the CTE
            is_recursive: Whether the CTE is recursive
            
        Returns:
            The Dataframe with the CTE added
        """
        if self._root_node is None:
            raise ValueError("Cannot add CTE to a Dataframe without a source")
            
        node = WithCteNode(
            parent=self._root_node,
            name=name,
            query=query,
            columns=columns,
            is_recursive=is_recursive
        )
        return Dataframe(root_node=node)
        
    def join(self, right: Union[DataFrame, 'Dataframe'], 
             condition: Callable[[Any, Any], bool], 
             join_type: JoinType = JoinType.INNER) -> 'Dataframe':
        """
        Join this Dataframe with another DataFrame or Dataframe.
        
        Args:
            right: The DataFrame or Dataframe to join with
            condition: A lambda function that defines the join condition
            join_type: The type of join to perform
            
        Returns:
            A new Dataframe representing the join
        """
        if self._root_node is None:
            raise ValueError("Cannot join a Dataframe without a source")
            
        node = JoinNode(
            parent=self._root_node,
            right=right,
            condition=condition,
            join_type=join_type
        )
        return Dataframe(root_node=node)
        
    def left_join(self, right: Union[DataFrame, 'Dataframe'], 
                  condition: Callable[[Any, Any], bool]) -> 'Dataframe':
        """
        Perform a LEFT JOIN with another DataFrame or Dataframe.
        
        Args:
            right: The DataFrame or Dataframe to join with
            condition: A lambda function that defines the join condition
            
        Returns:
            A new Dataframe representing the join
        """
        return self.join(right, condition, JoinType.LEFT)
        
    def right_join(self, right: Union[DataFrame, 'Dataframe'], 
                   condition: Callable[[Any, Any], bool]) -> 'Dataframe':
        """
        Perform a RIGHT JOIN with another DataFrame or Dataframe.
        
        Args:
            right: The DataFrame or Dataframe to join with
            condition: A lambda function that defines the join condition
            
        Returns:
            A new Dataframe representing the join
        """
        return self.join(right, condition, JoinType.RIGHT)
        
    def full_join(self, right: Union[DataFrame, 'Dataframe'], 
                  condition: Callable[[Any, Any], bool]) -> 'Dataframe':
        """
        Perform a FULL JOIN with another DataFrame or Dataframe.
        
        Args:
            right: The DataFrame or Dataframe to join with
            condition: A lambda function that defines the join condition
            
        Returns:
            A new Dataframe representing the join
        """
        return self.join(right, condition, JoinType.FULL)
        
    def cross_join(self, right: Union[DataFrame, 'Dataframe']) -> 'Dataframe':
        """
        Perform a CROSS JOIN with another DataFrame or Dataframe.
        
        Args:
            right: The DataFrame or Dataframe to join with
            
        Returns:
            A new Dataframe representing the join
        """
        return self.join(right, lambda x, y: True, JoinType.CROSS)
        
    def to_sql(self, dialect: str = "duckdb") -> str:
        """
        Generate SQL for the specified dialect.
        
        Args:
            dialect: The SQL dialect to generate (default: "duckdb")
            
        Returns:
            The generated SQL string
        """
        if self._root_node is None:
            raise ValueError("Cannot generate SQL from a Dataframe without a source")
            
        df = self._root_node.to_dataframe()
        
        return df.to_sql(dialect=dialect)
