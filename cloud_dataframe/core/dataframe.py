"""
Core DataFrame module for cloud-dataframe.

This module defines the base DataFrame class and core operations that can be
translated to SQL for execution against different database backends.
"""
from __future__ import annotations
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar, Union, Generic, Type, cast
from enum import Enum
import inspect
from dataclasses import dataclass, field

from ..type_system.column import Column, ColumnReference, Expression, LiteralExpression
from ..type_system.schema import TableSchema, ColSpec, create_dynamic_dataclass_from_schema

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
    needs_parentheses: bool = False


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
        self._table_class: Optional[Type] = None
    
    def copy(self) -> 'DataFrame':
        """
        Create a deep copy of this DataFrame.
        
        Returns:
            A new DataFrame with the same properties
        """
        result = DataFrame()
        result.columns = self.columns.copy()
        result.source = self.source  # DataSource objects are immutable
        result.filter_condition = self.filter_condition  # FilterCondition objects are immutable
        result.group_by_clause = self.group_by_clause  # GroupByClause objects are immutable
        result.having = self.having  # FilterCondition objects are immutable
        result.order_by_clauses = self.order_by_clauses.copy()
        result.limit_value = self.limit_value
        result.offset_value = self.offset_value
        result.distinct = self.distinct
        result.ctes = self.ctes.copy()
        result._table_class = self._table_class  # Copy the table class reference
        return result
    
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
        
    def select(self, *columns: Union[Column, str, ColSpec, Callable[[Any], Any]]) -> 'DataFrame':
        """
        Select columns from this DataFrame.
        
        Args:
            *columns: The columns to select. Can be:
                - Column objects
                - String column names
                - ColSpec objects
                - Lambda functions that access dataclass properties (e.g., lambda x: x.column_name)
                - Lambda functions that return arrays (e.g., lambda x: [x.name, x.age])
            
        Returns:
            The DataFrame with the columns selected
        """
        column_list = []
        for col in columns:
            if isinstance(col, Column):
                column_list.append(col)
            elif isinstance(col, str):
                column_list.append(ColumnReference(col))
            elif isinstance(col, ColSpec):
                column_list.append(ColumnReference(col.name))
            elif callable(col) and not isinstance(col, Column):
                # Handle lambda functions that access dataclass properties
                from ..utils.lambda_parser import LambdaParser
                expr = LambdaParser.parse_lambda(col)
                if isinstance(expr, list):
                    # Handle array returns from lambda functions
                    column_list.extend(expr)
                else:
                    column_list.append(expr)
            else:
                raise TypeError(f"Unsupported column type: {type(col)}")
        
        self.columns = column_list
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
        
        # Create a basic schema with Any type for columns
        # This is a placeholder until the actual schema is determined
        basic_schema = TableSchema(name=table_name, columns={"*": type(Any)})
        
        # Create a dynamic dataclass and store it on the DataFrame
        df._table_class = create_dynamic_dataclass_from_schema(table_name, basic_schema)
        
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
        
        # Create a dynamic dataclass and store it on the DataFrame
        df._table_class = create_dynamic_dataclass_from_schema(table_name, table_schema)
        
        return df
    
    def filter(self, condition: Callable[[Any], bool]) -> 'DataFrame':
        """
        Filter the DataFrame based on a lambda function.
        
        Args:
            condition: A lambda function or generator expression
            
        Returns:
            The DataFrame with the filter applied
        """
        # Validate that the condition is a lambda function
        if not callable(condition) or isinstance(condition, FilterCondition):
            raise TypeError("Filter condition must be a lambda function or generator expression")
        
        # Create a new DataFrame with the filter condition
        result = self.copy()
        
        # Convert the lambda to a filter condition
        filter_condition = self._lambda_to_filter_condition(condition)
        
        # If we already have a filter condition, combine them with AND
        if result.filter_condition:
            result.filter_condition = BinaryOperation(
                left=result.filter_condition,
                operator="AND",
                right=filter_condition
            )
        else:
            result.filter_condition = filter_condition
        
        return result
    
    def _lambda_to_filter_condition(self, lambda_func: Callable[[Any], bool]) -> FilterCondition:
        """
        Convert a lambda function to a FilterCondition.
        
        This is a complex operation that requires parsing the lambda's AST.
        
        Args:
            lambda_func: The lambda function to convert
            
        Returns:
            A FilterCondition representing the lambda
        """
        from ..utils.lambda_parser import LambdaParser
        
        # Get the table schema if available
        table_schema = None
        if isinstance(self.source, TableReference):
            table_schema = self.source.table_schema
        
        # Use the LambdaParser to convert the lambda to a FilterCondition
        expr = LambdaParser.parse_lambda(lambda_func, table_schema)
        
        # Cast to FilterCondition (this is safe because the parser returns a compatible type)
        return cast(FilterCondition, expr)
    
    def group_by(self, *columns: Union[str, Expression, ColSpec, Callable[[Any], Any]]) -> 'DataFrame':
        """
        Group the DataFrame by the specified columns.
        
        Args:
            *columns: The columns to group by. Can be:
                - String column names
                - Expression objects
                - ColSpec objects
                - Lambda functions that access dataclass properties (e.g., lambda x: x.column_name)
                - Lambda functions that return arrays (e.g., lambda x: [x.department, x.location])
            
        Returns:
            The DataFrame with the grouping applied
        """
        expressions = []
        for col in columns:
            if isinstance(col, str):
                expressions.append(ColumnReference(col))
            elif isinstance(col, ColSpec):
                expressions.append(ColumnReference(col.name))
            elif callable(col) and not isinstance(col, Expression):
                # Handle lambda functions that access dataclass properties
                from ..utils.lambda_parser import LambdaParser
                expr = LambdaParser.parse_lambda(col)
                if isinstance(expr, list):
                    # Handle array returns from lambda functions
                    expressions.extend(expr)
                else:
                    expressions.append(expr)
            else:
                expressions.append(col)
        
        self.group_by_clause = GroupByClause(columns=expressions)
        return self
    
    def order_by(self, *clauses: Union[OrderByClause, Expression, str, ColSpec, Callable[[Any], Any]], 
                 desc: bool = False) -> 'DataFrame':
        """
        Order the DataFrame by the specified columns.
        
        Args:
            *clauses: The columns or OrderByClauses to order by. Can be:
                - String column names
                - Expression objects
                - ColSpec objects
                - OrderByClause objects
                - Lambda functions that access dataclass properties (e.g., lambda x: x.column_name)
                - Lambda functions that return arrays (e.g., lambda x: [x.department, x.salary])
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
            elif callable(clause) and not isinstance(clause, Expression):
                # Handle lambda functions that access dataclass properties
                from ..utils.lambda_parser import LambdaParser
                expr = LambdaParser.parse_lambda(clause)
                if isinstance(expr, list):
                    # Handle array returns from lambda functions
                    for single_expr in expr:
                        self.order_by_clauses.append(OrderByClause(
                            expression=single_expr,
                            direction=direction
                        ))
                else:
                    self.order_by_clauses.append(OrderByClause(
                        expression=expr,
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
             condition: Callable[[Any, Any], bool], 
             join_type: JoinType = JoinType.INNER) -> 'DataFrame':
        """
        Join this DataFrame with another DataFrame or table.
        
        Args:
            right: The DataFrame or table to join with
            condition: A lambda function that defines the join condition
            join_type: The type of join to perform
            
        Returns:
            A new DataFrame representing the join
        """
        if self.source is None:
            raise ValueError("Cannot join a DataFrame without a source")
        
        # Get the right source
        if isinstance(right, DataFrame):
            # If the right DataFrame has a TableReference source, use it directly
            if isinstance(right.source, TableReference):
                right_source = right.source
            else:
                # Otherwise, wrap it in a SubquerySource
                right_source = SubquerySource(
                    dataframe=right,
                    alias=f"subquery_{len(self.ctes)}"
                )
        elif isinstance(right, TableReference):
            right_source = right
        else:
            raise TypeError("Right side of join must be a DataFrame or TableReference")
        
        # Convert lambda to join condition
        join_condition = self._lambda_to_join_condition(condition)
        
        result = DataFrame()
        result.source = JoinOperation(
            left=self.source,
            right=right_source,
            join_type=join_type,
            condition=join_condition
        )
        
        # Combine columns from both sides
        # This is a simplification - in reality, we'd need to handle column name conflicts
        result.columns = self.columns.copy()
        if isinstance(right, DataFrame):
            result.columns.extend(right.columns)
        
        return result
    
    def left_join(self, right: Union['DataFrame', TableReference], 
                  condition: Callable[[Any, Any], bool]) -> 'DataFrame':
        """
        Perform a LEFT JOIN with another DataFrame or table.
        
        Args:
            right: The DataFrame or table to join with
            condition: A lambda function that defines the join condition
            
        Returns:
            A new DataFrame representing the join
        """
        return self.join(right, condition, JoinType.LEFT)
    
    def right_join(self, right: Union['DataFrame', TableReference], 
                   condition: Callable[[Any, Any], bool]) -> 'DataFrame':
        """
        Perform a RIGHT JOIN with another DataFrame or table.
        
        Args:
            right: The DataFrame or table to join with
            condition: A lambda function that defines the join condition
            
        Returns:
            A new DataFrame representing the join
        """
        return self.join(right, condition, JoinType.RIGHT)
    
    def full_join(self, right: Union['DataFrame', TableReference], 
                  condition: Callable[[Any, Any], bool]) -> 'DataFrame':
        """
        Perform a FULL JOIN with another DataFrame or table.
        
        Args:
            right: The DataFrame or table to join with
            condition: A lambda function that defines the join condition
            
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
        # We use a lambda function that always returns True to satisfy the type checker
        return self.join(right, lambda x, y: True, JoinType.CROSS)
    
    def _lambda_to_join_condition(self, lambda_func: Callable[[Any, Any], bool]) -> FilterCondition:
        """
        Convert a lambda function to a join condition.
        
        This method parses a lambda function that takes two parameters (one for each table)
        and converts it to a FilterCondition representing the join condition.
        
        Args:
            lambda_func: The lambda function to convert
            
        Returns:
            A FilterCondition representing the join condition
        """
        from ..utils.lambda_parser import LambdaParser
        
        # Use the LambdaParser to convert the lambda to a FilterCondition
        expr = LambdaParser.parse_join_lambda(lambda_func)
        
        # Cast to FilterCondition (this is safe because the parser returns a compatible type)
        return cast(FilterCondition, expr)
    
    def get_table_class(self) -> Optional[Type]:
        """
        Get the dynamic dataclass for this DataFrame.
        
        Returns:
            The dynamic dataclass for this DataFrame, or None if not available
        """
        if hasattr(self, "_table_class") and self._table_class is not None:
            return self._table_class
        elif self.source and isinstance(self.source, TableReference) and self.source.table_schema:
            self._table_class = create_dynamic_dataclass_from_schema(
                self.source.table_name, self.source.table_schema)
            return self._table_class
        return None
        
    def _lambda_to_column_reference(self, lambda_func: Callable[[Any], Any]) -> ColumnReference:
        """
        Convert a lambda function that returns an attribute to a ColumnReference.
        
        Args:
            lambda_func: The lambda function to convert
            
        Returns:
            A ColumnReference representing the column accessed by the lambda
        """
        # Try to determine the column name by inspecting the lambda source
        import inspect
        source = inspect.getsource(lambda_func).strip()
        
        # Extract the column name from the lambda
        # Example: "lambda x: x.name" -> "name"
        if "lambda" in source and "." in source:
            parts = source.split(".")
            column_name = parts[-1].strip().rstrip(")")
            return ColumnReference(column_name)
        
        # If we can't determine the column name, raise an error
        raise ValueError("Could not determine column name from lambda function")
        
    def _create_sample_instance(self) -> Any:
        """
        Create a sample instance of the table dataclass for testing.
        
        Returns:
            A sample instance of the table dataclass
        """
        table_class = self.get_table_class()
        if not table_class:
            return None
        
        # Create a sample instance with default values
        sample_data = {}
        for field_name, field_type in table_class.__annotations__.items():
            # Assign default values based on type
            if field_type == int:
                sample_data[field_name] = 0
            elif field_type == float:
                sample_data[field_name] = 0.0
            elif field_type == str:
                sample_data[field_name] = ""
            elif field_type == bool:
                sample_data[field_name] = False
            else:
                sample_data[field_name] = None
        
        return table_class(**sample_data)
    
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
