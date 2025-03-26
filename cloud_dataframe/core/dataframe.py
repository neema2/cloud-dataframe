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

from ..type_system.column import Column, ColumnReference, Expression, LiteralExpression, Window, Frame
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


class Sort(Enum):
    """Sort directions for ORDER BY clauses."""
    ASC = "ASC"
    DESC = "DESC"


@dataclass
class OrderByClause:
    """Represents an ORDER BY clause in a SQL query."""
    expression: Expression
    direction: Sort = Sort.ASC


@dataclass
class GroupByClause:
    """Represents a GROUP BY clause in a SQL query."""
    columns: List[Expression] = field(default_factory=list)


@dataclass
class FilterCondition(Expression):
    """Base class for filter conditions."""
    condition: Expression


@dataclass
class BinaryOperation(Expression):
    """Binary operation (e.g., =, >, <, etc.)."""
    left: Expression
    operator: str
    right: Expression
    needs_parentheses: bool = False


@dataclass
class UnaryOperation(Expression):
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
    left_alias: Optional[str] = None
    right_alias: Optional[str] = None


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
        self.group_by_clauses: List[Expression] = []
        self.having_condition: Optional[FilterCondition] = None
        self.order_by_clauses: List[OrderByClause] = []
        self.limit_value: Optional[int] = None
        self.offset_value: Optional[int] = None
        self.distinct: bool = False
        self.ctes: List[CommonTableExpression] = []
        self.window_definitions: Dict[str, Window] = {}
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
        result.group_by_clauses = self.group_by_clauses.copy() if hasattr(self, 'group_by_clauses') else []
        result.having_condition = self.having_condition  # FilterCondition objects are immutable
        result.order_by_clauses = self.order_by_clauses.copy()
        result.limit_value = self.limit_value
        result.offset_value = self.offset_value
        result.distinct = self.distinct
        result.ctes = self.ctes.copy()
        result.window_definitions = self.window_definitions.copy() if hasattr(self, 'window_definitions') else {}
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
        
    def select(self, *columns: Union[Column, Callable[[Any], Any]]) -> 'DataFrame':
        """
        Select columns from this DataFrame.
        
        Args:
            *columns: The columns to select. Can be:
                - Column objects
                - Lambda functions that access dataclass properties (e.g., lambda x: x.column_name)
                - Lambda functions that return arrays (e.g., lambda x: [x.name, x.age])
                - Lambda functions with aggregate functions (e.g., lambda x: count(x.id).as_column('count'))
            
        Returns:
            The DataFrame with the columns selected
        """
        column_list = []
        for col in columns:
            if isinstance(col, Column):
                column_list.append(col)
            elif callable(col) and not isinstance(col, Column):
                # Handle lambda functions that access dataclass properties
                from ..utils.lambda_parser import LambdaParser
                # Get the table schema if available
                table_schema = None
                if isinstance(self.source, TableReference):
                    table_schema = self.source.table_schema
                
                # Parse the lambda function
                expr = LambdaParser.parse_lambda(col, table_schema)
                
                if isinstance(expr, list):
                    # Handle array returns from lambda functions
                    column_list.extend(expr)
                else:
                    # Handle single column reference
                    column_list.append(expr)
            else:
                raise ValueError(f"Unsupported column type: {type(col)}")
        
        result = self.copy()
        result.columns = column_list
        return result
    
    @classmethod
    def from_(cls, source: Union[str, 'DataFrame'], alias: Optional[str] = None) -> 'DataFrame':
        """
        Create a new DataFrame from a data source.
        
        Args:
            source: The data source (table name or subquery)
            alias: Optional alias for the data source
            
        Returns:
            A new DataFrame instance
        """
        df = cls()
        
        if isinstance(source, str):
            # Handle table reference
            df.source = TableReference(table_name=source, alias=alias)
        elif isinstance(source, DataFrame):
            # Handle subquery
            if not alias:
                raise ValueError("Alias is required for subquery")
            df.source = SubquerySource(dataframe=source, alias=alias)
        else:
            raise ValueError(f"Unsupported source type: {type(source)}")
        
        return df
    
    @classmethod
    def from_table_schema(cls, table_name: str, schema: TableSchema, alias: Optional[str] = None) -> 'DataFrame':
        """
        Create a new DataFrame from a table schema.
        
        Args:
            table_name: The name of the table
            schema: The table schema
            alias: Optional alias for the table
            
        Returns:
            A new DataFrame instance
        """
        df = cls()
        
        # Create a TableReference with the schema
        df.source = TableReference(
            table_name=table_name,
            alias=alias,
            table_schema=schema
        )
        
        # Create a dynamic dataclass for the table
        df._table_class = create_dynamic_dataclass_from_schema(table_name, schema)
        
        return df
    
    def filter(self, condition: Union[FilterCondition, Callable[[Any], Any]]) -> 'DataFrame':
        """
        Filter the DataFrame based on a condition.
        
        Args:
            condition: The filter condition. Can be:
                - A FilterCondition object
                - A lambda function that returns a boolean expression (e.g., lambda x: x.age > 30)
            
        Returns:
            The filtered DataFrame
        """
        result = self.copy()
        
        if isinstance(condition, FilterCondition):
            # Use the provided FilterCondition
            filter_condition = condition
        else:
            # Convert lambda to FilterCondition
            filter_condition = self._lambda_to_filter_condition(condition)
        
        if result.filter_condition:
            # Combine with existing filter using AND
            result.filter_condition = BinaryOperation(
                left=result.filter_condition,
                operator="AND",
                right=filter_condition
            )
        else:
            # Set as the only filter
            result.filter_condition = filter_condition
        
        return result
    
    def _lambda_to_filter_condition(self, lambda_func: Callable[[Any], Any]) -> FilterCondition:
        """
        Convert a lambda function to a FilterCondition.
        
        Args:
            lambda_func: The lambda function to convert
            
        Returns:
            The converted FilterCondition
        """
        from ..utils.lambda_parser import LambdaParser
        
        # Get the table schema if available
        table_schema = None
        if isinstance(self.source, TableReference):
            table_schema = self.source.table_schema
        
        try:
            # Parse the lambda function
            parsed_condition = LambdaParser.parse_lambda(lambda_func, table_schema)
            
            # Create a FilterCondition with the parsed expression
            return FilterCondition(parsed_condition)
        except Exception as e:
            raise ValueError(f"Error parsing filter lambda: {e}")
    
    def group_by(self, *columns: Union[Expression, Callable[[Any], Any]]) -> 'DataFrame':
        """
        Group the DataFrame by the specified columns.
        
        Args:
            *columns: The columns to group by. Can be:
                - Expression objects
                - Lambda functions that access dataclass properties (e.g., lambda x: x.column_name)
                - Lambda functions that return arrays (e.g., lambda x: [x.name, x.age])
            
        Returns:
            The grouped DataFrame
        """
        result = self.copy()
        group_by_list = []
        
        for col in columns:
            if isinstance(col, Expression):
                # Use the provided Expression
                group_by_list.append(col)
            elif callable(col) and not isinstance(col, Expression):
                # Handle lambda functions
                from ..utils.lambda_parser import LambdaParser
                
                # Get the table schema if available
                table_schema = None
                if isinstance(self.source, TableReference):
                    table_schema = self.source.table_schema
                
                # Parse the lambda function
                expr = LambdaParser.parse_lambda(col, table_schema)
                
                if isinstance(expr, list):
                    # Handle array returns from lambda functions
                    group_by_list.extend(expr)
                else:
                    # Handle single column reference
                    group_by_list.append(expr)
            else:
                raise ValueError(f"Unsupported group by column type: {type(col)}")
        
        result.group_by_clauses = group_by_list
        return result
    
    def order_by(self, *columns: Union[Expression, Callable[[Any], Any], Tuple[Expression, Sort], Tuple[Expression, str]]) -> 'DataFrame':
        """
        Order the DataFrame by the specified columns.
        
        Args:
            *columns: The columns to order by. Can be:
                - Expression objects
                - Lambda functions that access dataclass properties (e.g., lambda x: x.column_name)
                - Lambda functions that return arrays (e.g., lambda x: [x.name, x.age])
                - Lambda functions that return tuples with sort direction (e.g., lambda x: [(x.name, 'DESC'), (x.age, 'ASC')])
                - Tuples of (Expression, Sort) or (Expression, str) for sort direction
            
        Returns:
            The ordered DataFrame
        """
        result = self.copy()
        order_by_list = []
        
        for col in columns:
            if isinstance(col, Expression):
                # Use the provided Expression with default ASC ordering
                order_by_list.append(OrderByClause(expression=col, direction=Sort.ASC))
            elif isinstance(col, tuple) and len(col) == 2:
                # Handle tuple of (Expression, Sort) or (Expression, str)
                expr, direction = col
                
                if isinstance(direction, Sort):
                    # Use the provided Sort enum
                    dir_enum = direction
                elif isinstance(direction, str):
                    # Convert string to Sort enum
                    dir_enum = Sort.DESC if direction.upper() == 'DESC' else Sort.ASC
                else:
                    raise ValueError(f"Unsupported sort direction type: {type(direction)}")
                
                order_by_list.append(OrderByClause(expression=expr, direction=dir_enum))
            elif callable(col) and not isinstance(col, Expression):
                # Handle lambda functions
                from ..utils.lambda_parser import LambdaParser
                
                # Get the table schema if available
                table_schema = None
                if isinstance(self.source, TableReference):
                    table_schema = self.source.table_schema
                
                # Parse the lambda function
                expr = LambdaParser.parse_lambda(col, table_schema)
                
                if isinstance(expr, list):
                    # Process array lambdas
                    for item in expr:
                        # Check if this is a tuple with sort direction
                        if isinstance(item, tuple) and len(item) == 2:
                            col_expr, sort_dir = item
                            # Convert string sort direction to Sort enum
                            dir_enum = Sort.DESC if isinstance(sort_dir, str) and sort_dir.upper() == 'DESC' else Sort.ASC
                            order_by_list.append(OrderByClause(expression=col_expr, direction=dir_enum))
                        else:
                            # Use default ASC ordering
                            order_by_list.append(OrderByClause(expression=item, direction=Sort.ASC))
                else:
                    # Single expression with default ASC ordering
                    order_by_list.append(OrderByClause(expression=expr, direction=Sort.ASC))
            else:
                raise ValueError(f"Unsupported order by column type: {type(col)}")
        
        result.order_by_clauses = order_by_list
        return result
    
    def limit(self, value: int) -> 'DataFrame':
        """
        Limit the number of rows returned.
        
        Args:
            value: The maximum number of rows to return
            
        Returns:
            The limited DataFrame
        """
        result = self.copy()
        result.limit_value = value
        return result
    
    def offset(self, value: int) -> 'DataFrame':
        """
        Skip the specified number of rows.
        
        Args:
            value: The number of rows to skip
            
        Returns:
            The offset DataFrame
        """
        result = self.copy()
        result.offset_value = value
        return result
    
    def distinct_rows(self) -> 'DataFrame':
        """
        Return distinct rows.
        
        Returns:
            The DataFrame with distinct rows
        """
        result = self.copy()
        result.distinct = True
        return result
    
    def having(self, condition: Union[FilterCondition, Callable[[Any], Any]]) -> 'DataFrame':
        """
        Filter grouped rows based on a condition.
        
        Args:
            condition: The having condition. Can be:
                - A FilterCondition object
                - A lambda function that returns a boolean expression (e.g., lambda x: count(x.id) > 10)
            
        Returns:
            The filtered DataFrame
        """
        if not self.group_by_clauses:
            raise ValueError("HAVING requires a GROUP BY clause")
        
        df_copy = self.copy()
        
        if isinstance(condition, FilterCondition):
            # Use the provided FilterCondition
            df_copy.having_condition = condition
        else:
            # Convert lambda to FilterCondition
            from ..utils.lambda_parser import LambdaParser
            
            # Get the table schema if available
            table_schema = None
            if isinstance(self.source, TableReference):
                table_schema = self.source.table_schema
            
            try:
                # Parse the lambda function
                parsed_condition = LambdaParser.parse_lambda(condition, table_schema)
                
                # Create a FilterCondition with the parsed expression
                df_copy.having_condition = FilterCondition(parsed_condition)
            except Exception as e:
                raise ValueError(f"Error parsing having lambda: {e}")
        
        return df_copy
    
    def with_cte(self, name: str, query: Union['DataFrame', str], columns: Optional[List[str]] = None, recursive: bool = False) -> 'DataFrame':
        """
        Add a Common Table Expression (CTE) to the query.
        
        Args:
            name: The name of the CTE
            query: The query for the CTE (DataFrame or SQL string)
            columns: Optional list of column names for the CTE
            recursive: Whether the CTE is recursive
            
        Returns:
            The DataFrame with the CTE added
        """
        result = self.copy()
        
        # Create a new CTE
        cte = CommonTableExpression(
            name=name,
            query=query,
            columns=columns or [],
            is_recursive=recursive
        )
        
        # Add the CTE to the list
        result.ctes.append(cte)
        return result
    
    def join(self, right: Union[str, 'DataFrame'], condition: Union[FilterCondition, Callable[[Any, Any], Any]], join_type: JoinType = JoinType.INNER, right_alias: Optional[str] = None) -> 'DataFrame':
        """
        Join this DataFrame with another DataFrame or table.
        
        Args:
            right: The right side of the join (table name or DataFrame)
            condition: The join condition. Can be:
                - A FilterCondition object
                - A lambda function that returns a boolean expression (e.g., lambda x, y: x.id == y.user_id)
            join_type: The type of join to perform
            right_alias: Optional alias for the right table
            
        Returns:
            The joined DataFrame
        """
        result = DataFrame()
        
        # Create the right DataSource
        right_source = None
        if isinstance(right, str):
            # Handle table reference
            right_source = TableReference(table_name=right, alias=right_alias)
        elif isinstance(right, DataFrame):
            # Handle subquery
            if not right_alias:
                raise ValueError("Alias is required for subquery in join")
            right_source = SubquerySource(dataframe=right, alias=right_alias)
        else:
            raise ValueError(f"Unsupported right source type: {type(right)}")
        
        # Convert lambda to join condition if needed
        if not isinstance(condition, FilterCondition) and callable(condition):
            join_condition = self._lambda_to_join_condition(condition, right_source)
        else:
            join_condition = condition
        
        # Create the join operation
        join_op = JoinOperation(
            left=self.source,
            right=right_source,
            join_type=join_type,
            condition=join_condition,
            right_alias=right_alias
        )
        
        # Set the source of the result DataFrame
        result.source = join_op
        
        # Copy other properties from the left DataFrame
        result.columns = self.columns.copy()
        result.filter_condition = self.filter_condition
        result.group_by_clauses = self.group_by_clauses.copy() if hasattr(self, 'group_by_clauses') else []
        result.having_condition = self.having_condition
        result.order_by_clauses = self.order_by_clauses.copy()
        result.limit_value = self.limit_value
        result.offset_value = self.offset_value
        result.distinct = self.distinct
        result.ctes = self.ctes.copy()
        
        return result
    
    def left_join(self, right: Union[str, 'DataFrame'], condition: Union[FilterCondition, Callable[[Any, Any], Any]], right_alias: Optional[str] = None) -> 'DataFrame':
        """
        Perform a LEFT JOIN with another DataFrame or table.
        
        Args:
            right: The right side of the join (table name or DataFrame)
            condition: The join condition
            right_alias: Optional alias for the right table
            
        Returns:
            The joined DataFrame
        """
        return self.join(right, condition, JoinType.LEFT, right_alias)
    
    def right_join(self, right: Union[str, 'DataFrame'], condition: Union[FilterCondition, Callable[[Any, Any], Any]], right_alias: Optional[str] = None) -> 'DataFrame':
        """
        Perform a RIGHT JOIN with another DataFrame or table.
        
        Args:
            right: The right side of the join (table name or DataFrame)
            condition: The join condition
            right_alias: Optional alias for the right table
            
        Returns:
            The joined DataFrame
        """
        return self.join(right, condition, JoinType.RIGHT, right_alias)
    
    def full_join(self, right: Union[str, 'DataFrame'], condition: Union[FilterCondition, Callable[[Any, Any], Any]], right_alias: Optional[str] = None) -> 'DataFrame':
        """
        Perform a FULL JOIN with another DataFrame or table.
        
        Args:
            right: The right side of the join (table name or DataFrame)
            condition: The join condition
            right_alias: Optional alias for the right table
            
        Returns:
            The joined DataFrame
        """
        return self.join(right, condition, JoinType.FULL, right_alias)
    
    def cross_join(self, right: Union[str, 'DataFrame'], right_alias: Optional[str] = None) -> 'DataFrame':
        """
        Perform a CROSS JOIN with another DataFrame or table.
        
        Args:
            right: The right side of the join (table name or DataFrame)
            right_alias: Optional alias for the right table
            
        Returns:
            The joined DataFrame
        """
        # For CROSS JOIN, we don't need a condition
        return self.join(right, FilterCondition(LiteralExpression(True)), JoinType.CROSS, right_alias)
    
    def _lambda_to_join_condition(self, lambda_func: Callable[[Any, Any], Any], right_source: DataSource) -> FilterCondition:
        """
        Convert a lambda function to a join condition.
        
        Args:
            lambda_func: The lambda function to convert
            right_source: The right data source for the join
            
        Returns:
            The converted FilterCondition
        """
        from ..utils.lambda_parser import LambdaParser
        
        # Get the table schemas if available
        left_schema = None
        right_schema = None
        
        if isinstance(self.source, TableReference):
            left_schema = self.source.table_schema
        
        if isinstance(right_source, TableReference):
            right_schema = right_source.table_schema
        
        try:
            # Parse the lambda function with both schemas
            parsed_condition = LambdaParser.parse_join_lambda(lambda_func, left_schema, right_schema)
            
            # Create a FilterCondition with the parsed expression
            return FilterCondition(parsed_condition)
        except Exception as e:
            raise ValueError(f"Error parsing join lambda: {e}")
    
    def get_table_class(self) -> Optional[Type]:
        """
        Get the table class for this DataFrame.
        
        Returns:
            The table class, or None if not available
        """
        return self._table_class
    
    def _lambda_to_column_reference(self, lambda_func: Callable[[Any], Any]) -> ColumnReference:
        """
        Convert a lambda function to a column reference.
        
        Args:
            lambda_func: The lambda function to convert
            
        Returns:
            The converted ColumnReference
        """
        from ..utils.lambda_parser import LambdaParser
        
        # Get the table schema if available
        table_schema = None
        if isinstance(self.source, TableReference):
            table_schema = self.source.table_schema
        
        try:
            # Parse the lambda function
            parsed_expr = LambdaParser.parse_lambda(lambda_func, table_schema)
            
            if isinstance(parsed_expr, ColumnReference):
                return parsed_expr
            else:
                raise ValueError(f"Lambda function did not resolve to a column reference: {parsed_expr}")
        except Exception as e:
            raise ValueError(f"Error parsing column lambda: {e}")
    
    def _create_sample_instance(self) -> Any:
        """
        Create a sample instance of the table class for this DataFrame.
        
        Returns:
            A sample instance of the table class
        """
        if not self._table_class:
            raise ValueError("No table class available for this DataFrame")
        
        table_class = self._table_class
        
        # Create a dictionary of sample data for each field
        sample_data = {}
        for field_name, field_type in table_class.__annotations__.items():
            # Use appropriate default values based on field type
            if field_type == str:
                sample_data[field_name] = ""
            elif field_type == int:
                sample_data[field_name] = 0
            elif field_type == float:
                sample_data[field_name] = 0.0
            elif field_type == bool:
                sample_data[field_name] = False
            else:
                sample_data[field_name] = None
        
        return table_class(**sample_data)
    
    def window(self, name: str, 
              partition_by: Optional[Union[List[Expression], Callable]] = None,
              order_by: Optional[Union[List[Expression], Callable]] = None,
              frame: Optional[Frame] = None) -> 'DataFrame':
        """
        Define a named window specification.
        
        Args:
            name: The name of the window
            partition_by: Optional list of expressions or lambda function to partition by
                Can be a lambda that returns:
                - A single column reference (lambda x: x.column)
                - A list of column references (lambda x: [x.col1, x.col2])
            order_by: Optional list of expressions or lambda function to order by
                Can be a lambda that returns:
                - A single column reference (lambda x: x.column)
                - A list of column references (lambda x: [x.col1, x.col2])
                - A list with tuples specifying sort direction (lambda x: [(x.col1, 'DESC'), (x.col2, 'ASC')])
            frame: Optional frame specification created with row() or range() functions
            
        Returns:
            The DataFrame with the window definition added
        """
        from ..utils.lambda_parser import LambdaParser
        
        result = self.copy()
        window = Window()
        
        # Set the window name
        window.set_name(name)
        
        # Process partition_by
        if partition_by:
            partition_by_list = []
            if callable(partition_by):
                # Get the table schema if available
                table_schema = None
                if isinstance(self.source, TableReference):
                    table_schema = self.source.table_schema
                
                # Parse the lambda function
                parsed_expressions = LambdaParser.parse_lambda(partition_by, table_schema)
                if isinstance(parsed_expressions, list):
                    partition_by_list = parsed_expressions
                else:
                    partition_by_list = [parsed_expressions]
            else:
                # Handle list of expressions (already Expression objects)
                partition_by_list = partition_by
            
            window.set_partition_by(partition_by_list)
        
        # Process order_by
        if order_by:
            order_by_list = []
            if callable(order_by):
                # Get the table schema if available
                table_schema = None
                if isinstance(self.source, TableReference):
                    table_schema = self.source.table_schema
                
                # Parse the lambda function
                parsed_expressions = LambdaParser.parse_lambda(order_by, table_schema)
                
                if isinstance(parsed_expressions, list):
                    # Process array lambdas
                    for item in parsed_expressions:
                        # Check if this is a tuple with sort direction
                        if isinstance(item, tuple) and len(item) == 2:
                            col_expr, sort_dir = item
                            # Convert string sort direction to OrderByClause equivalent
                            dir_enum = Sort.DESC if isinstance(sort_dir, str) and sort_dir.upper() == 'DESC' else Sort.ASC
                            order_by_list.append(OrderByClause(expression=col_expr, direction=dir_enum))
                        else:
                            # Use default ASC ordering
                            order_by_list.append(OrderByClause(expression=item, direction=Sort.ASC))
                else:
                    # Single expression with default ASC ordering
                    order_by_list.append(OrderByClause(expression=parsed_expressions, direction=Sort.ASC))
            else:
                # Handle list of expressions (already OrderByClause objects)
                order_by_list = order_by
            
            window.set_order_by(order_by_list)
        
        # Add frame handling
        if frame:
            window.set_frame(frame)
        
        # Add the window definition to the DataFrame
        result.window_definitions[name] = window
        return result
    
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
