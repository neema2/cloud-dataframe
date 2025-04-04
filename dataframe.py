from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Optional, Type, Dict

from window import *
from functions import *

@dataclass
class TableSchema:
    name: str
    alias: str
    columns: Dict[str, Type] = field(default_factory=dict)

@dataclass
class Expression:
    """Base class for all expressions in the DataFrame DSL."""

@dataclass
class FunctionExpression:
    """Base class for all expressions in the DataFrame DSL."""

class Dataframe:

    def __init__(self, name: str, alias: str, columns):
        self.table_schema = TableSchema(name, alias, columns)

    @classmethod
    def from_(cls, name: str, alias: Optional[str] = None, columns: Optional[Dict[str, Type]] = None) -> Dataframe:
        if alias is None:
            alias = name
        return Dataframe(name, alias, columns)

    def let(self, df : Dataframe) -> Dataframe:
        # CommonTableExpression ("with" in SQL)
        return self

    def recurse(self, df : Dataframe) -> Dataframe:
        # CommonTableExpression ("with recursive" in SQL)
        return self

    def distinct(self) -> Dataframe:
        return self

    def select(self, columns: Callable) -> Dataframe:
        return self

    def extend(self, columns: Callable) -> Dataframe:
        return self

    def filter(self, condition: Callable) -> Dataframe:
        return self

    def group_by(self, columns: Callable, aggregations: Callable) -> Dataframe:
        return self

    def join(self, table: Dataframe, condition: Callable) -> Dataframe:
        return self

    def left_join(self, table: Dataframe, condition: Callable) -> Dataframe:
        return self

    def right_join(self, table: Dataframe, condition: Callable) -> Dataframe:
        return self

    def outer_join(self, table: Dataframe, condition: Callable) -> Dataframe:
        return self

    def asof_join(self, table: Dataframe, condition: Callable) -> Dataframe:
        return self

    def order_by(self, columns: Callable) -> Dataframe:
        return self

    def having(self, condition: Callable) -> Dataframe:
        return self

    def qualify(self, condition: Callable) -> Dataframe:
        # example: qualify(lambda df, x: (df.row_num <= 2) and (x.salary > 50000))
        return self

    def limit(self, limit: int) -> Dataframe:
        return self

    def offset(self, offset: int) -> Dataframe:
        return self

    def to_sql(self, dialect: str = "duckdb") -> str:
        return ""