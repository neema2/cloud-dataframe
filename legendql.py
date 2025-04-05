from __future__ import annotations

from typing import Callable, Type, Dict, List

from metamodel import Query
from window import *
from functions import *
from aggregate import *

class LegendQL:

    def __init__(self, name: str, columns: Dict[str, Type]):
        self.name = name
        self.columns = columns
        self.query: List[Query] = []

    @classmethod
    def from_(cls, name: str, columns: Dict[str, Type]) -> LegendQL:
        return LegendQL(name, columns)

    def let(self, df : LegendQL) -> LegendQL:
        # CommonTableExpression ("with" in SQL)
        return self

    def recurse(self, df : LegendQL) -> LegendQL:
        # CommonTableExpression ("with recursive" in SQL)
        return self

    def distinct(self) -> LegendQL:
        return self

    def select(self, columns: Callable) -> LegendQL:
        return self

    def extend(self, columns: Callable) -> LegendQL:
        return self

    def filter(self, condition: Callable) -> LegendQL:
        return self

    def group_by(self, columns: Callable) -> LegendQL:
        return self

    def join(self, table: LegendQL, condition: Callable, select: Optional[Callable]) -> LegendQL:
        return self

    def left_join(self, table: LegendQL, condition: Callable, select: Optional[Callable]) -> LegendQL:
        return self

    def right_join(self, table: LegendQL, condition: Callable, select: Optional[Callable]) -> LegendQL:
        return self

    def outer_join(self, table: LegendQL, condition: Callable, select: Optional[Callable]) -> LegendQL:
        return self

    def asof_join(self, table: LegendQL, condition: Callable, select: Optional[Callable]) -> LegendQL:
        return self

    def order_by(self, columns: Callable) -> LegendQL:
        return self

    def qualify(self, condition: Callable) -> LegendQL:
        # example: qualify(lambda df, x: (df.row_num <= 2) and (x.salary > 50000))
        return self

    def limit(self, limit: int) -> LegendQL:
        return self

    def offset(self, offset: int) -> LegendQL:
        return self

    def to_sql(self, dialect: str = "duckdb") -> str:
        return ""

    def validate_column(self, column_name: str) -> bool:
        """
        Validate that a column exists in the schema.

        Args:
            column_name: The name of the column to validate

        Returns:
            True if the column exists, False otherwise
        """
        return column_name in self.columns
