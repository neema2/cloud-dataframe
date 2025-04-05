from dataclasses import dataclass
from typing import Optional, Union

from metamodel import Expression, FunctionExpression

class AggregationFunction:
    def __init__(self,
                 columns: Union[Expression, list[Expression]],
                 functions: Union[FunctionExpression, list[FunctionExpression]],
                 filter: Optional[Expression] = None):
        return

aggregate = AggregationFunction