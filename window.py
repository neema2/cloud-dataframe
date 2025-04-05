from dataclasses import dataclass
from typing import Optional, Union

from metamodel import Expression, FunctionExpression

class Unbounded:
    pass

@dataclass
class Frame:
    start: Union[int, Unbounded]
    end: Union[int, Unbounded]

class OverFunction:
    def __init__(self, columns: Union[Expression, list[Expression]],
                 functions: Union[FunctionExpression, list[FunctionExpression]],
                 sort: Optional[Union[Expression, list[Expression]]],
                 frame: Optional[Frame],
                 filter: Optional[Expression] = None):
        return

over =  OverFunction
unbounded = Unbounded
rows = Frame
range = Frame