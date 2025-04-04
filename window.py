from dataclasses import dataclass
from typing import Optional, Union

from dataframe import Expression, FunctionExpression

class Unbounded:
    pass

@dataclass
class Frame:
    start: Union[int, Unbounded]
    end: Union[int, Unbounded]

class OverFunction:
    def __init__(self,
        func: Optional[Union[FunctionExpression, list[FunctionExpression]]],
        partition: Optional[Union[Expression, list[Expression]]],
        order_by: Optional[Union[Expression, list[Expression]]],
        frame: Optional[Frame]):
        return

over =  OverFunction
unbounded = Unbounded
rows = Frame
range = Frame