from typing import Optional

from legendql import FunctionExpression


class AggregateFunction(FunctionExpression):
    function_name = None
    parameter_types = []
    return_type = None

class ScalarFunction(FunctionExpression):
    function_name = None
    parameter_types = []
    return_type = None

class LeftFunction(ScalarFunction):

    def __init__(self, expression: str, chars: int):
        return

    function_name = "left"
    parameter_types = [("text", str)]
    return_type = str

class AverageFunction(AggregateFunction):

    def __init__(self, expression: str):
        return

    function_name = "avg"
    parameter_types = [("column", float)]
    return_type = float

class CountFunction(AggregateFunction):

    def __init__(self, expression: Optional[str] = "1"):
        return

    function_name = "avg"
    parameter_types = [("column", float)]
    return_type = float

left = LeftFunction
avg = AverageFunction
count = CountFunction