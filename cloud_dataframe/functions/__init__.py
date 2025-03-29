"""
Cloud DataFrame Functions Module

This module contains implementations of SQL scalar functions for different backends.
"""
from .registry import FunctionRegistry
from .string_functions import (
    UpperFunction, LowerFunction, ConcatFunction, 
    SubstringFunction, LengthFunction, ReplaceFunction
)
from .date_functions import (
    DateDiffFunction, DatePartFunction, DateTruncFunction,
    CurrentDateFunction, DateAddFunction, DateSubFunction
)
from .numeric_functions import (
    AbsFunction, RoundFunction, CeilFunction, FloorFunction,
    PowerFunction, SqrtFunction, ModFunction
)

upper = UpperFunction
lower = LowerFunction
concat = ConcatFunction
substring = SubstringFunction
length = LengthFunction
replace = ReplaceFunction

date_diff = DateDiffFunction
date_part = DatePartFunction
date_trunc = DateTruncFunction
current_date = CurrentDateFunction
date_add = DateAddFunction
date_sub = DateSubFunction

abs = AbsFunction
round = RoundFunction
ceil = CeilFunction
floor = FloorFunction
power = PowerFunction
sqrt = SqrtFunction
mod = ModFunction
