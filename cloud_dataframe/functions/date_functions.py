"""
Date and time functions for the DataFrame DSL.

This module provides implementations of date and time manipulation functions
that can work across different SQL backends.
"""
from typing import List

from cloud_dataframe.functions.base import ScalarFunction


class DateDiffFunction(ScalarFunction):
    """
    Calculates the difference between two dates in the specified part.
    
    Example:
        lambda x: date_diff('day', x.start_date, x.end_date)
    """
    function_name = "date_diff"
    parameter_types = [("part", str), ("startdate", "date"), ("enddate", "date")]
    return_type = int
    
    def __init__(self, parameters: List):
        super().__init__(parameters)
    
    def to_sql_default(self, backend_context):
        """Default implementation (DuckDB)"""
        part_sql = self.parameters[0].to_sql(backend_context)
        start_sql = self.parameters[1].to_sql(backend_context)
        end_sql = self.parameters[2].to_sql(backend_context)
        return f"DATE_DIFF({part_sql}, CAST({start_sql} AS DATE), CAST({end_sql} AS DATE))"
    
    def to_sql_postgres(self, backend_context):
        """PostgreSQL-specific implementation"""
        part_sql = self.parameters[0].to_sql(backend_context)
        start_sql = self.parameters[1].to_sql(backend_context)
        end_sql = self.parameters[2].to_sql(backend_context)
        return f"EXTRACT(EPOCH FROM ({end_sql}::timestamp - {start_sql}::timestamp))/86400"


class DatePartFunction(ScalarFunction):
    """
    Extracts a part from a date.
    
    Example:
        lambda x: date_part('year', x.date)
    """
    function_name = "date_part"
    parameter_types = [("part", str), ("date", "date")]
    return_type = int
    
    def __init__(self, parameters: List):
        super().__init__(parameters)
    
    def to_sql_default(self, backend_context):
        """Default implementation (DuckDB)"""
        part_sql = self.parameters[0].to_sql(backend_context)
        date_sql = self.parameters[1].to_sql(backend_context)
        return f"DATE_PART({part_sql}, CAST({date_sql} AS DATE))"
    
    def to_sql_postgres(self, backend_context):
        """PostgreSQL-specific implementation"""
        part_sql = self.parameters[0].to_sql(backend_context)
        date_sql = self.parameters[1].to_sql(backend_context)
        return f"EXTRACT({part_sql} FROM {date_sql})"


class DateTruncFunction(ScalarFunction):
    """
    Truncates a date to the specified part.
    
    Example:
        lambda x: date_trunc('month', x.date)
    """
    function_name = "date_trunc"
    parameter_types = [("part", str), ("date", "date")]
    return_type = "date"
    
    def __init__(self, parameters: List):
        super().__init__(parameters)
    
    def to_sql_default(self, backend_context):
        """Default implementation (DuckDB)"""
        part_sql = self.parameters[0].to_sql(backend_context)
        date_sql = self.parameters[1].to_sql(backend_context)
        return f"DATE_TRUNC({part_sql}, CAST({date_sql} AS DATE))"


class CurrentDateFunction(ScalarFunction):
    """
    Returns the current date.
    
    Example:
        lambda x: current_date()
    """
    function_name = "current_date"
    parameter_types = []
    return_type = "date"
    
    def __init__(self, parameters: List):
        super().__init__(parameters)
    
    def to_sql_default(self, backend_context):
        """Default implementation (DuckDB)"""
        return "CURRENT_DATE()"
    
    def to_sql_postgres(self, backend_context):
        """PostgreSQL-specific implementation"""
        return "CURRENT_DATE"


class DateAddFunction(ScalarFunction):
    """
    Adds an interval to a date.
    
    Example:
        lambda x: date_add('day', 7, x.date)
    """
    function_name = "date_add"
    parameter_types = [("part", str), ("interval", int), ("date", "date")]
    return_type = "date"
    
    def __init__(self, parameters: List):
        super().__init__(parameters)
    
    def to_sql_default(self, backend_context):
        """Default implementation (DuckDB)"""
        part = self.parameters[0].to_sql(backend_context).strip("'")
        interval_sql = self.parameters[1].to_sql(backend_context)
        date_sql = self.parameters[2].to_sql(backend_context)
        return f"(CAST({date_sql} AS DATE) + INTERVAL {interval_sql} {part})"
    
    def to_sql_postgres(self, backend_context):
        """PostgreSQL-specific implementation"""
        part_sql = self.parameters[0].to_sql(backend_context)
        interval_sql = self.parameters[1].to_sql(backend_context)
        date_sql = self.parameters[2].to_sql(backend_context)
        return f"({date_sql} + INTERVAL '{interval_sql} {part_sql}')"


class DateSubFunction(ScalarFunction):
    """
    Subtracts an interval from a date.
    
    Example:
        lambda x: date_sub('day', 7, x.date)
    """
    function_name = "date_sub"
    parameter_types = [("part", str), ("interval", int), ("date", "date")]
    return_type = "date"
    
    def __init__(self, parameters: List):
        super().__init__(parameters)
    
    def to_sql_default(self, backend_context):
        """Default implementation (DuckDB)"""
        part = self.parameters[0].to_sql(backend_context).strip("'")
        interval_sql = self.parameters[1].to_sql(backend_context)
        date_sql = self.parameters[2].to_sql(backend_context)
        return f"(CAST({date_sql} AS DATE) - INTERVAL {interval_sql} {part})"
    
    def to_sql_postgres(self, backend_context):
        """PostgreSQL-specific implementation"""
        part_sql = self.parameters[0].to_sql(backend_context)
        interval_sql = self.parameters[1].to_sql(backend_context)
        date_sql = self.parameters[2].to_sql(backend_context)
        return f"({date_sql} - INTERVAL '{interval_sql} {part_sql}')"
