"""
Debug script for scalar functions in cloud-dataframe.

This script helps debug and test the scalar function implementation by creating
DataFrames with scalar functions and printing the generated SQL.
"""

import ast
import inspect
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from cloud_dataframe.core.dataframe import DataFrame
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.functions import (
    date_diff, concat, upper, lower, round, ceil, floor, date_part, date_trunc
)


def print_sql(df: DataFrame, title: str = None):
    """
    Print the SQL generated for a DataFrame.
    
    Args:
        df: The DataFrame to generate SQL for
        title: Optional title to print before the SQL
    """
    if title:
        print(f"\n{title}:")
    
    sql = df.to_sql()
    print(sql)
    print("-" * 80)


def test_date_functions():
    """Test date functions."""
    print("\n=== Testing Date Functions ===")
    
    schema = TableSchema(
        name="Orders",
        columns={
            "order_id": int,
            "customer_id": int,
            "order_date": str,
            "ship_date": str
        }
    )
    
    df = DataFrame.from_table_schema("orders", schema)
    
    df_date_diff = df.select(
        lambda x: x.order_id,
        lambda x: (days_to_ship := date_diff(x.order_date, x.ship_date))
    )
    
    print_sql(df_date_diff, "DATE_DIFF Function")


def test_string_functions():
    """Test string functions."""
    print("\n=== Testing String Functions ===")
    
    schema = TableSchema(
        name="Customers",
        columns={
            "customer_id": int,
            "first_name": str,
            "last_name": str,
            "email": str
        }
    )
    
    df = DataFrame.from_table_schema("customers", schema)
    
    df_concat = df.select(
        lambda x: x.customer_id,
        lambda x: (full_name := concat(x.first_name, " ", x.last_name))
    )
    
    print_sql(df_concat, "CONCAT Function")
    
    df_upper = df.select(
        lambda x: x.customer_id,
        lambda x: (first_name_upper := upper(x.first_name))
    )
    
    print_sql(df_upper, "UPPER Function")
    
    df_lower = df.select(
        lambda x: x.customer_id,
        lambda x: (email_lower := lower(x.email))
    )
    
    print_sql(df_lower, "LOWER Function")


def test_math_functions():
    """Test math functions."""
    print("\n=== Testing Math Functions ===")
    
    schema = TableSchema(
        name="Products",
        columns={
            "product_id": int,
            "price": float,
            "cost": float,
            "weight": float
        }
    )
    
    df = DataFrame.from_table_schema("products", schema)
    
    df_round = df.select(
        lambda x: x.product_id,
        lambda x: (price_rounded := round(x.price, 2))
    )
    
    print_sql(df_round, "ROUND Function")
    
    df_ceil = df.select(
        lambda x: x.product_id,
        lambda x: (price_ceiling := ceil(x.price))
    )
    
    print_sql(df_ceil, "CEIL Function")
    
    df_floor = df.select(
        lambda x: x.product_id,
        lambda x: (price_floor := floor(x.price))
    )
    
    print_sql(df_floor, "FLOOR Function")


def test_time_functions():
    """Test time functions."""
    print("\n=== Testing Time Functions ===")
    
    schema = TableSchema(
        name="Orders",
        columns={
            "order_id": int,
            "order_date": str,
            "order_time": str
        }
    )
    
    df = DataFrame.from_table_schema("orders", schema)
    
    df_date_part = df.select(
        lambda x: x.order_id,
        lambda x: (order_year := date_part("year", x.order_date)),
        lambda x: (order_month := date_part("month", x.order_date))
    )
    
    print_sql(df_date_part, "DATE_PART Function")
    
    df_date_trunc = df.select(
        lambda x: x.order_id,
        lambda x: (order_month_start := date_trunc("month", x.order_date))
    )
    
    print_sql(df_date_trunc, "DATE_TRUNC Function")


def test_function_composition():
    """Test function composition."""
    print("\n=== Testing Function Composition ===")
    
    schema = TableSchema(
        name="Orders",
        columns={
            "order_id": int,
            "customer_id": int,
            "order_date": str,
            "total_amount": float
        }
    )
    
    df = DataFrame.from_table_schema("orders", schema)
    
    df_composition = df.select(
        lambda x: x.order_id,
        lambda x: (days_from_month_start := round(
            date_diff(
                date_trunc("month", x.order_date),
                x.order_date
            ),
            0
        ))
    )
    
    print_sql(df_composition, "Function Composition")


if __name__ == "__main__":
    test_date_functions()
    test_string_functions()
    test_math_functions()
    test_time_functions()
    test_function_composition()
