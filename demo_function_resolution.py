"""
Demo script to demonstrate function resolution in lambda expressions.
"""
import logging
import unittest

from cloud_dataframe.core.dataframe import DataFrame
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.functions.registry import FunctionRegistry

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class TestFunctionResolution(unittest.TestCase):
    """Test cases for function resolution in lambda expressions."""
    
    def setUp(self):
        """Set up the test schema."""
        self.schema = TableSchema(
            name="Employee",
            columns={
                "id": int,
                "name": str,
                "start_date": str,
                "end_date": str
            }
        )
        
        self.df = DataFrame.from_table_schema("employees", self.schema)
    
    def test_direct_function_call(self):
        """Test direct function call without wrapper."""
        df = self.df.select(
            lambda x: x.name,
            lambda x: (days_employed := date_diff('day', x.start_date, x.end_date))
        )
        
        sql = df.to_sql(dialect="duckdb")
        print("\nDirect function call SQL:")
        print(sql)
        
        self.assertIn("DATE_DIFF", sql)
    
    def test_with_wrapper_function(self):
        """Test with wrapper function."""
        def date_diff(unit, start_date, end_date):
            """Wrapper for DateDiffFunction to use in lambda expressions."""
            return FunctionRegistry.create_function("date_diff", [unit, start_date, end_date])
        
        df = self.df.select(
            lambda x: x.name,
            lambda x: (days_employed := date_diff('day', x.start_date, x.end_date))
        )
        
        sql = df.to_sql(dialect="duckdb")
        print("\nWrapper function SQL:")
        print(sql)
        
        self.assertIn("DATE_DIFF", sql)

if __name__ == "__main__":
    unittest.main()
