"""
Integration tests for UNION and UNION ALL operations with DuckDB.

Tests verifying both SQL generation and execution results for
UNION and UNION ALL operations following DuckDB's implementation.
"""
import unittest
import duckdb

from cloud_dataframe.core.dataframe import DataFrame
from cloud_dataframe.type_system.column import col, literal


class TestUnionOperationsDuckDB(unittest.TestCase):
    """Test cases for UNION and UNION ALL operations with DuckDB."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.db_path = ":memory:"
        self.conn = duckdb.connect(self.db_path)
        
        self.conn.execute("""
            CREATE TABLE employees1 (
                id INTEGER,
                name VARCHAR,
                department VARCHAR
            )
        """)
        
        self.conn.execute("""
            CREATE TABLE employees2 (
                id INTEGER,
                name VARCHAR,
                department VARCHAR
            )
        """)
        
        self.conn.execute("""
            INSERT INTO employees1 VALUES
            (1, 'Alice', 'Engineering'),
            (2, 'Bob', 'Sales'),
            (3, 'Carol', 'Marketing')
        """)
        
        self.conn.execute("""
            INSERT INTO employees2 VALUES
            (3, 'Carol', 'Marketing'),
            (4, 'Dave', 'Engineering'),
            (5, 'Eve', 'Sales')
        """)
        
        self.df1 = DataFrame.from_("employees1", alias="e1")
        self.df2 = DataFrame.from_("employees2", alias="e2")
    
    def tearDown(self):
        """Tear down test fixtures."""
        self.conn.close()
    
    def test_union_sql_generation(self):
        """Test SQL generation for UNION operation."""
        union_df = self.df1.union(self.df2)
        
        sql = union_df.to_sql(dialect="duckdb")
        
        self.assertIn("UNION", sql)
        self.assertNotIn("UNION ALL", sql)
        
        self.assertIn("SELECT", sql)
        
    def test_union_all_sql_generation(self):
        """Test SQL generation for UNION ALL operation."""
        union_all_df = self.df1.union_all(self.df2)
        
        sql = union_all_df.to_sql(dialect="duckdb")
        
        self.assertIn("UNION ALL", sql)
        
        self.assertIn("SELECT", sql)
    
    def test_union_results(self):
        """Test results of UNION operation (set semantics - no duplicates)."""
        union_df = self.df1.union(self.df2)
        
        sql = union_df.to_sql(dialect="duckdb")
        result = self.conn.execute(sql).fetchall()
        
        self.assertEqual(len(result), 5)
        
        direct_sql = """
        SELECT * FROM employees1
        UNION
        SELECT * FROM employees2
        """
        direct_result = self.conn.execute(direct_sql).fetchall()
        
        self.assertEqual(len(result), len(direct_result))
        
        ids = [row[0] for row in result]
        self.assertIn(1, ids)  # Alice
        self.assertIn(2, ids)  # Bob
        self.assertIn(3, ids)  # Carol (only once)
        self.assertIn(4, ids)  # Dave
        self.assertIn(5, ids)  # Eve
    
    def test_union_all_results(self):
        """Test results of UNION ALL operation (bag semantics - with duplicates)."""
        union_all_df = self.df1.union_all(self.df2)
        
        sql = union_all_df.to_sql(dialect="duckdb")
        result = self.conn.execute(sql).fetchall()
        
        self.assertEqual(len(result), 6)
        
        direct_sql = """
        SELECT * FROM employees1
        UNION ALL
        SELECT * FROM employees2
        """
        direct_result = self.conn.execute(direct_sql).fetchall()
        
        self.assertEqual(len(result), len(direct_result))
        
        carol_count = sum(1 for row in result if row[0] == 3)
        self.assertEqual(carol_count, 2)
    
    def test_mismatched_schemas(self):
        """Test error handling for mismatched schemas."""
        self.conn.execute("""
            CREATE TABLE employees3 (
                id INTEGER,
                name VARCHAR,
                department VARCHAR,
                salary FLOAT
            )
        """)
        
        self.conn.execute("""
            INSERT INTO employees3 VALUES
            (6, 'Frank', 'HR', 85000)
        """)
        
        df3 = DataFrame.from_("employees3", alias="e3")
        
        union_df = self.df1.union(df3)
        sql = union_df.to_sql(dialect="duckdb")
        
        with self.assertRaises(Exception):
            self.conn.execute(sql).fetchall()
    
    def test_union_with_select(self):
        """Test UNION with SELECT operations."""
        direct_sql = """
        SELECT id, name FROM employees1
        UNION
        SELECT id, name FROM employees2
        """
        direct_result = self.conn.execute(direct_sql).fetchall()
        self.assertEqual(len(direct_result), 5)  # 5 unique rows
        
        df1_selected = self.df1.select(lambda x: x.id, lambda x: x.name)
        df2_selected = self.df2.select(lambda x: x.id, lambda x: x.name)
        
        union_df = df1_selected.union(df2_selected)
        sql = union_df.to_sql(dialect="duckdb")
        
        self.assertIn("SELECT", sql)
        self.assertIn("UNION", sql)
        self.assertNotIn("UNION ALL", sql)


if __name__ == "__main__":
    unittest.main()
