"""
Integration tests for QUALIFY clause with DuckDB.

This module contains tests for using the QUALIFY clause to filter window function
results using the cloud-dataframe library with DuckDB.
"""
import unittest
import pandas as pd
import duckdb
from typing import Optional

from cloud_dataframe.core.dataframe import DataFrame, Sort
from cloud_dataframe.type_system.schema import TableSchema
from cloud_dataframe.type_system.column import (
    row_number, rank, dense_rank, sum, avg,
    row, range, unbounded, window
)


class TestQualifyDuckDB(unittest.TestCase):
    """Test cases for QUALIFY clause with DuckDB."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.conn = duckdb.connect(":memory:")
        
        employees_data = pd.DataFrame({
            "id": [1, 2, 3, 4, 5, 6, 7, 8],
            "name": ["Alice", "Bob", "Charlie", "Dave", "Eve", "Frank", "Grace", "Heidi"],
            "department": ["Engineering", "Engineering", "Engineering", "Sales", "Sales", "Marketing", "Marketing", "Marketing"],
            "location": ["NY", "SF", "NY", "SF", "NY", "SF", "NY", "SF"],
            "salary": [100000, 120000, 110000, 90000, 95000, 105000, 115000, 125000]
        })
        
        self.conn.register("employees_data", employees_data)
        self.conn.execute("CREATE TABLE employees AS SELECT * FROM employees_data")
        
        self.schema = TableSchema(
            name="Employees",
            columns={
                "id": int,
                "name": str,
                "department": str,
                "location": str,
                "salary": int,
            }
        )
        
        self.df = DataFrame.from_table_schema("employees", self.schema, alias="x")
    
    def tearDown(self):
        """Tear down test fixtures."""
        self.conn.close()
    
    def test_qualify_with_row_number(self):
        """Test QUALIFY clause with row_number() function."""
        query = self.df.select(
            lambda x: x.id,
            lambda x: x.name,
            lambda x: x.department,
            lambda x: x.salary,
            lambda x: (row_num := window(func=row_number(), partition=x.department, order_by=x.salary))
        ).qualify(
            lambda df: df.row_num <= 2  # Get top 2 employees by salary in each department
        ).order_by(
            lambda x: [x.department, x.salary]
        )
        
        sql = query.to_sql(dialect="duckdb")
        expected_sql = "SELECT x.id, x.name, x.department, x.salary, ROW_NUMBER() OVER (PARTITION BY x.department ORDER BY x.salary ASC) AS row_num\nFROM employees x\nQUALIFY row_num <= 2\nORDER BY x.department ASC, x.salary ASC"
        self.assertEqual(sql.strip(), expected_sql.strip())
        
        result = self.conn.execute(sql).fetchdf()
        
        self.assertEqual(len(result), 6)  # 2 employees from each of the 3 departments
        
        for dept in ["Engineering", "Sales", "Marketing"]:
            dept_rows = result[result["department"] == dept]
            self.assertEqual(len(dept_rows), 2)  # 2 employees per department
            self.assertTrue(all(dept_rows["row_num"] <= 2))  # All row_num values are <= 2
    
    def test_qualify_with_rank(self):
        """Test QUALIFY clause with rank() function."""
        query = self.df.select(
            lambda x: x.id,
            lambda x: x.name,
            lambda x: x.department,
            lambda x: x.salary,
            lambda x: (rank_val := window(func=rank(), partition=x.department, order_by=x.salary))
        ).qualify(
            lambda df: df.rank_val == 1  # Get employees with the lowest salary in each department
        ).order_by(
            lambda x: [x.department, x.salary]
        )
        
        sql = query.to_sql(dialect="duckdb")
        expected_sql = "SELECT x.id, x.name, x.department, x.salary, RANK() OVER (PARTITION BY x.department ORDER BY x.salary ASC) AS rank_val\nFROM employees x\nQUALIFY rank_val = 1\nORDER BY x.department ASC, x.salary ASC"
        self.assertEqual(sql.strip(), expected_sql.strip())
        
        result = self.conn.execute(sql).fetchdf()
        
        self.assertEqual(len(result), 3)  # 1 employee from each of the 3 departments
        
        departments = result["department"].unique()
        self.assertEqual(len(departments), 3)
        for dept in departments:
            dept_rows = result[result["department"] == dept]
            self.assertEqual(len(dept_rows), 1)
            self.assertEqual(dept_rows["rank_val"].iloc[0], 1)
    
    def test_qualify_with_dense_rank(self):
        """Test QUALIFY clause with dense_rank() function."""
        query = self.df.select(
            lambda x: x.id,
            lambda x: x.name,
            lambda x: x.department,
            lambda x: x.location,
            lambda x: x.salary,
            lambda x: (dense_rank_val := window(func=dense_rank(), partition=x.department, order_by=x.salary))
        ).qualify(
            lambda df: df.dense_rank_val <= 2  # Get employees with the two lowest salary ranks in each department
        ).order_by(
            lambda x: [x.department, x.salary]
        )
        
        sql = query.to_sql(dialect="duckdb")
        expected_sql = "SELECT x.id, x.name, x.department, x.location, x.salary, DENSE_RANK() OVER (PARTITION BY x.department ORDER BY x.salary ASC) AS dense_rank_val\nFROM employees x\nQUALIFY dense_rank_val <= 2\nORDER BY x.department ASC, x.salary ASC"
        self.assertEqual(sql.strip(), expected_sql.strip())
        
        result = self.conn.execute(sql).fetchdf()
        
        departments = result["department"].unique()
        self.assertEqual(len(departments), 3)
        
        for dept in departments:
            dept_rows = result[result["department"] == dept]
            self.assertTrue(len(dept_rows) >= 1)
            self.assertTrue(all(dept_rows["dense_rank_val"] <= 2))
    
    def test_qualify_with_desc_ordering(self):
        """Test QUALIFY clause with descending order in window function."""
        query = self.df.select(
            lambda x: x.id,
            lambda x: x.name,
            lambda x: x.department,
            lambda x: x.salary,
            lambda x: (row_num := window(func=row_number(), partition=x.department, order_by=(x.salary, Sort.DESC)))
        ).qualify(
            lambda df: df.row_num <= 2  # Get top 2 highest paid employees in each department
        ).order_by(
            lambda x: [x.department, (x.salary, Sort.DESC)]
        )
        
        sql = query.to_sql(dialect="duckdb")
        expected_sql = "SELECT x.id, x.name, x.department, x.salary, ROW_NUMBER() OVER (PARTITION BY x.department ORDER BY x.salary DESC) AS row_num\nFROM employees x\nQUALIFY row_num <= 2\nORDER BY x.department ASC, x.salary DESC"
        self.assertEqual(sql.strip(), expected_sql.strip())
        
        result = self.conn.execute(sql).fetchdf()
        
        self.assertEqual(len(result), 6)  # 2 employees from each of the 3 departments
        
        for dept in ["Engineering", "Sales", "Marketing"]:
            dept_rows = result[result["department"] == dept]
            self.assertEqual(len(dept_rows), 2)
            self.assertTrue(all(dept_rows["row_num"] <= 2))
            
            if len(dept_rows) > 1:
                self.assertTrue(dept_rows["salary"].iloc[0] >= dept_rows["salary"].iloc[1])
    
    def test_qualify_with_complex_condition(self):
        """Test QUALIFY clause with complex condition."""
        query = self.df.select(
            lambda x: x.id,
            lambda x: x.name,
            lambda x: x.department,
            lambda x: x.location,
            lambda x: x.salary,
            lambda x: (dept_rank := window(func=rank(), partition=x.department, order_by=x.salary)),
            lambda x: (loc_rank := window(func=rank(), partition=x.location, order_by=x.salary))
        ).qualify(
            lambda df: (df.dept_rank <= 2) and (df.loc_rank <= 2)  # Top 2 in both department and location
        ).order_by(
            lambda x: [x.department, x.location, x.salary]
        )
        
        sql = query.to_sql(dialect="duckdb")
        expected_sql = "SELECT x.id, x.name, x.department, x.location, x.salary, RANK() OVER (PARTITION BY x.department ORDER BY x.salary ASC) AS dept_rank, RANK() OVER (PARTITION BY x.location ORDER BY x.salary ASC) AS loc_rank\nFROM employees x\nQUALIFY dept_rank <= 2 AND loc_rank <= 2\nORDER BY x.department ASC, x.location ASC, x.salary ASC"
        self.assertEqual(sql.strip(), expected_sql.strip())
        
        result = self.conn.execute(sql).fetchdf()
        
        self.assertTrue(len(result) > 0)
        
        self.assertTrue(all(result["dept_rank"] <= 2))
        self.assertTrue(all(result["loc_rank"] <= 2))


if __name__ == "__main__":
    unittest.main()
