"""
Integration tests for QUALIFY clause with DuckDB.

This module contains tests for using the QUALIFY clause to filter window function
results using the cloud-dataframe library with DuckDB.
"""
import unittest
import duckdb
from typing import Optional, Dict, List, Any, Tuple

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
        
        self.conn.execute("""
            CREATE TABLE employees AS
            SELECT 1 AS id, 'Alice' AS name, 'Engineering' AS department, 'NY' AS location, 100000 AS salary UNION ALL
            SELECT 2, 'Bob', 'Engineering', 'SF', 120000 UNION ALL
            SELECT 3, 'Charlie', 'Engineering', 'NY', 110000 UNION ALL
            SELECT 4, 'Dave', 'Sales', 'SF', 90000 UNION ALL
            SELECT 5, 'Eve', 'Sales', 'NY', 95000 UNION ALL
            SELECT 6, 'Frank', 'Marketing', 'SF', 105000 UNION ALL
            SELECT 7, 'Grace', 'Marketing', 'NY', 115000 UNION ALL
            SELECT 8, 'Heidi', 'Marketing', 'SF', 125000
        """)
        
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
        expected_sql = "SELECT x.id, x.name, x.department, x.salary, ROW_NUMBER() OVER (PARTITION BY x.department ORDER BY x.salary ASC) AS row_num\nFROM employees AS x\nQUALIFY row_num <= 2\nORDER BY x.department ASC, x.salary ASC"
        self.assertEqual(sql.strip(), expected_sql.strip())
        
        result = self.conn.execute(sql).fetchall()
        
        column_names = ["id", "name", "department", "salary", "row_num"]
        result_dicts = [dict(zip(column_names, row)) for row in result]
        
        self.assertEqual(len(result_dicts), 6)  # 2 employees from each of the 3 departments
        
        for dept in ["Engineering", "Sales", "Marketing"]:
            dept_rows = [row for row in result_dicts if row["department"] == dept]
            self.assertEqual(len(dept_rows), 2)  # 2 employees per department
            self.assertTrue(all(row["row_num"] <= 2 for row in dept_rows))  # All row_num values are <= 2
    
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
        expected_sql = "SELECT x.id, x.name, x.department, x.salary, RANK() OVER (PARTITION BY x.department ORDER BY x.salary ASC) AS rank_val\nFROM employees AS x\nQUALIFY rank_val = 1\nORDER BY x.department ASC, x.salary ASC"
        self.assertEqual(sql.strip(), expected_sql.strip())
        
        result = self.conn.execute(sql).fetchall()
        
        column_names = ["id", "name", "department", "salary", "rank_val"]
        result_dicts = [dict(zip(column_names, row)) for row in result]
        
        self.assertEqual(len(result_dicts), 3)  # 1 employee from each of the 3 departments
        
        departments = set(row["department"] for row in result_dicts)
        self.assertEqual(len(departments), 3)
        
        for dept in departments:
            dept_rows = [row for row in result_dicts if row["department"] == dept]
            self.assertEqual(len(dept_rows), 1)
            self.assertEqual(dept_rows[0]["rank_val"], 1)
    
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
        expected_sql = "SELECT x.id, x.name, x.department, x.location, x.salary, DENSE_RANK() OVER (PARTITION BY x.department ORDER BY x.salary ASC) AS dense_rank_val\nFROM employees AS x\nQUALIFY dense_rank_val <= 2\nORDER BY x.department ASC, x.salary ASC"
        self.assertEqual(sql.strip(), expected_sql.strip())
        
        result = self.conn.execute(sql).fetchall()
        
        column_names = ["id", "name", "department", "location", "salary", "dense_rank_val"]
        result_dicts = [dict(zip(column_names, row)) for row in result]
        
        departments = set(row["department"] for row in result_dicts)
        self.assertEqual(len(departments), 3)
        
        for dept in departments:
            dept_rows = [row for row in result_dicts if row["department"] == dept]
            self.assertTrue(len(dept_rows) >= 1)
            self.assertTrue(all(row["dense_rank_val"] <= 2 for row in dept_rows))
    
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
        expected_sql = "SELECT x.id, x.name, x.department, x.salary, ROW_NUMBER() OVER (PARTITION BY x.department ORDER BY x.salary DESC) AS row_num\nFROM employees AS x\nQUALIFY row_num <= 2\nORDER BY x.department ASC, x.salary DESC"
        self.assertEqual(sql.strip(), expected_sql.strip())
        
        result = self.conn.execute(sql).fetchall()
        
        column_names = ["id", "name", "department", "salary", "row_num"]
        result_dicts = [dict(zip(column_names, row)) for row in result]
        
        self.assertEqual(len(result_dicts), 6)  # 2 employees from each of the 3 departments
        
        for dept in ["Engineering", "Sales", "Marketing"]:
            dept_rows = [row for row in result_dicts if row["department"] == dept]
            self.assertEqual(len(dept_rows), 2)
            self.assertTrue(all(row["row_num"] <= 2 for row in dept_rows))
            
            if len(dept_rows) > 1:
                self.assertTrue(dept_rows[0]["salary"] >= dept_rows[1]["salary"])
    
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
        expected_sql = "SELECT x.id, x.name, x.department, x.location, x.salary, RANK() OVER (PARTITION BY x.department ORDER BY x.salary ASC) AS dept_rank, RANK() OVER (PARTITION BY x.location ORDER BY x.salary ASC) AS loc_rank\nFROM employees AS x\nQUALIFY dept_rank <= 2 AND loc_rank <= 2\nORDER BY x.department ASC, x.location ASC, x.salary ASC"
        self.assertEqual(sql.strip(), expected_sql.strip())
        
        result = self.conn.execute(sql).fetchall()
        
        column_names = ["id", "name", "department", "location", "salary", "dept_rank", "loc_rank"]
        result_dicts = [dict(zip(column_names, row)) for row in result]
        
        self.assertTrue(len(result_dicts) > 0)
        
        self.assertTrue(all(row["dept_rank"] <= 2 for row in result_dicts))
        self.assertTrue(all(row["loc_rank"] <= 2 for row in result_dicts))


if __name__ == "__main__":
    unittest.main()
