"""
Debug script to investigate how DuckDB handles boolean values.
"""
import duckdb

conn = duckdb.connect(":memory:")

conn.execute("""
    CREATE TABLE test_booleans (
        id INTEGER,
        value BOOLEAN
    )
""")

conn.execute("""
    INSERT INTO test_booleans VALUES
    (1, TRUE),
    (2, FALSE)
""")

result = conn.execute("SELECT * FROM test_booleans").fetchall()
print("Raw boolean values from DuckDB:")
for row in result:
    print(f"ID: {row[0]}, Value: {row[1]}, Type: {type(row[1])}")

result = conn.execute("SELECT id, value, value = TRUE FROM test_booleans").fetchall()
print("\nBoolean expressions:")
for row in result:
    print(f"ID: {row[0]}, Value: {row[1]}, Value = TRUE: {row[2]}")

print("\nPython comparison:")
for row in result:
    value = row[1]
    print(f"ID: {row[0]}, Value: {value}, Value == True: {value == True}")
