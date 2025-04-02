from cloud_dataframe.core.dataframe import DataFrame

def main():
    """Validate the issue with AS column aliases in pure relation syntax."""
    # Create a simple DataFrame with a select operation
    df = DataFrame.from_("employees", alias="employees_0")
    
    selected_df = df.select(
        lambda employees_0: (id := employees_0.id),
        lambda employees_0: (name := employees_0.name),
        lambda employees_0: (salary := employees_0.salary)
    )
    
    # Generate Pure code with pure_relation dialect
    pure_code = selected_df.to_sql(dialect="pure_relation")
    
    print("\n=== Generated Pure Code ===")
    print(pure_code.strip())
    
    # Expected format without AS clauses
    expected_without_as = "$employees->select(~[id, name, salary])"
    print("\n=== Expected Format (without AS) ===")
    print(expected_without_as)
    
    # Current expected format with AS clauses
    expected_with_as = "$employees->select(~[$employees_0.id AS \"id\", $employees_0.name AS \"name\", $employees_0.salary AS \"salary\"])"
    print("\n=== Current Expected Format (with AS) ===")
    print(expected_with_as)
    
    # Check if the generated code matches either format
    if pure_code.strip() == expected_without_as:
        print("\nVALIDATION: Code is correctly generated WITHOUT AS clauses")
    elif pure_code.strip() == expected_with_as:
        print("\nVALIDATION: Code is incorrectly generated WITH AS clauses")
    else:
        print("\nVALIDATION: Code format doesn't match either expected format")
        print(f"Generated: {pure_code.strip()}")

if __name__ == "__main__":
    main()
