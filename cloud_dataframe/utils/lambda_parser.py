"""
Lambda parser for the cloud-dataframe DSL.

This module provides utilities for parsing Python lambda functions
and converting them to SQL expressions.
"""
import ast
import inspect
import textwrap
from typing import Any, Callable, Dict, List, Optional, Tuple, Union, cast

from ..type_system.column import (
    Expression, LiteralExpression, ColumnReference, 
    SumFunction, AvgFunction, CountFunction, MinFunction, MaxFunction,
    DateDiffFunction, FunctionExpression
)
from ..core.dataframe import BinaryOperation, OrderByClause, Sort


def parse_lambda(lambda_func: Callable, table_schema=None) -> Union[Expression, List[Union[Expression, Tuple[Expression, Any]]]]:
    """
    Parse a lambda function and convert it to an Expression or list of Expressions.
    
    Args:
        lambda_func: The lambda function to parse. Can be:
            - A lambda that returns a boolean expression (e.g., lambda x: x.age > 30)
            - A lambda that returns a column reference (e.g., lambda x: x.name)
            - A lambda that returns an array of column references (e.g., lambda x: [x.name, x.age])
            - A lambda that returns tuples with sort direction (e.g., lambda x: [(x.department, Sort.DESC)])
        table_schema: Optional schema for type checking
        
    Returns:
        An Expression or list of Expressions representing the lambda function,
        or list containing tuples of (Expression, sort_direction) for order_by clauses
    """
    return LambdaParser.parse_lambda(lambda_func, table_schema)


class LambdaParser:
    """
    Parser for converting Python lambda functions to SQL expressions.
    
    This class provides methods to analyze a lambda function's AST
    and convert it to an equivalent SQL expression.
    """
    
    @staticmethod
    def parse_lambda(lambda_func: Callable, table_schema=None) -> Union[Expression, List[Union[Expression, Tuple[Expression, Any]]]]:
        """
        Parse a lambda function and convert it to an Expression or list of Expressions.
        
        Args:
            lambda_func: The lambda function to parse. Can be:
                - A lambda that returns a boolean expression (e.g., lambda x: x.age > 30)
                - A lambda that returns a column reference (e.g., lambda x: x.name)
                - A lambda that returns an array of column references (e.g., lambda x: [x.name, x.age])
                - A lambda that returns tuples with sort direction (e.g., lambda x: [(x.department, Sort.DESC)])
            table_schema: Optional schema for type checking
            
        Returns:
            An Expression or list of Expressions representing the lambda function,
            or list containing tuples of (Expression, sort_direction) for order_by clauses
        """
        # Get the source code of the lambda function
        try:
            try:
                source = inspect.getsource(lambda_func)
                
                # Handle multiline lambda expressions
                if "\\" in source:
                    # Remove line continuations and normalize whitespace
                    source = source.replace("\\", "").strip()
                
                lambda_start = source.find("lambda")
                if lambda_start >= 0:
                    source = source[lambda_start:]
                    
                    assign_pos = source.find("=")
                    if assign_pos > 0 and assign_pos < source.find(":"):
                        source = source[assign_pos+1:].strip()
                        lambda_start = source.find("lambda")
                        if lambda_start >= 0:
                            source = source[lambda_start:]
                
                if source.startswith("lambda"):
                    colon_pos = source.find(":")
                    if colon_pos > 0:
                        args_str = source[6:colon_pos].strip()
                        
                        body_start = colon_pos + 1
                        body = source[body_start:].strip()
                        
                        comma_pos = body.find(",")
                        if comma_pos > 0 and "=" in body[comma_pos:]:
                            body = body[:comma_pos].strip()
                        
                        paren_depth = 0
                        bracket_depth = 0
                        in_string = False
                        string_char = None
                        body_end = len(body)
                        
                        for i, char in enumerate(body):
                            if char in ('"', "'") and (i == 0 or body[i-1] != '\\'):
                                if not in_string:
                                    in_string = True
                                    string_char = char
                                elif char == string_char:
                                    in_string = False
                            
                            if not in_string:
                                if char == '[':
                                    bracket_depth += 1
                                elif char == ']':
                                    bracket_depth -= 1
                                elif char == '(':
                                    paren_depth += 1
                                elif char == ')':
                                    paren_depth -= 1
                                    if paren_depth < 0:
                                        body_end = i
                                        break
                        
                        if bracket_depth > 0:
                            body = body + ']' * bracket_depth
                            
                        body = body[:body_end].strip()
                        source = f"lambda {args_str}: {body}"
            except (OSError, TypeError):
                code = lambda_func.__code__
                source = f"lambda {', '.join(code.co_varnames[:code.co_argcount])}: <body>"
            
            # Parse the source code into an AST
            try:
                tree = ast.parse(source.strip())
                
                # Find the lambda expression in the AST
                lambda_node = None
                for node in ast.walk(tree):
                    if isinstance(node, ast.Lambda):
                        lambda_node = node
                        break
                
                if not lambda_node:
                    raise ValueError("Could not find lambda expression in source code")
                
                # We can't add parent references directly due to type checking issues
                # Instead, we'll use a simpler approach for handling complex boolean operations
                
                # Parse the lambda body
                result = LambdaParser._parse_expression(lambda_node.body, lambda_node.args.args, table_schema)
                return result
            except SyntaxError as syntax_err:
                if "unmatched ')'" in str(syntax_err):
                    if source.endswith(')'):
                        source = source[:-1]
                        tree = ast.parse(source.strip())
                        
                        # Find the lambda expression in the AST
                        lambda_node = None
                        for node in ast.walk(tree):
                            if isinstance(node, ast.Lambda):
                                lambda_node = node
                                break
                        
                        if not lambda_node:
                            raise ValueError("Could not find lambda expression in source code")
                        
                        # Parse the lambda body
                        result = LambdaParser._parse_expression(lambda_node.body, lambda_node.args.args, table_schema)
                        return result
                    else:
                        raise ValueError(f"Parse Error: {str(syntax_err)}")
                elif "'[' was never closed" in str(syntax_err):
                    bracket_count = source.count('[') - source.count(']')
                    if bracket_count > 0:
                        fixed_source = source + ']' * bracket_count
                        try:
                            tree = ast.parse(fixed_source.strip())
                            
                            # Find the lambda expression in the AST
                            lambda_node = None
                            for node in ast.walk(tree):
                                if isinstance(node, ast.Lambda):
                                    lambda_node = node
                                    break
                            
                            if not lambda_node:
                                raise ValueError("Could not find lambda expression in source code")
                            
                            # Parse the lambda body
                            result = LambdaParser._parse_expression(lambda_node.body, lambda_node.args.args, table_schema)
                            return result
                        except SyntaxError as nested_err:
                            raise ValueError(f"Parse Error: {str(nested_err)}")
                    else:
                        raise ValueError(f"Parse Error: {str(syntax_err)}")
                else:
                    raise ValueError(f"Parse Error: {str(syntax_err)}")
        except Exception as e:
            raise ValueError(f"Parse Error: {str(e)}")
        
    @staticmethod
    def _parse_expression(node: ast.AST, args: List[ast.arg], table_schema=None) -> Union[Expression, List[Union[Expression, Tuple[Expression, Any]]]]:
        """
        Parse an AST node and convert it to an Expression or list of Expressions.
        
        Args:
            node: The AST node to parse
            args: The lambda function arguments
            table_schema: Optional schema for type checking
            
        Returns:
            An Expression or list of Expressions representing the AST node,
            or list containing tuples of (Expression, sort_direction) for order_by clauses
        
        Note:
            This method ensures all code paths return a value of the appropriate type.
            Default fallback is a ColumnReference with name="*" when a specific node type
            cannot be properly parsed.
        """
        from ..type_system.column import (
            ColumnReference, LiteralExpression, FunctionExpression,
            SumFunction, AvgFunction, CountFunction, MinFunction, MaxFunction,
            DateDiffFunction
        )
        
        if node is None:
            return ColumnReference(name="*")
            
        default_return = ColumnReference(name="*")
        
        # Handle different types of AST nodes
        if isinstance(node, ast.NamedExpr):
            target_name = node.target.id if isinstance(node.target, ast.Name) else "expr"
            expr = LambdaParser._parse_expression(node.value, args, table_schema)
            
            if isinstance(expr, list) or isinstance(expr, tuple):
                if expr and len(expr) > 0:
                    first_expr = expr[0]
                    if isinstance(first_expr, Expression):
                        return BinaryOperation(
                            left=cast(Expression, first_expr),
                            operator="AS",
                            right=LiteralExpression(value=target_name)
                        )
                return default_return
            elif isinstance(expr, Expression):
                return BinaryOperation(
                    left=expr,
                    operator="AS",
                    right=LiteralExpression(value=target_name)
                )
            else:
                raise ValueError("Lambda expressions must use explicit column references (e.g., x.column_name)")
                
        if isinstance(node, ast.Compare):
            # Handle comparison operations (e.g., x > 5, y == 'value')
            left = LambdaParser._parse_expression(node.left, args, table_schema)
            
            # We only handle the first comparator for simplicity
            # In a real implementation, we would handle multiple comparators
            op = node.ops[0]
            right = LambdaParser._parse_expression(node.comparators[0], args, table_schema)
            
            operator = LambdaParser._get_comparison_operator(op)
            
            # Ensure left and right are Expression objects, not lists or tuples
            if isinstance(left, list) or isinstance(left, tuple):
                left = ColumnReference(name="*")  # Fallback
            if isinstance(right, list) or isinstance(right, tuple):
                right = ColumnReference(name="*")  # Fallback
                
            return BinaryOperation(left=left, operator=operator, right=right)
        
        elif isinstance(node, ast.BinOp):
            # Handle binary operations (e.g., x + y, x - y, x * y)
            left = LambdaParser._parse_expression(node.left, args, table_schema)
            right = LambdaParser._parse_expression(node.right, args, table_schema)
            
            # Map Python operators to SQL operators
            op_map = {
                ast.Add: "+",
                ast.Sub: "-",
                ast.Mult: "*",
                ast.Div: "/",
                ast.Mod: "%",
                ast.Pow: "^",
                ast.BitOr: "|",
                ast.BitAnd: "&",
            }
            
            operator = op_map.get(type(node.op), "+")  # Default to + if unknown
            
            # Ensure left and right are Expression objects, not lists or tuples
            if isinstance(left, list) or isinstance(left, tuple):
                left = ColumnReference(name="*")  # Fallback
            if isinstance(right, list) or isinstance(right, tuple):
                right = ColumnReference(name="*")  # Fallback
                
            return BinaryOperation(left=left, operator=operator, right=right, needs_parentheses=True)
            
        elif isinstance(node, ast.BoolOp):
            # Handle boolean operations (e.g., x and y, x or y)
            values = [LambdaParser._parse_expression(val, args, table_schema) for val in node.values]
            
            # Combine the values with the appropriate operator
            operator = "AND" if isinstance(node.op, ast.And) else "OR"
            
            # Ensure all values are Expression objects, not lists or tuples
            processed_values = []
            for val in values:
                if isinstance(val, list) or isinstance(val, tuple):
                    # Use a fallback for list/tuple values in boolean operations
                    processed_values.append(ColumnReference(name="*"))
                else:
                    processed_values.append(val)
            
            # For complex boolean operations, we need to handle parentheses
            # We can't use parent attribute directly due to type checking issues
            # Instead, we'll use a simpler approach for now
            if operator == "OR" and len(processed_values) == 2:
                # Add parentheses around OR conditions by default for safety
                return BinaryOperation(
                    left=processed_values[0],
                    operator=operator,
                    right=processed_values[1],
                    needs_parentheses=True
                )
            
            # Start with the first two values
            result = BinaryOperation(left=processed_values[0], operator=operator, right=processed_values[1])
            
            # Add the remaining values
            for value in processed_values[2:]:
                result = BinaryOperation(left=result, operator=operator, right=value)
            
            return result
        
        elif isinstance(node, ast.Attribute):
            if isinstance(node.value, ast.Name):
                table_alias = node.value.id
                
                # If table_schema is provided, validate the column name
                if table_schema and not table_schema.validate_column(node.attr):
                    raise ValueError(f"Column '{node.attr}' not found in table schema '{table_schema.name}'")
                
                return ColumnReference(name=node.attr, table_alias=table_alias)
            elif isinstance(node.value, ast.Attribute) and node.attr == "alias":
                return node
            elif isinstance(node.value, ast.Attribute) and isinstance(node.value.value, ast.Name):
                table_name = node.value.attr
                column_name = node.attr
                lambda_param = node.value.value.id
                
                return ColumnReference(name=column_name, table_alias=table_name, table_name=table_name)
        
        elif isinstance(node, ast.Constant):
            # Handle literal values (e.g., 5, 'value', True)
            from ..type_system.column import LiteralExpression
            return LiteralExpression(value=node.value)
        
        elif isinstance(node, ast.Name):
            # Handle variable names (e.g., x, y)
            if node.id == args[0].arg:
                # This is the lambda parameter itself
                # In a real implementation, we would handle this more robustly
                return ColumnReference(name="*")
            elif node.id == "True":
                from ..type_system.column import LiteralExpression
                return LiteralExpression(value=True)
            elif node.id == "False":
                from ..type_system.column import LiteralExpression
                return LiteralExpression(value=False)
            else:
                # This is a variable reference
                # In a real implementation, we would handle this more robustly
                return ColumnReference(name=node.id)
        
        elif isinstance(node, ast.UnaryOp):
            # Handle unary operations (e.g., not x)
            operand = LambdaParser._parse_expression(node.operand, args, table_schema)
            
            # Ensure operand is an Expression object, not a list or tuple
            if isinstance(operand, list) or isinstance(operand, tuple):
                # Use a fallback for list/tuple values in unary operations
                operand = ColumnReference(name="*")
                
            if isinstance(node.op, ast.Not):
                # Handle NOT operation
                if isinstance(operand, BinaryOperation):
                    # Negate the operator if possible
                    if operand.operator == "=":
                        operand.operator = "!="
                    elif operand.operator == "!=":
                        operand.operator = "="
                    elif operand.operator == "<":
                        operand.operator = ">="
                    elif operand.operator == ">":
                        operand.operator = "<="
                    elif operand.operator == "<=":
                        operand.operator = ">"
                    elif operand.operator == ">=":
                        operand.operator = "<"
                    else:
                        # For operators that can't be directly negated, wrap in NOT
                        return BinaryOperation(
                            left=LiteralExpression(value=True),
                            operator="=",
                            right=BinaryOperation(
                                left=LiteralExpression(value=False),
                                operator="=",
                                right=operand
                            )
                        )
                    return operand
                else:
                    # For non-binary operations, use NOT
                    return BinaryOperation(
                        left=operand,
                        operator="=",
                        right=LiteralExpression(value=False)
                    )
            else:
                # Other unary operations (e.g., +, -)
                # In a real implementation, we would handle this more robustly
                return operand
        
        elif isinstance(node, ast.Call):
            # Handle function calls (e.g., sum(x.col1 - x.col2))
            if isinstance(node.func, ast.Name):
                # Parse the arguments to the function
                args_list = []
                for arg in node.args:
                    parsed_arg = LambdaParser._parse_expression(arg, args, table_schema)
                    args_list.append(parsed_arg)
                
                # Handle keyword arguments
                kwargs = {}
                for kw in node.keywords:
                    if isinstance(kw.value, ast.Constant):
                        kwargs[kw.arg] = kw.value.value
                
                # Create the appropriate Function object based on function name
                if node.func.id in ('sum', 'avg', 'count', 'min', 'max'):
                    from ..type_system.column import (
                        SumFunction, AvgFunction, CountFunction, MinFunction, MaxFunction
                    )
                    
                    # Allow complex expressions as arguments (e.g., sum(x.col1 - x.col2))
                    if node.func.id == 'sum':
                        # Create a SumFunction with the parsed arguments
                        func = SumFunction(function_name="SUM", parameters=args_list)
                        return func
                    elif node.func.id == 'avg':
                        # Create an AvgFunction with the parsed arguments
                        func = AvgFunction(function_name="AVG", parameters=args_list)
                        
                        return func
                    elif node.func.id == 'count':
                        distinct = kwargs.get('distinct', False)
                        # Also check for distinct in keywords
                        for kw in node.keywords:
                            if kw.arg == 'distinct' and isinstance(kw.value, ast.Constant):
                                distinct = kw.value.value
                        
                        # Handle count() with no arguments - convert to COUNT(1)
                        if not args_list:
                            from ..type_system.column import LiteralExpression
                            args_list = [LiteralExpression(value=1)]
                        
                        # Create a CountFunction with the parsed arguments
                        func = CountFunction(function_name="COUNT", parameters=args_list, distinct=distinct)
                        
                        return func
                    elif node.func.id == 'min':
                        # Create a MinFunction with the parsed arguments
                        func = MinFunction(function_name="MIN", parameters=args_list)
                        
                        return func
                    elif node.func.id == 'max':
                        # Create a MaxFunction with the parsed arguments
                        func = MaxFunction(function_name="MAX", parameters=args_list)
                        
                        return func
                # Support for scalar functions
                elif node.func.id in ('date_diff'):
                    from ..type_system.column import DateDiffFunction
                    
                    if len(args_list) != 2:
                        raise ValueError(f"Function {node.func.id}() expects exactly two arguments")
                        
                    return DateDiffFunction(function_name="DATE_DIFF", parameters=args_list)
            elif isinstance(node.func, ast.Attribute) and node.func.attr == "alias" and len(node.args) == 1:
                if isinstance(node.args[0], ast.Constant):
                    alias_name = node.args[0].value
                    col_ref = LambdaParser._parse_expression(node.func.value, args, table_schema)
                    if isinstance(col_ref, ColumnReference):
                        return col_ref.alias(alias_name)
                    else:
                        raise ValueError(f"Cannot apply alias to non-column reference: {col_ref}")
                else:
                    raise ValueError("Alias name must be a string literal")
            # Handle nested function calls inside lambda expressions
            elif isinstance(node.func, ast.Attribute):
                if node.func.attr == "alias" and len(node.args) == 1 and isinstance(node.args[0], ast.Constant):
                    alias_name = node.args[0].value
                    
                    if isinstance(node.func.value, ast.Attribute) and isinstance(node.func.value.value, ast.Attribute):
                        table_name = node.func.value.value.attr
                        column_name = node.func.value.attr
                        
                        return ColumnReference(
                            name=column_name, 
                            table_alias=table_name, 
                            table_name=table_name,
                            column_alias=alias_name
                        )
                
                # This handles cases like lambda x: x.func(arg1, arg2)
                # Parse the arguments to the function
                args_list = []
                for arg in node.args:
                    parsed_arg = LambdaParser._parse_expression(arg, args, table_schema)
                    args_list.append(parsed_arg)
                
                # Create a function expression with the attribute name as the function name
                from ..type_system.column import FunctionExpression
                return FunctionExpression(
                    function_name=node.func.attr,
                    parameters=args_list
                )
            
            # Default case for other function calls
            return ColumnReference(name="*")
        
        elif isinstance(node, ast.IfExp):
            # Handle conditional expressions (e.g., x if y else z)
            # In a real implementation, we would handle this more robustly
            test = LambdaParser._parse_expression(node.test, args, table_schema)
            body = LambdaParser._parse_expression(node.body, args, table_schema)
            orelse = LambdaParser._parse_expression(node.orelse, args, table_schema)
            
            # Ensure all values are Expression objects, not lists or tuples
            if isinstance(test, list) or isinstance(test, tuple):
                test = ColumnReference(name="*")
            if isinstance(body, list) or isinstance(body, tuple):
                body = ColumnReference(name="*")
            if isinstance(orelse, list) or isinstance(orelse, tuple):
                orelse = ColumnReference(name="*")
                
            # Create a CASE WHEN expression
            return BinaryOperation(
                left=test,
                operator="CASE",
                right=BinaryOperation(
                    left=body,
                    operator="ELSE",
                    right=orelse
                )
            )
        
        elif isinstance(node, ast.Subscript):
            # Handle subscript operations (e.g., x[0], x['key'])
            # In a real implementation, we would handle this more robustly
            return ColumnReference(name="*")
        
        elif isinstance(node, ast.Tuple) or isinstance(node, ast.List):
            # Handle tuples and lists (e.g., (1, 2, 3), [1, 2, 3])
            # This is used for array returns in lambdas like lambda x: [x.name, x.age]
            # If the list contains tuples of (column, sort_direction), preserve them
            elements = []
            for elt in node.elts:
                if isinstance(elt, ast.Tuple) and len(elt.elts) == 2:
                    # This is a tuple of (column, sort_direction)
                    col_expr = LambdaParser._parse_expression(elt.elts[0], args, table_schema)
                    
                    # Import needed classes
                    from ..type_system.column import LiteralExpression
                    from ..core.dataframe import Sort
                    
                    # Handle Sort enum references
                    if isinstance(elt.elts[1], ast.Attribute) and elt.elts[1].attr in ('DESC', 'ASC'):
                        # Sort enum reference like Sort.DESC
                        sort_direction = Sort.DESC if elt.elts[1].attr == 'DESC' else Sort.ASC
                        elements.append((col_expr, sort_direction))
                    elif isinstance(elt.elts[1], ast.Name) and elt.elts[1].id in ('DESC', 'ASC'):
                        sort_direction = Sort.DESC if elt.elts[1].id == 'DESC' else Sort.ASC
                        elements.append((col_expr, sort_direction))
                    elif isinstance(elt.elts[1], ast.Constant) and isinstance(elt.elts[1].value, str) and elt.elts[1].value.upper() in ('DESC', 'ASC'):
                        sort_direction = Sort.DESC if elt.elts[1].value.upper() == 'DESC' else Sort.ASC
                        elements.append((col_expr, sort_direction))
                    else:
                        try:
                            sort_dir = LambdaParser._parse_expression(elt.elts[1], args, table_schema)
                            elements.append((col_expr, sort_dir))
                        except ValueError:
                            if isinstance(elt.elts[1], ast.Attribute) and elt.elts[1].attr in ('desc', 'asc'):
                                sort_direction = Sort.DESC if elt.elts[1].attr == 'desc' else Sort.ASC
                                elements.append((col_expr, sort_direction))
                            else:
                                elements.append((col_expr, Sort.ASC))
                elif isinstance(elt, ast.Attribute) and isinstance(elt.value, ast.Name):
                    col_expr = LambdaParser._parse_expression(elt, args, table_schema)
                    elements.append(col_expr)
                else:
                    elements.append(LambdaParser._parse_expression(elt, args, table_schema))
            return elements
        
        elif isinstance(node, ast.Dict):
            # Handle dictionaries (e.g., {'a': 1, 'b': 2})
            # In a real implementation, we would handle this more robustly
            return LiteralExpression(value={})
        
        elif isinstance(node, ast.Set):
            # Handle sets (e.g., {1, 2, 3})
            # In a real implementation, we would handle this more robustly
            return LiteralExpression(value=set())
        
        elif isinstance(node, ast.ListComp) or isinstance(node, ast.SetComp) or isinstance(node, ast.DictComp) or isinstance(node, ast.GeneratorExp):
            # Handle comprehensions (e.g., [x for x in y], {x: y for x in z})
            # In a real implementation, we would handle this more robustly
            return LiteralExpression(value=[])
        
        else:
            # Handle other types of AST nodes
            # In a real implementation, we would handle more types of nodes
            return ColumnReference(name="*")
    
    @staticmethod
    def parse_join_lambda(lambda_func: Callable, table_schema=None) -> Expression:
        """
        Parse a lambda function that represents a join condition.
        
        Args:
            lambda_func: The lambda function to parse
            table_schema: Optional schema for type checking
            
        Returns:
            An Expression representing the join condition
        """
        # Get the source code of the lambda function
        try:
            source = inspect.getsource(lambda_func)
            
            # Handle multiline lambda expressions
            if "\\" in source:
                # Remove line continuations and normalize whitespace
                source = source.replace("\\", "").strip()
            
            # Parse the source code into an AST
            tree = ast.parse(source.strip())
            
            # Find the lambda expression in the AST
            lambda_node = None
            for node in ast.walk(tree):
                if isinstance(node, ast.Lambda):
                    lambda_node = node
                    break
            
            if not lambda_node:
                raise ValueError("Could not find lambda expression in source code")
            
            # Check if the lambda has exactly two arguments
            if len(lambda_node.args.args) != 2:
                raise ValueError("Join condition lambda must have exactly two arguments (one for each table)")
            
            # We can't add parent references directly due to type checking issues
            # Instead, we'll use a simpler approach for handling complex boolean operations
            
            # Parse the lambda body
            return LambdaParser._parse_join_expression(lambda_node.body, lambda_node.args.args, table_schema)
        except (SyntaxError, AttributeError) as e:
            # Alternative approach for complex lambdas or when source extraction fails
            raise ValueError(f"Failed to parse join lambda: {e}")
    
    @staticmethod
    def _parse_join_expression(node: ast.AST, args: List[ast.arg], table_schema=None) -> Expression:
        """
        Parse a join expression AST node and convert it to an Expression.
        
        Args:
            node: The AST node to parse
            args: The lambda function arguments (left table, right table)
            table_schema: Optional schema for type checking
            
        Returns:
            An Expression representing the AST node
        """
        # Handle different types of AST nodes
        if isinstance(node, ast.Compare):
            # Handle comparison operations (e.g., x.col1 == y.col2)
            left = LambdaParser._parse_join_expression(node.left, args, table_schema)
            
            # We only handle the first comparator for simplicity
            op = node.ops[0]
            right = LambdaParser._parse_join_expression(node.comparators[0], args, table_schema)
            
            operator = LambdaParser._get_comparison_operator(op)
            
            return BinaryOperation(left=left, operator=operator, right=right)
        
        elif isinstance(node, ast.BoolOp):
            # Handle boolean operations (e.g., x.col1 == y.col2 and x.col3 > y.col4)
            values = [LambdaParser._parse_join_expression(val, args, table_schema) for val in node.values]
            
            # Combine the values with the appropriate operator
            operator = "AND" if isinstance(node.op, ast.And) else "OR"
            
            # Start with the first two values
            result = BinaryOperation(left=values[0], operator=operator, right=values[1])
            
            # Add the remaining values
            for value in values[2:]:
                result = BinaryOperation(left=result, operator=operator, right=value)
            
            return result
        
        elif isinstance(node, ast.Attribute):
            if isinstance(node.value, ast.Name):
                table_alias = node.value.id
                
                # If table_schema is provided, validate the column name
                if table_schema and not table_schema.validate_column(node.attr):
                    raise ValueError(f"Column '{node.attr}' not found in table schema '{table_schema.name}'")
                
                return ColumnReference(name=node.attr, table_alias=table_alias)
            elif isinstance(node.value, ast.Attribute) and isinstance(node.value.value, ast.Name):
                table_name = node.value.attr
                column_name = node.attr
                lambda_param = node.value.value.id
                
                return ColumnReference(name=column_name, table_alias=table_name, table_name=table_name)
            
            # If we can't determine the table, return a default column reference
            return ColumnReference(name=node.attr)
        
        elif isinstance(node, ast.Constant):
            # Handle literal values
            from ..type_system.column import LiteralExpression
            return LiteralExpression(value=node.value)
        
        elif isinstance(node, ast.Name):
            # Handle variable names
            if node.id in [arg.arg for arg in args]:
                # This is one of the lambda parameters
                return ColumnReference(name="*", table_alias=node.id)
            elif node.id == "True":
                from ..type_system.column import LiteralExpression
                return LiteralExpression(value=True)
            elif node.id == "False":
                from ..type_system.column import LiteralExpression
                return LiteralExpression(value=False)
            else:
                # This is a variable reference
                return ColumnReference(name=node.id)
        
        # For other node types, return a default expression
        return ColumnReference(name="*")
    
    @staticmethod
    def _get_comparison_operator(op: ast.cmpop) -> str:
        """
        Convert an AST comparison operator to a SQL operator.
        
        Args:
            op: The AST comparison operator
            
        Returns:
            The equivalent SQL operator
        """
        if isinstance(op, ast.Eq):
            return "="
        elif isinstance(op, ast.NotEq):
            return "!="
        elif isinstance(op, ast.Lt):
            return "<"
        elif isinstance(op, ast.LtE):
            return "<="
        elif isinstance(op, ast.Gt):
            return ">"
        elif isinstance(op, ast.GtE):
            return ">="
        elif isinstance(op, ast.In):
            return "IN"
        elif isinstance(op, ast.NotIn):
            return "NOT IN"
        elif isinstance(op, ast.Is):
            return "IS"
        elif isinstance(op, ast.IsNot):
            return "IS NOT"
        else:
            # Default to equality for unsupported operators
            return "="
