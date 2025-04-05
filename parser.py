"""
Lambda parser for the cloud-dataframe DSL.

This module provides utilities for parsing Python lambda functions
and converting them to SQL expressions.
"""
import ast
import inspect
import logging
from typing import Any, Callable, Dict, List, Optional, Tuple, Union, cast

from metamodel import Expression, ColumnExpression, LiteralExpression, BinaryExpression, BinaryOperator, \
    ColumnReference, BooleanLiteral, IfExpression, NotExpression
from legendql import LegendQL

class Parser:

    @staticmethod
    def parse(func: Callable, lq: LegendQL) -> Union[Expression, List[Expression]]:
        """
        Parse a lambda function and convert it to an Expression or list of Expressions.

        Args:
            func: The lambda function to parse. Can be:
                - A lambda that returns a boolean expression (e.g., lambda x: x.age > 30)
                - A lambda that returns a column reference (e.g., lambda x: x.name)
                - A lambda that returns an array of column references (e.g., lambda x: [x.name, x.age])
                - A lambda that returns tuples with sort direction (e.g., lambda x: [(x.department, Sort.DESC)])
            lq: the current LegendQL query context

        Returns:
            An Expression or list of Expressions representing the lambda function
        """
        # Get the source code of the lambda function
        try:
            source_lines, _ = inspect.getsourcelines(func)
            source_text = ''.join(source_lines).strip()

            # Parse the source code using ast
            source_ast = ast.parse(source_text)
            lambda_node = next((node for node in ast.walk(source_ast) if isinstance(node, ast.Lambda)), None)

            if not lambda_node:
                raise ValueError("Could not find lambda expression in source code")

        except Exception:
            raise ValueError("Error getting Lambda")

        # Parse the lambda body
        result = Parser._parse_expression(lambda_node.body, lambda_node.args.args, lq)
        return result

    @staticmethod
    def _parse_expression(node: ast.AST, args: List[ast.arg], lq: LegendQL) -> Union[Expression, List[Expression]]:
        """
        Parse an AST node and convert it to an Expression or list of Expressions.

        Args:
            node: The AST node to parse
            args: The lambda function arguments
            lq: the current LegendQL query context

        Returns:
            An Expression or list of Expressions representing the AST node,
            or list containing tuples of (Expression, sort_direction) for order_by clauses
        """
        if node is None:
            raise ValueError("node in Parser._parse_expression is None")

        if isinstance(node, ast.NamedExpr):
            # column rename or new column using := (walrus) operator
            if isinstance(node.target, ast.Name):
                target_name = node.target.id
            else:
                raise ValueError("Rename column must be valid column name")

            expr = Parser._parse_expression(node.value, args, lq)

            if isinstance(expr, Expression):
                return ColumnExpression(name=target_name, expression=expr)
            else:
                raise ValueError("Lambda expressions must use explicit column references (e.g., x.column_name)")

        if isinstance(node, ast.Compare):
            # Handle comparison operations (e.g., x > 5, y == 'value')
            left = Parser._parse_expression(node.left, args, lq)

            # We only handle the first comparator for simplicity
            # In a real implementation, we would handle multiple comparators
            op = node.ops[0]
            right = Parser._parse_expression(node.comparators[0], args, lq)

            operator = Parser._get_comparison_operator(op)

            # Ensure left and right are Expression objects, not lists or tuples
            if isinstance(left, list) or isinstance(left, tuple):
                raise ValueError(f"Unsupported Compare object {left}")
            if isinstance(right, list) or isinstance(right, tuple):
                raise ValueError(f"Unsupported Compare object {right}")

            return BinaryExpression(left=left, operator=operator, right=right)

        elif isinstance(node, ast.BinOp):
            # Handle binary operations (e.g., x + y, x - y, x * y)
            left = Parser._parse_expression(node.left, args, lq)
            right = Parser._parse_expression(node.right, args, lq)

            # Map Python operators to SQL operators
            op_map = {
                ast.Add: BinaryOperator("+"),
                ast.Sub: BinaryOperator("-"),
                ast.Mult: BinaryOperator("*"),
                ast.Div: BinaryOperator("/"),
                ast.Mod: BinaryOperator("%"),
                ast.Pow: BinaryOperator("^"),
                ast.BitOr: BinaryOperator("|"),
                ast.BitAnd: BinaryOperator("&"),
            }

            operator = op_map.get(type(node.op), None)

            # Ensure left and right are Expression objects, not lists or tuples
            if isinstance(left, list) or isinstance(left, tuple):
                raise ValueError(f"Unsupported BinOp object {left}")
            if isinstance(right, list) or isinstance(right, tuple):
                raise ValueError(f"Unsupported BinOp object {right}")

            return BinaryExpression(left=left, operator=operator, right=right)

        elif isinstance(node, ast.BoolOp):
            # Handle boolean operations (e.g., x and y, x or y)
            values = [Parser._parse_expression(val, args, lq) for val in node.values]

            # Combine the values with the appropriate operator
            operator = BinaryOperator("AND") if isinstance(node.op, ast.And) else BinaryOperator("OR")

            # Ensure all values are Expression objects, not lists or tuples
            processed_values = []
            for val in values:
                if isinstance(val, list) or isinstance(val, tuple):
                    raise ValueError(f"Unsupported BoolOp object {val}")
                else:
                    processed_values.append(val)

            # Start with the first two values
            result = BinaryExpression(left=processed_values[0], operator=operator, right=processed_values[1])

            # Add the remaining values
            for value in processed_values[2:]:
                result = BinaryExpression(left=result, operator=operator, right=value)

            return result

        elif isinstance(node, ast.Attribute):
            # Handle column references (e.g. x.column_name)
            if isinstance(node.value, ast.Name):
                # validate the column name
                if not lq.validate_column(node.attr):
                    raise ValueError(f"Column '{node.attr}' not found in table schema '{lq.schema}'")

                return ColumnReference(name=node.attr, table=lq.schema.alias)
            else:
                raise ValueError(f"Unsupported Column Reference {node.value}")

        elif isinstance(node, ast.Constant):
            # Handle literal values (e.g., 5, 'value', True)
            from metamodel import LiteralExpression
            return LiteralExpression(value=node.value)

        elif isinstance(node, ast.Name):
            # Handle variable names (e.g., x, y)
            if node.id == args[0].arg:
                # This is the lambda parameter itself
                raise ValueError(f"Cannot reference the lambda parameter by itself {node.id}")
            elif node.id == "True":
                from metamodel import LiteralExpression
                return LiteralExpression(value=BooleanLiteral(True))
            elif node.id == "False":
                from metamodel import LiteralExpression
                return LiteralExpression(value=BooleanLiteral(False))
            else:
                # This is a variable reference
                # In a real implementation, we would handle this more robustly
                return ColumnReference(name=node.id, table=lq.schema.alias)

        elif isinstance(node, ast.UnaryOp):
            # Handle unary operations (e.g., not x)
            operand = Parser._parse_expression(node.operand, args, lq)

            # Ensure operand is an Expression object, not a list or tuple
            if isinstance(operand, list) or isinstance(operand, tuple):
                # Use a fallback for list/tuple values in unary operations
                raise ValueError(f"Unsupported expression to UnaryOp: {operand}")

            if isinstance(node.op, ast.Not):
                return NotExpression(operand)
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
                    parsed_arg = Parser._parse_expression(arg, args, lq)
                    args_list.append(parsed_arg)

                # Handle keyword arguments
                kwargs = {}
                for kw in node.keywords:
                    if isinstance(kw.value, ast.Constant):
                        kwargs[kw.arg] = kw.value.value

                # Create the appropriate Function object based on function name
                if node.func.id in (
                'sum', 'avg', 'count', 'min', 'max', 'window', 'rank', 'row_number', 'dense_rank', 'row', 'range',
                'unbounded'):
                    from ..type_system.column import (
                        SumFunction, AvgFunction, CountFunction, MinFunction, MaxFunction,
                        WindowFunction, Window, RankFunction, RowNumberFunction, DenseRankFunction,
                        Frame
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
                    elif node.func.id == 'window':

                        func_expr = None
                        partition_expr = None
                        order_by_expr = None
                        frame_expr = None

                        for kw in node.keywords:
                            kw_name = kw.arg
                            kw_value = kw.value

                            if kw_name == 'func':
                                func_expr = Parser._parse_expression(kw_value, args, table_schema)
                            elif kw_name == 'partition':
                                partition_expr = Parser._parse_expression(kw_value, args, table_schema)
                            elif kw_name == 'order_by':
                                order_by_expr = Parser._parse_expression(kw_value, args, table_schema)
                            elif kw_name == 'frame':
                                frame_expr = Parser._parse_expression(kw_value, args, table_schema)

                        if len(node.args) > 0 and func_expr is None:
                            func_expr = Parser._parse_expression(node.args[0], args, table_schema)

                        result = window(func=func_expr, partition=partition_expr, order_by=order_by_expr,
                                        frame=frame_expr)

                        if func_expr is not None and isinstance(func_expr, FunctionExpression):
                            result.function_name = func_expr.function_name

                        return result
                    elif node.func.id == 'rank':
                        return RankFunction(function_name="RANK")
                    elif node.func.id == 'row_number':
                        return RowNumberFunction(function_name="ROW_NUMBER")
                    elif node.func.id == 'dense_rank':
                        return DenseRankFunction(function_name="DENSE_RANK")
                    elif node.func.id == 'row':
                        start = 0
                        end = 0
                        if len(args_list) >= 1:
                            if isinstance(args_list[0], LiteralExpression):
                                start = args_list[0].value
                            elif isinstance(args_list[0], ColumnReference) and args_list[0].name == "*":
                                start = "UNBOUNDED"
                        if len(args_list) >= 2:
                            if isinstance(args_list[1], LiteralExpression):
                                end = args_list[1].value
                            elif isinstance(args_list[1], ColumnReference) and args_list[1].name == "*":
                                end = "UNBOUNDED"

                        return row(start, end)
                    elif node.func.id == 'range':
                        start = 0
                        end = 0
                        if len(args_list) >= 1:
                            if isinstance(args_list[0], LiteralExpression):
                                start = args_list[0].value
                            elif isinstance(args_list[0], ColumnReference) and args_list[0].name == "*":
                                start = "UNBOUNDED"
                        if len(args_list) >= 2:
                            if isinstance(args_list[1], LiteralExpression):
                                end = args_list[1].value
                            elif isinstance(args_list[1], ColumnReference) and args_list[1].name == "*":
                                end = "UNBOUNDED"
                        return range(start, end)
                    elif node.func.id == 'unbounded':
                        return LiteralExpression(value="UNBOUNDED")
                elif FunctionRegistry.get_function_class(node.func.id):
                    try:
                        logging.debug(f"Attempting to create function: {node.func.id} with args: {args_list}")
                        return FunctionRegistry.create_function(node.func.id, args_list)
                    except ValueError as e:
                        logging.debug(f"Failed to create function: {node.func.id}. Error: {str(e)}")
                        logging.debug(f"Args: {args_list}")
                        return FunctionExpression(function_name=node.func.id, parameters=args_list)
            elif isinstance(node.func, ast.Attribute) and node.func.attr == "alias" and len(node.args) == 1:
                if isinstance(node.args[0], ast.Constant):
                    alias_name = node.args[0].value
                    col_ref = Parser._parse_expression(node.func.value, args, table_schema)
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
                    parsed_arg = Parser._parse_expression(arg, args, lq)
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
            test = Parser._parse_expression(node.test, args, lq)
            body = Parser._parse_expression(node.body, args, lq)
            orelse = Parser._parse_expression(node.orelse, args, lq)

            # Ensure all values are Expression objects, not lists or tuples
            if isinstance(test, list) or isinstance(test, tuple):
                raise ValueError(f"Unsupported IfExp: {test}")
            if isinstance(body, list) or isinstance(body, tuple):
                raise ValueError(f"Unsupported IfExp: {body}")
            if isinstance(orelse, list) or isinstance(orelse, tuple):
                raise ValueError(f"Unsupported IfExp: {orelse}")

            # Create a CASE WHEN expression
            return IfExpression(test=test, body=body, orelse=orelse)

        elif isinstance(node, ast.List):
            # Handle tuples and lists (e.g., (1, 2, 3), [1, 2, 3])
            # This is used for array returns in lambdas like lambda x: [x.name, x.age]
            elements = []
            for elt in node.elts:
                elements.append(Parser._parse_expression(elt, args, lq))
            return elements

        elif isinstance(node, ast.Subscript):
            # Handle subscript operations (e.g., x[0], x['key'])
            # In a real implementation, we would handle this more robustly
            raise ValueError(f"Unsupported expression: {node}")

        elif isinstance(node, ast.Tuple):
            # Handle subscript operations (e.g., x[0], x['key'])
            # In a real implementation, we would handle this more robustly
            raise ValueError(f"Unsupported expression: {node}")

        elif isinstance(node, ast.Dict):
            # Handle dictionaries (e.g., {'a': 1, 'b': 2})
            # In a real implementation, we would handle this more robustly
            raise ValueError(f"Unsupported expression: {node}")

        elif isinstance(node, ast.Set):
            # Handle sets (e.g., {1, 2, 3})
            # In a real implementation, we would handle this more robustly
            raise ValueError(f"Unsupported expression: {node}")

        elif isinstance(node, ast.ListComp) or isinstance(node, ast.SetComp) or isinstance(node, ast.DictComp) or isinstance(node, ast.GeneratorExp):
            # Handle comprehensions (e.g., [x for x in y], {x: y for x in z})
            # In a real implementation, we would handle this more robustly
            raise ValueError(f"Unsupported expression: {node}")

        else:
            # Handle other types of AST nodes
            # In a real implementation, we would handle more types of nodes
            raise ValueError(f"Unsupported expression: {node}")

    @staticmethod
    def _get_comparison_operator(op: ast.cmpop) -> BinaryOperator:
        """
        Convert an AST comparison operator to a SQL operator.

        Args:
            op: The AST comparison operator

        Returns:
            The equivalent SQL operator
        """
        if isinstance(op, ast.Eq):
            return BinaryOperator("=")
        elif isinstance(op, ast.NotEq):
            return BinaryOperator("!=")
        elif isinstance(op, ast.Lt):
            return BinaryOperator("<")
        elif isinstance(op, ast.LtE):
            return BinaryOperator("<=")
        elif isinstance(op, ast.Gt):
            return BinaryOperator(">")
        elif isinstance(op, ast.GtE):
            return BinaryOperator(">=")
        elif isinstance(op, ast.In):
            return BinaryOperator("IN")
        elif isinstance(op, ast.NotIn):
            return BinaryOperator("NOT IN")
        elif isinstance(op, ast.Is):
            return BinaryOperator("IS")
        elif isinstance(op, ast.IsNot):
            return BinaryOperator("IS NOT")
        else:
            # Default to equality for unsupported operators
            raise ValueError(f"Unsupported comparison operator {op}")