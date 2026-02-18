"""Compile expression trees to q functional form strings.

q functional select:  ?[table; where_clauses; by_clauses; select_clauses]
q functional update:  ![table; where_clauses; by_clauses; update_clauses]
q functional delete:  ![table; where_clauses; 0b; columns_to_delete]
"""

from __future__ import annotations

from typing import Any

from .expressions import (
    Expr, Column, Literal, BinOp, UnaryOp, FuncCall, AggFunc,
    FbyExpr, EachExpr, _QSentinel,
)


def compile_expr(expr: Expr) -> str:
    """Compile a single expression to a q string."""
    if isinstance(expr, Column):
        return expr.name

    if isinstance(expr, Literal):
        return _compile_literal(expr.value)

    if isinstance(expr, BinOp):
        left = compile_expr(expr.left)
        right = compile_expr(expr.right)
        if expr.op == 'mod':
            return f"({left} mod {right})"
        return f"({left}{expr.op}{right})"

    if isinstance(expr, UnaryOp):
        operand = compile_expr(expr.operand)
        return f"({expr.op} {operand})"

    if isinstance(expr, FuncCall):
        args = ';'.join(compile_expr(a) for a in expr.args)
        if expr.func_name in ('like', 'in', 'within', 'xbar'):
            # Infix form
            left = compile_expr(expr.args[0])
            right = compile_expr(expr.args[1])
            return f"({left} {expr.func_name} {right})"
        return f"{expr.func_name}[{args}]"

    if isinstance(expr, AggFunc):
        col = compile_expr(expr.column)
        return f"{expr.func_name} {col}"

    if isinstance(expr, FbyExpr):
        col = compile_expr(expr.col)
        group = compile_expr(expr.group_col)
        return f"({expr.agg_name};{col}) fby {group}"

    if isinstance(expr, EachExpr):
        inner = compile_expr(expr.func_expr)
        return f"{inner} {expr.adverb}"

    raise ValueError(f"Cannot compile expression: {expr!r}")


def _compile_literal(value: Any) -> str:
    """Compile a Python literal to its q representation."""
    if isinstance(value, _QSentinel):
        return value.q_repr
    if value is None:
        return '(::)'  # q generic null
    if isinstance(value, bool):
        return '1b' if value else '0b'
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return f'{value}f' if value == value else '0Nf'  # NaN check
    if isinstance(value, str):
        # Check if it looks like a symbol
        if value.isidentifier():
            return f'`{value}'
        return f'"{value}"'
    if isinstance(value, (list, tuple)):
        if not value:
            return '()'
        items = ';'.join(_compile_literal(v) for v in value)
        return f'({items})'
    if isinstance(value, bytes):
        hex_str = ''.join(f'{b:02x}' for b in value)
        return f'0x{hex_str}'
    return str(value)


def compile_where(clauses: list[Expr]) -> str:
    """Compile WHERE clauses to q constraint list.

    Returns: enlist (expr1;expr2;...)
    """
    if not clauses:
        return '()'
    parts = [f'({compile_expr(c)})' for c in clauses]
    if len(parts) == 1:
        return f'enlist {parts[0]}'
    return f'({";".join(parts)})'


def compile_by(
    by_exprs: list[Expr | Column],
    named: dict[str, Expr] | None = None,
) -> str:
    """Compile GROUP BY expressions.

    Returns: ([] col1; col2; ...) or 0b for no grouping.
    """
    if not by_exprs and not named:
        return '0b'
    parts = []
    for expr in by_exprs:
        if isinstance(expr, Column):
            parts.append(f'{expr.name}:{expr.name}')
        else:
            parts.append(compile_expr(expr))
    if named:
        for alias, expr in named.items():
            parts.append(f'{alias}:{compile_expr(expr)}')
    return f'([] {"; ".join(parts)})'


def compile_select_columns(
    columns: list[Expr | Column] | None,
    named: dict[str, Expr] | None = None,
) -> str:
    """Compile the select (aggregation) dictionary.

    Returns a q dictionary expression like:
        ([] sym:sym; avg_price:avg price)
    or () for select-all.
    """
    if not columns and not named:
        return '()'  # select all

    parts = []
    if columns:
        for col in columns:
            if isinstance(col, Column):
                parts.append(f'{col.name}:{col.name}')
            elif isinstance(col, AggFunc):
                compiled = compile_expr(col)
                col_name = _infer_agg_name(col)
                parts.append(f'{col_name}:{compiled}')
            else:
                parts.append(compile_expr(col))

    if named:
        for alias, expr in named.items():
            compiled = compile_expr(expr)
            parts.append(f'{alias}:{compiled}')

    return f'([] {"; ".join(parts)})'


def _infer_agg_name(agg: AggFunc) -> str:
    """Infer a column name for an aggregate expression."""
    if isinstance(agg.column, Column):
        return f'{agg.func_name}_{agg.column.name}'
    return agg.func_name


def compile_functional_select(
    table: str,
    where_clauses: list[Expr],
    by_exprs: list[Expr],
    columns: list[Expr] | None,
    named: dict[str, Expr] | None,
    by_named: dict[str, Expr] | None = None,
) -> str:
    """Compile a full functional select: ?[t;c;b;a].

    Parameters
    ----------
    table : str
        Table name.
    where_clauses : list[Expr]
        WHERE filter expressions.
    by_exprs : list[Expr]
        GROUP BY expressions.
    columns : list[Expr] | None
        SELECT columns (None = all).
    named : dict[str, Expr] | None
        Named (aliased) select expressions.
    by_named : dict[str, Expr] | None
        Named (aliased) GROUP BY expressions.
    """
    t = table
    c = compile_where(where_clauses)
    b = compile_by(by_exprs, by_named)
    a = compile_select_columns(columns, named)
    return f'?[{t};{c};{b};{a}]'


def compile_functional_update(
    table: str,
    where_clauses: list[Expr],
    by_exprs: list[Expr],
    assignments: dict[str, Expr],
) -> str:
    """Compile a full functional update: ![t;c;b;a]."""
    t = table
    c = compile_where(where_clauses)
    b = compile_by(by_exprs)
    parts = [f'{name}:{compile_expr(expr)}' for name, expr in assignments.items()]
    a = f'([] {"; ".join(parts)})'
    return f'![{t};{c};{b};{a}]'


def compile_functional_delete(
    table: str,
    where_clauses: list[Expr],
    columns: list[str] | None = None,
) -> str:
    """Compile a functional delete: ![t;c;0b;a].

    If columns is None, deletes matching rows.
    If columns is provided, deletes those columns.
    """
    t = table
    c = compile_where(where_clauses)
    if columns:
        a = '`' + '`'.join(columns)
    else:
        a = '`symbol$()'
    return f'![{t};{c};0b;{a}]'


def compile_exec_columns(
    columns: list[Expr | Column] | None,
    named: dict[str, Expr] | None = None,
) -> str:
    """Compile the exec column list.

    Single column, no alias → atom: ``price``
    Multiple or named → dict without ``[]``: `` `sym`price!(sym;price) ``
    """
    all_parts: list[tuple[str, str]] = []

    if columns:
        for col in columns:
            if isinstance(col, Column):
                all_parts.append((col.name, col.name))
            elif isinstance(col, AggFunc):
                compiled = compile_expr(col)
                col_name = _infer_agg_name(col)
                all_parts.append((col_name, compiled))
            else:
                all_parts.append((compile_expr(col), compile_expr(col)))

    if named:
        for alias, expr in named.items():
            all_parts.append((alias, compile_expr(expr)))

    if not all_parts:
        return '()'

    # Single unnamed column → atom form
    if len(all_parts) == 1 and not named:
        name, compiled = all_parts[0]
        if isinstance(columns[0], Column):
            return f'`{name}'
        return compiled

    # Multiple → dict form
    keys = '`' + '`'.join(name for name, _ in all_parts)
    vals = ';'.join(compiled for _, compiled in all_parts)
    return f'{keys}!({vals})'


def compile_functional_exec(
    table: str,
    where_clauses: list[Expr],
    by_exprs: list[Expr],
    columns: list[Expr] | None,
    named: dict[str, Expr] | None,
) -> str:
    """Compile a full functional exec: ?[t;c;b;a] with exec-style columns."""
    t = table
    c = compile_where(where_clauses)
    b = compile_by(by_exprs)
    a = compile_exec_columns(columns, named)
    return f'?[{t};{c};{b};{a}]'
