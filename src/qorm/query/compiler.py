"""Compile expression trees to q functional form strings.

q functional select:  ?[table; where_clauses; by_clauses; select_clauses]
q functional update:  ![table; where_clauses; by_clauses; update_clauses]
q functional delete:  ![table; where_clauses; 0b; columns_to_delete]

All expressions compile to q **parse tree** notation::

    (operator; arg1; arg2)   — binary ops
    (function; arg)          — unary / aggregate
    `column_name             — column references
"""

from __future__ import annotations

import datetime
import re
from typing import Any

from .expressions import (
    Expr, Column, Literal, BinOp, UnaryOp, FuncCall, AggFunc,
    FbyExpr, EachExpr, _QSentinel,
)

# Patterns for detecting q temporal literals passed as strings
_Q_DATE_RE = re.compile(r'^\d{4}\.\d{2}\.\d{2}$')
_Q_TIMESTAMP_RE = re.compile(r'^\d{4}\.\d{2}\.\d{2}D')
_Q_TIME_RE = re.compile(r'^\d{2}:\d{2}:\d{2}')


def compile_expr(expr: Expr) -> str:
    """Compile a single expression to q parse tree form."""
    if isinstance(expr, Column):
        return f'`{expr.name}'

    if isinstance(expr, Literal):
        return _compile_literal(expr.value)

    if isinstance(expr, BinOp):
        left = compile_expr(expr.left)
        right = compile_expr(expr.right)
        return f'({expr.op};{left};{right})'

    if isinstance(expr, UnaryOp):
        operand = compile_expr(expr.operand)
        return f'({expr.op};{operand})'

    if isinstance(expr, FuncCall):
        args = ';'.join(compile_expr(a) for a in expr.args)
        return f'({expr.func_name};{args})'

    if isinstance(expr, AggFunc):
        col = compile_expr(expr.column)
        return f'({expr.func_name};{col})'

    if isinstance(expr, FbyExpr):
        col = compile_expr(expr.col)
        group = compile_expr(expr.group_col)
        return f'(fby;(enlist;{expr.agg_name};{col});{group})'

    if isinstance(expr, EachExpr):
        if isinstance(expr.func_expr, AggFunc):
            func_name = expr.func_expr.func_name
            col = compile_expr(expr.func_expr.column)
            if expr.adverb == 'each':
                return f"(';{func_name};{col})"
            return f"({expr.adverb};{func_name};{col})"
        inner = compile_expr(expr.func_expr)
        return f"({expr.adverb};{inner})"

    raise ValueError(f"Cannot compile expression: {expr!r}")


def _compile_literal(value: Any) -> str:
    """Compile a Python literal to its q representation."""
    if isinstance(value, _QSentinel):
        return value.q_repr
    if value is None:
        return '(::)'
    if isinstance(value, bool):
        return '1b' if value else '0b'
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if value != value:  # NaN
            return '0Nf'
        if value == float('inf'):
            return '0w'
        if value == float('-inf'):
            return '-0w'
        return str(value)
    if isinstance(value, datetime.datetime):
        ns = f'{value.microsecond * 1000:09d}'
        return value.strftime(f'%Y.%m.%dD%H:%M:%S.') + ns
    if isinstance(value, datetime.date):
        return value.strftime('%Y.%m.%d')
    if isinstance(value, datetime.time):
        ns = f'{value.microsecond * 1000:09d}'
        return value.strftime(f'%H:%M:%S.') + ns
    if isinstance(value, datetime.timedelta):
        total_ns = int(value.total_seconds() * 1_000_000_000)
        sign = '-' if total_ns < 0 else ''
        total_ns = abs(total_ns)
        days, rem = divmod(total_ns, 86_400_000_000_000)
        hours, rem = divmod(rem, 3_600_000_000_000)
        minutes, rem = divmod(rem, 60_000_000_000)
        seconds, nanos = divmod(rem, 1_000_000_000)
        return f'{sign}{days}D{hours:02d}:{minutes:02d}:{seconds:02d}.{nanos:09d}'
    if isinstance(value, str):
        # q date-like strings (YYYY.MM.DD) → emit unquoted
        if _Q_DATE_RE.match(value):
            return value
        # q timestamp-like strings (YYYY.MM.DDDhh:mm:ss)
        if _Q_TIMESTAMP_RE.match(value):
            return value
        # q time-like strings (hh:mm:ss)
        if _Q_TIME_RE.match(value):
            return value
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


# ── Dictionary helpers ─────────────────────────────────────────────

def _compile_dict(entries: list[tuple[str, str]]) -> str:
    """Build a q dictionary from ``(key_name, compiled_value)`` pairs.

    Single entry uses ``enlist`` to create one-element lists.
    Multiple entries use the compact ``key1`key2!(val1;val2)`` form.
    """
    if not entries:
        return '()'
    if len(entries) == 1:
        name, value = entries[0]
        return f'(enlist `{name})!(enlist {value})'
    keys = '`' + '`'.join(name for name, _ in entries)
    values = ';'.join(value for _, value in entries)
    return f'{keys}!({values})'


# ── WHERE / BY / SELECT ───────────────────────────────────────────

def compile_where(clauses: list[Expr]) -> str:
    """Compile WHERE clauses to q constraint list.

    Each clause compiles to a parse tree, e.g. ``(=;`date;2026.02.17)``.
    """
    if not clauses:
        return '()'
    parts = [compile_expr(c) for c in clauses]
    if len(parts) == 1:
        return f'enlist {parts[0]}'
    return f'({";".join(parts)})'


def compile_by(
    by_exprs: list[Expr | Column],
    named: dict[str, Expr] | None = None,
) -> str:
    """Compile GROUP BY expressions to a dictionary.

    Returns ``0b`` when there is no grouping.
    """
    if not by_exprs and not named:
        return '0b'
    entries: list[tuple[str, str]] = []
    for expr in by_exprs:
        if isinstance(expr, Column):
            entries.append((expr.name, f'`{expr.name}'))
        else:
            compiled = compile_expr(expr)
            name = _infer_expr_name(expr) or f'x{len(entries)}'
            entries.append((name, compiled))
    if named:
        for alias, expr in named.items():
            entries.append((alias, compile_expr(expr)))
    return _compile_dict(entries)


def compile_select_columns(
    columns: list[Expr | Column] | None,
    named: dict[str, Expr] | None = None,
) -> str:
    """Compile the select (aggregation) dictionary.

    Returns a q dictionary like ``\`sym\`avg_price!(\`sym;(avg;\`price))``,
    using ``enlist`` for single entries, or ``()`` for select-all.
    """
    if not columns and not named:
        return '()'

    entries: list[tuple[str, str]] = []
    if columns:
        for col in columns:
            if isinstance(col, Column):
                entries.append((col.name, f'`{col.name}'))
            elif isinstance(col, AggFunc):
                compiled = compile_expr(col)
                col_name = _infer_agg_name(col)
                entries.append((col_name, compiled))
            else:
                compiled = compile_expr(col)
                name = _infer_expr_name(col) or compiled
                entries.append((name, compiled))

    if named:
        for alias, expr in named.items():
            entries.append((alias, compile_expr(expr)))

    return _compile_dict(entries)


def _infer_agg_name(agg: AggFunc) -> str:
    """Infer a column name for an aggregate expression."""
    if isinstance(agg.column, Column):
        return f'{agg.func_name}_{agg.column.name}'
    return agg.func_name


def _infer_expr_name(expr: Expr) -> str | None:
    """Try to infer a name for an unnamed expression."""
    if isinstance(expr, Column):
        return expr.name
    if isinstance(expr, AggFunc) and isinstance(expr.column, Column):
        return f'{expr.func_name}_{expr.column.name}'
    if isinstance(expr, FuncCall):
        for arg in reversed(expr.args):
            if isinstance(arg, Column):
                return arg.name
    return None


# ── Top-level compilation ─────────────────────────────────────────

def compile_functional_select(
    table: str,
    where_clauses: list[Expr],
    by_exprs: list[Expr],
    columns: list[Expr] | None,
    named: dict[str, Expr] | None,
    by_named: dict[str, Expr] | None = None,
) -> str:
    """Compile a full functional select: ?[t;c;b;a]."""
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
    entries = [(name, compile_expr(expr)) for name, expr in assignments.items()]
    a = _compile_dict(entries)
    return f'![{t};{c};{b};{a}]'


def compile_functional_delete(
    table: str,
    where_clauses: list[Expr],
    columns: list[str] | None = None,
) -> str:
    """Compile a functional delete: ![t;c;0b;a]."""
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
    """Compile exec column list.

    Single column without alias → atom symbol: ``\`price``
    Multiple or named → dictionary: ``\`sym\`price!(\`sym;\`price)``
    """
    all_parts: list[tuple[str, str]] = []

    if columns:
        for col in columns:
            if isinstance(col, Column):
                all_parts.append((col.name, f'`{col.name}'))
            elif isinstance(col, AggFunc):
                compiled = compile_expr(col)
                col_name = _infer_agg_name(col)
                all_parts.append((col_name, compiled))
            else:
                compiled = compile_expr(col)
                name = _infer_expr_name(col) or compiled
                all_parts.append((name, compiled))

    if named:
        for alias, expr in named.items():
            all_parts.append((alias, compile_expr(expr)))

    if not all_parts:
        return '()'

    # Single unnamed column → atom form
    if len(all_parts) == 1 and not named:
        name, _compiled = all_parts[0]
        return f'`{name}'

    return _compile_dict(all_parts)


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
