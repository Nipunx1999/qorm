"""Expression tree for building q queries.

Supports operator overloading so that ``Trade.price > 100`` produces
a BinOp expression tree that compiles to q.
"""

from __future__ import annotations

from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from ..model.base import Model


class Expr:
    """Base class for all expression nodes."""

    def __gt__(self, other: Any) -> BinOp:
        return BinOp('>', self, _wrap(other))

    def __ge__(self, other: Any) -> BinOp:
        return BinOp('>=', self, _wrap(other))

    def __lt__(self, other: Any) -> BinOp:
        return BinOp('<', self, _wrap(other))

    def __le__(self, other: Any) -> BinOp:
        return BinOp('<=', self, _wrap(other))

    def __eq__(self, other: Any) -> BinOp:  # type: ignore[override]
        return BinOp('=', self, _wrap(other))

    def __ne__(self, other: Any) -> BinOp:  # type: ignore[override]
        return BinOp('<>', self, _wrap(other))

    def __add__(self, other: Any) -> BinOp:
        return BinOp('+', self, _wrap(other))

    def __sub__(self, other: Any) -> BinOp:
        return BinOp('-', self, _wrap(other))

    def __mul__(self, other: Any) -> BinOp:
        return BinOp('*', self, _wrap(other))

    def __truediv__(self, other: Any) -> BinOp:
        return BinOp('%', self, _wrap(other))  # q uses % for division

    def __mod__(self, other: Any) -> BinOp:
        return BinOp('mod', self, _wrap(other))

    def __and__(self, other: Any) -> BinOp:
        return BinOp('&', self, _wrap(other))

    def __or__(self, other: Any) -> BinOp:
        return BinOp('|', self, _wrap(other))

    def __neg__(self) -> UnaryOp:
        return UnaryOp('neg', self)

    def __invert__(self) -> UnaryOp:
        return UnaryOp('not', self)

    def within(self, low: Any, high: Any) -> FuncCall:
        """x within (low; high)."""
        return FuncCall('within', [self, Literal((low, high))])

    def like(self, pattern: str) -> FuncCall:
        """x like pattern."""
        return FuncCall('like', [self, Literal(pattern)])

    def in_(self, values: list) -> FuncCall:
        """x in values."""
        return FuncCall('in', [self, Literal(values)])

    def asc(self) -> FuncCall:
        return FuncCall('asc', [self])

    def desc(self) -> FuncCall:
        return FuncCall('desc', [self])


class Column(Expr):
    """Reference to a model column."""
    __slots__ = ('name', 'model')

    def __init__(self, name: str, model: type[Model] | None = None) -> None:
        self.name = name
        self.model = model

    def __repr__(self) -> str:
        if self.model:
            return f"Column({self.model.__name__}.{self.name})"
        return f"Column({self.name})"


class Literal(Expr):
    """A literal value in an expression."""
    __slots__ = ('value',)

    def __init__(self, value: Any) -> None:
        self.value = value

    def __repr__(self) -> str:
        return f"Literal({self.value!r})"


class BinOp(Expr):
    """Binary operation expression."""
    __slots__ = ('op', 'left', 'right')

    def __init__(self, op: str, left: Expr, right: Expr) -> None:
        self.op = op
        self.left = left
        self.right = right

    def __repr__(self) -> str:
        return f"BinOp({self.left!r} {self.op} {self.right!r})"


class UnaryOp(Expr):
    """Unary operation expression."""
    __slots__ = ('op', 'operand')

    def __init__(self, op: str, operand: Expr) -> None:
        self.op = op
        self.operand = operand

    def __repr__(self) -> str:
        return f"UnaryOp({self.op} {self.operand!r})"


class FuncCall(Expr):
    """Function call expression (e.g., avg, sum, count)."""
    __slots__ = ('func_name', 'args')

    def __init__(self, func_name: str, args: list[Expr]) -> None:
        self.func_name = func_name
        self.args = args

    def __repr__(self) -> str:
        args_str = ', '.join(repr(a) for a in self.args)
        return f"FuncCall({self.func_name}({args_str}))"


class AggFunc(Expr):
    """Aggregate function (avg, sum, min, max, count, etc.)."""
    __slots__ = ('func_name', 'column')

    def __init__(self, func_name: str, column: Expr) -> None:
        self.func_name = func_name
        self.column = column

    def each(self) -> EachExpr:
        """Apply this aggregate with the ``each`` adverb."""
        return EachExpr(self, 'each')

    def peach(self) -> EachExpr:
        """Apply this aggregate with the ``peach`` adverb."""
        return EachExpr(self, 'peach')

    def __repr__(self) -> str:
        return f"AggFunc({self.func_name}({self.column!r}))"


class FbyExpr(Expr):
    """Filter-by expression: ``(agg;col) fby group_col``."""
    __slots__ = ('agg_name', 'col', 'group_col')

    def __init__(self, agg_name: str, col: Expr, group_col: Expr) -> None:
        self.agg_name = agg_name
        self.col = col
        self.group_col = group_col

    def __repr__(self) -> str:
        return f"FbyExpr({self.agg_name}, {self.col!r}, {self.group_col!r})"


class EachExpr(Expr):
    """Adverb expression: ``func each col`` or ``func peach col``."""
    __slots__ = ('func_expr', 'adverb')

    def __init__(self, func_expr: Expr, adverb: str) -> None:
        self.func_expr = func_expr
        self.adverb = adverb

    def __repr__(self) -> str:
        return f"EachExpr({self.func_expr!r}, {self.adverb!r})"


class _QSentinel:
    """Sentinel for q built-in values that compile to exact q strings."""
    __slots__ = ('q_repr',)

    def __init__(self, q_repr: str) -> None:
        self.q_repr = q_repr

    def __repr__(self) -> str:
        return f"_QSentinel({self.q_repr!r})"


# ── Helper functions ───────────────────────────────────────────────

def _wrap(value: Any) -> Expr:
    """Wrap a Python value as a Literal if it's not already an Expr."""
    if isinstance(value, Expr):
        return value
    return Literal(value)


# ── Aggregate function constructors ───────────────────────────────

def avg_(col: Expr | str) -> AggFunc:
    """avg aggregate."""
    return AggFunc('avg', _wrap_col(col))

def sum_(col: Expr | str) -> AggFunc:
    """sum aggregate."""
    return AggFunc('sum', _wrap_col(col))

def min_(col: Expr | str) -> AggFunc:
    """min aggregate."""
    return AggFunc('min', _wrap_col(col))

def max_(col: Expr | str) -> AggFunc:
    """max aggregate."""
    return AggFunc('max', _wrap_col(col))

def count_(col: Expr | str | None = None) -> AggFunc:
    """count aggregate."""
    if col is None:
        return AggFunc('count', Column('i'))  # count i
    return AggFunc('count', _wrap_col(col))

def first_(col: Expr | str) -> AggFunc:
    """first aggregate."""
    return AggFunc('first', _wrap_col(col))

def last_(col: Expr | str) -> AggFunc:
    """last aggregate."""
    return AggFunc('last', _wrap_col(col))

def med_(col: Expr | str) -> AggFunc:
    """med (median) aggregate."""
    return AggFunc('med', _wrap_col(col))

def dev_(col: Expr | str) -> AggFunc:
    """dev (standard deviation) aggregate."""
    return AggFunc('dev', _wrap_col(col))

def var_(col: Expr | str) -> AggFunc:
    """var (variance) aggregate."""
    return AggFunc('var', _wrap_col(col))

def wavg_(weights: Expr | str, values: Expr | str) -> AggFunc:
    """wavg (weighted average)."""
    return AggFunc('wavg', _wrap_col(weights))  # simplified

def _wrap_col(col: Expr | str) -> Expr:
    if isinstance(col, str):
        return Column(col)
    return col


# ── Temporal helpers ──────────────────────────────────────────────

def xbar_(bucket: int, col: Expr | str) -> FuncCall:
    """``bucket xbar col`` — bucket timestamps/values."""
    return FuncCall('xbar', [Literal(bucket), _wrap_col(col)])


def today_() -> Literal:
    """Compile to ``.z.d`` (current date)."""
    return Literal(_QSentinel('.z.d'))


def now_() -> Literal:
    """Compile to ``.z.p`` (current timestamp)."""
    return Literal(_QSentinel('.z.p'))


# ── fby (filter by) ──────────────────────────────────────────────

def fby_(agg_name: str, col: Expr | str, group_col: Expr | str) -> FbyExpr:
    """``(agg;col) fby group_col``."""
    return FbyExpr(agg_name, _wrap_col(col), _wrap_col(group_col))


# ── each / peach ─────────────────────────────────────────────────

def each_(func_name: str, col: Expr | str) -> EachExpr:
    """``func_name each col``."""
    return EachExpr(AggFunc(func_name, _wrap_col(col)), 'each')


def peach_(func_name: str, col: Expr | str) -> EachExpr:
    """``func_name peach col``."""
    return EachExpr(AggFunc(func_name, _wrap_col(col)), 'peach')
