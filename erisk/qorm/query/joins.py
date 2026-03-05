"""Join builders: aj, lj, ij, wj for q/kdb+.

Supports column mapping for joins across tables with different column names.
When ``column_map`` is provided, the right table is renamed before joining::

    aj(["sym", "time"], Trade, Quote, column_map={"sym": "symbol", "time": "ts"})
    # -> aj[`sym`time; trade; `sym`time xcol `symbol`ts xcol quote]
"""

from __future__ import annotations

from typing import Any, TYPE_CHECKING

from .expressions import Column, Expr

if TYPE_CHECKING:
    from ..model.base import Model


def _xcol_expr(right_table: str, column_map: dict[str, str]) -> str:
    """Build a chain of ``xcol`` renames for the right table.

    In q, ``xcol`` renames columns:  `newname xcol table
    Multiple renames are chained left-to-right.

    Parameters
    ----------
    right_table : str
        The right table name.
    column_map : dict[str, str]
        Mapping of left_column_name -> right_column_name.
        Only entries where the names actually differ produce a rename.
    """
    renames = [(left, right) for left, right in column_map.items() if left != right]
    if not renames:
        return right_table
    expr = right_table
    for left_name, right_name in renames:
        expr = f'`{left_name} xcol `{right_name} xcol {expr}'
    return expr


class JoinQuery:
    """Base join query with optional column mapping."""

    def __init__(self, join_type: str, left: type[Model], right: type[Model],
                 on: list[str | Column],
                 column_map: dict[str, str] | None = None) -> None:
        self.join_type = join_type
        self.left = left
        self.right = right
        self.on_columns = [
            c.name if isinstance(c, Column) else c for c in on
        ]
        # column_map: left_col_name -> right_col_name (for cols that differ)
        self.column_map = column_map or {}

    def _right_expr(self) -> str:
        """Return the right table expression, applying xcol renames if needed."""
        table = self.right.__tablename__
        if self.column_map:
            return _xcol_expr(table, self.column_map)
        return table

    def compile(self) -> str:
        raise NotImplementedError

    def explain(self) -> str:
        """Return the compiled q string without executing."""
        return f"-- {self.join_type} join: {self.left.__tablename__} <-> {self.right.__tablename__}\n{self.compile()}"

    def __repr__(self) -> str:
        return f"{self.join_type}({self.left.__tablename__}, {self.right.__tablename__})"


class AsOfJoin(JoinQuery):
    """As-of join: aj[columns; left_table; right_table].

    Usage::

        aj([Trade.sym, Trade.time], Trade, Quote)
        # -> aj[`sym`time; trade; quote]

        # With column mapping (right has 'symbol' instead of 'sym'):
        aj(["sym", "time"], Trade, Quote, column_map={"sym": "symbol"})
        # -> aj[`sym`time; trade; `sym xcol `symbol xcol quote]
    """

    def __init__(self, on: list[str | Column], left: type[Model],
                 right: type[Model],
                 column_map: dict[str, str] | None = None) -> None:
        super().__init__('aj', left, right, on, column_map)

    def compile(self) -> str:
        cols = '`' + '`'.join(self.on_columns)
        right = self._right_expr()
        return f'aj[{cols};{self.left.__tablename__};{right}]'


class LeftJoin(JoinQuery):
    """Left join: lj[left_table; right_table].

    For keyed tables, the right table is the keyed table.
    """

    def __init__(self, on: list[str | Column], left: type[Model],
                 right: type[Model],
                 column_map: dict[str, str] | None = None) -> None:
        super().__init__('lj', left, right, on, column_map)

    def compile(self) -> str:
        cols = '`' + '`'.join(self.on_columns)
        right = self._right_expr()
        return f'{self.left.__tablename__} lj `{cols} xkey {right}'


class InnerJoin(JoinQuery):
    """Inner join: ij[left_table; right_table]."""

    def __init__(self, on: list[str | Column], left: type[Model],
                 right: type[Model],
                 column_map: dict[str, str] | None = None) -> None:
        super().__init__('ij', left, right, on, column_map)

    def compile(self) -> str:
        cols = '`' + '`'.join(self.on_columns)
        right = self._right_expr()
        return f'{self.left.__tablename__} ij `{cols} xkey {right}'


class WindowJoin(JoinQuery):
    """Window join: wj[windows; columns; left_table; (right_table; (agg_exprs))].

    Usage::

        wj(
            windows=(-2000000000, 0),  # 2 second window
            on=[Trade.sym, Trade.time],
            left=Trade,
            right=Quote,
            aggs={"bid": "avg", "ask": "avg"},
        )
    """

    def __init__(self, windows: tuple[int, int], on: list[str | Column],
                 left: type[Model], right: type[Model],
                 aggs: dict[str, str] | None = None,
                 column_map: dict[str, str] | None = None) -> None:
        super().__init__('wj', left, right, on, column_map)
        self.windows = windows
        self.aggs = aggs or {}

    def compile(self) -> str:
        cols = '`' + '`'.join(self.on_columns)
        w_lo, w_hi = self.windows
        time_col = self.on_columns[-1]  # last column is typically time
        right = self._right_expr()

        agg_parts = []
        for col, func in self.aggs.items():
            agg_parts.append(f'({func};`{col})')
        agg_str = ';'.join(agg_parts) if agg_parts else ''

        return (
            f'wj[{w_lo} {w_hi}+{self.left.__tablename__}.{time_col};'
            f'{cols};{self.left.__tablename__};'
            f'({right};{agg_str})]'
        )


# ── Convenience functions ──────────────────────────────────────────

def aj(on: list[str | Column | Any], left: type[Model],
       right: type[Model],
       column_map: dict[str, str] | None = None) -> AsOfJoin:
    """Create an as-of join.

    Parameters
    ----------
    on : list
        Join columns (by left-table name).
    left, right : Model
        Left and right table models.
    column_map : dict, optional
        Mapping of ``{left_col: right_col}`` for columns with different names.
    """
    return AsOfJoin(on, left, right, column_map)


def lj(on: list[str | Column | Any], left: type[Model],
       right: type[Model],
       column_map: dict[str, str] | None = None) -> LeftJoin:
    """Create a left join."""
    return LeftJoin(on, left, right, column_map)


def ij(on: list[str | Column | Any], left: type[Model],
       right: type[Model],
       column_map: dict[str, str] | None = None) -> InnerJoin:
    """Create an inner join."""
    return InnerJoin(on, left, right, column_map)


def wj(windows: tuple[int, int], on: list[str | Column | Any],
       left: type[Model], right: type[Model],
       aggs: dict[str, str] | None = None,
       column_map: dict[str, str] | None = None) -> WindowJoin:
    """Create a window join."""
    return WindowJoin(windows, on, left, right, aggs, column_map)
