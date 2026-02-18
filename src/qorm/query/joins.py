"""Join builders: aj, lj, ij, wj for q/kdb+."""

from __future__ import annotations

from typing import Any, TYPE_CHECKING

from .expressions import Column, Expr

if TYPE_CHECKING:
    from ..model.base import Model


class JoinQuery:
    """Base join query."""

    def __init__(self, join_type: str, left: type[Model], right: type[Model],
                 on: list[str | Column]) -> None:
        self.join_type = join_type
        self.left = left
        self.right = right
        self.on_columns = [
            c.name if isinstance(c, Column) else c for c in on
        ]

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
    """

    def __init__(self, on: list[str | Column], left: type[Model],
                 right: type[Model]) -> None:
        super().__init__('aj', left, right, on)

    def compile(self) -> str:
        cols = '`' + '`'.join(self.on_columns)
        return f'aj[{cols};{self.left.__tablename__};{self.right.__tablename__}]'


class LeftJoin(JoinQuery):
    """Left join: lj[left_table; right_table].

    For keyed tables, the right table is the keyed table.
    """

    def __init__(self, on: list[str | Column], left: type[Model],
                 right: type[Model]) -> None:
        super().__init__('lj', left, right, on)

    def compile(self) -> str:
        cols = '`' + '`'.join(self.on_columns)
        return f'{self.left.__tablename__} lj `{cols} xkey {self.right.__tablename__}'


class InnerJoin(JoinQuery):
    """Inner join: ij[left_table; right_table]."""

    def __init__(self, on: list[str | Column], left: type[Model],
                 right: type[Model]) -> None:
        super().__init__('ij', left, right, on)

    def compile(self) -> str:
        cols = '`' + '`'.join(self.on_columns)
        return f'{self.left.__tablename__} ij `{cols} xkey {self.right.__tablename__}'


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
                 aggs: dict[str, str] | None = None) -> None:
        super().__init__('wj', left, right, on)
        self.windows = windows
        self.aggs = aggs or {}

    def compile(self) -> str:
        cols = '`' + '`'.join(self.on_columns)
        w_lo, w_hi = self.windows
        time_col = self.on_columns[-1]  # last column is typically time

        agg_parts = []
        for col, func in self.aggs.items():
            agg_parts.append(f'({func};`{col})')
        agg_str = ';'.join(agg_parts) if agg_parts else ''

        return (
            f'wj[{w_lo} {w_hi}+{self.left.__tablename__}.{time_col};'
            f'{cols};{self.left.__tablename__};'
            f'({self.right.__tablename__};{agg_str})]'
        )


# ── Convenience functions ──────────────────────────────────────────

def aj(on: list[str | Column | Any], left: type[Model],
       right: type[Model]) -> AsOfJoin:
    """Create an as-of join."""
    return AsOfJoin(on, left, right)


def lj(on: list[str | Column | Any], left: type[Model],
       right: type[Model]) -> LeftJoin:
    """Create a left join."""
    return LeftJoin(on, left, right)


def ij(on: list[str | Column | Any], left: type[Model],
       right: type[Model]) -> InnerJoin:
    """Create an inner join."""
    return InnerJoin(on, left, right)


def wj(windows: tuple[int, int], on: list[str | Column | Any],
       left: type[Model], right: type[Model],
       aggs: dict[str, str] | None = None) -> WindowJoin:
    """Create a window join."""
    return WindowJoin(windows, on, left, right, aggs)
