"""SelectQuery: chainable SELECT query builder -> ?[t;c;b;a]."""

from __future__ import annotations

from typing import Any, TYPE_CHECKING

from .expressions import Expr, Column, AggFunc
from .compiler import compile_functional_select

if TYPE_CHECKING:
    from ..model.base import Model


class SelectQuery:
    """Chainable SELECT query builder.

    Usage::

        query = (Trade.select(Trade.sym, avg_price=avg_(Trade.price))
                      .where(Trade.price > 100)
                      .by(Trade.sym)
                      .limit(10))
    """

    def __init__(
        self,
        model: type[Model],
        columns: tuple[Any, ...] = (),
        named_columns: dict[str, Any] | None = None,
    ) -> None:
        self.model = model
        self._columns: list[Expr] = list(columns)
        self._named: dict[str, Expr] = dict(named_columns or {})
        self._where: list[Expr] = []
        self._by: list[Expr] = []
        self._limit_n: int | None = None

    def where(self, *conditions: Expr) -> SelectQuery:
        """Add WHERE conditions (ANDed together)."""
        self._where.extend(conditions)
        return self

    def by(self, *columns: Expr | Column) -> SelectQuery:
        """Add GROUP BY columns."""
        self._by.extend(columns)
        return self

    def limit(self, n: int) -> SelectQuery:
        """Limit the number of results."""
        self._limit_n = n
        return self

    def compile(self) -> str:
        """Compile to q functional form string."""
        q_str = compile_functional_select(
            table=self.model.__tablename__,
            where_clauses=self._where,
            by_exprs=self._by,
            columns=self._columns or None,
            named=self._named or None,
        )
        if self._limit_n is not None:
            q_str = f'{self._limit_n}#({q_str})'
        return q_str

    def explain(self) -> str:
        """Return the compiled q string without executing.

        Useful for debugging and understanding the generated query.
        """
        return f"-- SelectQuery on `{self.model.__tablename__}\n{self.compile()}"

    def __repr__(self) -> str:
        return f"SelectQuery({self.compile()})"
