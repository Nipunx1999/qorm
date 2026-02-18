"""ExecQuery: chainable EXEC query builder -> ?[t;c;b;a] with exec-style columns."""

from __future__ import annotations

from typing import Any, TYPE_CHECKING

from .expressions import Expr, Column, AggFunc
from .compiler import compile_functional_exec

if TYPE_CHECKING:
    from ..model.base import Model


class ExecQuery:
    """Chainable EXEC query builder.

    q's ``exec`` returns values (vectors/dicts) instead of tables.

    Usage::

        query = Trade.exec_(Trade.price)                     # single column -> vector
        query = Trade.exec_(Trade.sym, Trade.price)           # multi column -> dict
        query = Trade.exec_(avg_price=avg_(Trade.price))      # named
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
        self._by_named: dict[str, Expr] = {}
        self._limit_n: int | None = None

    def where(self, *conditions: Expr) -> ExecQuery:
        """Add WHERE conditions (ANDed together)."""
        self._where.extend(conditions)
        return self

    def by(self, *columns: Expr | Column, **named: Expr) -> ExecQuery:
        """Add GROUP BY columns."""
        self._by.extend(columns)
        self._by_named.update(named)
        return self

    def limit(self, n: int) -> ExecQuery:
        """Limit the number of results."""
        self._limit_n = n
        return self

    def compile(self) -> str:
        """Compile to q functional exec form string."""
        q_str = compile_functional_exec(
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
        """Return the compiled q string without executing."""
        return f"-- ExecQuery on `{self.model.__tablename__}\n{self.compile()}"

    def __repr__(self) -> str:
        return f"ExecQuery({self.compile()})"
