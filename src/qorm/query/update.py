"""UpdateQuery: chainable UPDATE query builder -> ![t;c;b;a]."""

from __future__ import annotations

from typing import Any, TYPE_CHECKING

from .expressions import Expr, Column, _wrap
from .compiler import compile_functional_update

if TYPE_CHECKING:
    from ..model.base import Model


class UpdateQuery:
    """Chainable UPDATE query builder.

    Usage::

        query = (Trade.update()
                      .set(price=Trade.price * 1.1)
                      .where(Trade.sym == "AAPL"))
    """

    def __init__(self, model: type[Model]) -> None:
        self.model = model
        self._assignments: dict[str, Expr] = {}
        self._where: list[Expr] = []
        self._by: list[Expr] = []

    def set(self, **assignments: Any) -> UpdateQuery:
        """Set column values.

        Values can be Expr objects or plain Python values.
        """
        for name, value in assignments.items():
            self._assignments[name] = _wrap(value)
        return self

    def where(self, *conditions: Expr) -> UpdateQuery:
        """Add WHERE conditions."""
        self._where.extend(conditions)
        return self

    def by(self, *columns: Expr | Column) -> UpdateQuery:
        """Add GROUP BY columns."""
        self._by.extend(columns)
        return self

    def compile(self) -> str:
        """Compile to q functional form string."""
        return compile_functional_update(
            table=self.model.__tablename__,
            where_clauses=self._where,
            by_exprs=self._by,
            assignments=self._assignments,
        )

    def __repr__(self) -> str:
        return f"UpdateQuery({self.compile()})"
