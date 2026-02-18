"""DeleteQuery: chainable DELETE query builder -> ![t;c;0b;a]."""

from __future__ import annotations

from typing import Any, TYPE_CHECKING

from .expressions import Expr
from .compiler import compile_functional_delete

if TYPE_CHECKING:
    from ..model.base import Model


class DeleteQuery:
    """Chainable DELETE query builder.

    Usage::

        # Delete rows matching condition
        query = Trade.delete().where(Trade.sym == "AAPL")

        # Delete specific columns (rare)
        query = Trade.delete().columns("price", "size")
    """

    def __init__(self, model: type[Model]) -> None:
        self.model = model
        self._where: list[Expr] = []
        self._columns: list[str] | None = None

    def where(self, *conditions: Expr) -> DeleteQuery:
        """Add WHERE conditions."""
        self._where.extend(conditions)
        return self

    def columns(self, *col_names: str) -> DeleteQuery:
        """Specify columns to delete (instead of rows)."""
        self._columns = list(col_names)
        return self

    def compile(self) -> str:
        """Compile to q functional form string."""
        return compile_functional_delete(
            table=self.model.__tablename__,
            where_clauses=self._where,
            columns=self._columns,
        )

    def __repr__(self) -> str:
        return f"DeleteQuery({self.compile()})"
