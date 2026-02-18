"""Pagination helpers for iterating over large result sets."""

from __future__ import annotations

from typing import Any, Generator, AsyncGenerator, TYPE_CHECKING

if TYPE_CHECKING:
    from .session import Session, AsyncSession, ModelResultSet
    from .query.select import SelectQuery


def paginate(
    session: Session,
    query: SelectQuery,
    page_size: int,
) -> Generator[ModelResultSet, None, None]:
    """Yield successive pages of *page_size* rows.

    Stops when a page has fewer rows than *page_size* or is empty.
    """
    page = 0
    while True:
        paged = _clone_with_page(query, page, page_size)
        result = session.exec(paged)
        if result is None or (hasattr(result, '__len__') and len(result) == 0):
            break
        yield result
        if hasattr(result, '__len__') and len(result) < page_size:
            break
        page += 1


async def async_paginate(
    session: AsyncSession,
    query: SelectQuery,
    page_size: int,
) -> AsyncGenerator[ModelResultSet, None]:
    """Async version of :func:`paginate`."""
    page = 0
    while True:
        paged = _clone_with_page(query, page, page_size)
        result = await session.exec(paged)
        if result is None or (hasattr(result, '__len__') and len(result) == 0):
            break
        yield result
        if hasattr(result, '__len__') and len(result) < page_size:
            break
        page += 1


def _clone_with_page(query: SelectQuery, page: int, page_size: int) -> SelectQuery:
    """Clone a SelectQuery with offset/limit for the given page."""
    from .query.select import SelectQuery

    clone = SelectQuery(
        model=query.model,
        columns=tuple(query._columns),
        named_columns=dict(query._named),
    )
    clone._where = list(query._where)
    clone._by = list(query._by)
    clone._by_named = dict(query._by_named)
    clone._offset_n = page * page_size
    clone._limit_n = page_size
    return clone
