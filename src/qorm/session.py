"""Session, AsyncSession, ResultMapper, and ModelResultSet."""

from __future__ import annotations

import logging
import time
from typing import Any, Iterator, TYPE_CHECKING

from .connection.sync_conn import SyncConnection
from .connection.async_conn import AsyncConnection
from .exc import ReflectionError
from .model.schema import create_table_q, drop_table_q, table_exists_q
from .model.reflect import build_model_from_meta
from .query.select import SelectQuery
from .query.update import UpdateQuery
from .query.delete import DeleteQuery
from .query.insert import InsertQuery
from .query.joins import JoinQuery
from .query.exec_ import ExecQuery

if TYPE_CHECKING:
    from .engine import Engine
    from .model.base import Model

log = logging.getLogger("qorm")


class ModelResultSet:
    """Lazy result set wrapping column-oriented data from kdb+.

    Iterating yields model instances (row-oriented view).
    Accessing columns directly preserves the column-oriented layout.
    """

    def __init__(self, data: dict[str, list], model: type[Model] | None = None) -> None:
        self._data = data
        self._model = model
        # Remove internal marker
        self._data.pop('__table__', None)
        self._columns = list(self._data.keys())
        self._length = len(next(iter(self._data.values()), []))

    def __len__(self) -> int:
        return self._length

    def __iter__(self) -> Iterator[Any]:
        """Iterate over rows, yielding model instances or SimpleNamespace."""
        if self._model:
            for i in range(self._length):
                row = {col: self._data[col][i] for col in self._columns}
                yield self._model(**row)
        else:
            from types import SimpleNamespace
            for i in range(self._length):
                row = {col: self._data[col][i] for col in self._columns}
                yield SimpleNamespace(**row)

    def __getitem__(self, key: str | int) -> Any:
        if isinstance(key, str):
            return self._data[key]
        # Integer index -> row
        if isinstance(key, int):
            row = {col: self._data[col][key] for col in self._columns}
            if self._model:
                return self._model(**row)
            from types import SimpleNamespace
            return SimpleNamespace(**row)
        raise TypeError(f"Key must be str or int, got {type(key)}")

    @property
    def columns(self) -> list[str]:
        return self._columns

    def to_dict(self) -> dict[str, list]:
        """Return column-oriented dictionary."""
        return dict(self._data)

    def to_dataframe(self) -> Any:
        """Convert to pandas DataFrame (requires pandas)."""
        try:
            import pandas as pd
        except ImportError:
            raise ImportError("pandas is required for to_dataframe(). "
                              "Install with: pip install qorm[pandas]")
        return pd.DataFrame(self._data)

    def __repr__(self) -> str:
        model_name = self._model.__name__ if self._model else 'raw'
        return f"ModelResultSet({model_name}, {self._length} rows, {self._columns})"


def _map_result(data: Any, model: type[Model] | None = None) -> Any:
    """Map raw kdb+ response to appropriate Python type."""
    if isinstance(data, dict) and data.get('__table__'):
        return ModelResultSet(data, model)
    return data


class Session:
    """Synchronous session for interacting with kdb+.

    Usage::

        with Session(engine) as session:
            session.create_table(Trade)
            result = session.exec(Trade.select().where(Trade.price > 100))
            session.raw("select from trade")
    """

    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        self._conn: SyncConnection | None = None
        self._retry = engine.retry

    def __enter__(self) -> Session:
        self._conn = self.engine.connect()
        self._conn.open()
        log.debug("Session opened to %s:%s", self.engine.host, self.engine.port)
        return self

    def __exit__(self, *exc: Any) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None
            log.debug("Session closed")

    @property
    def connection(self) -> SyncConnection:
        if self._conn is None:
            raise RuntimeError("Session is not open. Use 'with Session(engine) as s:'")
        return self._conn

    def _reconnect(self) -> None:
        """Close stale connection and open a fresh one."""
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass
        self._conn = self.engine.connect()
        self._conn.open()
        log.debug("Session reconnected to %s:%s", self.engine.host, self.engine.port)

    def _with_retry(self, func: Any) -> Any:
        """Wrap *func* with retry logic if a policy is configured."""
        if self._retry is None:
            return func()
        from .retry import retry_sync
        return retry_sync(func, self._retry, reconnect_fn=self._reconnect)

    def raw(self, q_expr: str, *args: Any) -> Any:
        """Execute a raw q expression."""
        log.debug("raw: %s", q_expr)
        t0 = time.perf_counter()
        result = self._with_retry(lambda: self.connection.query(q_expr, *args))
        elapsed = time.perf_counter() - t0
        log.debug("raw completed in %.3fms", elapsed * 1000)
        return _map_result(result)

    def exec(self, query: SelectQuery | UpdateQuery | DeleteQuery | InsertQuery | JoinQuery | ExecQuery) -> Any:
        """Execute a query object and return mapped results."""
        q_str = query.compile()
        log.debug("exec: %s", q_str)
        t0 = time.perf_counter()
        result = self._with_retry(lambda: self.connection.query(q_str))
        elapsed = time.perf_counter() - t0
        log.debug("exec completed in %.3fms", elapsed * 1000)
        model = getattr(query, 'model', None)
        return _map_result(result, model)

    def call(self, func_name: str, *args: Any) -> Any:
        """Call a named q function with the given arguments.

        Sends ``func_name`` (or ``(func_name; arg1; ...)`` for multiple args)
        over IPC and returns the result.
        """
        log.debug("call: %s(%s)", func_name, ", ".join(repr(a) for a in args))
        t0 = time.perf_counter()
        if args:
            result = self._with_retry(lambda: self.connection.query(func_name, *args))
        else:
            result = self._with_retry(lambda: self.connection.query(func_name))
        elapsed = time.perf_counter() - t0
        log.debug("call completed in %.3fms", elapsed * 1000)
        return _map_result(result)

    def create_table(self, model: type[Model]) -> Any:
        """Create a table from a Model class."""
        return self.raw(create_table_q(model))

    def drop_table(self, model: type[Model]) -> Any:
        """Drop a table."""
        return self.raw(drop_table_q(model))

    def table_exists(self, model: type[Model]) -> bool:
        """Check if a table exists."""
        return bool(self.raw(table_exists_q(model)))

    # ── Reflection ────────────────────────────────────────────────

    def tables(self) -> list[str]:
        """List all table names in the kdb+ process."""
        result = self.connection.query("tables[]")
        if isinstance(result, list):
            return result
        return list(result) if result else []

    def reflect(self, tablename: str) -> type[Model]:
        """Reflect a kdb+ table and return a dynamic Model class.

        Queries ``meta tablename`` for column info and ``keys tablename``
        for key columns.  Returns a KeyedModel if the table has keys.
        """
        try:
            meta_data = self.connection.query(f"meta {tablename}")
        except Exception as e:
            raise ReflectionError(
                f"Failed to get metadata for table {tablename!r}: {e}"
            ) from e

        # Try to get key columns; non-keyed tables return empty list
        key_columns: list[str] | None = None
        try:
            keys_result = self.connection.query(f"keys {tablename}")
            if isinstance(keys_result, list) and keys_result:
                key_columns = keys_result
        except Exception:
            pass  # If keys query fails, treat as non-keyed

        return build_model_from_meta(tablename, meta_data, key_columns=key_columns)

    def reflect_all(self) -> dict[str, type[Model]]:
        """Reflect all tables in the kdb+ process.

        Returns a dict mapping table name → Model class.
        """
        table_names = self.tables()
        models: dict[str, type[Model]] = {}
        for name in table_names:
            models[name] = self.reflect(name)
        return models


class AsyncSession:
    """Asynchronous session for interacting with kdb+.

    Usage::

        async with AsyncSession(engine) as session:
            await session.create_table(Trade)
            result = await session.exec(Trade.select())
    """

    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        self._conn: AsyncConnection | None = None
        self._retry = engine.retry

    async def __aenter__(self) -> AsyncSession:
        self._conn = self.engine.async_connect()
        await self._conn.open()
        log.debug("AsyncSession opened to %s:%s", self.engine.host, self.engine.port)
        return self

    async def __aexit__(self, *exc: Any) -> None:
        if self._conn:
            await self._conn.close()
            self._conn = None
            log.debug("AsyncSession closed")

    @property
    def connection(self) -> AsyncConnection:
        if self._conn is None:
            raise RuntimeError("AsyncSession is not open")
        return self._conn

    async def _reconnect(self) -> None:
        """Close stale connection and open a fresh one."""
        if self._conn:
            try:
                await self._conn.close()
            except Exception:
                pass
        self._conn = self.engine.async_connect()
        await self._conn.open()
        log.debug("AsyncSession reconnected to %s:%s", self.engine.host, self.engine.port)

    async def _with_retry(self, func: Any) -> Any:
        """Wrap async *func* with retry logic if a policy is configured."""
        if self._retry is None:
            return await func()
        from .retry import retry_async
        return await retry_async(func, self._retry, reconnect_fn=self._reconnect)

    async def raw(self, q_expr: str, *args: Any) -> Any:
        """Execute a raw q expression."""
        log.debug("async raw: %s", q_expr)
        t0 = time.perf_counter()
        result = await self._with_retry(lambda: self.connection.query(q_expr, *args))
        elapsed = time.perf_counter() - t0
        log.debug("async raw completed in %.3fms", elapsed * 1000)
        return _map_result(result)

    async def exec(self, query: SelectQuery | UpdateQuery | DeleteQuery | InsertQuery | JoinQuery | ExecQuery) -> Any:
        """Execute a query object and return mapped results."""
        q_str = query.compile()
        log.debug("async exec: %s", q_str)
        t0 = time.perf_counter()
        result = await self._with_retry(lambda: self.connection.query(q_str))
        elapsed = time.perf_counter() - t0
        log.debug("async exec completed in %.3fms", elapsed * 1000)
        model = getattr(query, 'model', None)
        return _map_result(result, model)

    async def call(self, func_name: str, *args: Any) -> Any:
        """Call a named q function with the given arguments."""
        log.debug("async call: %s(%s)", func_name, ", ".join(repr(a) for a in args))
        t0 = time.perf_counter()
        if args:
            result = await self._with_retry(lambda: self.connection.query(func_name, *args))
        else:
            result = await self._with_retry(lambda: self.connection.query(func_name))
        elapsed = time.perf_counter() - t0
        log.debug("async call completed in %.3fms", elapsed * 1000)
        return _map_result(result)

    async def create_table(self, model: type[Model]) -> Any:
        return await self.raw(create_table_q(model))

    async def drop_table(self, model: type[Model]) -> Any:
        return await self.raw(drop_table_q(model))

    async def table_exists(self, model: type[Model]) -> bool:
        return bool(await self.raw(table_exists_q(model)))

    # ── Reflection ────────────────────────────────────────────────

    async def tables(self) -> list[str]:
        """List all table names in the kdb+ process."""
        result = await self.connection.query("tables[]")
        if isinstance(result, list):
            return result
        return list(result) if result else []

    async def reflect(self, tablename: str) -> type[Model]:
        """Reflect a kdb+ table and return a dynamic Model class."""
        try:
            meta_data = await self.connection.query(f"meta {tablename}")
        except Exception as e:
            raise ReflectionError(
                f"Failed to get metadata for table {tablename!r}: {e}"
            ) from e

        key_columns: list[str] | None = None
        try:
            keys_result = await self.connection.query(f"keys {tablename}")
            if isinstance(keys_result, list) and keys_result:
                key_columns = keys_result
        except Exception:
            pass

        return build_model_from_meta(tablename, meta_data, key_columns=key_columns)

    async def reflect_all(self) -> dict[str, type[Model]]:
        """Reflect all tables in the kdb+ process."""
        table_names = await self.tables()
        models: dict[str, type[Model]] = {}
        for name in table_names:
            models[name] = await self.reflect(name)
        return models
