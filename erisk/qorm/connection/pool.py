"""Connection pools (Queue-based) for sync and async connections."""

from __future__ import annotations

import asyncio
import logging
import queue
import threading
from typing import Any, TYPE_CHECKING

from ..exc import PoolExhaustedError, PoolError

if TYPE_CHECKING:
    from ..engine import Engine
    from .sync_conn import SyncConnection
    from .async_conn import AsyncConnection

log = logging.getLogger("qorm.pool")


class SyncPool:
    """Thread-safe synchronous connection pool.

    Connections are health-checked on acquire.  Stale connections are
    replaced transparently — the caller always receives a usable connection.

    Usage::

        pool = SyncPool(engine, min_size=2, max_size=10)
        conn = pool.acquire()
        try:
            result = conn.query("select from trade")
        finally:
            pool.release(conn)
    """

    def __init__(self, engine: Engine, min_size: int = 1,
                 max_size: int = 10, timeout: float = 30.0,
                 check_on_acquire: bool = True) -> None:
        self._engine = engine
        self._min_size = min_size
        self._max_size = max_size
        self._timeout = timeout
        self._check_on_acquire = check_on_acquire
        self._pool: queue.Queue[SyncConnection] = queue.Queue(maxsize=max_size)
        self._size = 0
        self._lock = threading.Lock()
        self._closed = False

        # Pre-populate with min_size connections
        for _ in range(min_size):
            self._add_connection()

    def _add_connection(self) -> None:
        conn = self._engine.connect()
        conn.open()
        self._pool.put(conn)
        self._size += 1

    def _replace_connection(self, dead_conn: SyncConnection) -> SyncConnection:
        """Close a dead connection and create a fresh one."""
        try:
            dead_conn.close()
        except Exception:
            pass
        log.debug("Replacing dead connection to %s:%s", self._engine.host, self._engine.port)
        conn = self._engine.connect()
        conn.open()
        return conn

    def acquire(self) -> SyncConnection:
        """Acquire a connection from the pool.

        If ``check_on_acquire`` is True, pings the connection before returning.
        Dead connections are replaced automatically.
        """
        if self._closed:
            raise PoolError("Pool is closed")

        try:
            conn = self._pool.get(timeout=0)
        except queue.Empty:
            with self._lock:
                if self._size < self._max_size:
                    self._add_connection()
                    return self._pool.get(timeout=0)
            # Pool is full, wait
            try:
                conn = self._pool.get(timeout=self._timeout)
            except queue.Empty:
                raise PoolExhaustedError(
                    f"No connections available (pool size: {self._max_size})")

        # Health check
        if self._check_on_acquire and not conn.is_open:
            conn = self._replace_connection(conn)

        return conn

    def release(self, conn: SyncConnection) -> None:
        """Return a connection to the pool."""
        if self._closed:
            conn.close()
            return
        if conn.is_open:
            self._pool.put(conn)
        else:
            with self._lock:
                self._size -= 1

    def close(self) -> None:
        """Close all connections in the pool."""
        self._closed = True
        while not self._pool.empty():
            try:
                conn = self._pool.get_nowait()
                conn.close()
            except queue.Empty:
                break
        self._size = 0

    @property
    def size(self) -> int:
        """Current number of connections (in-use + idle)."""
        return self._size

    def __enter__(self) -> SyncPool:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()


class AsyncPool:
    """Asyncio connection pool.

    Connections are health-checked on acquire.  Stale connections are
    replaced transparently.

    Usage::

        pool = AsyncPool(engine, min_size=2, max_size=10)
        conn = await pool.acquire()
        try:
            result = await conn.query("select from trade")
        finally:
            await pool.release(conn)
    """

    def __init__(self, engine: Engine, min_size: int = 1,
                 max_size: int = 10, timeout: float = 30.0,
                 check_on_acquire: bool = True) -> None:
        self._engine = engine
        self._min_size = min_size
        self._max_size = max_size
        self._timeout = timeout
        self._check_on_acquire = check_on_acquire
        self._pool: asyncio.Queue[AsyncConnection] = asyncio.Queue(maxsize=max_size)
        self._size = 0
        self._lock = asyncio.Lock()
        self._closed = False

    async def initialize(self) -> None:
        """Pre-populate the pool with min_size connections."""
        for _ in range(self._min_size):
            await self._add_connection()

    async def _add_connection(self) -> None:
        conn = self._engine.async_connect()
        await conn.open()
        await self._pool.put(conn)
        self._size += 1

    async def _replace_connection(self, dead_conn: AsyncConnection) -> AsyncConnection:
        """Close a dead connection and create a fresh one."""
        try:
            await dead_conn.close()
        except Exception:
            pass
        log.debug("Replacing dead connection to %s:%s", self._engine.host, self._engine.port)
        conn = self._engine.async_connect()
        await conn.open()
        return conn

    async def acquire(self) -> AsyncConnection:
        """Acquire a connection from the pool.

        If ``check_on_acquire`` is True, pings the connection before returning.
        Dead connections are replaced automatically.
        """
        if self._closed:
            raise PoolError("Pool is closed")

        try:
            conn = self._pool.get_nowait()
        except asyncio.QueueEmpty:
            async with self._lock:
                if self._size < self._max_size:
                    await self._add_connection()
                    return self._pool.get_nowait()
            try:
                conn = await asyncio.wait_for(
                    self._pool.get(), timeout=self._timeout)
            except asyncio.TimeoutError:
                raise PoolExhaustedError(
                    f"No connections available (pool size: {self._max_size})")

        # Health check
        if self._check_on_acquire and not conn.is_open:
            conn = await self._replace_connection(conn)

        return conn

    async def release(self, conn: AsyncConnection) -> None:
        """Return a connection to the pool."""
        if self._closed:
            await conn.close()
            return
        if conn.is_open:
            await self._pool.put(conn)
        else:
            self._size -= 1

    async def close(self) -> None:
        """Close all connections in the pool."""
        self._closed = True
        while not self._pool.empty():
            try:
                conn = self._pool.get_nowait()
                await conn.close()
            except asyncio.QueueEmpty:
                break
        self._size = 0

    @property
    def size(self) -> int:
        """Current number of connections (in-use + idle)."""
        return self._size

    async def __aenter__(self) -> AsyncPool:
        await self.initialize()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()
