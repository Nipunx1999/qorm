"""Subscription support for kdb+ tick / pub-sub (.u.sub pattern).

Provides async subscription to real-time data published via kdb+ tickerplant.
The subscriber connects, sends ``.u.sub[table; syms]``, and then listens
for incoming async messages (type 0) containing table updates.

Usage::

    async def on_trade(table_name, data):
        print(f"Got {len(data)} rows for {table_name}")

    sub = Subscriber(engine, callback=on_trade)
    await sub.subscribe("trade", ["AAPL", "MSFT"])
    await sub.listen()  # blocks, calling on_trade for each update
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, Awaitable

from .connection.async_conn import AsyncConnection
from .exc import ConnectionError as QConnError
from .protocol.constants import ASYNC_MSG, HEADER_SIZE
from .protocol.compress import decompress
from .protocol.framing import unpack_header

log = logging.getLogger("qorm.subscription")

# Type for the callback: receives (table_name: str, data: Any)
SubscriptionCallback = Callable[[str, Any], Awaitable[None] | None]


class Subscriber:
    """Async subscriber for kdb+ real-time data.

    Parameters
    ----------
    engine : Engine
        Engine pointing to the tickerplant / publisher.
    callback : SubscriptionCallback
        Called with (table_name, data) for each incoming update.
    """

    def __init__(
        self,
        engine: Any,
        callback: SubscriptionCallback,
    ) -> None:
        self._engine = engine
        self._callback = callback
        self._conn: AsyncConnection | None = None
        self._running = False
        self._subscriptions: list[tuple[str, list[str] | str]] = []

    async def connect(self) -> None:
        """Open the connection to the tickerplant."""
        self._conn = self._engine.async_connect()
        await self._conn.open()
        log.debug("Subscriber connected to %s:%s", self._engine.host, self._engine.port)

    async def subscribe(self, table: str, syms: list[str] | str = "") -> Any:
        """Subscribe to a table.

        Parameters
        ----------
        table : str
            Table name to subscribe to (e.g., "trade").
        syms : list[str] | str
            Symbol filter. Empty string or ``"`"`` for all symbols.
        """
        if self._conn is None:
            await self.connect()

        sym_arg = syms if isinstance(syms, str) else syms
        self._subscriptions.append((table, sym_arg))

        # Send .u.sub[table; syms] as a sync query to get schema back
        result = await self._conn.query(".u.sub", f"`{table}", sym_arg)
        log.debug("Subscribed to %s (syms=%s)", table, syms)
        return result

    async def listen(self) -> None:
        """Listen for incoming updates in a loop.

        This blocks (asyncio-style) until ``stop()`` is called or the
        connection is closed.
        """
        if self._conn is None:
            raise QConnError("Not connected. Call connect() or subscribe() first.")

        self._running = True
        log.info("Subscriber listening for updates...")
        reader = self._conn._reader

        while self._running and reader is not None:
            try:
                header_bytes = await reader.readexactly(HEADER_SIZE)
            except (asyncio.IncompleteReadError, ConnectionError):
                log.warning("Connection lost during listen")
                self._running = False
                break

            _, msg_type, total_length = unpack_header(header_bytes)
            remaining = total_length - HEADER_SIZE
            if remaining > 0:
                payload = await reader.readexactly(remaining)
            else:
                payload = b''

            if header_bytes[2]:
                full_msg = decompress(payload)
            else:
                full_msg = header_bytes + payload
            _, result = self._conn._deserializer.deserialize_message(full_msg)

            # kdb+ pub-sub sends async messages (type 0) as
            # (function_name; table_name; data) or just (table_name; data)
            if isinstance(result, list) and len(result) >= 2:
                # Common pattern: (`upd; `trade; data) or (`trade; data)
                if len(result) == 3:
                    table_name = str(result[1])
                    data = result[2]
                else:
                    table_name = str(result[0])
                    data = result[1]

                log.debug("Received update for %s", table_name)
                cb_result = self._callback(table_name, data)
                if asyncio.iscoroutine(cb_result):
                    await cb_result
            else:
                log.debug("Received non-table message: %s", type(result).__name__)

    def stop(self) -> None:
        """Signal the listener to stop."""
        self._running = False
        log.debug("Subscriber stop requested")

    async def close(self) -> None:
        """Stop listening and close the connection."""
        self.stop()
        if self._conn is not None:
            await self._conn.close()
            self._conn = None
            log.debug("Subscriber connection closed")

    async def __aenter__(self) -> Subscriber:
        await self.connect()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()
