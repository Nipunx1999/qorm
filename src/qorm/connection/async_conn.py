"""Asyncio connection to q/kdb+."""

from __future__ import annotations

import asyncio
from typing import Any

from ..exc import ConnectionError as QConnError
from ..protocol.constants import SYNC_MSG, HEADER_SIZE
from ..protocol.framing import unpack_header
from ..protocol.serializer import Serializer
from ..protocol.deserializer import Deserializer
from .base import AsyncBaseConnection
from .handshake import build_handshake, parse_handshake_response


class AsyncConnection(AsyncBaseConnection):
    """Asynchronous connection to a q/kdb+ process using asyncio."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 5000,
        username: str = "",
        password: str = "",
        timeout: float | None = None,
    ) -> None:
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.timeout = timeout
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._serializer = Serializer()
        self._deserializer = Deserializer()
        self._capability: int = 0

    async def open(self) -> None:
        if self._writer is not None:
            return
        try:
            self._reader, self._writer = await asyncio.wait_for(
                asyncio.open_connection(self.host, self.port),
                timeout=self.timeout,
            )
        except OSError as e:
            raise QConnError(f"Cannot connect to {self.host}:{self.port}: {e}") from e

        try:
            await self._handshake()
        except Exception:
            await self.close()
            raise

    async def _handshake(self) -> None:
        hs = build_handshake(self.username, self.password)
        self._writer.write(hs)
        await self._writer.drain()
        resp = await asyncio.wait_for(self._reader.read(1), timeout=self.timeout)
        self._capability = parse_handshake_response(resp)

    async def close(self) -> None:
        if self._writer is not None:
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except OSError:
                pass
            self._writer = None
            self._reader = None

    @property
    def is_open(self) -> bool:
        return self._writer is not None

    async def send(self, obj: Any, msg_type: int = SYNC_MSG) -> None:
        if self._writer is None:
            raise QConnError("Connection is not open")
        data = self._serializer.serialize_message(obj, msg_type)
        self._writer.write(data)
        await self._writer.drain()

    async def receive(self) -> Any:
        if self._reader is None:
            raise QConnError("Connection is not open")
        header_bytes = await self._reader.readexactly(HEADER_SIZE)
        _, msg_type, total_length = unpack_header(header_bytes)
        remaining = total_length - HEADER_SIZE
        if remaining > 0:
            payload = await self._reader.readexactly(remaining)
        else:
            payload = b''
        full_msg = header_bytes + payload
        _, result = self._deserializer.deserialize_message(full_msg)
        return result

    async def query(self, q_expr: str, *args: Any) -> Any:
        """Send a q expression asynchronously and return the result."""
        if args:
            await self.send([q_expr, *args])
        else:
            await self.send(q_expr)
        return await self.receive()
