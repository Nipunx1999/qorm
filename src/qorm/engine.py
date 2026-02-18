"""Engine: DSN configuration and connection factory."""

from __future__ import annotations

from typing import Any

from .connection.sync_conn import SyncConnection
from .connection.async_conn import AsyncConnection


class Engine:
    """Central configuration point for connecting to a q/kdb+ process.

    Usage::

        engine = Engine(host="localhost", port=5000)
        engine = Engine.from_dsn("kdb://user:pass@localhost:5000")
    """

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

    @classmethod
    def from_dsn(cls, dsn: str) -> Engine:
        """Create an Engine from a DSN string.

        Format: ``kdb://[user:pass@]host:port``
        """
        dsn = dsn.removeprefix('kdb://')
        username = password = ''
        if '@' in dsn:
            creds, dsn = dsn.rsplit('@', 1)
            if ':' in creds:
                username, password = creds.split(':', 1)
            else:
                username = creds
        host, port_str = dsn.rsplit(':', 1)
        return cls(host=host, port=int(port_str),
                   username=username, password=password)

    def connect(self) -> SyncConnection:
        """Create a new synchronous connection."""
        return SyncConnection(
            host=self.host, port=self.port,
            username=self.username, password=self.password,
            timeout=self.timeout,
        )

    def async_connect(self) -> AsyncConnection:
        """Create a new async connection."""
        return AsyncConnection(
            host=self.host, port=self.port,
            username=self.username, password=self.password,
            timeout=self.timeout,
        )

    def __repr__(self) -> str:
        return f"Engine(host={self.host!r}, port={self.port})"
