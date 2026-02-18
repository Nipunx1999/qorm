"""Engine: DSN configuration and connection factory."""

from __future__ import annotations

import ssl
from typing import Any

from .connection.sync_conn import SyncConnection
from .connection.async_conn import AsyncConnection


class Engine:
    """Central configuration point for connecting to a q/kdb+ process.

    Usage::

        engine = Engine(host="localhost", port=5000)
        engine = Engine.from_dsn("kdb://user:pass@localhost:5000")

        # With TLS
        engine = Engine(host="kdb-server", port=5000, tls=True)
        engine = Engine.from_dsn("kdb+tls://user:pass@kdb-server:5000")
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 5000,
        username: str = "",
        password: str = "",
        timeout: float | None = None,
        tls: bool = False,
        tls_context: ssl.SSLContext | None = None,
        tls_verify: bool = True,
        retry: Any = None,
    ) -> None:
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.timeout = timeout
        self.tls = tls
        self.tls_context = tls_context
        self.tls_verify = tls_verify
        self.retry = retry

    def _get_ssl_context(self) -> ssl.SSLContext | None:
        """Build or return the SSL context for TLS connections."""
        if not self.tls:
            return None
        if self.tls_context is not None:
            return self.tls_context
        ctx = ssl.create_default_context()
        if not self.tls_verify:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        return ctx

    @classmethod
    def from_dsn(cls, dsn: str) -> Engine:
        """Create an Engine from a DSN string.

        Format: ``kdb://[user:pass@]host:port`` or ``kdb+tls://[user:pass@]host:port``
        """
        tls = False
        if dsn.startswith('kdb+tls://'):
            tls = True
            dsn = dsn.removeprefix('kdb+tls://')
        else:
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
                   username=username, password=password, tls=tls)

    def connect(self) -> SyncConnection:
        """Create a new synchronous connection."""
        return SyncConnection(
            host=self.host, port=self.port,
            username=self.username, password=self.password,
            timeout=self.timeout,
            tls_context=self._get_ssl_context(),
        )

    def async_connect(self) -> AsyncConnection:
        """Create a new async connection."""
        return AsyncConnection(
            host=self.host, port=self.port,
            username=self.username, password=self.password,
            timeout=self.timeout,
            tls_context=self._get_ssl_context(),
        )

    def __repr__(self) -> str:
        tls_str = ', tls=True' if self.tls else ''
        return f"Engine(host={self.host!r}, port={self.port}{tls_str})"
