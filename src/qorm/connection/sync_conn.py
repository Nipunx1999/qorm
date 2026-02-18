"""Blocking socket connection to q/kdb+."""

from __future__ import annotations

import logging
import socket
import ssl
from typing import Any

from ..exc import ConnectionError as QConnError
from ..protocol.constants import SYNC_MSG, HEADER_SIZE
from ..protocol.framing import unpack_header
from ..protocol.serializer import Serializer
from ..protocol.deserializer import Deserializer
from .base import BaseConnection
from .handshake import build_handshake, parse_handshake_response


log = logging.getLogger("qorm.connection")


class SyncConnection(BaseConnection):
    """Synchronous (blocking) connection to a q/kdb+ process."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 5000,
        username: str = "",
        password: str = "",
        timeout: float | None = None,
        tls_context: ssl.SSLContext | None = None,
    ) -> None:
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.timeout = timeout
        self.tls_context = tls_context
        self._sock: socket.socket | None = None
        self._serializer = Serializer()
        self._deserializer = Deserializer()
        self._capability: int = 0

    def open(self) -> None:
        """Connect and perform IPC handshake."""
        if self._sock is not None:
            return
        try:
            sock = socket.create_connection(
                (self.host, self.port), timeout=self.timeout
            )
        except OSError as e:
            raise QConnError(f"Cannot connect to {self.host}:{self.port}: {e}") from e

        if self.tls_context is not None:
            log.debug("Wrapping socket with TLS for %s:%s", self.host, self.port)
            try:
                sock = self.tls_context.wrap_socket(sock, server_hostname=self.host)
            except ssl.SSLError as e:
                sock.close()
                raise QConnError(f"TLS handshake failed with {self.host}:{self.port}: {e}") from e

        self._sock = sock
        try:
            self._handshake()
            log.debug("Connected to %s:%s (capability=%d)", self.host, self.port, self._capability)
        except Exception:
            self._sock.close()
            self._sock = None
            raise

    def _handshake(self) -> None:
        hs = build_handshake(self.username, self.password)
        self._sock.sendall(hs)
        resp = self._sock.recv(1)
        self._capability = parse_handshake_response(resp)

    def close(self) -> None:
        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None

    @property
    def is_open(self) -> bool:
        return self._sock is not None

    def send(self, obj: Any, msg_type: int = SYNC_MSG) -> None:
        if self._sock is None:
            raise QConnError("Connection is not open")
        data = self._serializer.serialize_message(obj, msg_type)
        self._sock.sendall(data)

    def _recv_exact(self, n: int) -> bytes:
        """Receive exactly n bytes from the socket."""
        buf = bytearray()
        while len(buf) < n:
            chunk = self._sock.recv(n - len(buf))
            if not chunk:
                raise QConnError("Connection closed by kdb+")
            buf.extend(chunk)
        return bytes(buf)

    def receive(self) -> Any:
        if self._sock is None:
            raise QConnError("Connection is not open")
        # Read header
        header_bytes = self._recv_exact(HEADER_SIZE)
        _, msg_type, total_length = unpack_header(header_bytes)
        # Read remaining payload
        remaining = total_length - HEADER_SIZE
        if remaining > 0:
            payload = self._recv_exact(remaining)
        else:
            payload = b''
        full_msg = header_bytes + payload
        _, result = self._deserializer.deserialize_message(full_msg)
        return result

    def query(self, q_expr: str, *args: Any) -> Any:
        """Send a q expression synchronously and return the result."""
        if args:
            self.send([q_expr, *args])
        else:
            self.send(q_expr)
        return self.receive()

    def ping(self) -> bool:
        """Check if the connection is alive by sending a lightweight query.

        Returns True if the connection is responsive, False otherwise.
        """
        if self._sock is None:
            return False
        try:
            result = self.query("1b")
            return result is not None
        except Exception:
            return False
