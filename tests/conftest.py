"""Test fixtures including a mock kdb+ server."""

from __future__ import annotations

import asyncio
import socket
import struct
import threading
from typing import Any

import pytest

from qorm.protocol.constants import LITTLE_ENDIAN, RESPONSE_MSG, HEADER_SIZE
from qorm.protocol.serializer import Serializer
from qorm.protocol.framing import pack_header


class MockKdbServer:
    """A minimal mock kdb+ server that speaks IPC protocol.

    Accepts connections, performs handshake, and returns pre-configured
    responses to queries.
    """

    def __init__(self, host: str = "127.0.0.1", port: int = 0) -> None:
        self.host = host
        self.port = port
        self._sock: socket.socket | None = None
        self._thread: threading.Thread | None = None
        self._running = False
        self._responses: dict[str, Any] = {}
        self._default_response: Any = None
        self._serializer = Serializer()
        self.compress_responses: bool = False

    def set_response(self, query: str, response: Any) -> None:
        """Set a canned response for a specific query string."""
        self._responses[query] = response

    def set_default_response(self, response: Any) -> None:
        """Set a default response for any unmatched query."""
        self._default_response = response

    def start(self) -> int:
        """Start the mock server and return the port number."""
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((self.host, self.port))
        self._sock.listen(5)
        self._sock.settimeout(1.0)
        self.port = self._sock.getsockname()[1]
        self._running = True
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()
        return self.port

    def stop(self) -> None:
        """Stop the mock server."""
        self._running = False
        if self._sock:
            self._sock.close()
        if self._thread:
            self._thread.join(timeout=3)

    def _serve(self) -> None:
        while self._running:
            try:
                client, addr = self._sock.accept()
            except (socket.timeout, OSError):
                continue
            threading.Thread(
                target=self._handle_client, args=(client,), daemon=True
            ).start()

    def _handle_client(self, client: socket.socket) -> None:
        try:
            # Handshake: read client credentials
            data = client.recv(1024)
            if not data:
                return
            # Respond with capability byte
            client.sendall(bytes([3]))

            # Message loop
            while self._running:
                # Read header
                header = self._recv_exact(client, HEADER_SIZE)
                if not header:
                    break
                endian = header[0]
                prefix = '<' if endian == LITTLE_ENDIAN else '>'
                msg_type = header[1]
                total_len = struct.unpack(f'{prefix}i', header[4:8])[0]

                # Read payload
                remaining = total_len - HEADER_SIZE
                if remaining > 0:
                    payload = self._recv_exact(client, remaining)
                    if not payload:
                        break
                else:
                    payload = b''

                # Parse the query (simplistic: look for char vector)
                query_str = self._extract_query(payload)
                response = self._responses.get(query_str, self._default_response)

                # Serialize and send response
                resp_bytes = self._serializer.serialize_message(
                    response, RESPONSE_MSG
                )
                if self.compress_responses:
                    from qorm.protocol.compress import compress
                    compressed = compress(resp_bytes, level=1)
                    if compressed != resp_bytes:
                        # Build header with compressed flag (byte 2 = 1)
                        new_header = struct.pack(
                            '<BBHi', LITTLE_ENDIAN, RESPONSE_MSG,
                            1, len(compressed) + HEADER_SIZE,
                        )
                        client.sendall(new_header + compressed)
                        continue
                client.sendall(resp_bytes)
        except (OSError, Exception):
            pass
        finally:
            try:
                client.close()
            except OSError:
                pass

    def _recv_exact(self, sock: socket.socket, n: int) -> bytes | None:
        buf = bytearray()
        while len(buf) < n:
            try:
                chunk = sock.recv(n - len(buf))
            except OSError:
                return None
            if not chunk:
                return None
            buf.extend(chunk)
        return bytes(buf)

    def _extract_query(self, payload: bytes) -> str:
        """Extract query string from serialized payload (best-effort)."""
        # Char vector starts with type=10, attr, then 4-byte length, then chars
        try:
            if payload and payload[0] == 10:
                length = struct.unpack('<i', payload[2:6])[0]
                return payload[6:6 + length].decode('utf-8')
        except (IndexError, struct.error):
            pass
        return ''


@pytest.fixture
def mock_server():
    """Fixture providing a running MockKdbServer."""
    server = MockKdbServer()
    port = server.start()
    yield server
    server.stop()


@pytest.fixture
def mock_port(mock_server):
    """Fixture providing just the port of a running mock server."""
    return mock_server.port
