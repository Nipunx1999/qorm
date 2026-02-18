"""Integration tests for connection layer using MockKdbServer."""

import pytest

from qorm.connection.sync_conn import SyncConnection
from qorm.connection.handshake import build_handshake, parse_handshake_response
from qorm.exc import AuthenticationError, HandshakeError


class TestHandshake:
    def test_build_handshake_no_auth(self):
        hs = build_handshake()
        assert hs == b'\x03\x00'

    def test_build_handshake_with_creds(self):
        hs = build_handshake("user", "pass")
        assert hs == b'user:pass\x03\x00'

    def test_parse_response_success(self):
        cap = parse_handshake_response(bytes([3]))
        assert cap == 3

    def test_parse_response_empty(self):
        with pytest.raises(AuthenticationError):
            parse_handshake_response(b'')

    def test_parse_response_too_long(self):
        with pytest.raises(HandshakeError):
            parse_handshake_response(b'\x03\x00')


class TestSyncConnection:
    def test_connect_to_mock(self, mock_server):
        mock_server.set_default_response(42)
        conn = SyncConnection(host="127.0.0.1", port=mock_server.port)
        conn.open()
        assert conn.is_open
        conn.close()
        assert not conn.is_open

    def test_query_mock(self, mock_server):
        mock_server.set_default_response(42)
        conn = SyncConnection(host="127.0.0.1", port=mock_server.port)
        conn.open()
        try:
            result = conn.query("1+1")
            assert result == 42  # mock always returns configured response
        finally:
            conn.close()

    def test_context_manager(self, mock_server):
        mock_server.set_default_response("ok")
        conn = SyncConnection(host="127.0.0.1", port=mock_server.port)
        with conn:
            assert conn.is_open
        assert not conn.is_open

    def test_string_response(self, mock_server):
        mock_server.set_default_response("hello world")
        with SyncConnection(host="127.0.0.1", port=mock_server.port) as conn:
            result = conn.query("test")
            assert result == "hello world"

    def test_list_response(self, mock_server):
        mock_server.set_default_response([1, 2, 3])
        with SyncConnection(host="127.0.0.1", port=mock_server.port) as conn:
            result = conn.query("test")
            assert result == [1, 2, 3]
