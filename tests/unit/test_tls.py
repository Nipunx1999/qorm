"""Unit tests for TLS/SSL support."""

import ssl
from unittest.mock import MagicMock, patch

import pytest

from qorm import Engine, SyncConnection, AsyncConnection


class TestEngineTLS:
    def test_tls_defaults_off(self):
        e = Engine(host="localhost", port=5000)
        assert e.tls is False
        assert e.tls_context is None
        assert e.tls_verify is True

    def test_tls_enabled(self):
        e = Engine(host="localhost", port=5000, tls=True)
        assert e.tls is True

    def test_tls_custom_context(self):
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        e = Engine(host="localhost", port=5000, tls=True, tls_context=ctx)
        assert e._get_ssl_context() is ctx

    def test_tls_auto_context_created(self):
        e = Engine(host="localhost", port=5000, tls=True)
        ctx = e._get_ssl_context()
        assert isinstance(ctx, ssl.SSLContext)

    def test_tls_no_verify(self):
        e = Engine(host="localhost", port=5000, tls=True, tls_verify=False)
        ctx = e._get_ssl_context()
        assert ctx.check_hostname is False
        assert ctx.verify_mode == ssl.CERT_NONE

    def test_tls_off_returns_none_context(self):
        e = Engine(host="localhost", port=5000, tls=False)
        assert e._get_ssl_context() is None


class TestFromDsnTLS:
    def test_kdb_tls_scheme(self):
        e = Engine.from_dsn("kdb+tls://user:pass@kdb-server:5000")
        assert e.tls is True
        assert e.host == "kdb-server"
        assert e.port == 5000
        assert e.username == "user"
        assert e.password == "pass"

    def test_regular_scheme_no_tls(self):
        e = Engine.from_dsn("kdb://user:pass@localhost:5000")
        assert e.tls is False

    def test_tls_no_auth(self):
        e = Engine.from_dsn("kdb+tls://server:5001")
        assert e.tls is True
        assert e.host == "server"
        assert e.port == 5001
        assert e.username == ""


class TestEngineRepr:
    def test_repr_no_tls(self):
        e = Engine(host="localhost", port=5000)
        assert "tls" not in repr(e)

    def test_repr_with_tls(self):
        e = Engine(host="localhost", port=5000, tls=True)
        assert "tls=True" in repr(e)


class TestConnectionTLSParams:
    def test_sync_conn_accepts_tls_context(self):
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        conn = SyncConnection(host="h", port=1, tls_context=ctx)
        assert conn.tls_context is ctx

    def test_sync_conn_default_no_tls(self):
        conn = SyncConnection(host="h", port=1)
        assert conn.tls_context is None

    def test_async_conn_accepts_tls_context(self):
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        conn = AsyncConnection(host="h", port=1, tls_context=ctx)
        assert conn.tls_context is ctx

    def test_engine_connect_passes_tls(self):
        e = Engine(host="h", port=1, tls=True, tls_verify=False)
        conn = e.connect()
        assert conn.tls_context is not None

    def test_engine_async_connect_passes_tls(self):
        e = Engine(host="h", port=1, tls=True)
        conn = e.async_connect()
        assert conn.tls_context is not None

    def test_engine_no_tls_connect(self):
        e = Engine(host="h", port=1)
        conn = e.connect()
        assert conn.tls_context is None
