"""Unit tests for pool health checking and reconnection."""

import queue
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

from qorm import Engine
from qorm.connection.pool import SyncPool
from qorm.exc import PoolError, PoolExhaustedError


class TestPoolHealthCheck:
    def _make_mock_engine(self):
        engine = MagicMock(spec=Engine)
        engine.host = "localhost"
        engine.port = 5000

        def make_conn():
            conn = MagicMock()
            conn.is_open = True
            conn.open = MagicMock()
            conn.close = MagicMock()
            return conn

        engine.connect.side_effect = lambda: make_conn()
        return engine

    def test_pool_creates_min_connections(self):
        engine = self._make_mock_engine()
        pool = SyncPool(engine, min_size=3, max_size=5)
        assert pool.size == 3

    def test_pool_check_on_acquire_default_true(self):
        engine = self._make_mock_engine()
        pool = SyncPool(engine, min_size=1)
        assert pool._check_on_acquire is True

    def test_pool_acquire_returns_open_conn(self):
        engine = self._make_mock_engine()
        pool = SyncPool(engine, min_size=1)
        conn = pool.acquire()
        assert conn.is_open is True

    def test_pool_replace_dead_connection(self):
        engine = self._make_mock_engine()
        pool = SyncPool(engine, min_size=1, check_on_acquire=True)

        # Make the pooled connection appear dead
        dead_conn = pool.acquire()
        dead_conn.is_open = False
        pool.release(dead_conn)

        # Pool size decreases when dead conn is released
        assert pool.size == 0

    def test_pool_size_property(self):
        engine = self._make_mock_engine()
        pool = SyncPool(engine, min_size=2, max_size=5)
        assert pool.size == 2

    def test_pool_closed_raises(self):
        engine = self._make_mock_engine()
        pool = SyncPool(engine, min_size=1)
        pool.close()
        with pytest.raises(PoolError, match="Pool is closed"):
            pool.acquire()

    def test_pool_release_to_closed_pool(self):
        engine = self._make_mock_engine()
        pool = SyncPool(engine, min_size=1)
        conn = pool.acquire()
        pool.close()
        # Releasing to closed pool should close the connection
        pool.release(conn)
        conn.close.assert_called()


class TestSyncConnectionPing:
    def test_ping_with_no_socket(self):
        from qorm.connection.sync_conn import SyncConnection
        conn = SyncConnection(host="localhost", port=5000)
        assert conn.ping() is False
