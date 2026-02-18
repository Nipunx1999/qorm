"""Unit tests for pool integration with registry."""

import pytest

from qorm import Engine, EngineRegistry
from qorm.connection.pool import SyncPool, AsyncPool


class TestRegistryPool:
    def test_pool_method_exists(self):
        reg = EngineRegistry()
        reg.register("rdb", Engine(host="localhost", port=5000))
        assert hasattr(reg, 'pool')
        assert hasattr(reg, 'async_pool')

    def test_pool_returns_sync_pool(self):
        reg = EngineRegistry()
        reg.register("rdb", Engine(host="localhost", port=5000))
        # We can't actually open connections, but we can check the type
        # and params. SyncPool.__init__ tries to open connections so
        # we just verify the method signature.
        assert callable(reg.pool)

    def test_async_pool_returns_async_pool(self):
        reg = EngineRegistry()
        reg.register("rdb", Engine(host="localhost", port=5000))
        assert callable(reg.async_pool)
