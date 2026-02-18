"""Unit tests for the multi-instance engine registry."""

import os
from unittest import mock

import pytest

from qorm import Engine, EngineRegistry, EngineGroup, EngineNotFoundError
from qorm.session import Session, AsyncSession


class TestEngineRegistry:
    def test_register_and_get(self):
        reg = EngineRegistry()
        e = Engine(host="h1", port=5010)
        reg.register("rdb", e)
        assert reg.get("rdb") is e

    def test_first_registered_is_default(self):
        reg = EngineRegistry()
        e1 = Engine(host="h1", port=5010)
        e2 = Engine(host="h2", port=5012)
        reg.register("rdb", e1)
        reg.register("hdb", e2)
        assert reg.default == "rdb"
        assert reg.get() is e1

    def test_set_default(self):
        reg = EngineRegistry()
        reg.register("rdb", Engine(host="h1", port=5010))
        reg.register("hdb", Engine(host="h2", port=5012))
        reg.set_default("hdb")
        assert reg.default == "hdb"
        assert reg.get().host == "h2"

    def test_set_default_not_registered(self):
        reg = EngineRegistry()
        with pytest.raises(EngineNotFoundError, match="nope"):
            reg.set_default("nope")

    def test_get_missing_raises(self):
        reg = EngineRegistry()
        reg.register("rdb", Engine(host="h1", port=5010))
        with pytest.raises(EngineNotFoundError, match="missing"):
            reg.get("missing")

    def test_get_empty_raises(self):
        reg = EngineRegistry()
        with pytest.raises(EngineNotFoundError, match="No engines"):
            reg.get()

    def test_names(self):
        reg = EngineRegistry()
        reg.register("rdb", Engine(host="h1", port=5010))
        reg.register("hdb", Engine(host="h2", port=5012))
        assert reg.names == ["rdb", "hdb"]

    def test_session_returns_session(self):
        reg = EngineRegistry()
        reg.register("rdb", Engine(host="h1", port=5010))
        s = reg.session("rdb")
        assert isinstance(s, Session)
        assert s.engine.host == "h1"

    def test_async_session_returns_async_session(self):
        reg = EngineRegistry()
        reg.register("rdb", Engine(host="h1", port=5010))
        s = reg.async_session("rdb")
        assert isinstance(s, AsyncSession)
        assert s.engine.host == "h1"

    def test_session_default(self):
        reg = EngineRegistry()
        reg.register("rdb", Engine(host="h1", port=5010))
        s = reg.session()
        assert s.engine.host == "h1"

    def test_from_config(self):
        reg = EngineRegistry.from_config({
            "rdb": {"host": "eq-rdb", "port": 5010},
            "hdb": {"host": "eq-hdb", "port": 5012},
        })
        assert reg.names == ["rdb", "hdb"]
        assert reg.get("rdb").host == "eq-rdb"
        assert reg.get("rdb").port == 5010
        assert reg.get("hdb").host == "eq-hdb"

    def test_from_dsn(self):
        reg = EngineRegistry.from_dsn({
            "rdb": "kdb://eq-rdb:5010",
            "hdb": "kdb://user:pass@eq-hdb:5012",
        })
        assert reg.get("rdb").host == "eq-rdb"
        assert reg.get("rdb").port == 5010
        assert reg.get("hdb").username == "user"
        assert reg.get("hdb").password == "pass"

    def test_from_env(self):
        env = {
            "QORM_EQ_RDB_HOST": "eq-rdb-host",
            "QORM_EQ_RDB_PORT": "5010",
            "QORM_EQ_RDB_USER": "admin",
            "QORM_EQ_RDB_PASS": "secret",
            "QORM_EQ_HDB_HOST": "eq-hdb-host",
            "QORM_EQ_HDB_PORT": "5012",
        }
        with mock.patch.dict(os.environ, env, clear=False):
            reg = EngineRegistry.from_env(names=["rdb", "hdb"], prefix="QORM_EQ")

        assert reg.get("rdb").host == "eq-rdb-host"
        assert reg.get("rdb").port == 5010
        assert reg.get("rdb").username == "admin"
        assert reg.get("rdb").password == "secret"
        assert reg.get("hdb").host == "eq-hdb-host"
        assert reg.get("hdb").port == 5012

    def test_from_env_defaults(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            reg = EngineRegistry.from_env(names=["rdb"], prefix="QORM_TEST_NOEXIST")
        assert reg.get("rdb").host == "localhost"
        assert reg.get("rdb").port == 5000

    def test_repr(self):
        reg = EngineRegistry()
        reg.register("rdb", Engine(host="h1", port=5010))
        r = repr(reg)
        assert "EngineRegistry" in r
        assert "rdb" in r


class TestEngineGroup:
    def _make_registries(self):
        eq = EngineRegistry.from_config({
            "rdb": {"host": "eq-rdb", "port": 5010},
            "hdb": {"host": "eq-hdb", "port": 5012},
        })
        fx = EngineRegistry.from_config({
            "rdb": {"host": "fx-rdb", "port": 5020},
        })
        return eq, fx

    def test_register_and_get(self):
        eq, fx = self._make_registries()
        group = EngineGroup()
        group.register("equities", eq)
        group.register("fx", fx)
        assert group.get("equities") is eq
        assert group.get("fx") is fx

    def test_get_missing_raises(self):
        group = EngineGroup()
        with pytest.raises(EngineNotFoundError, match="crypto"):
            group.get("crypto")

    def test_getattr(self):
        eq, fx = self._make_registries()
        group = EngineGroup()
        group.register("equities", eq)
        assert group.equities is eq

    def test_getattr_missing_raises(self):
        group = EngineGroup()
        with pytest.raises(EngineNotFoundError):
            _ = group.nonexistent

    def test_names(self):
        eq, fx = self._make_registries()
        group = EngineGroup()
        group.register("equities", eq)
        group.register("fx", fx)
        assert group.names == ["equities", "fx"]

    def test_session_shortcut(self):
        eq, fx = self._make_registries()
        group = EngineGroup()
        group.register("equities", eq)
        s = group.session("equities", "rdb")
        assert isinstance(s, Session)
        assert s.engine.host == "eq-rdb"

    def test_session_default_instance(self):
        eq, _ = self._make_registries()
        group = EngineGroup()
        group.register("equities", eq)
        s = group.session("equities")
        assert s.engine.host == "eq-rdb"  # first registered = default

    def test_from_config(self):
        group = EngineGroup.from_config({
            "equities": {
                "rdb": {"host": "eq-rdb", "port": 5010},
                "hdb": {"host": "eq-hdb", "port": 5012},
            },
            "fx": {
                "rdb": {"host": "fx-rdb", "port": 5020},
            },
        })
        assert group.names == ["equities", "fx"]
        assert group.get("equities").get("rdb").host == "eq-rdb"
        assert group.get("equities").get("hdb").port == 5012
        assert group.get("fx").get("rdb").host == "fx-rdb"

    def test_repr(self):
        group = EngineGroup()
        group.register("equities", EngineRegistry())
        r = repr(group)
        assert "EngineGroup" in r
        assert "equities" in r
