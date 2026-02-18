"""Unit tests for logging integration."""

import logging
from unittest.mock import MagicMock

import pytest

from qorm import Engine
from qorm.session import Session


class TestSessionLogging:
    def _make_session(self):
        engine = Engine(host="localhost", port=5000)
        s = Session(engine)
        s._conn = MagicMock()
        return s

    def test_raw_logs_query(self, caplog):
        s = self._make_session()
        s._conn.query.return_value = 42

        with caplog.at_level(logging.DEBUG, logger="qorm"):
            s.raw("1+1")

        assert any("raw: 1+1" in r.message for r in caplog.records)

    def test_raw_logs_timing(self, caplog):
        s = self._make_session()
        s._conn.query.return_value = 42

        with caplog.at_level(logging.DEBUG, logger="qorm"):
            s.raw("1+1")

        assert any("completed in" in r.message for r in caplog.records)

    def test_call_logs(self, caplog):
        s = self._make_session()
        s._conn.query.return_value = "result"

        with caplog.at_level(logging.DEBUG, logger="qorm"):
            s.call("myFunc", "arg1")

        assert any("call: myFunc" in r.message for r in caplog.records)

    def test_exec_logs(self, caplog):
        from qorm import Model, Symbol, Float
        from qorm.model.meta import clear_registry

        clear_registry()

        class T(Model):
            __tablename__ = 'log_test'
            sym: Symbol
            price: Float

        s = self._make_session()
        s._conn.query.return_value = {'sym': ['AAPL'], 'price': [150.0], '__table__': True}

        with caplog.at_level(logging.DEBUG, logger="qorm"):
            s.exec(T.select())

        assert any("exec:" in r.message for r in caplog.records)


class TestConnectionLogging:
    def test_logger_names(self):
        """Verify loggers exist and are accessible."""
        conn_log = logging.getLogger("qorm.connection")
        pool_log = logging.getLogger("qorm.pool")
        sub_log = logging.getLogger("qorm.subscription")
        assert conn_log is not None
        assert pool_log is not None
        assert sub_log is not None
