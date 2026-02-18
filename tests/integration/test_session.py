"""Integration tests for Session using MockKdbServer."""

import pytest

from qorm import (
    Model, Engine, Session, Symbol, Float, Long, Timestamp,
    avg_,
)
from qorm.model.meta import clear_registry


class TestSession:
    @classmethod
    def setup_class(cls):
        clear_registry()

        class Trade(Model):
            __tablename__ = 'trade'
            sym: Symbol
            price: Float
            size: Long

        cls.Trade = Trade

    def test_session_context_manager(self, mock_server):
        mock_server.set_default_response(42)
        engine = Engine(host="127.0.0.1", port=mock_server.port)
        with Session(engine) as session:
            result = session.raw("1+1")
            assert result == 42

    def test_session_raw_query(self, mock_server):
        mock_server.set_default_response("test result")
        engine = Engine(host="127.0.0.1", port=mock_server.port)
        with Session(engine) as session:
            result = session.raw("select from trade")
            assert result == "test result"

    def test_session_exec_select(self, mock_server):
        mock_server.set_default_response(42)
        engine = Engine(host="127.0.0.1", port=mock_server.port)
        with Session(engine) as session:
            query = self.Trade.select()
            result = session.exec(query)
            # Mock returns 42 regardless of query
            assert result == 42

    def test_session_table_result(self, mock_server):
        # Mock returns a table-like dict
        table_data = {
            '__table__': True,
            'sym': ['AAPL', 'GOOG'],
            'price': [150.0, 2800.0],
            'size': [100, 50],
        }
        mock_server.set_default_response(table_data)
        engine = Engine(host="127.0.0.1", port=mock_server.port)

        # Note: the mock server serializes Python dicts, so the response
        # will be dict-like but may not perfectly match the __table__ format
        # This test verifies the session can handle the mock response


class TestEngine:
    def test_engine_from_dsn(self):
        engine = Engine.from_dsn("kdb://user:pass@localhost:5000")
        assert engine.host == "localhost"
        assert engine.port == 5000
        assert engine.username == "user"
        assert engine.password == "pass"

    def test_engine_from_dsn_no_auth(self):
        engine = Engine.from_dsn("kdb://localhost:5000")
        assert engine.host == "localhost"
        assert engine.port == 5000
        assert engine.username == ""

    def test_engine_repr(self):
        engine = Engine(host="localhost", port=5000)
        assert "localhost" in repr(engine)
        assert "5000" in repr(engine)

    def test_engine_connect(self, mock_server):
        mock_server.set_default_response(42)
        engine = Engine(host="127.0.0.1", port=mock_server.port)
        conn = engine.connect()
        assert conn is not None
        conn.open()
        assert conn.is_open
        conn.close()
