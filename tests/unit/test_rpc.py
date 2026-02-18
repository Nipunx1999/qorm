"""Unit tests for RPC: QFunction, q_api, and session.call()."""

from unittest.mock import MagicMock, patch

import pytest

from qorm.rpc import QFunction, q_api


class TestQFunction:
    def test_init(self):
        qf = QFunction("getTradesByDate")
        assert qf.func_name == "getTradesByDate"

    def test_call_delegates_to_session(self):
        qf = QFunction("getSnapshot")
        session = MagicMock()
        session.call.return_value = [1, 2, 3]

        result = qf(session, "AAPL")
        session.call.assert_called_once_with("getSnapshot", "AAPL")
        assert result == [1, 2, 3]

    def test_call_no_args(self):
        qf = QFunction("getTables")
        session = MagicMock()
        session.call.return_value = ["trade", "quote"]

        result = qf(session)
        session.call.assert_called_once_with("getTables")
        assert result == ["trade", "quote"]

    def test_call_multiple_args(self):
        qf = QFunction("calcVWAP")
        session = MagicMock()
        session.call.return_value = 150.5

        result = qf(session, "AAPL", "2024.01.15")
        session.call.assert_called_once_with("calcVWAP", "AAPL", "2024.01.15")
        assert result == 150.5

    def test_repr(self):
        qf = QFunction("myFunc")
        assert repr(qf) == "QFunction('myFunc')"


class TestQApi:
    def test_basic_decorator(self):
        @q_api("getTradesByDate")
        def get_trades_by_date(session, date: str): ...

        session = MagicMock()
        session.call.return_value = {"sym": ["AAPL"], "price": [150.0]}

        result = get_trades_by_date(session, "2024.01.15")
        session.call.assert_called_once_with("getTradesByDate", "2024.01.15")

    def test_preserves_function_name(self):
        @q_api("myQFunc")
        def my_python_func(session, arg1: str, arg2: int): ...

        assert my_python_func.__name__ == "my_python_func"

    def test_multiple_args(self):
        @q_api("calcVWAP")
        def calc_vwap(session, sym: str, date: str): ...

        session = MagicMock()
        session.call.return_value = 150.5

        result = calc_vwap(session, "AAPL", "2024.01.15")
        session.call.assert_called_once_with("calcVWAP", "AAPL", "2024.01.15")
        assert result == 150.5

    def test_no_args(self):
        @q_api("getStatus")
        def get_status(session): ...

        session = MagicMock()
        session.call.return_value = "ok"

        result = get_status(session)
        session.call.assert_called_once_with("getStatus")

    def test_qfunction_attribute(self):
        @q_api("myFunc")
        def my_func(session): ...

        assert hasattr(my_func, '_qfunction')
        assert my_func._qfunction.func_name == "myFunc"


class TestSessionCall:
    def test_call_with_args(self):
        session = MagicMock()
        conn = MagicMock()
        session._conn = conn

        # Test via the actual Session class
        from qorm.session import Session, _map_result
        from qorm import Engine

        engine = Engine(host="localhost", port=5000)
        s = Session(engine)
        s._conn = conn
        conn.query.return_value = [1, 2, 3]

        result = s.call("myFunc", "arg1", 42)
        conn.query.assert_called_once_with("myFunc", "arg1", 42)
        assert result == [1, 2, 3]

    def test_call_no_args(self):
        from qorm.session import Session
        from qorm import Engine

        engine = Engine(host="localhost", port=5000)
        s = Session(engine)
        conn = MagicMock()
        s._conn = conn
        conn.query.return_value = "ok"

        result = s.call("getStatus")
        conn.query.assert_called_once_with("getStatus")
        assert result == "ok"

    def test_call_returns_mapped_result(self):
        from qorm.session import Session, ModelResultSet
        from qorm import Engine

        engine = Engine(host="localhost", port=5000)
        s = Session(engine)
        conn = MagicMock()
        s._conn = conn
        # Return a table-like dict
        conn.query.return_value = {
            '__table__': True,
            'sym': ['AAPL', 'GOOG'],
            'price': [150.0, 2800.0],
        }

        result = s.call("getSnapshot", "AAPL")
        assert isinstance(result, ModelResultSet)
        assert len(result) == 2
