"""Unit tests for keyed table reflection."""

from unittest.mock import MagicMock

import pytest

from qorm import Model, Engine, KeyedModel, ReflectionError
from qorm.model.reflect import build_model_from_meta
from qorm.model.keyed import KeyedModel
from qorm.protocol.constants import QTypeCode
from qorm.session import Session


class TestKeyedReflection:
    def test_keyed_model_from_meta(self):
        meta = {
            'c': ['sym', 'date', 'close', 'volume'],
            't': ['s', 'd', 'f', 'j'],
            'f': ['', '', '', ''],
            'a': ['', '', '', ''],
        }
        M = build_model_from_meta('daily_price', meta, key_columns=['sym', 'date'])
        assert issubclass(M, KeyedModel)
        assert M.__key_fields__ == ['sym', 'date']
        assert M.__fields__['sym'].primary_key is True
        assert M.__fields__['date'].primary_key is True
        assert M.__fields__['close'].primary_key is False
        assert M.__fields__['volume'].primary_key is False

    def test_non_keyed_model_from_meta(self):
        meta = {
            'c': ['sym', 'price'],
            't': ['s', 'f'],
            'f': ['', ''],
            'a': ['', ''],
        }
        M = build_model_from_meta('trade', meta, key_columns=None)
        assert issubclass(M, Model)
        assert not issubclass(M, KeyedModel) or M is Model
        assert M.__key_fields__ == []

    def test_empty_keys_list_treated_as_non_keyed(self):
        meta = {
            'c': ['sym', 'price'],
            't': ['s', 'f'],
            'f': ['', ''],
            'a': ['', ''],
        }
        M = build_model_from_meta('trade', meta, key_columns=[])
        assert M.__key_fields__ == []

    def test_keyed_model_key_columns_method(self):
        meta = {
            'c': ['sym', 'close', 'volume'],
            't': ['s', 'f', 'j'],
            'f': ['', '', ''],
            'a': ['', '', ''],
        }
        M = build_model_from_meta('kx_tbl', meta, key_columns=['sym'])
        assert M.key_columns() == ['sym']
        assert M.value_columns() == ['close', 'volume']

    def test_keyed_model_instantiation(self):
        meta = {
            'c': ['sym', 'close'],
            't': ['s', 'f'],
            'f': ['', ''],
            'a': ['', ''],
        }
        M = build_model_from_meta('kx', meta, key_columns=['sym'])
        inst = M(sym="AAPL", close=150.0)
        assert inst.sym == "AAPL"
        assert inst.close == 150.0


class TestSessionKeyedReflection:
    def _make_session(self):
        engine = Engine(host="localhost", port=5000)
        s = Session(engine)
        s._conn = MagicMock()
        return s

    def test_reflect_keyed_table(self):
        s = self._make_session()

        def mock_query(expr):
            if expr == "meta keyed_tbl":
                return {
                    'c': ['sym', 'date', 'close'],
                    't': ['s', 'd', 'f'],
                    'f': ['', '', ''],
                    'a': ['', '', ''],
                }
            elif expr == "keys keyed_tbl":
                return ['sym', 'date']
            return None

        s._conn.query.side_effect = mock_query

        M = s.reflect("keyed_tbl")
        assert issubclass(M, KeyedModel)
        assert M.__key_fields__ == ['sym', 'date']

    def test_reflect_non_keyed_table(self):
        s = self._make_session()

        def mock_query(expr):
            if expr == "meta trade":
                return {
                    'c': ['sym', 'price'],
                    't': ['s', 'f'],
                    'f': ['', ''],
                    'a': ['', ''],
                }
            elif expr == "keys trade":
                return []
            return None

        s._conn.query.side_effect = mock_query

        M = s.reflect("trade")
        assert not issubclass(M, KeyedModel)
        assert M.__key_fields__ == []

    def test_reflect_keys_query_failure_graceful(self):
        """If keys query fails, table is treated as non-keyed."""
        s = self._make_session()

        def mock_query(expr):
            if expr == "meta trade":
                return {
                    'c': ['sym', 'price'],
                    't': ['s', 'f'],
                    'f': ['', ''],
                    'a': ['', ''],
                }
            elif expr == "keys trade":
                raise RuntimeError("not supported")
            return None

        s._conn.query.side_effect = mock_query

        M = s.reflect("trade")
        assert M.__key_fields__ == []
        assert M.__tablename__ == 'trade'
