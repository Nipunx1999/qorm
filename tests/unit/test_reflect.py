"""Unit tests for table reflection: meta parsing, dynamic model creation, type mapping."""

from unittest.mock import MagicMock

import pytest

from qorm import Model, Engine, ReflectionError
from qorm.model.reflect import (
    _CHAR_TO_QTYPE_CODE,
    _parse_meta_result,
    build_model_from_meta,
)
from qorm.model.fields import Field
from qorm.protocol.constants import QTypeCode
from qorm.session import Session
from qorm.query.expressions import Column


class TestCharToQTypeCode:
    def test_all_19_types_mapped(self):
        expected = {
            ' ': QTypeCode.MIXED_LIST,
            'b': QTypeCode.BOOLEAN,
            'g': QTypeCode.GUID,
            'x': QTypeCode.BYTE,
            'h': QTypeCode.SHORT,
            'i': QTypeCode.INT,
            'j': QTypeCode.LONG,
            'e': QTypeCode.REAL,
            'f': QTypeCode.FLOAT,
            'c': QTypeCode.CHAR,
            's': QTypeCode.SYMBOL,
            'p': QTypeCode.TIMESTAMP,
            'm': QTypeCode.MONTH,
            'd': QTypeCode.DATE,
            'z': QTypeCode.DATETIME,
            'n': QTypeCode.TIMESPAN,
            'u': QTypeCode.MINUTE,
            'v': QTypeCode.SECOND,
            't': QTypeCode.TIME,
        }
        assert _CHAR_TO_QTYPE_CODE == expected

    def test_all_chars_are_single_char(self):
        for char in _CHAR_TO_QTYPE_CODE:
            assert len(char) == 1


class TestParseMetaResult:
    def test_basic_parse(self):
        meta = {
            'c': ['sym', 'price', 'size'],
            't': ['s', 'f', 'j'],
            'f': ['', '', ''],
            'a': ['', '', ''],
        }
        result = _parse_meta_result(meta)
        assert result == [('sym', 's'), ('price', 'f'), ('size', 'j')]

    def test_single_column(self):
        meta = {'c': ['value'], 't': ['j'], 'f': [''], 'a': ['']}
        result = _parse_meta_result(meta)
        assert result == [('value', 'j')]

    def test_all_type_chars(self):
        cols = list('abcdefghijklmnopqr')
        chars = list('bgxhijefcspmdznutv')
        meta = {'c': cols, 't': chars, 'f': [''] * len(cols), 'a': [''] * len(cols)}
        result = _parse_meta_result(meta)
        assert len(result) == 18
        for (col, tchar), expected_char in zip(result, chars):
            assert tchar == expected_char

    def test_non_dict_raises(self):
        with pytest.raises(ReflectionError, match="Expected dict"):
            _parse_meta_result([1, 2, 3])

    def test_missing_c_key_raises(self):
        with pytest.raises(ReflectionError, match="missing 'c' or 't'"):
            _parse_meta_result({'t': ['s']})

    def test_missing_t_key_raises(self):
        with pytest.raises(ReflectionError, match="missing 'c' or 't'"):
            _parse_meta_result({'c': ['sym']})

    def test_length_mismatch_raises(self):
        with pytest.raises(ReflectionError, match="Column count"):
            _parse_meta_result({'c': ['sym', 'price'], 't': ['s']})

    def test_int_type_chars_converted(self):
        meta = {'c': ['sym'], 't': [ord('s')], 'f': [''], 'a': ['']}
        result = _parse_meta_result(meta)
        assert result == [('sym', 's')]


class TestBuildModelFromMeta:
    def test_basic_model(self):
        meta = {
            'c': ['sym', 'price', 'size', 'time'],
            't': ['s', 'f', 'j', 'p'],
            'f': ['', '', '', ''],
            'a': ['', '', '', ''],
        }
        Trade = build_model_from_meta('trade', meta)

        assert Trade.__tablename__ == 'trade'
        assert Trade.__name__ == 'Trade'
        assert set(Trade.__fields__) == {'sym', 'price', 'size', 'time'}

    def test_field_types(self):
        meta = {
            'c': ['sym', 'price', 'size'],
            't': ['s', 'f', 'j'],
            'f': ['', '', ''],
            'a': ['', '', ''],
        }
        M = build_model_from_meta('trade', meta)

        assert M.__fields__['sym'].qtype.code == QTypeCode.SYMBOL
        assert M.__fields__['price'].qtype.code == QTypeCode.FLOAT
        assert M.__fields__['size'].qtype.code == QTypeCode.LONG

    def test_class_name_generation(self):
        meta = {'c': ['val'], 't': ['j'], 'f': [''], 'a': ['']}

        M1 = build_model_from_meta('trade', meta)
        assert M1.__name__ == 'Trade'

        M2 = build_model_from_meta('daily_price', meta)
        assert M2.__name__ == 'DailyPrice'

        M3 = build_model_from_meta('my_long_table_name', meta)
        assert M3.__name__ == 'MyLongTableName'

    def test_model_is_subclass_of_model(self):
        meta = {'c': ['val'], 't': ['j'], 'f': [''], 'a': ['']}
        M = build_model_from_meta('test_tbl', meta)
        assert issubclass(M, Model)

    def test_model_instantiation(self):
        meta = {
            'c': ['sym', 'price'],
            't': ['s', 'f'],
            'f': ['', ''],
            'a': ['', ''],
        }
        Trade = build_model_from_meta('trade', meta)
        t = Trade(sym="AAPL", price=150.0)
        assert t.sym == "AAPL"
        assert t.price == 150.0

    def test_model_repr(self):
        meta = {'c': ['sym', 'price'], 't': ['s', 'f'], 'f': ['', ''], 'a': ['', '']}
        Trade = build_model_from_meta('trade', meta)
        t = Trade(sym="AAPL", price=150.0)
        r = repr(t)
        assert "Trade" in r
        assert "AAPL" in r

    def test_model_column_access(self):
        meta = {'c': ['sym', 'price'], 't': ['s', 'f'], 'f': ['', ''], 'a': ['', '']}
        Trade = build_model_from_meta('trade', meta)
        col = Trade.sym
        assert isinstance(col, Column)
        assert col.name == 'sym'

    def test_model_select_query(self):
        meta = {'c': ['sym', 'price'], 't': ['s', 'f'], 'f': ['', ''], 'a': ['', '']}
        Trade = build_model_from_meta('trade', meta)
        q = Trade.select(Trade.sym).where(Trade.price > 100).compile()
        assert 'trade' in q
        assert 'sym' in q
        assert 'price' in q

    def test_empty_table_raises(self):
        with pytest.raises(ReflectionError, match="no columns"):
            build_model_from_meta('empty', {'c': [], 't': [], 'f': [], 'a': []})

    def test_unknown_type_char_raises(self):
        with pytest.raises(ReflectionError, match="Unknown q type char"):
            build_model_from_meta('bad', {'c': ['col'], 't': ['?'], 'f': [''], 'a': ['']})

    def test_all_type_chars_produce_valid_fields(self):
        for char, code in _CHAR_TO_QTYPE_CODE.items():
            meta = {'c': ['col'], 't': [char], 'f': [''], 'a': ['']}
            M = build_model_from_meta(f'tbl_{char}', meta)
            assert M.__fields__['col'].qtype.code == code


class TestSessionReflection:
    def _make_session(self):
        engine = Engine(host="localhost", port=5000)
        s = Session(engine)
        s._conn = MagicMock()
        return s

    def test_tables(self):
        s = self._make_session()
        s._conn.query.return_value = ['trade', 'quote', 'order']

        result = s.tables()
        s._conn.query.assert_called_once_with("tables[]")
        assert result == ['trade', 'quote', 'order']

    def test_reflect(self):
        s = self._make_session()

        def mock_query(expr):
            if expr == "meta trade":
                return {
                    'c': ['sym', 'price', 'size'],
                    't': ['s', 'f', 'j'],
                    'f': ['', '', ''],
                    'a': ['', '', ''],
                }
            elif expr == "keys trade":
                return []
            return None

        s._conn.query.side_effect = mock_query

        Trade = s.reflect("trade")
        assert Trade.__tablename__ == 'trade'
        assert set(Trade.__fields__) == {'sym', 'price', 'size'}

    def test_reflect_query_error_wraps(self):
        s = self._make_session()
        s._conn.query.side_effect = RuntimeError("connection lost")

        with pytest.raises(ReflectionError, match="Failed to get metadata"):
            s.reflect("missing_table")

    def test_reflect_all(self):
        s = self._make_session()

        def mock_query(expr):
            if expr == "tables[]":
                return ['trade', 'quote']
            elif expr == "meta trade":
                return {
                    'c': ['sym', 'price'],
                    't': ['s', 'f'],
                    'f': ['', ''],
                    'a': ['', ''],
                }
            elif expr == "meta quote":
                return {
                    'c': ['sym', 'bid', 'ask'],
                    't': ['s', 'f', 'f'],
                    'f': ['', '', ''],
                    'a': ['', '', ''],
                }
            return None

        s._conn.query.side_effect = mock_query

        models = s.reflect_all()
        assert set(models.keys()) == {'trade', 'quote'}
        assert models['trade'].__tablename__ == 'trade'
        assert set(models['trade'].__fields__) == {'sym', 'price'}
        assert models['quote'].__tablename__ == 'quote'
        assert set(models['quote'].__fields__) == {'sym', 'bid', 'ask'}

    def test_reflected_model_equality(self):
        meta = {'c': ['sym', 'price'], 't': ['s', 'f'], 'f': ['', ''], 'a': ['', '']}
        Trade = build_model_from_meta('trade', meta)
        t1 = Trade(sym="AAPL", price=150.0)
        t2 = Trade(sym="AAPL", price=150.0)
        assert t1 == t2

    def test_reflected_model_to_dict(self):
        meta = {'c': ['sym', 'price'], 't': ['s', 'f'], 'f': ['', ''], 'a': ['', '']}
        Trade = build_model_from_meta('trade', meta)
        t = Trade(sym="AAPL", price=150.0)
        d = t.to_dict()
        assert d == {'sym': 'AAPL', 'price': 150.0}

    def test_reflect_table_with_list_column(self):
        """Reflect a table that has a mixed/nested list column (type char ' ')."""
        s = self._make_session()
        s._conn.query.return_value = {
            'c': ['sym', 'price', 'tags'],
            't': ['s', 'f', ' '],
            'f': ['', '', ''],
            'a': ['', '', ''],
        }

        M = s.reflect("order")
        assert M.__tablename__ == 'order'
        assert set(M.__fields__) == {'sym', 'price', 'tags'}
        assert M.__fields__['tags'].qtype.code == QTypeCode.MIXED_LIST
        assert M.__fields__['tags'].qtype.python_type == list


class TestListColumnSupport:
    def test_reflect_space_char_maps_to_mixed_list(self):
        meta = {'c': ['data'], 't': [' '], 'f': [''], 'a': ['']}
        M = build_model_from_meta('nested', meta)
        assert M.__fields__['data'].qtype.code == QTypeCode.MIXED_LIST

    def test_list_field_type_char(self):
        meta = {'c': ['data'], 't': [' '], 'f': [''], 'a': ['']}
        M = build_model_from_meta('nested', meta)
        assert M.__fields__['data'].q_type_char == ' '

    def test_list_column_instantiation(self):
        meta = {'c': ['sym', 'fills'], 't': ['s', ' '], 'f': ['', ''], 'a': ['', '']}
        Order = build_model_from_meta('order', meta)
        o = Order(sym="AAPL", fills=[100.5, 200.3, 50.1])
        assert o.sym == "AAPL"
        assert o.fills == [100.5, 200.3, 50.1]

    def test_list_column_to_dict(self):
        meta = {'c': ['sym', 'fills'], 't': ['s', ' '], 'f': ['', ''], 'a': ['', '']}
        Order = build_model_from_meta('order', meta)
        o = Order(sym="AAPL", fills=[100.5, 200.3])
        assert o.to_dict() == {'sym': 'AAPL', 'fills': [100.5, 200.3]}

    def test_infer_qtype_from_list(self):
        from qorm.types.coerce import infer_qtype
        qt = infer_qtype(list)
        assert qt.code == QTypeCode.MIXED_LIST
        assert qt.python_type == list

    def test_annotated_list_type(self):
        from qorm.types import List
        from qorm.types.coerce import infer_qtype
        qt = infer_qtype(List)
        assert qt.code == QTypeCode.MIXED_LIST

    def test_hand_defined_model_with_list(self):
        from qorm import Model
        from qorm.types import List, Symbol
        from qorm.model.meta import clear_registry

        clear_registry()

        class Order(Model):
            __tablename__ = 'order_list_test'
            sym: Symbol
            fills: List

        assert Order.__fields__['fills'].qtype.code == QTypeCode.MIXED_LIST
        assert Order.__fields__['fills'].q_type_char == ' '

    def test_ddl_for_list_column(self):
        from qorm import Model
        from qorm.types import List, Symbol, Float
        from qorm.model.meta import clear_registry
        from qorm.model.schema import create_table_q

        clear_registry()

        class Order(Model):
            __tablename__ = 'order_ddl_test'
            sym: Symbol
            price: Float
            tags: List

        q = create_table_q(Order)
        # list columns use () not `<type>$()
        assert 'tags:()' in q
        assert 'sym:`s$()' in q
        assert 'price:`f$()' in q
