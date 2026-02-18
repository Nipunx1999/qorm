"""Unit tests for the model layer."""

import datetime

import pytest

from qorm import (
    Model, KeyedModel, Field, field,
    Symbol, Float, Long, Timestamp, Date, Int, Short,
)
from qorm.model.meta import get_model, clear_registry
from qorm.model.schema import create_table_q, drop_table_q, table_exists_q
from qorm.model.fields import Field as FieldClass
from qorm.protocol.constants import QTypeCode, ATTR_SORTED


class TestModelDefinition:
    def setup_method(self):
        clear_registry()

    def test_basic_model(self):
        class Trade(Model):
            __tablename__ = 'trade'
            sym: Symbol
            price: Float
            size: Long
            time: Timestamp

        assert 'sym' in Trade.__fields__
        assert 'price' in Trade.__fields__
        assert 'size' in Trade.__fields__
        assert 'time' in Trade.__fields__
        assert len(Trade.__fields__) == 4

    def test_field_types(self):
        class Trade(Model):
            __tablename__ = 'trade'
            sym: Symbol
            price: Float
            size: Long

        assert Trade.__fields__['sym'].qtype.code == QTypeCode.SYMBOL
        assert Trade.__fields__['price'].qtype.code == QTypeCode.FLOAT
        assert Trade.__fields__['size'].qtype.code == QTypeCode.LONG

    def test_model_registry(self):
        class TestTable(Model):
            __tablename__ = 'test_table'
            value: Long

        assert get_model('test_table') is TestTable

    def test_model_instantiation(self):
        class Trade(Model):
            __tablename__ = 'trade2'
            sym: Symbol
            price: Float

        t = Trade(sym="AAPL", price=150.25)
        assert t.sym == "AAPL"
        assert t.price == 150.25

    def test_model_defaults(self):
        class Trade(Model):
            __tablename__ = 'trade3'
            sym: Symbol
            price: Float

        t = Trade()
        assert t.sym is None
        assert t.price is None

    def test_model_repr(self):
        class Trade(Model):
            __tablename__ = 'trade4'
            sym: Symbol
            price: Float

        t = Trade(sym="AAPL", price=150.25)
        r = repr(t)
        assert "Trade" in r
        assert "AAPL" in r

    def test_model_equality(self):
        class Trade(Model):
            __tablename__ = 'trade5'
            sym: Symbol
            price: Float

        t1 = Trade(sym="AAPL", price=150.0)
        t2 = Trade(sym="AAPL", price=150.0)
        assert t1 == t2

    def test_model_to_dict(self):
        class Trade(Model):
            __tablename__ = 'trade6'
            sym: Symbol
            price: Float

        t = Trade(sym="AAPL", price=150.0)
        d = t.to_dict()
        assert d == {'sym': 'AAPL', 'price': 150.0}

    def test_model_from_dict(self):
        class Trade(Model):
            __tablename__ = 'trade7'
            sym: Symbol
            price: Float

        t = Trade.from_dict({'sym': 'AAPL', 'price': 150.0})
        assert t.sym == 'AAPL'
        assert t.price == 150.0

    def test_field_with_default(self):
        class Trade(Model):
            __tablename__ = 'trade8'
            sym: Symbol
            size: Long = field(default=100)

        t = Trade(sym="AAPL")
        assert t.size == 100

    def test_field_with_attr(self):
        class Trade(Model):
            __tablename__ = 'trade9'
            sym: Symbol = field(attr=ATTR_SORTED)
            price: Float

        assert Trade.__fields__['sym'].attr == ATTR_SORTED


class TestKeyedModel:
    def setup_method(self):
        clear_registry()

    def test_keyed_model(self):
        class DailyPrice(KeyedModel):
            __tablename__ = 'daily_price'
            sym: Symbol = field(primary_key=True)
            date: Date = field(primary_key=True)
            close: Float
            volume: Long

        assert DailyPrice.__key_fields__ == ['sym', 'date']
        assert DailyPrice.key_columns() == ['sym', 'date']
        assert DailyPrice.value_columns() == ['close', 'volume']


class TestSchema:
    def setup_method(self):
        clear_registry()

    def test_create_table(self):
        class Trade(Model):
            __tablename__ = 'trade'
            sym: Symbol
            price: Float
            size: Long
            time: Timestamp

        q = create_table_q(Trade)
        assert q.startswith('trade:(')
        assert 'sym:' in q
        assert 'price:' in q
        assert 'size:' in q
        assert 'time:' in q

    def test_create_keyed_table(self):
        class DailyPrice(KeyedModel):
            __tablename__ = 'daily_price2'
            sym: Symbol = field(primary_key=True)
            close: Float
            volume: Long

        q = create_table_q(DailyPrice)
        assert '[sym:' in q
        assert 'close:' in q

    def test_drop_table(self):
        class Trade(Model):
            __tablename__ = 'trade_drop'
            sym: Symbol

        q = drop_table_q(Trade)
        assert q == 'delete trade_drop from `.'

    def test_table_exists(self):
        class Trade(Model):
            __tablename__ = 'trade_exists'
            sym: Symbol

        q = table_exists_q(Trade)
        assert '`trade_exists' in q
        assert 'tables[]' in q

    def test_type_chars(self):
        class AllTypes(Model):
            __tablename__ = 'all_types'
            b: Symbol
            f: Float
            j: Long
            p: Timestamp

        fields = AllTypes.__fields__
        assert fields['b'].q_type_char == 's'
        assert fields['f'].q_type_char == 'f'
        assert fields['j'].q_type_char == 'j'
        assert fields['p'].q_type_char == 'p'
