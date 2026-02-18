"""Unit tests for query builders."""

import pytest

from qorm import (
    Model, Symbol, Float, Long, Timestamp,
    avg_, sum_, count_, min_, max_,
    aj, lj, ij,
)
from qorm.model.meta import clear_registry


# Define test models
class _TestModels:
    """Mixin to set up test models in each test class."""

    @classmethod
    def setup_class(cls):
        clear_registry()

        class Trade(Model):
            __tablename__ = 'trade'
            sym: Symbol
            price: Float
            size: Long
            time: Timestamp

        class Quote(Model):
            __tablename__ = 'quote'
            sym: Symbol
            bid: Float
            ask: Float
            time: Timestamp

        cls.Trade = Trade
        cls.Quote = Quote


class TestSelectQuery(_TestModels):
    def test_select_all(self):
        q = self.Trade.select().compile()
        assert q.startswith('?[trade;')

    def test_select_columns(self):
        q = self.Trade.select(self.Trade.sym, self.Trade.price).compile()
        assert '?[trade;' in q
        assert 'sym:sym' in q
        assert 'price:price' in q

    def test_select_with_where(self):
        q = self.Trade.select().where(self.Trade.price > 100).compile()
        assert '?[trade;' in q
        assert 'price' in q
        assert '100' in q

    def test_select_with_multiple_where(self):
        q = (self.Trade.select()
             .where(self.Trade.price > 100)
             .where(self.Trade.size > 50)
             .compile())
        assert 'price' in q
        assert 'size' in q

    def test_select_with_by(self):
        q = (self.Trade.select(
                 self.Trade.sym,
                 avg_price=avg_(self.Trade.price))
             .by(self.Trade.sym)
             .compile())
        assert '?[trade;' in q
        assert 'sym:sym' in q
        assert 'avg price' in q or 'avg_price' in q

    def test_select_with_aggregates(self):
        q = (self.Trade.select(
                 total=sum_(self.Trade.size),
                 avg_price=avg_(self.Trade.price))
             .by(self.Trade.sym)
             .compile())
        assert 'sum size' in q or 'total' in q
        assert 'avg price' in q or 'avg_price' in q

    def test_select_with_limit(self):
        q = self.Trade.select().limit(10).compile()
        assert '10#' in q

    def test_select_named_columns(self):
        q = self.Trade.select(avg_price=avg_(self.Trade.price)).compile()
        assert 'avg_price:avg price' in q


class TestUpdateQuery(_TestModels):
    def test_basic_update(self):
        q = (self.Trade.update()
             .set(price=100.0)
             .compile())
        assert '![trade;' in q
        assert 'price' in q

    def test_update_with_expression(self):
        q = (self.Trade.update()
             .set(price=self.Trade.price * 1.1)
             .where(self.Trade.sym == "AAPL")
             .compile())
        assert '![trade;' in q
        assert 'price' in q

    def test_update_multiple_columns(self):
        q = (self.Trade.update()
             .set(price=100.0, size=50)
             .compile())
        assert 'price' in q
        assert 'size' in q


class TestDeleteQuery(_TestModels):
    def test_delete_with_where(self):
        q = (self.Trade.delete()
             .where(self.Trade.sym == "AAPL")
             .compile())
        assert '![trade;' in q
        assert 'sym' in q
        assert '0b' in q

    def test_delete_columns(self):
        q = self.Trade.delete().columns("price", "size").compile()
        assert '`price' in q
        assert '`size' in q


class TestInsertQuery(_TestModels):
    def test_basic_insert(self):
        trades = [
            self.Trade(sym="AAPL", price=150.25, size=100),
            self.Trade(sym="GOOG", price=2800.0, size=50),
        ]
        q = self.Trade.insert(trades).compile()
        assert '`trade insert' in q
        assert 'AAPL' in q
        assert 'GOOG' in q

    def test_empty_insert(self):
        q = self.Trade.insert([]).compile()
        assert '`trade insert' in q

    def test_insert_repr(self):
        trades = [self.Trade(sym="AAPL", price=150.0, size=100)]
        r = repr(self.Trade.insert(trades))
        assert 'InsertQuery' in r
        assert '1 rows' in r


class TestJoinQueries(_TestModels):
    def test_aj(self):
        q = aj([self.Trade.sym, self.Trade.time], self.Trade, self.Quote).compile()
        assert 'aj[' in q
        assert '`sym`time' in q
        assert 'trade' in q
        assert 'quote' in q

    def test_lj(self):
        q = lj([self.Trade.sym], self.Trade, self.Quote).compile()
        assert 'lj' in q
        assert 'trade' in q
        assert 'quote' in q

    def test_ij(self):
        q = ij([self.Trade.sym], self.Trade, self.Quote).compile()
        assert 'ij' in q
        assert 'trade' in q
        assert 'quote' in q

    def test_aj_with_string_columns(self):
        q = aj(["sym", "time"], self.Trade, self.Quote).compile()
        assert 'aj[' in q
        assert '`sym`time' in q
