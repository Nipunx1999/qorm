"""Unit tests for debug/EXPLAIN mode on query builders."""

import pytest

from qorm import Model, Symbol, Float, Long, Timestamp
from qorm.model.meta import clear_registry
from qorm.query.expressions import avg_


# Set up a model for testing
clear_registry()


class Trade(Model):
    __tablename__ = 'trade'
    sym: Symbol
    price: Float
    size: Long
    time: Timestamp


class TestSelectExplain:
    def test_explain_basic(self):
        q = Trade.select()
        ex = q.explain()
        assert "SelectQuery" in ex
        assert "trade" in ex
        assert q.compile() in ex

    def test_explain_with_where(self):
        q = Trade.select(Trade.sym).where(Trade.price > 100)
        ex = q.explain()
        assert "trade" in ex
        assert "price" in ex

    def test_explain_with_by(self):
        q = Trade.select(avg_price=avg_(Trade.price)).by(Trade.sym)
        ex = q.explain()
        assert "trade" in ex


class TestUpdateExplain:
    def test_explain_basic(self):
        q = Trade.update().set(price=Trade.price * 1.1).where(Trade.sym == "AAPL")
        ex = q.explain()
        assert "UpdateQuery" in ex
        assert "trade" in ex
        assert q.compile() in ex


class TestDeleteExplain:
    def test_explain_basic(self):
        q = Trade.delete().where(Trade.sym == "AAPL")
        ex = q.explain()
        assert "DeleteQuery" in ex
        assert "trade" in ex
        assert q.compile() in ex


class TestInsertExplain:
    def test_explain_basic(self):
        t1 = Trade(sym="AAPL", price=150.0, size=100, time=None)
        q = Trade.insert([t1])
        ex = q.explain()
        assert "InsertQuery" in ex
        assert "trade" in ex
        assert "1 rows" in ex


class TestJoinExplain:
    def test_explain_aj(self):
        from qorm import aj

        clear_registry()

        class Quote(Model):
            __tablename__ = 'quote'
            sym: Symbol
            bid: Float
            ask: Float
            time: Timestamp

        q = aj(["sym", "time"], Trade, Quote)
        ex = q.explain()
        assert "aj" in ex
        assert "trade" in ex
        assert "quote" in ex
        assert q.compile() in ex
