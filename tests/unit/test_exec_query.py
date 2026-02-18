"""Unit tests for ExecQuery."""

import pytest

from qorm import (
    Model, Symbol, Float, Long, Timestamp,
    avg_, ExecQuery,
)
from qorm.model.meta import clear_registry


class TestExecQuery:
    @classmethod
    def setup_class(cls):
        clear_registry()

        class Trade(Model):
            __tablename__ = 'trade'
            sym: Symbol
            price: Float
            size: Long
            time: Timestamp

        cls.Trade = Trade

    def test_single_column_atom_form(self):
        q = self.Trade.exec_(self.Trade.price).compile()
        assert '?[trade;' in q
        assert '`price' in q

    def test_multiple_columns_dict_form(self):
        q = self.Trade.exec_(self.Trade.sym, self.Trade.price).compile()
        assert '?[trade;' in q
        assert '`sym`price' in q
        assert '(sym;price)' in q

    def test_where_chain(self):
        q = (self.Trade.exec_(self.Trade.sym)
             .where(self.Trade.size > 100)
             .compile())
        assert '?[trade;' in q
        assert '100' in q

    def test_by_chain(self):
        q = (self.Trade.exec_(self.Trade.sym, self.Trade.price)
             .by(self.Trade.sym)
             .compile())
        assert '?[trade;' in q
        assert 'sym:sym' in q

    def test_limit(self):
        q = self.Trade.exec_(self.Trade.price).limit(10).compile()
        assert '10#' in q

    def test_explain(self):
        explain = self.Trade.exec_(self.Trade.price).explain()
        assert '-- ExecQuery on `trade' in explain
        assert '?[trade;' in explain

    def test_named_column(self):
        q = self.Trade.exec_(avg_price=avg_(self.Trade.price)).compile()
        assert 'avg_price' in q
        assert 'avg price' in q
