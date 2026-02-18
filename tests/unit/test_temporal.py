"""Unit tests for temporal helpers: xbar_, today_, now_."""

import pytest

from qorm import (
    Model, Symbol, Float, Long, Timestamp,
    avg_, xbar_, today_, now_,
)
from qorm.query.expressions import Column, Literal, FuncCall, _QSentinel
from qorm.query.compiler import compile_expr
from qorm.model.meta import clear_registry


class TestTemporalHelpers:
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

    def test_xbar_compiles_infix(self):
        expr = xbar_(5, Column('time'))
        result = compile_expr(expr)
        assert result == '(5 xbar time)'

    def test_xbar_in_by_with_alias(self):
        q = (self.Trade.select(self.Trade.sym, vwap=avg_(self.Trade.price))
             .by(self.Trade.sym, t=xbar_(5, self.Trade.time))
             .compile())
        assert '5 xbar time' in q
        assert 't:' in q
        assert 'sym:sym' in q

    def test_xbar_in_where(self):
        expr = xbar_(1, Column('time')) > 100
        result = compile_expr(expr)
        assert '1 xbar time' in result
        assert '100' in result

    def test_today_compiles_to_zd(self):
        expr = today_()
        result = compile_expr(expr)
        assert result == '.z.d'

    def test_now_compiles_to_zp(self):
        expr = now_()
        result = compile_expr(expr)
        assert result == '.z.p'

    def test_combined_xbar_today_in_query(self):
        q = (self.Trade.select()
             .where(self.Trade.time > today_())
             .compile())
        assert '.z.d' in q
        assert 'time' in q
