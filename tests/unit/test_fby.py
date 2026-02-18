"""Unit tests for fby (filter by) expressions."""

import pytest

from qorm import Model, Symbol, Float, Long, Timestamp, fby_
from qorm.query.expressions import Column, FbyExpr
from qorm.query.compiler import compile_expr
from qorm.model.meta import clear_registry


class TestFby:
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

    def test_fby_max_compiles(self):
        expr = fby_("max", Column('price'), Column('sym'))
        result = compile_expr(expr)
        assert result == '(fby;(enlist;max;`price);`sym)'

    def test_fby_in_where_eq(self):
        q = (self.Trade.select()
             .where(self.Trade.price == fby_("max", self.Trade.price, self.Trade.sym))
             .compile())
        assert '(fby;(enlist;max;`price);`sym)' in q
        assert '`price' in q

    def test_fby_in_where_gt(self):
        q = (self.Trade.select()
             .where(self.Trade.size > fby_("avg", self.Trade.size, self.Trade.sym))
             .compile())
        assert '(fby;(enlist;avg;`size);`sym)' in q

    def test_fby_different_aggregates(self):
        for agg in ('avg', 'min', 'sum', 'max'):
            expr = fby_(agg, Column('price'), Column('sym'))
            result = compile_expr(expr)
            assert result == f'(fby;(enlist;{agg};`price);`sym)'

    def test_fby_standalone(self):
        expr = fby_("max", Column('price'), Column('sym'))
        assert isinstance(expr, FbyExpr)
        assert expr.agg_name == 'max'
        result = compile_expr(expr)
        assert result == '(fby;(enlist;max;`price);`sym)'
