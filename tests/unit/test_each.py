"""Unit tests for each/peach adverbs."""

import pytest

from qorm import (
    Model, Symbol, Float, Long, Timestamp,
    count_, avg_, each_, peach_,
)
from qorm.query.expressions import Column, EachExpr, AggFunc
from qorm.query.compiler import compile_expr
from qorm.model.meta import clear_registry


class TestEachPeach:
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

    def test_count_each(self):
        expr = count_(Column('tags')).each()
        result = compile_expr(expr)
        assert result == 'count tags each'

    def test_avg_peach(self):
        expr = avg_(Column('prices')).peach()
        result = compile_expr(expr)
        assert result == 'avg prices peach'

    def test_each_standalone(self):
        expr = each_("count", Column('tags'))
        assert isinstance(expr, EachExpr)
        result = compile_expr(expr)
        assert result == 'count tags each'

    def test_peach_standalone(self):
        expr = peach_("sum", Column('sizes'))
        result = compile_expr(expr)
        assert result == 'sum sizes peach'

    def test_each_in_select_named(self):
        q = (self.Trade.select(self.Trade.sym, tag_count=each_("count", Column('tags')))
             .compile())
        assert 'tag_count:count tags each' in q

    def test_each_chained_with_comparison(self):
        expr = each_("count", Column('tags')) > 5
        result = compile_expr(expr)
        assert 'count tags each' in result
        assert '5' in result
