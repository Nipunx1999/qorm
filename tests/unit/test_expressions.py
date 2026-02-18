"""Unit tests for expressions and aggregates."""

import pytest

from qorm import (
    Model, Symbol, Float, Long, Timestamp,
    avg_, sum_, min_, max_, count_, first_, last_,
)
from qorm.query.expressions import (
    Column, Literal, BinOp, UnaryOp, FuncCall, AggFunc, Expr,
)
from qorm.query.compiler import compile_expr, compile_where, compile_by
from qorm.model.meta import clear_registry


class TestExprOperators:
    def test_gt(self):
        col = Column('price')
        expr = col > 100
        assert isinstance(expr, BinOp)
        assert expr.op == '>'

    def test_lt(self):
        col = Column('price')
        expr = col < 100
        assert isinstance(expr, BinOp)
        assert expr.op == '<'

    def test_eq(self):
        col = Column('sym')
        expr = col == "AAPL"
        assert isinstance(expr, BinOp)
        assert expr.op == '='

    def test_ne(self):
        col = Column('sym')
        expr = col != "AAPL"
        assert isinstance(expr, BinOp)
        assert expr.op == '<>'

    def test_add(self):
        col = Column('price')
        expr = col + 10
        assert isinstance(expr, BinOp)
        assert expr.op == '+'

    def test_mul(self):
        col = Column('price')
        expr = col * 1.1
        assert isinstance(expr, BinOp)
        assert expr.op == '*'

    def test_div(self):
        col = Column('price')
        expr = col / 2
        assert isinstance(expr, BinOp)
        assert expr.op == '%'  # q uses % for division

    def test_neg(self):
        col = Column('price')
        expr = -col
        assert isinstance(expr, UnaryOp)
        assert expr.op == 'neg'

    def test_and(self):
        c1 = Column('price') > 100
        c2 = Column('size') > 50
        expr = c1 & c2
        assert isinstance(expr, BinOp)
        assert expr.op == '&'

    def test_or(self):
        c1 = Column('price') > 100
        c2 = Column('size') > 50
        expr = c1 | c2
        assert isinstance(expr, BinOp)
        assert expr.op == '|'


class TestExprMethods:
    def test_within(self):
        col = Column('price')
        expr = col.within(100, 200)
        assert isinstance(expr, FuncCall)
        assert expr.func_name == 'within'

    def test_like(self):
        col = Column('sym')
        expr = col.like("A*")
        assert isinstance(expr, FuncCall)
        assert expr.func_name == 'like'

    def test_in_(self):
        col = Column('sym')
        expr = col.in_(["AAPL", "GOOG"])
        assert isinstance(expr, FuncCall)
        assert expr.func_name == 'in'


class TestCompileExpr:
    def test_column(self):
        assert compile_expr(Column('price')) == '`price'

    def test_literal_int(self):
        assert compile_expr(Literal(42)) == '42'

    def test_literal_float(self):
        result = compile_expr(Literal(3.14))
        assert '3.14' in result

    def test_literal_string(self):
        result = compile_expr(Literal("AAPL"))
        assert result == '`AAPL'

    def test_literal_bool_true(self):
        assert compile_expr(Literal(True)) == '1b'

    def test_literal_bool_false(self):
        assert compile_expr(Literal(False)) == '0b'

    def test_literal_none(self):
        assert compile_expr(Literal(None)) == '(::)'

    def test_binop(self):
        expr = Column('price') > Literal(100)
        result = compile_expr(expr)
        assert result == '(>;`price;100)'

    def test_nested_binop(self):
        expr = (Column('price') + Literal(10)) * Literal(2)
        result = compile_expr(expr)
        assert result == '(*;(+;`price;10);2)'

    def test_unary_neg(self):
        expr = -Column('price')
        result = compile_expr(expr)
        assert result == '(neg;`price)'

    def test_agg_avg(self):
        expr = avg_(Column('price'))
        result = compile_expr(expr)
        assert result == '(avg;`price)'

    def test_agg_sum(self):
        expr = sum_(Column('size'))
        result = compile_expr(expr)
        assert result == '(sum;`size)'

    def test_agg_count(self):
        expr = count_()
        result = compile_expr(expr)
        assert result == '(count;`i)'

    def test_literal_date_string(self):
        result = compile_expr(Literal("2026.02.17"))
        assert result == '2026.02.17'

    def test_literal_datetime_date(self):
        import datetime
        result = compile_expr(Literal(datetime.date(2026, 2, 17)))
        assert result == '2026.02.17'

    def test_literal_datetime_datetime(self):
        import datetime
        result = compile_expr(Literal(datetime.datetime(2026, 2, 17, 12, 30, 0)))
        assert '2026.02.17D12:30:00' in result

    def test_literal_timedelta(self):
        import datetime
        result = compile_expr(Literal(datetime.timedelta(hours=1, minutes=30)))
        assert '0D01:30:00' in result


class TestCompileWhere:
    def test_empty(self):
        assert compile_where([]) == '()'

    def test_single(self):
        expr = Column('price') > Literal(100)
        result = compile_where([expr])
        assert 'enlist' in result
        assert '(>;`price;100)' in result

    def test_multiple(self):
        e1 = Column('price') > Literal(100)
        e2 = Column('size') > Literal(50)
        result = compile_where([e1, e2])
        assert '(>;`price;100)' in result
        assert '(>;`size;50)' in result


class TestCompileBy:
    def test_empty(self):
        assert compile_by([]) == '0b'

    def test_single_column(self):
        result = compile_by([Column('sym')])
        assert result == '(enlist `sym)!(enlist `sym)'


class TestModelColumnAccess:
    def setup_method(self):
        clear_registry()

    def test_model_getattr_column(self):
        class Trade(Model):
            __tablename__ = 'trade_col'
            sym: Symbol
            price: Float

        col = Trade.sym
        assert isinstance(col, Column)
        assert col.name == 'sym'

    def test_model_getattr_raises(self):
        class Trade(Model):
            __tablename__ = 'trade_col2'
            sym: Symbol

        with pytest.raises(AttributeError):
            Trade.nonexistent
