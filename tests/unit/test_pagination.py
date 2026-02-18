"""Unit tests for pagination (.offset and paginate helpers)."""

import pytest
from unittest.mock import MagicMock

from qorm import Model, Symbol, Float, Long, Timestamp
from qorm.session import ModelResultSet
from qorm.pagination import paginate
from qorm.model.meta import clear_registry


class TestOffset:
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

    def test_offset_only(self):
        q = self.Trade.select().offset(100).compile()
        assert '100_(' in q
        assert '?[trade;' in q

    def test_offset_with_limit(self):
        q = self.Trade.select().offset(100).limit(50).compile()
        assert '50#(' in q
        assert '100_(' in q

    def test_limit_only_unchanged(self):
        q = self.Trade.select().limit(50).compile()
        assert '50#(' in q
        assert '_(' not in q

    def test_offset_without_limit(self):
        q = self.Trade.select().offset(200).compile()
        assert '200_(' in q
        assert '#(' not in q


class TestPaginate:
    @classmethod
    def setup_class(cls):
        clear_registry()

        class Trade(Model):
            __tablename__ = 'trade_pg'
            sym: Symbol
            price: Float

        cls.Trade = Trade

    def _make_result(self, n_rows):
        """Create a ModelResultSet with n_rows rows."""
        if n_rows == 0:
            return ModelResultSet({'sym': [], 'price': []})
        return ModelResultSet({
            'sym': ['AAPL'] * n_rows,
            'price': [100.0] * n_rows,
        })

    def test_paginate_yields_pages(self):
        session = MagicMock()
        # 2 full pages then a partial page
        session.exec.side_effect = [
            self._make_result(100),
            self._make_result(100),
            self._make_result(50),
        ]
        pages = list(paginate(session, self.Trade.select(), page_size=100))
        assert len(pages) == 3
        assert len(pages[0]) == 100
        assert len(pages[2]) == 50

    def test_paginate_stops_on_empty(self):
        session = MagicMock()
        session.exec.side_effect = [
            self._make_result(100),
            self._make_result(0),
        ]
        pages = list(paginate(session, self.Trade.select(), page_size=100))
        assert len(pages) == 1

    def test_paginate_stops_on_partial(self):
        session = MagicMock()
        session.exec.side_effect = [
            self._make_result(30),
        ]
        pages = list(paginate(session, self.Trade.select(), page_size=100))
        assert len(pages) == 1
        assert len(pages[0]) == 30
