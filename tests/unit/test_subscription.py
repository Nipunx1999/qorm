"""Unit tests for subscription/pub-sub support."""

import pytest

from qorm import Engine, Subscriber
from qorm.subscription import Subscriber


class TestSubscriberInit:
    def test_init(self):
        engine = Engine(host="localhost", port=5000)
        callback = lambda table, data: None
        sub = Subscriber(engine, callback=callback)
        assert sub._engine is engine
        assert sub._callback is callback
        assert sub._conn is None
        assert sub._running is False

    def test_stop(self):
        engine = Engine(host="localhost", port=5000)
        sub = Subscriber(engine, callback=lambda t, d: None)
        sub._running = True
        sub.stop()
        assert sub._running is False

    def test_subscriptions_list(self):
        engine = Engine(host="localhost", port=5000)
        sub = Subscriber(engine, callback=lambda t, d: None)
        assert sub._subscriptions == []
