"""Unit tests for retry/reconnection policy."""

import pytest
from unittest.mock import MagicMock, patch, AsyncMock

from qorm.retry import RetryPolicy, compute_delay, retry_sync, retry_async
from qorm.exc import ConnectionError as QormConnectionError, QError


class TestRetryPolicy:
    def test_defaults(self):
        p = RetryPolicy()
        assert p.max_retries == 3
        assert p.base_delay == 0.1
        assert p.max_delay == 30.0
        assert p.backoff_factor == 2.0
        assert p.retryable_errors == (QormConnectionError,)


class TestComputeDelay:
    def test_first_attempt(self):
        p = RetryPolicy(base_delay=0.1, backoff_factor=2.0)
        assert compute_delay(0, p) == pytest.approx(0.1)

    def test_second_attempt(self):
        p = RetryPolicy(base_delay=0.1, backoff_factor=2.0)
        assert compute_delay(1, p) == pytest.approx(0.2)

    def test_third_attempt(self):
        p = RetryPolicy(base_delay=0.1, backoff_factor=2.0)
        assert compute_delay(2, p) == pytest.approx(0.4)

    def test_caps_at_max_delay(self):
        p = RetryPolicy(base_delay=1.0, backoff_factor=10.0, max_delay=5.0)
        assert compute_delay(5, p) == 5.0


class TestRetrySync:
    def test_succeeds_first_try(self):
        func = MagicMock(return_value="ok")
        p = RetryPolicy(max_retries=3)
        result = retry_sync(func, p)
        assert result == "ok"
        assert func.call_count == 1

    @patch('qorm.retry.time.sleep')
    def test_retries_then_succeeds(self, mock_sleep):
        func = MagicMock(side_effect=[QormConnectionError("fail"), "ok"])
        reconnect = MagicMock()
        p = RetryPolicy(max_retries=3, base_delay=0.1)
        result = retry_sync(func, p, reconnect_fn=reconnect)
        assert result == "ok"
        assert func.call_count == 2
        assert reconnect.call_count == 1
        mock_sleep.assert_called_once()

    @patch('qorm.retry.time.sleep')
    def test_raises_after_max_retries(self, mock_sleep):
        func = MagicMock(side_effect=QormConnectionError("fail"))
        p = RetryPolicy(max_retries=2, base_delay=0.01)
        with pytest.raises(QormConnectionError):
            retry_sync(func, p)
        assert func.call_count == 3  # initial + 2 retries

    def test_does_not_retry_non_retryable(self):
        func = MagicMock(side_effect=QError("q-error"))
        p = RetryPolicy(max_retries=3)
        with pytest.raises(QError):
            retry_sync(func, p)
        assert func.call_count == 1


class TestRetryAsync:
    @pytest.mark.asyncio
    @patch('qorm.retry.asyncio.sleep', new_callable=AsyncMock)
    async def test_async_retry_works(self, mock_sleep):
        call_count = 0

        async def func():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise QormConnectionError("fail")
            return "ok"

        reconnect = AsyncMock()
        p = RetryPolicy(max_retries=3, base_delay=0.01)
        result = await retry_async(func, p, reconnect_fn=reconnect)
        assert result == "ok"
        assert call_count == 2
        reconnect.assert_called_once()


class TestSessionRetry:
    @patch('qorm.retry.time.sleep')
    def test_session_with_retry_reconnects(self, mock_sleep):
        """Test that Session wraps calls with retry when policy is set."""
        from qorm import Engine, Session
        from qorm.retry import RetryPolicy

        policy = RetryPolicy(max_retries=2, base_delay=0.01)
        engine = Engine(host="localhost", port=5000, retry=policy)

        session = Session(engine)
        # Mock connection
        mock_conn = MagicMock()
        mock_conn.query.side_effect = [QormConnectionError("fail"), "result"]
        session._conn = mock_conn

        # Mock _reconnect to avoid real connection
        session._reconnect = MagicMock()

        result = session.raw("select from trade")
        assert result == "result"
        session._reconnect.assert_called_once()
