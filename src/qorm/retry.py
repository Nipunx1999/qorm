"""Retry / reconnection policy with exponential backoff."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Awaitable

from .exc import ConnectionError as QormConnectionError


@dataclass
class RetryPolicy:
    """Configurable retry policy with exponential backoff."""

    max_retries: int = 3
    base_delay: float = 0.1
    max_delay: float = 30.0
    backoff_factor: float = 2.0
    retryable_errors: tuple[type[BaseException], ...] = field(
        default=(QormConnectionError,),
    )


def compute_delay(attempt: int, policy: RetryPolicy) -> float:
    """Compute the delay for a given attempt number (0-indexed)."""
    delay = policy.base_delay * (policy.backoff_factor ** attempt)
    return min(delay, policy.max_delay)


def retry_sync(
    func: Callable[[], Any],
    policy: RetryPolicy,
    reconnect_fn: Callable[[], None] | None = None,
) -> Any:
    """Execute *func*, retrying on retryable errors with backoff.

    If *reconnect_fn* is provided it is called before each retry attempt.
    """
    last_exc: BaseException | None = None
    for attempt in range(policy.max_retries + 1):
        try:
            return func()
        except policy.retryable_errors as exc:
            last_exc = exc
            if attempt >= policy.max_retries:
                raise
            if reconnect_fn is not None:
                reconnect_fn()
            delay = compute_delay(attempt, policy)
            time.sleep(delay)
    raise last_exc  # pragma: no cover – unreachable


async def retry_async(
    func: Callable[[], Awaitable[Any]],
    policy: RetryPolicy,
    reconnect_fn: Callable[[], Awaitable[None]] | None = None,
) -> Any:
    """Async version of :func:`retry_sync`."""
    last_exc: BaseException | None = None
    for attempt in range(policy.max_retries + 1):
        try:
            return await func()
        except policy.retryable_errors as exc:
            last_exc = exc
            if attempt >= policy.max_retries:
                raise
            if reconnect_fn is not None:
                await reconnect_fn()
            delay = compute_delay(attempt, policy)
            await asyncio.sleep(delay)
    raise last_exc  # pragma: no cover – unreachable
