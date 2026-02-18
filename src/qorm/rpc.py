"""Remote procedure call helpers for invoking q functions over IPC."""

from __future__ import annotations

import functools
from typing import Any, Callable, TYPE_CHECKING

if TYPE_CHECKING:
    from .session import Session


class QFunction:
    """Reusable wrapper around a named q function.

    Usage::

        get_trades = QFunction("getTradesByDate")
        result = get_trades(session, "2024.01.15")
    """

    def __init__(self, func_name: str) -> None:
        self.func_name = func_name

    def __call__(self, session: Session, *args: Any) -> Any:
        return session.call(self.func_name, *args)

    def __repr__(self) -> str:
        return f"QFunction({self.func_name!r})"


def q_api(func_name: str) -> Callable[..., Callable[..., Any]]:
    """Decorator that maps a typed Python signature to a q function call.

    The decorated function's body is never executed — calls are routed
    to ``session.call(func_name, ...)`` instead. The original signature
    serves as documentation.

    Usage::

        @q_api("getTradesByDate")
        def get_trades_by_date(session, date: str): ...

        trades = get_trades_by_date(session, "2024.01.15")
    """
    qfunc = QFunction(func_name)

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(fn)
        def wrapper(session: Session, *args: Any, **kwargs: Any) -> Any:
            # kwargs are passed as positional args in order
            all_args = list(args)
            if kwargs:
                all_args.extend(kwargs.values())
            return qfunc(session, *all_args)

        wrapper._qfunction = qfunc  # type: ignore[attr-defined]
        return wrapper

    return decorator
