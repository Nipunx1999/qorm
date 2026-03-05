"""Abstract connection interface for q/kdb+ IPC."""

from __future__ import annotations

import abc
from typing import Any


class BaseConnection(abc.ABC):
    """Abstract base for q/kdb+ connections."""

    @abc.abstractmethod
    def open(self) -> None:
        """Establish the connection."""

    @abc.abstractmethod
    def close(self) -> None:
        """Close the connection."""

    @abc.abstractmethod
    def send(self, obj: Any, msg_type: int = 1) -> None:
        """Send a serialized object to kdb+."""

    @abc.abstractmethod
    def receive(self) -> Any:
        """Receive and deserialize a response from kdb+."""

    @abc.abstractmethod
    def query(self, q_expr: str, *args: Any) -> Any:
        """Send a q expression and return the result.

        Parameters
        ----------
        q_expr : str
            q expression to evaluate.
        *args : Any
            Additional arguments passed to the q expression.
        """

    @property
    @abc.abstractmethod
    def is_open(self) -> bool:
        """Whether the connection is currently open."""

    def __enter__(self) -> BaseConnection:
        self.open()
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()


class AsyncBaseConnection(abc.ABC):
    """Abstract base for async q/kdb+ connections."""

    @abc.abstractmethod
    async def open(self) -> None:
        """Establish the connection."""

    @abc.abstractmethod
    async def close(self) -> None:
        """Close the connection."""

    @abc.abstractmethod
    async def send(self, obj: Any, msg_type: int = 1) -> None:
        """Send a serialized object to kdb+."""

    @abc.abstractmethod
    async def receive(self) -> Any:
        """Receive and deserialize a response from kdb+."""

    @abc.abstractmethod
    async def query(self, q_expr: str, *args: Any) -> Any:
        """Send a q expression and return the result."""

    @property
    @abc.abstractmethod
    def is_open(self) -> bool:
        """Whether the connection is currently open."""

    async def __aenter__(self) -> AsyncBaseConnection:
        await self.open()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()
