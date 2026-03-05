"""KeyedModel for keyed tables in q/kdb+.

A keyed table in q is a dictionary mapping a key table to a value table.
KeyedModel marks certain fields as keys using ``field(primary_key=True)``.
"""

from __future__ import annotations

from typing import Any, ClassVar

from .base import Model
from .fields import Field, field


class KeyedModel(Model):
    """Base class for keyed table models.

    Usage::

        class DailyPrice(KeyedModel):
            __tablename__ = 'daily_price'
            sym: Symbol = field(primary_key=True)
            date: Date = field(primary_key=True)
            close: Float
            volume: Long
    """

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)

        if not cls.__dict__.get('__tablename__'):
            return

        if not cls.__key_fields__:
            # Auto-detect: if no explicit primary_key, use the first field
            first_field = next(iter(cls.__fields__), None)
            if first_field:
                cls.__fields__[first_field].primary_key = True
                cls.__key_fields__ = [first_field]

    @classmethod
    def key_columns(cls) -> list[str]:
        """Return the list of key column names."""
        return list(cls.__key_fields__)

    @classmethod
    def value_columns(cls) -> list[str]:
        """Return the list of non-key column names."""
        return [n for n in cls.__fields__ if n not in cls.__key_fields__]
