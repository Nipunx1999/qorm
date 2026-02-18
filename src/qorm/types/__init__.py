"""Public type aliases for qorm model annotations.

Usage::

    from qorm.types import Symbol, Float, Long, Timestamp

    class Trade(Model):
        sym: Symbol
        price: Float
        size: Long
        time: Timestamp
"""

from __future__ import annotations

import datetime
import uuid
from typing import Annotated

from .base import QType, QTypeCode, get_type_by_code, get_type_by_name, all_types
from .atoms import (
    q_boolean, q_guid, q_byte, q_short, q_int, q_long,
    q_real, q_float, q_char, q_symbol, q_timestamp, q_month,
    q_date, q_datetime, q_timespan, q_minute, q_second, q_time,
)
from .nulls import QNull, is_null
from .coerce import infer_qtype
from .temporal import (
    Q_EPOCH, Q_EPOCH_DT,
    timestamp_to_datetime, date_to_python, month_to_python,
    datetime_to_python, timespan_to_timedelta, minute_to_time,
    second_to_time, time_to_python,
    datetime_to_timestamp, python_to_date, python_to_month,
    python_to_datetime, timedelta_to_timespan, time_to_minute,
    time_to_second, python_to_time,
)

# ── Annotated type aliases ─────────────────────────────────────────
# These carry QType metadata for the model layer to pick up.

Boolean = Annotated[bool, q_boolean]
Guid = Annotated[uuid.UUID, q_guid]
Byte = Annotated[int, q_byte]
Short = Annotated[int, q_short]
Int = Annotated[int, q_int]
Long = Annotated[int, q_long]
Real = Annotated[float, q_real]
Float = Annotated[float, q_float]
Char = Annotated[str, q_char]
Symbol = Annotated[str, q_symbol]
Timestamp = Annotated[datetime.datetime, q_timestamp]
Month = Annotated[datetime.date, q_month]
Date = Annotated[datetime.date, q_date]
DateTime = Annotated[datetime.datetime, q_datetime]
Timespan = Annotated[datetime.timedelta, q_timespan]
Minute = Annotated[datetime.time, q_minute]
Second = Annotated[datetime.time, q_second]
Time = Annotated[datetime.time, q_time]

__all__ = [
    # Type aliases
    'Boolean', 'Guid', 'Byte', 'Short', 'Int', 'Long',
    'Real', 'Float', 'Char', 'Symbol',
    'Timestamp', 'Month', 'Date', 'DateTime',
    'Timespan', 'Minute', 'Second', 'Time',
    # QType descriptors
    'QType', 'QTypeCode', 'QNull',
    'q_boolean', 'q_guid', 'q_byte', 'q_short', 'q_int', 'q_long',
    'q_real', 'q_float', 'q_char', 'q_symbol', 'q_timestamp', 'q_month',
    'q_date', 'q_datetime', 'q_timespan', 'q_minute', 'q_second', 'q_time',
    # Utilities
    'infer_qtype', 'is_null',
    'get_type_by_code', 'get_type_by_name', 'all_types',
    'Q_EPOCH', 'Q_EPOCH_DT',
]
