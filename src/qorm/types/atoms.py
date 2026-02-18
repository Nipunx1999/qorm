"""All 19 q atom type descriptors."""

from __future__ import annotations

import datetime
import uuid

from ..protocol.constants import QTypeCode, GUID_SIZE, TYPE_STRUCT
from .base import QType, register_type

# ── Register all atom types ───────────────────────────────────────

q_boolean = register_type(QType(
    code=QTypeCode.BOOLEAN, name="boolean",
    python_type=bool, size=TYPE_STRUCT[QTypeCode.BOOLEAN][1],
))

q_guid = register_type(QType(
    code=QTypeCode.GUID, name="guid",
    python_type=uuid.UUID, size=GUID_SIZE,
))

q_byte = register_type(QType(
    code=QTypeCode.BYTE, name="byte",
    python_type=int, size=TYPE_STRUCT[QTypeCode.BYTE][1],
))

q_short = register_type(QType(
    code=QTypeCode.SHORT, name="short",
    python_type=int, size=TYPE_STRUCT[QTypeCode.SHORT][1],
))

q_int = register_type(QType(
    code=QTypeCode.INT, name="int",
    python_type=int, size=TYPE_STRUCT[QTypeCode.INT][1],
))

q_long = register_type(QType(
    code=QTypeCode.LONG, name="long",
    python_type=int, size=TYPE_STRUCT[QTypeCode.LONG][1],
))

q_real = register_type(QType(
    code=QTypeCode.REAL, name="real",
    python_type=float, size=TYPE_STRUCT[QTypeCode.REAL][1],
))

q_float = register_type(QType(
    code=QTypeCode.FLOAT, name="float",
    python_type=float, size=TYPE_STRUCT[QTypeCode.FLOAT][1],
))

q_char = register_type(QType(
    code=QTypeCode.CHAR, name="char",
    python_type=str, size=TYPE_STRUCT[QTypeCode.CHAR][1],
))

q_symbol = register_type(QType(
    code=QTypeCode.SYMBOL, name="symbol",
    python_type=str, size=0,  # variable-width (null-terminated)
))

q_timestamp = register_type(QType(
    code=QTypeCode.TIMESTAMP, name="timestamp",
    python_type=datetime.datetime, size=TYPE_STRUCT[QTypeCode.TIMESTAMP][1],
))

q_month = register_type(QType(
    code=QTypeCode.MONTH, name="month",
    python_type=datetime.date, size=TYPE_STRUCT[QTypeCode.MONTH][1],
))

q_date = register_type(QType(
    code=QTypeCode.DATE, name="date",
    python_type=datetime.date, size=TYPE_STRUCT[QTypeCode.DATE][1],
))

q_datetime = register_type(QType(
    code=QTypeCode.DATETIME, name="datetime",
    python_type=datetime.datetime, size=TYPE_STRUCT[QTypeCode.DATETIME][1],
))

q_timespan = register_type(QType(
    code=QTypeCode.TIMESPAN, name="timespan",
    python_type=datetime.timedelta, size=TYPE_STRUCT[QTypeCode.TIMESPAN][1],
))

q_minute = register_type(QType(
    code=QTypeCode.MINUTE, name="minute",
    python_type=datetime.time, size=TYPE_STRUCT[QTypeCode.MINUTE][1],
))

q_second = register_type(QType(
    code=QTypeCode.SECOND, name="second",
    python_type=datetime.time, size=TYPE_STRUCT[QTypeCode.SECOND][1],
))

q_time = register_type(QType(
    code=QTypeCode.TIME, name="time",
    python_type=datetime.time, size=TYPE_STRUCT[QTypeCode.TIME][1],
))
