"""q epoch (2000-01-01) <-> Python datetime conversions."""

from __future__ import annotations

import datetime
import math

from ..protocol.constants import QTypeCode
from .nulls import is_null, QNull

# ── q epoch ────────────────────────────────────────────────────────
Q_EPOCH = datetime.date(2000, 1, 1)
Q_EPOCH_DT = datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc)
UNIX_EPOCH = datetime.date(1970, 1, 1)
# Days between Unix epoch and q epoch
_DAYS_OFFSET = (Q_EPOCH - UNIX_EPOCH).days  # 10957

_NANOS_PER_SEC = 1_000_000_000
_NANOS_PER_MILLI = 1_000_000
_NANOS_PER_MICRO = 1_000
_MILLIS_PER_SEC = 1_000
_SECS_PER_MIN = 60
_SECS_PER_DAY = 86400


# ── To Python ──────────────────────────────────────────────────────

def timestamp_to_datetime(nanos: int) -> datetime.datetime | QNull:
    """Convert q timestamp (nanos since 2000.01.01) to Python datetime."""
    if is_null(nanos, QTypeCode.TIMESTAMP):
        return QNull(QTypeCode.TIMESTAMP)
    secs, rem = divmod(nanos, _NANOS_PER_SEC)
    micros = rem // _NANOS_PER_MICRO
    return Q_EPOCH_DT + datetime.timedelta(seconds=secs, microseconds=micros)


def date_to_python(days: int) -> datetime.date | QNull:
    """Convert q date (days since 2000.01.01) to Python date."""
    if is_null(days, QTypeCode.DATE):
        return QNull(QTypeCode.DATE)
    return Q_EPOCH + datetime.timedelta(days=days)


def month_to_python(months: int) -> datetime.date | QNull:
    """Convert q month (months since 2000.01) to Python date (first of month)."""
    if is_null(months, QTypeCode.MONTH):
        return QNull(QTypeCode.MONTH)
    year = 2000 + months // 12
    month = 1 + months % 12
    return datetime.date(year, month, 1)


def datetime_to_python(frac_days: float) -> datetime.datetime | QNull:
    """Convert q datetime (fractional days since 2000.01.01) to Python datetime."""
    if is_null(frac_days, QTypeCode.DATETIME):
        return QNull(QTypeCode.DATETIME)
    return Q_EPOCH_DT + datetime.timedelta(days=frac_days)


def timespan_to_timedelta(nanos: int) -> datetime.timedelta | QNull:
    """Convert q timespan (nanos since midnight) to Python timedelta."""
    if is_null(nanos, QTypeCode.TIMESPAN):
        return QNull(QTypeCode.TIMESPAN)
    secs, rem = divmod(nanos, _NANOS_PER_SEC)
    micros = rem // _NANOS_PER_MICRO
    return datetime.timedelta(seconds=secs, microseconds=micros)


def minute_to_time(minutes: int) -> datetime.time | QNull:
    """Convert q minute (minutes since midnight) to Python time."""
    if is_null(minutes, QTypeCode.MINUTE):
        return QNull(QTypeCode.MINUTE)
    h, m = divmod(minutes, 60)
    return datetime.time(h, m)


def second_to_time(seconds: int) -> datetime.time | QNull:
    """Convert q second (seconds since midnight) to Python time."""
    if is_null(seconds, QTypeCode.SECOND):
        return QNull(QTypeCode.SECOND)
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    return datetime.time(h, m, s)


def time_to_python(millis: int) -> datetime.time | QNull:
    """Convert q time (millis since midnight) to Python time."""
    if is_null(millis, QTypeCode.TIME):
        return QNull(QTypeCode.TIME)
    secs, ms = divmod(millis, _MILLIS_PER_SEC)
    m, s = divmod(secs, 60)
    h, m = divmod(m, 60)
    return datetime.time(h, m, s, ms * 1000)  # microseconds


# ── From Python ────────────────────────────────────────────────────

def datetime_to_timestamp(dt: datetime.datetime) -> int:
    """Convert Python datetime to q timestamp (nanos since 2000.01.01)."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    delta = dt - Q_EPOCH_DT
    return int(delta.total_seconds() * _NANOS_PER_SEC)


def python_to_date(d: datetime.date) -> int:
    """Convert Python date to q date (days since 2000.01.01)."""
    return (d - Q_EPOCH).days


def python_to_month(d: datetime.date) -> int:
    """Convert Python date to q month (months since 2000.01)."""
    return (d.year - 2000) * 12 + (d.month - 1)


def python_to_datetime(dt: datetime.datetime) -> float:
    """Convert Python datetime to q datetime (fractional days since 2000.01.01)."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    delta = dt - Q_EPOCH_DT
    return delta.total_seconds() / _SECS_PER_DAY


def timedelta_to_timespan(td: datetime.timedelta) -> int:
    """Convert Python timedelta to q timespan (nanos since midnight)."""
    return int(td.total_seconds() * _NANOS_PER_SEC)


def time_to_minute(t: datetime.time) -> int:
    """Convert Python time to q minute."""
    return t.hour * 60 + t.minute


def time_to_second(t: datetime.time) -> int:
    """Convert Python time to q second."""
    return t.hour * 3600 + t.minute * 60 + t.second


def python_to_time(t: datetime.time) -> int:
    """Convert Python time to q time (millis since midnight)."""
    return (t.hour * 3600 + t.minute * 60 + t.second) * _MILLIS_PER_SEC + t.microsecond // 1000
