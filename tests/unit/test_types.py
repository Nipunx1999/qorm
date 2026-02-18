"""Unit tests for the type system."""

import datetime
import uuid

import pytest

from qorm.types import (
    Symbol, Float, Long, Timestamp, Date, Time, Boolean, Guid,
    Short, Int, Real, Char, Month, DateTime, Timespan, Minute, Second, Byte,
    QType, QTypeCode, QNull,
    infer_qtype, is_null,
    q_symbol, q_float, q_long, q_timestamp,
    Q_EPOCH,
)
from qorm.types.temporal import (
    datetime_to_timestamp, timestamp_to_datetime,
    python_to_date, date_to_python,
    python_to_month, month_to_python,
    timedelta_to_timespan, timespan_to_timedelta,
    python_to_time, time_to_python,
    time_to_minute, minute_to_time,
    time_to_second, second_to_time,
)
from qorm.types.nulls import (
    NULL_LONG, NULL_INT, NULL_FLOAT, NULL_SHORT,
)


class TestQTypeRegistry:
    def test_get_by_code(self):
        from qorm.types.base import get_type_by_code
        qt = get_type_by_code(QTypeCode.SYMBOL)
        assert qt.name == "symbol"
        assert qt.python_type == str

    def test_get_by_name(self):
        from qorm.types.base import get_type_by_name
        qt = get_type_by_name("float")
        assert qt.code == QTypeCode.FLOAT
        assert qt.python_type == float

    def test_all_types(self):
        from qorm.types.base import all_types
        types = all_types()
        assert len(types) == 19  # 18 atom types + mixed list


class TestInferQType:
    def test_plain_int(self):
        qt = infer_qtype(int)
        assert qt.code == QTypeCode.LONG

    def test_plain_float(self):
        qt = infer_qtype(float)
        assert qt.code == QTypeCode.FLOAT

    def test_plain_str(self):
        qt = infer_qtype(str)
        assert qt.code == QTypeCode.SYMBOL

    def test_plain_bool(self):
        qt = infer_qtype(bool)
        assert qt.code == QTypeCode.BOOLEAN

    def test_plain_datetime(self):
        qt = infer_qtype(datetime.datetime)
        assert qt.code == QTypeCode.TIMESTAMP

    def test_plain_date(self):
        qt = infer_qtype(datetime.date)
        assert qt.code == QTypeCode.DATE

    def test_annotated_symbol(self):
        qt = infer_qtype(Symbol)
        assert qt.code == QTypeCode.SYMBOL

    def test_annotated_float(self):
        qt = infer_qtype(Float)
        assert qt.code == QTypeCode.FLOAT

    def test_annotated_long(self):
        qt = infer_qtype(Long)
        assert qt.code == QTypeCode.LONG

    def test_annotated_short(self):
        qt = infer_qtype(Short)
        assert qt.code == QTypeCode.SHORT

    def test_annotated_int(self):
        qt = infer_qtype(Int)
        assert qt.code == QTypeCode.INT

    def test_annotated_timestamp(self):
        qt = infer_qtype(Timestamp)
        assert qt.code == QTypeCode.TIMESTAMP

    def test_unknown_type_raises(self):
        with pytest.raises(TypeError, match="Cannot infer"):
            infer_qtype(dict)


class TestQNull:
    def test_null_equality(self):
        n1 = QNull(QTypeCode.LONG)
        n2 = QNull(QTypeCode.LONG)
        assert n1 == n2

    def test_null_inequality(self):
        n1 = QNull(QTypeCode.LONG)
        n2 = QNull(QTypeCode.DATE)
        assert n1 != n2

    def test_null_is_falsy(self):
        assert not QNull(QTypeCode.LONG)

    def test_null_repr(self):
        n = QNull(QTypeCode.FLOAT)
        assert "FLOAT" in repr(n)

    def test_is_null_with_qnull(self):
        assert is_null(QNull(QTypeCode.LONG), QTypeCode.LONG)

    def test_is_null_with_value(self):
        assert is_null(NULL_LONG, QTypeCode.LONG)

    def test_is_null_float_nan(self):
        import math
        assert is_null(float('nan'), QTypeCode.FLOAT)

    def test_not_null(self):
        assert not is_null(42, QTypeCode.LONG)


class TestTemporalConversions:
    def test_q_epoch(self):
        assert Q_EPOCH == datetime.date(2000, 1, 1)

    def test_date_roundtrip(self):
        d = datetime.date(2024, 6, 15)
        q_days = python_to_date(d)
        result = date_to_python(q_days)
        assert result == d

    def test_date_epoch(self):
        assert python_to_date(Q_EPOCH) == 0

    def test_timestamp_roundtrip(self):
        dt = datetime.datetime(2024, 6, 15, 10, 30, 0, tzinfo=datetime.timezone.utc)
        q_nanos = datetime_to_timestamp(dt)
        result = timestamp_to_datetime(q_nanos)
        assert result == dt

    def test_month_roundtrip(self):
        d = datetime.date(2024, 6, 1)
        q_months = python_to_month(d)
        result = month_to_python(q_months)
        assert result == d

    def test_time_roundtrip(self):
        t = datetime.time(14, 30, 45, 500000)  # 500ms
        q_millis = python_to_time(t)
        result = time_to_python(q_millis)
        assert result == t

    def test_minute_roundtrip(self):
        t = datetime.time(14, 30)
        q_mins = time_to_minute(t)
        result = minute_to_time(q_mins)
        assert result == t

    def test_second_roundtrip(self):
        t = datetime.time(14, 30, 45)
        q_secs = time_to_second(t)
        result = second_to_time(q_secs)
        assert result == t

    def test_timespan_roundtrip(self):
        td = datetime.timedelta(hours=2, minutes=30, seconds=15)
        q_nanos = timedelta_to_timespan(td)
        result = timespan_to_timedelta(q_nanos)
        assert result == td

    def test_null_date(self):
        result = date_to_python(NULL_INT)
        assert isinstance(result, QNull)

    def test_null_timestamp(self):
        result = timestamp_to_datetime(NULL_LONG)
        assert isinstance(result, QNull)
