"""Unit tests for the deserializer."""

import struct
import datetime
import uuid

import pytest

from qorm.protocol.serializer import Serializer, QVector
from qorm.protocol.deserializer import Deserializer
from qorm.protocol.constants import (
    QTypeCode, SYNC_MSG, RESPONSE_MSG, ATTR_NONE,
)
from qorm.types.nulls import QNull, NULL_LONG, NULL_INT


class TestRoundTrip:
    """Test serialize -> deserialize round-trips."""

    def setup_method(self):
        self.ser = Serializer()
        self.des = Deserializer()

    def _roundtrip(self, obj, msg_type=SYNC_MSG):
        msg = self.ser.serialize_message(obj, msg_type)
        result_type, result = self.des.deserialize_message(msg)
        return result_type, result

    def test_boolean_true(self):
        mt, result = self._roundtrip(True)
        assert result is True

    def test_boolean_false(self):
        mt, result = self._roundtrip(False)
        assert result is False

    def test_long(self):
        _, result = self._roundtrip(42)
        assert result == 42

    def test_long_negative(self):
        _, result = self._roundtrip(-100)
        assert result == -100

    def test_long_zero(self):
        _, result = self._roundtrip(0)
        assert result == 0

    def test_long_large(self):
        _, result = self._roundtrip(9223372036854775806)
        assert result == 9223372036854775806

    def test_float(self):
        _, result = self._roundtrip(3.14)
        assert abs(result - 3.14) < 1e-10

    def test_float_negative(self):
        _, result = self._roundtrip(-2.5)
        assert abs(result - (-2.5)) < 1e-10

    def test_string(self):
        _, result = self._roundtrip("hello world")
        assert result == "hello world"

    def test_empty_string(self):
        _, result = self._roundtrip("")
        assert result == ""

    def test_guid(self):
        g = uuid.UUID('12345678-1234-5678-1234-567812345678')
        _, result = self._roundtrip(g)
        assert result == g

    def test_bytes(self):
        _, result = self._roundtrip(b'\x01\x02\x03\x04')
        assert result == [1, 2, 3, 4]

    def test_mixed_list(self):
        _, result = self._roundtrip([1, 2, 3])
        assert result == [1, 2, 3]

    def test_empty_list(self):
        _, result = self._roundtrip([])
        assert result == []

    def test_nested_list(self):
        _, result = self._roundtrip([1, "abc", 3.14])
        assert result[0] == 1
        assert result[1] == "abc"
        assert abs(result[2] - 3.14) < 1e-10

    def test_dict(self):
        _, result = self._roundtrip({"a": 1, "b": 2})
        # Dict round-trips through mixed list keys/values
        assert isinstance(result, dict)

    def test_long_null(self):
        null = QNull(QTypeCode.LONG)
        _, result = self._roundtrip(null)
        assert isinstance(result, QNull)
        assert result.type_code == QTypeCode.LONG

    def test_msg_type_preserved(self):
        mt, _ = self._roundtrip(42, RESPONSE_MSG)
        assert mt == RESPONSE_MSG


class TestTypedVectorRoundTrip:
    def setup_method(self):
        self.ser = Serializer()
        self.des = Deserializer()

    def _roundtrip(self, obj):
        msg = self.ser.serialize_message(obj)
        _, result = self.des.deserialize_message(msg)
        return result

    def test_long_vector(self):
        vec = QVector(QTypeCode.LONG, [10, 20, 30])
        result = self._roundtrip(vec)
        assert result == [10, 20, 30]

    def test_float_vector(self):
        vec = QVector(QTypeCode.FLOAT, [1.1, 2.2, 3.3])
        result = self._roundtrip(vec)
        for a, b in zip(result, [1.1, 2.2, 3.3]):
            assert abs(a - b) < 1e-10

    def test_boolean_vector(self):
        vec = QVector(QTypeCode.BOOLEAN, [1, 0, 1, 1])
        result = self._roundtrip(vec)
        assert result == [True, False, True, True]

    def test_symbol_vector(self):
        vec = QVector(QTypeCode.SYMBOL, ["AAPL", "GOOG", "MSFT"])
        result = self._roundtrip(vec)
        assert result == ["AAPL", "GOOG", "MSFT"]

    def test_int_vector(self):
        vec = QVector(QTypeCode.INT, [1, 2, 3])
        result = self._roundtrip(vec)
        assert result == [1, 2, 3]

    def test_short_vector(self):
        vec = QVector(QTypeCode.SHORT, [1, 2, 3])
        result = self._roundtrip(vec)
        assert result == [1, 2, 3]


class TestTemporalRoundTrip:
    def setup_method(self):
        self.ser = Serializer()
        self.des = Deserializer()

    def _roundtrip(self, obj):
        msg = self.ser.serialize_message(obj)
        _, result = self.des.deserialize_message(msg)
        return result

    def test_datetime(self):
        dt = datetime.datetime(2024, 6, 15, 10, 30, 0, tzinfo=datetime.timezone.utc)
        result = self._roundtrip(dt)
        assert isinstance(result, datetime.datetime)
        assert result == dt

    def test_date(self):
        d = datetime.date(2024, 6, 15)
        result = self._roundtrip(d)
        assert isinstance(result, datetime.date)
        assert result == d

    def test_time(self):
        t = datetime.time(14, 30, 45, 500000)
        result = self._roundtrip(t)
        assert isinstance(result, datetime.time)
        assert result == t

    def test_timedelta(self):
        td = datetime.timedelta(hours=2, minutes=30, seconds=15)
        result = self._roundtrip(td)
        assert isinstance(result, datetime.timedelta)
        assert result == td


class TestKnownByteSequences:
    """Test against known q IPC byte sequences."""

    def setup_method(self):
        self.des = Deserializer()

    def test_long_atom_42(self):
        """q) -8!42 -> known bytes."""
        # Build manually: header + type(-7) + value(42)
        payload = bytes([256 - 7]) + struct.pack('<q', 42)
        header = struct.pack('<BBHi', 1, 2, 0, 8 + len(payload))
        msg = header + payload
        mt, result = self.des.deserialize_message(msg)
        assert result == 42

    def test_boolean_atom_true(self):
        payload = bytes([256 - 1, 1])
        header = struct.pack('<BBHi', 1, 2, 0, 8 + len(payload))
        msg = header + payload
        _, result = self.des.deserialize_message(msg)
        assert result is True

    def test_float_atom(self):
        payload = bytes([256 - 9]) + struct.pack('<d', 3.14)
        header = struct.pack('<BBHi', 1, 2, 0, 8 + len(payload))
        msg = header + payload
        _, result = self.des.deserialize_message(msg)
        assert abs(result - 3.14) < 1e-10

    def test_char_vector(self):
        text = b"test"
        payload = bytes([10, 0]) + struct.pack('<i', 4) + text
        header = struct.pack('<BBHi', 1, 2, 0, 8 + len(payload))
        msg = header + payload
        _, result = self.des.deserialize_message(msg)
        assert result == "test"

    def test_long_vector(self):
        values = [10, 20, 30]
        payload = bytes([7, 0]) + struct.pack('<i', 3) + struct.pack('<3q', *values)
        header = struct.pack('<BBHi', 1, 2, 0, 8 + len(payload))
        msg = header + payload
        _, result = self.des.deserialize_message(msg)
        assert result == [10, 20, 30]

    def test_symbol_vector(self):
        syms = b"AAPL\x00GOOG\x00"
        payload = bytes([11, 0]) + struct.pack('<i', 2) + syms
        header = struct.pack('<BBHi', 1, 2, 0, 8 + len(payload))
        msg = header + payload
        _, result = self.des.deserialize_message(msg)
        assert result == ["AAPL", "GOOG"]

    def test_long_null(self):
        payload = bytes([256 - 7]) + struct.pack('<q', -9223372036854775808)
        header = struct.pack('<BBHi', 1, 2, 0, 8 + len(payload))
        msg = header + payload
        _, result = self.des.deserialize_message(msg)
        assert isinstance(result, QNull)
        assert result.type_code == QTypeCode.LONG


class TestFunctionTypes:
    """Test deserialization of kdb+ function/operator types (100-117)."""

    def setup_method(self):
        self.des = Deserializer()

    def _msg(self, payload: bytes) -> bytes:
        header = struct.pack('<BBHi', 1, 2, 0, 8 + len(payload))
        return header + payload

    def test_unary_primitive_101(self):
        # Type 101 + 1 byte operator index
        payload = bytes([101, 0])
        _, result = self.des.deserialize_message(self._msg(payload))
        assert "primitive" in result.lower()

    def test_binary_primitive_102(self):
        payload = bytes([102, 1])
        _, result = self.des.deserialize_message(self._msg(payload))
        assert "primitive" in result.lower()

    def test_projection_104(self):
        # Type 104 + count(2) + two long atoms
        inner1 = bytes([256 - 7]) + struct.pack('<q', 10)
        inner2 = bytes([256 - 7]) + struct.pack('<q', 20)
        payload = bytes([104]) + struct.pack('<i', 2) + inner1 + inner2
        _, result = self.des.deserialize_message(self._msg(payload))
        assert isinstance(result, list)
        assert result == [10, 20]

    def test_adverb_type_106_each(self):
        # Type 106 (each) wraps a long atom
        inner = bytes([256 - 7]) + struct.pack('<q', 42)
        payload = bytes([106]) + inner
        _, result = self.des.deserialize_message(self._msg(payload))
        assert result == 42

    def test_adverb_type_116(self):
        # Type 116 (iterator-applied) wraps a serialized object
        inner = bytes([256 - 7]) + struct.pack('<q', 99)
        payload = bytes([116]) + inner
        _, result = self.des.deserialize_message(self._msg(payload))
        assert result == 99

    def test_adverb_type_117(self):
        # Type 117 wraps a serialized object
        inner_str = b"hello\x00"
        inner = bytes([256 - 11]) + inner_str
        payload = bytes([117]) + inner
        _, result = self.des.deserialize_message(self._msg(payload))
        assert result == "hello"

    def test_function_in_mixed_list(self):
        # Mixed list containing a type-116 function wrapping a long
        inner_func = bytes([116]) + bytes([256 - 7]) + struct.pack('<q', 5)
        inner_long = bytes([256 - 7]) + struct.pack('<q', 10)
        payload = bytes([0, 0]) + struct.pack('<i', 2) + inner_func + inner_long
        _, result = self.des.deserialize_message(self._msg(payload))
        assert isinstance(result, list)
        assert result == [5, 10]
