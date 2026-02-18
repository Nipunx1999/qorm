"""Unit tests for the serializer."""

import struct
import datetime
import uuid

import pytest

from qorm.protocol.serializer import Serializer, QVector, QTable
from qorm.protocol.constants import (
    QTypeCode, LITTLE_ENDIAN, SYNC_MSG, HEADER_SIZE,
    ATTR_NONE, ASYNC_MSG,
)
from qorm.protocol.framing import unpack_header
from qorm.types.nulls import QNull


class TestSerializerHeader:
    def test_header_structure(self):
        s = Serializer()
        msg = s.serialize_message(42)
        assert len(msg) >= HEADER_SIZE
        endian, msg_type, total_length = unpack_header(msg)
        assert endian == LITTLE_ENDIAN
        assert msg_type == SYNC_MSG
        assert total_length == len(msg)

    def test_async_message_type(self):
        s = Serializer()
        msg = s.serialize_message(42, msg_type=ASYNC_MSG)
        _, msg_type, _ = unpack_header(msg)
        assert msg_type == ASYNC_MSG


class TestSerializeAtoms:
    def setup_method(self):
        self.s = Serializer()

    def test_boolean_true(self):
        msg = self.s.serialize_message(True)
        # After header: type_byte (0xFF = -1 unsigned), value (1)
        assert msg[8] == 0xFF  # -1 as unsigned = 255
        assert msg[9] == 1

    def test_boolean_false(self):
        msg = self.s.serialize_message(False)
        assert msg[8] == 0xFF
        assert msg[9] == 0

    def test_long(self):
        msg = self.s.serialize_message(42)
        # type_byte for long atom: 256-7 = 249
        assert msg[8] == 256 - QTypeCode.LONG
        value = struct.unpack_from('<q', msg, 9)[0]
        assert value == 42

    def test_long_negative(self):
        msg = self.s.serialize_message(-100)
        value = struct.unpack_from('<q', msg, 9)[0]
        assert value == -100

    def test_float(self):
        msg = self.s.serialize_message(3.14)
        assert msg[8] == 256 - QTypeCode.FLOAT
        value = struct.unpack_from('<d', msg, 9)[0]
        assert abs(value - 3.14) < 1e-10

    def test_string_as_char_vector(self):
        msg = self.s.serialize_message("hello")
        # Type byte for char vector: 10
        assert msg[8] == QTypeCode.CHAR
        # Attr byte
        assert msg[9] == ATTR_NONE
        # Length
        length = struct.unpack_from('<i', msg, 10)[0]
        assert length == 5
        # Content
        content = msg[14:19].decode('utf-8')
        assert content == "hello"

    def test_guid(self):
        g = uuid.UUID('12345678-1234-5678-1234-567812345678')
        msg = self.s.serialize_message(g)
        assert msg[8] == 256 - QTypeCode.GUID
        assert msg[9:25] == g.bytes


class TestSerializeVectors:
    def setup_method(self):
        self.s = Serializer()

    def test_mixed_list(self):
        msg = self.s.serialize_message([1, 2, 3])
        # Type 0 = mixed list
        assert msg[8] == QTypeCode.MIXED_LIST
        # Attr byte
        assert msg[9] == ATTR_NONE
        # Count = 3
        count = struct.unpack_from('<i', msg, 10)[0]
        assert count == 3

    def test_typed_long_vector(self):
        vec = QVector(QTypeCode.LONG, [10, 20, 30])
        msg = self.s.serialize_message(vec)
        assert msg[8] == QTypeCode.LONG
        count = struct.unpack_from('<i', msg, 10)[0]
        assert count == 3
        values = struct.unpack_from('<3q', msg, 14)
        assert values == (10, 20, 30)

    def test_typed_symbol_vector(self):
        vec = QVector(QTypeCode.SYMBOL, ["AAPL", "GOOG", "MSFT"])
        msg = self.s.serialize_message(vec)
        assert msg[8] == QTypeCode.SYMBOL
        count = struct.unpack_from('<i', msg, 10)[0]
        assert count == 3

    def test_byte_vector(self):
        msg = self.s.serialize_message(b'\x01\x02\x03')
        assert msg[8] == QTypeCode.BYTE
        count = struct.unpack_from('<i', msg, 10)[0]
        assert count == 3

    def test_empty_list(self):
        msg = self.s.serialize_message([])
        assert msg[8] == QTypeCode.MIXED_LIST
        count = struct.unpack_from('<i', msg, 10)[0]
        assert count == 0


class TestSerializeDict:
    def setup_method(self):
        self.s = Serializer()

    def test_dict(self):
        msg = self.s.serialize_message({"a": 1, "b": 2})
        assert msg[8] == QTypeCode.DICT


class TestSerializeNull:
    def setup_method(self):
        self.s = Serializer()

    def test_long_null(self):
        null = QNull(QTypeCode.LONG)
        msg = self.s.serialize_message(null)
        assert msg[8] == 256 - QTypeCode.LONG
        value = struct.unpack_from('<q', msg, 9)[0]
        assert value == -9223372036854775808

    def test_float_null(self):
        null = QNull(QTypeCode.FLOAT)
        msg = self.s.serialize_message(null)
        assert msg[8] == 256 - QTypeCode.FLOAT


class TestSerializeTemporal:
    def setup_method(self):
        self.s = Serializer()

    def test_datetime(self):
        dt = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
        msg = self.s.serialize_message(dt)
        assert msg[8] == 256 - QTypeCode.TIMESTAMP

    def test_date(self):
        d = datetime.date(2024, 1, 1)
        msg = self.s.serialize_message(d)
        assert msg[8] == 256 - QTypeCode.DATE

    def test_time(self):
        t = datetime.time(12, 30, 45)
        msg = self.s.serialize_message(t)
        assert msg[8] == 256 - QTypeCode.TIME

    def test_timedelta(self):
        td = datetime.timedelta(hours=1, minutes=30)
        msg = self.s.serialize_message(td)
        assert msg[8] == 256 - QTypeCode.TIMESPAN
