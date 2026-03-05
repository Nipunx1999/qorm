"""Serialize Python objects to q IPC binary format.

Uses a pre-allocated bytearray with struct.pack_into for performance.
"""

from __future__ import annotations

import datetime
import struct
import uuid
from typing import Any

from ..exc import SerializationError
from ..types.nulls import QNull, NULL_VALUES
from ..types.temporal import (
    datetime_to_timestamp, python_to_date, python_to_month,
    python_to_datetime, timedelta_to_timespan, time_to_minute,
    time_to_second, python_to_time,
)
from .constants import (
    QTypeCode, TYPE_STRUCT, GUID_SIZE, ATTR_NONE,
    SYNC_MSG, ASYNC_MSG, RESPONSE_MSG,
)
from .framing import pack_header

_INITIAL_BUF_SIZE = 4096


class Serializer:
    """Serialize Python values into q IPC binary format."""

    def __init__(self) -> None:
        self._buf = bytearray(_INITIAL_BUF_SIZE)
        self._pos = 0

    def _ensure(self, n: int) -> None:
        """Ensure at least n bytes are available at _pos."""
        needed = self._pos + n
        if needed > len(self._buf):
            new_size = max(len(self._buf) * 2, needed)
            self._buf.extend(b'\x00' * (new_size - len(self._buf)))

    def _write_byte(self, b: int) -> None:
        self._ensure(1)
        self._buf[self._pos] = b & 0xFF
        self._pos += 1

    def _write_bytes(self, data: bytes | bytearray) -> None:
        n = len(data)
        self._ensure(n)
        self._buf[self._pos:self._pos + n] = data
        self._pos += n

    def _pack_into(self, fmt: str, *values: Any) -> None:
        size = struct.calcsize(fmt)
        self._ensure(size)
        struct.pack_into(f'<{fmt}', self._buf, self._pos, *values)
        self._pos += size

    def serialize_message(self, obj: Any, msg_type: int = SYNC_MSG) -> bytes:
        """Serialize a Python object as a complete IPC message.

        Returns the full message bytes including the 8-byte header.
        """
        self._pos = 0
        # Reserve space for header
        self._ensure(8)
        self._pos = 8
        # Serialize payload
        self._serialize(obj)
        # Write header
        total_len = self._pos
        header = pack_header(msg_type, total_len)
        self._buf[0:8] = header
        return bytes(self._buf[:total_len])

    def _serialize(self, obj: Any) -> None:
        """Dispatch serialization based on Python type."""
        if isinstance(obj, QNull):
            self._serialize_atom_null(obj.type_code)
        elif isinstance(obj, bool):
            self._serialize_boolean(obj)
        elif isinstance(obj, int):
            self._serialize_long(obj)
        elif isinstance(obj, float):
            self._serialize_float(obj)
        elif isinstance(obj, str):
            self._serialize_string(obj)
        elif isinstance(obj, bytes):
            self._serialize_byte_vector(obj)
        elif isinstance(obj, datetime.datetime):
            self._serialize_timestamp(obj)
        elif isinstance(obj, datetime.date):
            self._serialize_date(obj)
        elif isinstance(obj, datetime.timedelta):
            self._serialize_timespan(obj)
        elif isinstance(obj, datetime.time):
            self._serialize_time(obj)
        elif isinstance(obj, uuid.UUID):
            self._serialize_guid(obj)
        elif isinstance(obj, list):
            self._serialize_list(obj)
        elif isinstance(obj, dict):
            self._serialize_dict(obj)
        elif isinstance(obj, QVector):
            self._serialize_typed_vector(obj)
        elif isinstance(obj, QTable):
            self._serialize_table(obj)
        else:
            raise SerializationError(f"Cannot serialize type {type(obj).__name__}")

    # ── Atoms ──────────────────────────────────────────────────────

    def _serialize_atom_null(self, type_code: QTypeCode) -> None:
        """Serialize a typed null atom."""
        self._write_byte(256 - type_code)  # negative type code as unsigned byte
        null_val = NULL_VALUES[type_code]
        if type_code == QTypeCode.GUID:
            self._write_bytes(b'\x00' * GUID_SIZE)
        elif type_code == QTypeCode.SYMBOL:
            self._write_byte(0)  # empty null-terminated string
        else:
            fmt, _ = TYPE_STRUCT[type_code]
            self._pack_into(fmt, null_val)

    def _serialize_boolean(self, value: bool) -> None:
        self._write_byte(256 - QTypeCode.BOOLEAN)
        self._write_byte(1 if value else 0)

    def _serialize_long(self, value: int) -> None:
        self._write_byte(256 - QTypeCode.LONG)
        self._pack_into('q', value)

    def _serialize_float(self, value: float) -> None:
        self._write_byte(256 - QTypeCode.FLOAT)
        self._pack_into('d', value)

    def _serialize_string(self, value: str) -> None:
        """Serialize a string as a char vector (type 10)."""
        encoded = value.encode('utf-8')
        self._write_byte(QTypeCode.CHAR)  # vector type byte
        self._write_byte(ATTR_NONE)
        self._pack_into('i', len(encoded))
        self._write_bytes(encoded)

    def _serialize_symbol_atom(self, value: str) -> None:
        """Serialize a single symbol atom."""
        self._write_byte(256 - QTypeCode.SYMBOL)
        self._write_bytes(value.encode('utf-8'))
        self._write_byte(0)  # null terminator

    def _serialize_byte_vector(self, value: bytes) -> None:
        self._write_byte(QTypeCode.BYTE)
        self._write_byte(ATTR_NONE)
        self._pack_into('i', len(value))
        self._write_bytes(value)

    def _serialize_timestamp(self, value: datetime.datetime) -> None:
        self._write_byte(256 - QTypeCode.TIMESTAMP)
        self._pack_into('q', datetime_to_timestamp(value))

    def _serialize_date(self, value: datetime.date) -> None:
        self._write_byte(256 - QTypeCode.DATE)
        self._pack_into('i', python_to_date(value))

    def _serialize_timespan(self, value: datetime.timedelta) -> None:
        self._write_byte(256 - QTypeCode.TIMESPAN)
        self._pack_into('q', timedelta_to_timespan(value))

    def _serialize_time(self, value: datetime.time) -> None:
        self._write_byte(256 - QTypeCode.TIME)
        self._pack_into('i', python_to_time(value))

    def _serialize_guid(self, value: uuid.UUID) -> None:
        self._write_byte(256 - QTypeCode.GUID)
        self._write_bytes(value.bytes)

    # ── Vectors ────────────────────────────────────────────────────

    def _serialize_list(self, items: list) -> None:
        """Serialize a Python list as a q mixed list (type 0)."""
        self._write_byte(QTypeCode.MIXED_LIST)
        self._write_byte(ATTR_NONE)
        self._pack_into('i', len(items))
        for item in items:
            self._serialize(item)

    def _serialize_typed_vector(self, vec: QVector) -> None:
        """Serialize a typed vector."""
        tc = vec.type_code
        self._write_byte(tc)
        self._write_byte(vec.attr)

        if tc == QTypeCode.SYMBOL:
            self._pack_into('i', len(vec.data))
            for s in vec.data:
                encoded = s.encode('utf-8') if isinstance(s, str) else b''
                self._write_bytes(encoded)
                self._write_byte(0)
        elif tc == QTypeCode.GUID:
            self._pack_into('i', len(vec.data))
            for g in vec.data:
                if isinstance(g, uuid.UUID):
                    self._write_bytes(g.bytes)
                else:
                    self._write_bytes(b'\x00' * GUID_SIZE)
        else:
            fmt, width = TYPE_STRUCT[tc]
            self._pack_into('i', len(vec.data))
            for val in vec.data:
                self._pack_into(fmt, val)

    # ── Dict ───────────────────────────────────────────────────────

    def _serialize_dict(self, d: dict) -> None:
        """Serialize a Python dict as a q dictionary."""
        self._write_byte(QTypeCode.DICT)
        keys = list(d.keys())
        values = list(d.values())
        self._serialize(keys)
        self._serialize(values)

    # ── Table ──────────────────────────────────────────────────────

    def _serialize_table(self, table: QTable) -> None:
        """Serialize a QTable (flip of column dict)."""
        self._write_byte(QTypeCode.TABLE)
        self._write_byte(ATTR_NONE)
        # Table body is a dict: symbol-vector-of-names -> mixed-list-of-columns
        self._serialize_dict(table.data)


class QVector:
    """Typed vector for serialization.

    Wraps a list of raw values (already converted to wire-format integers/floats)
    with a type code and optional attribute.
    """
    __slots__ = ('type_code', 'data', 'attr')

    def __init__(self, type_code: int, data: list, attr: int = ATTR_NONE) -> None:
        self.type_code = type_code
        self.data = data
        self.attr = attr


class QTable:
    """Table structure for serialization.

    data is a dict mapping column-name strings to QVector instances.
    """
    __slots__ = ('data',)

    def __init__(self, data: dict[str, QVector]) -> None:
        self.data = data
