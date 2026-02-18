"""Deserialize q IPC binary to Python objects.

Uses memoryview for zero-copy slicing.
"""

from __future__ import annotations

import datetime
import struct
import uuid
from typing import Any

from ..exc import DeserializationError, QError
from ..types.nulls import QNull, is_null
from ..types.temporal import (
    timestamp_to_datetime, date_to_python, month_to_python,
    datetime_to_python, timespan_to_timedelta, minute_to_time,
    second_to_time, time_to_python,
)
from .constants import (
    QTypeCode, TYPE_STRUCT, GUID_SIZE, ATTR_NONE, HEADER_SIZE,
    LITTLE_ENDIAN,
)
from .framing import unpack_header


class Deserializer:
    """Deserialize q IPC binary format to Python objects."""

    def __init__(self) -> None:
        self._data: memoryview = memoryview(b'')
        self._pos: int = 0
        self._little_endian: bool = True

    def deserialize_message(self, raw: bytes | bytearray) -> tuple[int, Any]:
        """Deserialize a complete IPC message.

        Returns (msg_type, python_object).
        """
        endian, msg_type, total_length = unpack_header(raw)
        self._little_endian = (endian == LITTLE_ENDIAN)
        self._data = memoryview(raw)
        self._pos = HEADER_SIZE
        obj = self._deserialize()
        return msg_type, obj

    def deserialize_payload(self, payload: bytes | bytearray) -> Any:
        """Deserialize a payload (without header)."""
        self._data = memoryview(payload)
        self._pos = 0
        self._little_endian = True
        return self._deserialize()

    def _read_byte(self) -> int:
        b = self._data[self._pos]
        self._pos += 1
        return b

    def _read_bytes(self, n: int) -> bytes:
        result = bytes(self._data[self._pos:self._pos + n])
        self._pos += n
        return result

    def _unpack(self, fmt: str) -> Any:
        prefix = '<' if self._little_endian else '>'
        full_fmt = f'{prefix}{fmt}'
        size = struct.calcsize(full_fmt)
        result = struct.unpack_from(full_fmt, self._data, self._pos)
        self._pos += size
        return result[0]

    def _unpack_many(self, fmt: str, count: int) -> tuple:
        """Unpack `count` items of the same format in one call."""
        prefix = '<' if self._little_endian else '>'
        full_fmt = f'{prefix}{count}{fmt}'
        size = struct.calcsize(full_fmt)
        result = struct.unpack_from(full_fmt, self._data, self._pos)
        self._pos += size
        return result

    def _deserialize(self) -> Any:
        type_byte = self._read_byte()

        # Signed interpretation: type_byte > 128 means negative atom type;
        # type_byte == 128 is -128 (error).
        if type_byte == 128:
            return self._deserialize_error()
        elif type_byte > 128:
            type_code = 256 - type_byte  # recover positive type code
            return self._deserialize_atom(type_code)
        elif type_byte == QTypeCode.MIXED_LIST:
            return self._deserialize_mixed_list()
        elif 1 <= type_byte <= 19:
            return self._deserialize_vector(type_byte)
        elif 20 <= type_byte <= 76:
            return self._deserialize_enum_vector()
        elif type_byte == QTypeCode.TABLE:
            return self._deserialize_table()
        elif type_byte == QTypeCode.DICT:
            return self._deserialize_dict()
        elif type_byte == QTypeCode.SORTED_DICT:
            return self._deserialize_dict()  # sorted dict same structure
        elif 100 <= type_byte <= 111:
            # Lambda/operator types - read as string
            return self._deserialize_lambda(type_byte)
        else:
            raise DeserializationError(f"Unknown type byte: {type_byte}")

    # ── Atoms ──────────────────────────────────────────────────────

    def _deserialize_atom(self, type_code: int) -> Any:
        tc = QTypeCode(type_code)

        if tc == QTypeCode.BOOLEAN:
            val = self._read_byte()
            return bool(val)

        if tc == QTypeCode.GUID:
            raw = self._read_bytes(GUID_SIZE)
            guid = uuid.UUID(bytes=raw)
            if is_null(guid, tc):
                return QNull(tc)
            return guid

        if tc == QTypeCode.SYMBOL:
            return self._read_symbol()

        if tc == QTypeCode.CHAR:
            return chr(self._read_byte())

        fmt, _ = TYPE_STRUCT[tc]
        raw_val = self._unpack(fmt)

        if is_null(raw_val, tc):
            return QNull(tc)

        return self._convert_atom(tc, raw_val)

    def _convert_atom(self, tc: QTypeCode, raw_val: Any) -> Any:
        """Convert raw wire value to Python type."""
        if tc == QTypeCode.TIMESTAMP:
            return timestamp_to_datetime(raw_val)
        if tc == QTypeCode.DATE:
            return date_to_python(raw_val)
        if tc == QTypeCode.MONTH:
            return month_to_python(raw_val)
        if tc == QTypeCode.DATETIME:
            return datetime_to_python(raw_val)
        if tc == QTypeCode.TIMESPAN:
            return timespan_to_timedelta(raw_val)
        if tc == QTypeCode.MINUTE:
            return minute_to_time(raw_val)
        if tc == QTypeCode.SECOND:
            return second_to_time(raw_val)
        if tc == QTypeCode.TIME:
            return time_to_python(raw_val)
        # Numeric types return raw value
        return raw_val

    def _read_symbol(self) -> str:
        """Read a null-terminated symbol string."""
        start = self._pos
        while self._data[self._pos] != 0:
            self._pos += 1
        sym = bytes(self._data[start:self._pos]).decode('utf-8')
        self._pos += 1  # skip null terminator
        return sym

    # ── Vectors ────────────────────────────────────────────────────

    def _deserialize_mixed_list(self) -> list:
        _attr = self._read_byte()
        count = self._unpack('i')
        return [self._deserialize() for _ in range(count)]

    def _deserialize_vector(self, type_code: int) -> list:
        tc = QTypeCode(type_code)
        _attr = self._read_byte()
        count = self._unpack('i')

        if tc == QTypeCode.SYMBOL:
            return [self._read_symbol() for _ in range(count)]

        if tc == QTypeCode.GUID:
            result = []
            for _ in range(count):
                raw = self._read_bytes(GUID_SIZE)
                g = uuid.UUID(bytes=raw)
                result.append(QNull(tc) if is_null(g, tc) else g)
            return result

        if tc == QTypeCode.CHAR:
            raw = self._read_bytes(count)
            return raw.decode('utf-8', errors='replace')

        if tc == QTypeCode.BOOLEAN:
            return [bool(b) for b in self._read_bytes(count)]

        fmt, width = TYPE_STRUCT[tc]
        # Batch unpack for performance
        raw_values = self._unpack_many(fmt, count)
        return [self._convert_atom(tc, v) if not is_null(v, tc)
                else QNull(tc) for v in raw_values]

    # ── Enumerated vectors ────────────────────────────────────────

    def _deserialize_enum_vector(self) -> list:
        """Deserialize an enumerated type vector (type 20-76).

        Enumerated columns (e.g. from splayed/partitioned tables) are
        stored as int32 indices into a symbol domain.  The wire format
        is identical to an int vector: attr(1) + count(4) + int32[].

        We read the raw symbol values via the nested symbol list that
        kdb+ prepends for enumerated vectors when possible; otherwise
        we return the integer indices.
        """
        _attr = self._read_byte()
        count = self._unpack('i')
        if count == 0:
            return []
        raw_values = self._unpack_many('i', count)
        return list(raw_values)

    # ── Dict ───────────────────────────────────────────────────────

    def _deserialize_dict(self) -> dict:
        keys = self._deserialize()
        values = self._deserialize()
        if isinstance(keys, list) and isinstance(values, list):
            return dict(zip(keys, values))
        # Keyed table: dict of table -> table
        return {'keys': keys, 'values': values}

    # ── Table ──────────────────────────────────────────────────────

    def _deserialize_table(self) -> dict:
        """Deserialize a q table (flip of column dict).

        Returns a dict with '__table__': True and column-name -> list mappings.
        """
        _attr = self._read_byte()
        inner = self._deserialize()  # should be a dict
        if isinstance(inner, dict):
            inner['__table__'] = True
        return inner

    # ── Error ──────────────────────────────────────────────────────

    def _deserialize_error(self) -> None:
        msg = self._read_symbol()
        raise QError(msg)

    # ── Lambda (stub) ──────────────────────────────────────────────

    def _deserialize_lambda(self, type_byte: int) -> str:
        """Stub for lambda/operator types."""
        if type_byte == 100:
            # Lambda: namespace + body
            ns = self._read_symbol()
            body = self._deserialize()
            return f"{{lambda: {body}}}"
        # Other function types - skip
        return f"<function type {type_byte}>"
