"""IPC protocol constants for q/kdb+."""

from __future__ import annotations

import enum

# ── IPC message types ──────────────────────────────────────────────
ASYNC_MSG = 0  # async (no response expected)
SYNC_MSG = 1   # sync (response expected)
RESPONSE_MSG = 2  # response to a sync message

# ── IPC header ─────────────────────────────────────────────────────
HEADER_SIZE = 8  # 1 (endian) + 1 (msg type) + 2 (reserved) + 4 (total length)
LITTLE_ENDIAN = 1
BIG_ENDIAN = 0

# ── q type codes ───────────────────────────────────────────────────
# Positive = atom, Negative = vector of that atom type
# Reference: https://code.kx.com/q/basics/datatypes/


class QTypeCode(enum.IntEnum):
    """q type byte codes. Atoms are positive; vectors are negative."""
    # Mixed list
    MIXED_LIST = 0

    # Atoms (positive codes)
    BOOLEAN = 1
    GUID = 2
    BYTE = 4
    SHORT = 5
    INT = 6
    LONG = 7
    REAL = 8
    FLOAT = 9
    CHAR = 10
    SYMBOL = 11
    TIMESTAMP = 12
    MONTH = 13
    DATE = 14
    DATETIME = 15  # deprecated z type
    TIMESPAN = 16
    MINUTE = 17
    SECOND = 18
    TIME = 19

    # Special types
    TABLE = 98
    DICT = 99
    SORTED_DICT = 127
    ERROR = -128

    # Lambda / operator types (for completeness)
    LAMBDA = 100
    UNARY_PRIM = 101
    BINARY_PRIM = 102
    TERNARY_OP = 103
    PROJECTION = 104
    COMPOSITION = 105
    F_EACH = 106
    F_OVER = 107
    F_SCAN = 108
    F_EACH_PRIOR = 109
    F_EACH_RIGHT = 110
    F_EACH_LEFT = 111


# Map from type code to struct format char and byte width
TYPE_STRUCT: dict[int, tuple[str, int]] = {
    QTypeCode.BOOLEAN:   ('b', 1),   # signed byte (0/1)
    QTypeCode.BYTE:      ('B', 1),   # unsigned byte
    QTypeCode.SHORT:     ('h', 2),
    QTypeCode.INT:       ('i', 4),
    QTypeCode.LONG:      ('q', 8),
    QTypeCode.REAL:      ('f', 4),   # 4-byte float
    QTypeCode.FLOAT:     ('d', 8),   # 8-byte double
    QTypeCode.CHAR:      ('c', 1),
    QTypeCode.TIMESTAMP: ('q', 8),   # nanos since 2000.01.01
    QTypeCode.MONTH:     ('i', 4),   # months since 2000.01
    QTypeCode.DATE:      ('i', 4),   # days since 2000.01.01
    QTypeCode.DATETIME:  ('d', 8),   # fractional days since 2000.01.01
    QTypeCode.TIMESPAN:  ('q', 8),   # nanos since midnight
    QTypeCode.MINUTE:    ('i', 4),   # minutes since midnight
    QTypeCode.SECOND:    ('i', 4),   # seconds since midnight
    QTypeCode.TIME:      ('i', 4),   # millis since midnight
}

# Symbol type has no fixed width (null-terminated strings)
GUID_SIZE = 16  # 16 bytes for GUID

# ── Vector attributes ──────────────────────────────────────────────
ATTR_NONE = 0
ATTR_SORTED = 1    # `s#
ATTR_UNIQUE = 2    # `u#
ATTR_PARTED = 3    # `p#
ATTR_GROUPED = 5   # `g#
