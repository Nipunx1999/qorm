"""QNull sentinels and infinity constants for q types."""

from __future__ import annotations

import math
import uuid
from typing import Any

from ..protocol.constants import QTypeCode

# ── Null integer values (q uses specific bit patterns) ─────────────
NULL_BOOLEAN = False
NULL_BYTE = 0x00
NULL_SHORT = -32768                  # 0x8000
NULL_INT = -2147483648               # 0x80000000
NULL_LONG = -9223372036854775808     # 0x8000000000000000
NULL_REAL = float('nan')
NULL_FLOAT = float('nan')
NULL_CHAR = ' '
NULL_SYMBOL = ''
NULL_GUID = uuid.UUID(int=0)

# Temporal nulls use the integer null of their underlying storage type
NULL_TIMESTAMP = NULL_LONG   # nanos
NULL_MONTH = NULL_INT        # months
NULL_DATE = NULL_INT         # days
NULL_DATETIME = NULL_FLOAT   # fractional days
NULL_TIMESPAN = NULL_LONG    # nanos
NULL_MINUTE = NULL_INT       # minutes
NULL_SECOND = NULL_INT       # seconds
NULL_TIME = NULL_INT         # millis

# ── Infinity values ────────────────────────────────────────────────
INF_SHORT = 32767
INF_INT = 2147483647
INF_LONG = 9223372036854775807
INF_REAL = float('inf')
INF_FLOAT = float('inf')

NEG_INF_SHORT = -32767
NEG_INF_INT = -2147483647
NEG_INF_LONG = -9223372036854775807
NEG_INF_REAL = float('-inf')
NEG_INF_FLOAT = float('-inf')

# Temporal infinities
INF_TIMESTAMP = INF_LONG
INF_DATE = INF_INT
INF_TIMESPAN = INF_LONG
INF_TIME = INF_INT

# ── Map type code -> null raw value ────────────────────────────────
NULL_VALUES: dict[QTypeCode, Any] = {
    QTypeCode.BOOLEAN:   NULL_BOOLEAN,
    QTypeCode.GUID:      NULL_GUID,
    QTypeCode.BYTE:      NULL_BYTE,
    QTypeCode.SHORT:     NULL_SHORT,
    QTypeCode.INT:       NULL_INT,
    QTypeCode.LONG:      NULL_LONG,
    QTypeCode.REAL:      NULL_REAL,
    QTypeCode.FLOAT:     NULL_FLOAT,
    QTypeCode.CHAR:      NULL_CHAR,
    QTypeCode.SYMBOL:    NULL_SYMBOL,
    QTypeCode.TIMESTAMP: NULL_TIMESTAMP,
    QTypeCode.MONTH:     NULL_MONTH,
    QTypeCode.DATE:      NULL_DATE,
    QTypeCode.DATETIME:  NULL_DATETIME,
    QTypeCode.TIMESPAN:  NULL_TIMESPAN,
    QTypeCode.MINUTE:    NULL_MINUTE,
    QTypeCode.SECOND:    NULL_SECOND,
    QTypeCode.TIME:      NULL_TIME,
}


class QNull:
    """Typed null sentinel for q values.

    Preserves type information so that e.g. a long null and a date null
    are distinguishable and serialize correctly.
    """
    __slots__ = ('type_code',)

    def __init__(self, type_code: QTypeCode) -> None:
        self.type_code = type_code

    def __repr__(self) -> str:
        return f"QNull({QTypeCode(self.type_code).name})"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, QNull):
            return self.type_code == other.type_code
        return NotImplemented

    def __hash__(self) -> int:
        return hash(('QNull', self.type_code))

    def __bool__(self) -> bool:
        return False

    @property
    def raw_value(self) -> Any:
        """Return the raw null value for this type's wire format."""
        return NULL_VALUES[self.type_code]


def is_null(value: Any, type_code: QTypeCode) -> bool:
    """Check if a value represents q null for the given type code."""
    if isinstance(value, QNull):
        return True
    null_val = NULL_VALUES.get(type_code)
    if null_val is None:
        return False
    if isinstance(null_val, float) and math.isnan(null_val):
        return isinstance(value, float) and math.isnan(value)
    return value == null_val
