"""Field descriptors with q metadata for model columns."""

from __future__ import annotations

import dataclasses
from typing import Any

from ..types.base import QType
from ..protocol.constants import QTypeCode, ATTR_NONE


@dataclasses.dataclass(slots=True)
class Field:
    """Describes a single column/field in a qorm Model.

    Attributes
    ----------
    name : str
        Python attribute name (set during model registration).
    qtype : QType
        The q type descriptor for this field.
    primary_key : bool
        Whether this field is part of the keyed columns.
    attr : int
        q attribute (`s#, `u#, `p#, `g#, or none).
    default : Any
        Default value for the field (None means no default).
    nullable : bool
        Whether the field accepts QNull values.
    """
    name: str = ''
    qtype: QType | None = None
    primary_key: bool = False
    attr: int = ATTR_NONE
    default: Any = None
    nullable: bool = True

    @property
    def q_name(self) -> str:
        """The column name as used in q (same as Python name)."""
        return self.name

    @property
    def type_code(self) -> QTypeCode:
        """Shortcut to the q type code."""
        return self.qtype.code if self.qtype else QTypeCode.MIXED_LIST

    @property
    def q_type_char(self) -> str:
        """Single-character q type identifier for DDL."""
        _TYPE_CHARS = {
            QTypeCode.MIXED_LIST: ' ',
            QTypeCode.BOOLEAN: 'b',
            QTypeCode.GUID: 'g',
            QTypeCode.BYTE: 'x',
            QTypeCode.SHORT: 'h',
            QTypeCode.INT: 'i',
            QTypeCode.LONG: 'j',
            QTypeCode.REAL: 'e',
            QTypeCode.FLOAT: 'f',
            QTypeCode.CHAR: 'c',
            QTypeCode.SYMBOL: 's',
            QTypeCode.TIMESTAMP: 'p',
            QTypeCode.MONTH: 'm',
            QTypeCode.DATE: 'd',
            QTypeCode.DATETIME: 'z',
            QTypeCode.TIMESPAN: 'n',
            QTypeCode.MINUTE: 'u',
            QTypeCode.SECOND: 'v',
            QTypeCode.TIME: 't',
        }
        return _TYPE_CHARS.get(self.type_code, '*')


def field(
    *,
    primary_key: bool = False,
    attr: int = ATTR_NONE,
    default: Any = None,
    nullable: bool = True,
) -> Any:
    """Create field metadata for use as a default value in model annotations.

    Usage::

        class Trade(Model):
            sym: Symbol = field(attr=ATTR_SORTED)
            price: Float
            size: Long = field(default=0)
    """
    return Field(
        primary_key=primary_key,
        attr=attr,
        default=default,
        nullable=nullable,
    )
