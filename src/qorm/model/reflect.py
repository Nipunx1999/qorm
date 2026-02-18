"""Dynamic model creation from kdb+ table metadata (``meta`` output)."""

from __future__ import annotations

from typing import Any

from ..exc import ReflectionError
from ..protocol.constants import QTypeCode
from .base import Model, ModelMeta
from .fields import Field
from ..types.base import get_type_by_code

# ── q type char → QTypeCode reverse map ─────────────────────────────
_CHAR_TO_QTYPE_CODE: dict[str, QTypeCode] = {
    ' ': QTypeCode.MIXED_LIST,   # nested / mixed list column
    'b': QTypeCode.BOOLEAN,
    'g': QTypeCode.GUID,
    'x': QTypeCode.BYTE,
    'h': QTypeCode.SHORT,
    'i': QTypeCode.INT,
    'j': QTypeCode.LONG,
    'e': QTypeCode.REAL,
    'f': QTypeCode.FLOAT,
    'c': QTypeCode.CHAR,
    's': QTypeCode.SYMBOL,
    'p': QTypeCode.TIMESTAMP,
    'm': QTypeCode.MONTH,
    'd': QTypeCode.DATE,
    'z': QTypeCode.DATETIME,
    'n': QTypeCode.TIMESPAN,
    'u': QTypeCode.MINUTE,
    'v': QTypeCode.SECOND,
    't': QTypeCode.TIME,
}


def _parse_meta_result(meta_data: dict[str, list] | Any) -> list[tuple[str, str]]:
    """Parse the output of q ``meta tablename`` into (column_name, type_char) pairs.

    The q ``meta`` function returns a keyed table with columns:
    - ``c`` : column names (symbol vector)
    - ``t`` : type chars (char vector)
    - ``f`` : foreign key info
    - ``a`` : attributes

    The deserialized form is a dict with these keys.
    """
    if not isinstance(meta_data, dict):
        raise ReflectionError(
            f"Expected dict from meta, got {type(meta_data).__name__}"
        )

    columns = meta_data.get('c')
    type_chars = meta_data.get('t')

    if columns is None or type_chars is None:
        raise ReflectionError(
            f"meta result missing 'c' or 't' keys. Got keys: {list(meta_data.keys())}"
        )

    if len(columns) != len(type_chars):
        raise ReflectionError(
            f"Column count ({len(columns)}) != type char count ({len(type_chars)})"
        )

    result = []
    for col, tchar in zip(columns, type_chars):
        col_str = col if isinstance(col, str) else str(col)
        tchar_str = tchar if isinstance(tchar, str) else chr(tchar) if isinstance(tchar, int) else str(tchar)
        result.append((col_str, tchar_str))

    return result


def build_model_from_meta(
    tablename: str,
    meta_data: dict[str, list] | Any,
) -> type[Model]:
    """Dynamically create a Model class from kdb+ ``meta`` output.

    Parameters
    ----------
    tablename : str
        The kdb+ table name.
    meta_data : dict
        The deserialized result of ``meta tablename``.

    Returns
    -------
    type[Model]
        A dynamically created Model subclass with fields matching the table.
    """
    parsed = _parse_meta_result(meta_data)

    if not parsed:
        raise ReflectionError(f"Table {tablename!r} has no columns")

    fields: dict[str, Field] = {}
    for col_name, type_char in parsed:
        code = _CHAR_TO_QTYPE_CODE.get(type_char)
        if code is None:
            raise ReflectionError(
                f"Unknown q type char {type_char!r} for column {col_name!r} "
                f"in table {tablename!r}"
            )
        qtype = get_type_by_code(code)
        fld = Field(name=col_name, qtype=qtype)
        fields[col_name] = fld

    # Build the class name: trade -> Trade, daily_price -> DailyPrice
    class_name = ''.join(part.capitalize() for part in tablename.split('_'))

    # Create the Model subclass dynamically, bypassing __init_subclass__
    # annotation processing by pre-setting __fields__
    cls = ModelMeta(class_name, (Model,), {
        '__tablename__': tablename,
        '__fields__': fields,
        '__key_fields__': [],
        '__annotations__': {},
    })

    return cls
