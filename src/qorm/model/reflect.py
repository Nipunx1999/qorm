"""Dynamic model creation from kdb+ table metadata (``meta`` output)."""

from __future__ import annotations

from typing import Any

from ..exc import ReflectionError
from ..protocol.constants import QTypeCode
from .base import Model, ModelMeta
from .keyed import KeyedModel
from .fields import Field
from ..types.base import get_type_by_code

# ── q type char → QTypeCode reverse map ─────────────────────────────
# Lowercase chars = atom vectors, uppercase chars = nested vectors (lists of vectors)
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

# Uppercase type chars represent vector-of-vector columns (e.g., 'C' = list of strings,
# 'J' = list of long vectors). We map them to MIXED_LIST since Python represents them as lists.
_UPPER_CHAR_TO_QTYPE_CODE: dict[str, QTypeCode] = {
    ch.upper(): QTypeCode.MIXED_LIST for ch in _CHAR_TO_QTYPE_CODE if ch != ' '
}


def _resolve_type_char(type_char: str) -> QTypeCode:
    """Resolve a q type character (lower or upper) to a QTypeCode."""
    code = _CHAR_TO_QTYPE_CODE.get(type_char)
    if code is not None:
        return code
    code = _UPPER_CHAR_TO_QTYPE_CODE.get(type_char)
    if code is not None:
        return code
    return None


def _parse_meta_result(meta_data: dict[str, list] | Any) -> list[tuple[str, str]]:
    """Parse the output of q ``meta tablename`` into (column_name, type_char) pairs.

    The q ``meta`` function returns a keyed table with columns:
    - ``c`` : column names (symbol vector)
    - ``t`` : type chars (char vector)
    - ``f`` : foreign key info
    - ``a`` : attributes

    The deserialized form may be either:
    - A flat dict with keys ``c``, ``t``, ``f``, ``a`` (simple table)
    - A keyed table dict with ``keys`` and ``values`` sub-dicts,
      where ``c`` is in ``keys`` and ``t``, ``f``, ``a`` are in ``values``
    """
    if not isinstance(meta_data, dict):
        raise ReflectionError(
            f"Expected dict from meta, got {type(meta_data).__name__}"
        )

    # Handle keyed table format: {'keys': {'c': [...]}, 'values': {'t': '...', ...}}
    if 'keys' in meta_data and 'values' in meta_data:
        keys_part = meta_data['keys']
        values_part = meta_data['values']
        if isinstance(keys_part, dict) and isinstance(values_part, dict):
            # Strip internal markers
            columns = keys_part.get('c')
            type_chars = values_part.get('t')
        else:
            columns = None
            type_chars = None
    else:
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
    key_columns: list[str] | None = None,
) -> type[Model]:
    """Dynamically create a Model class from kdb+ ``meta`` output.

    Parameters
    ----------
    tablename : str
        The kdb+ table name.
    meta_data : dict
        The deserialized result of ``meta tablename``.
    key_columns : list[str] | None
        Optional list of key column names (from ``keys tablename``).
        When provided, the returned class will be a KeyedModel subclass.

    Returns
    -------
    type[Model]
        A dynamically created Model (or KeyedModel) subclass with fields matching the table.
    """
    parsed = _parse_meta_result(meta_data)

    if not parsed:
        raise ReflectionError(f"Table {tablename!r} has no columns")

    key_set = set(key_columns) if key_columns else set()

    fields: dict[str, Field] = {}
    for col_name, type_char in parsed:
        code = _resolve_type_char(type_char)
        if code is None:
            raise ReflectionError(
                f"Unknown q type char {type_char!r} for column {col_name!r} "
                f"in table {tablename!r}"
            )
        qtype = get_type_by_code(code)
        is_key = col_name in key_set
        fld = Field(name=col_name, qtype=qtype, primary_key=is_key)
        fields[col_name] = fld

    # Build the class name: trade -> Trade, daily_price -> DailyPrice
    class_name = ''.join(part.capitalize() for part in tablename.split('_'))

    # Choose base class
    base_class = KeyedModel if key_set else Model
    key_field_names = [c for c in key_columns if c in fields] if key_columns else []

    # Create the Model subclass dynamically, bypassing __init_subclass__
    # annotation processing by pre-setting __fields__
    cls = ModelMeta(class_name, (base_class,), {
        '__tablename__': tablename,
        '__fields__': fields,
        '__key_fields__': key_field_names,
        '__annotations__': {},
    })

    return cls
