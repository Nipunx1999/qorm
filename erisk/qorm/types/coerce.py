"""Python annotation -> QType inference.

Maps Python type hints (including Annotated[...]) to the appropriate QType.
"""

from __future__ import annotations

import datetime
import uuid
from typing import Any, get_args, get_origin, Annotated

from ..protocol.constants import QTypeCode
from .base import QType, get_type_by_code
from . import atoms as _atoms  # ensure atoms are registered


# ── Python type -> default QType code mapping ──────────────────────
_PYTHON_TO_QTYPE: dict[type, QTypeCode] = {
    bool: QTypeCode.BOOLEAN,
    int: QTypeCode.LONG,
    float: QTypeCode.FLOAT,
    str: QTypeCode.SYMBOL,       # default string -> symbol
    bytes: QTypeCode.BYTE,
    list: QTypeCode.MIXED_LIST,  # nested / mixed list column
    datetime.datetime: QTypeCode.TIMESTAMP,
    datetime.date: QTypeCode.DATE,
    datetime.time: QTypeCode.TIME,
    datetime.timedelta: QTypeCode.TIMESPAN,
    uuid.UUID: QTypeCode.GUID,
}


def infer_qtype(annotation: Any) -> QType:
    """Infer the QType from a Python type annotation.

    Supports:
    - Plain types: ``int``, ``float``, ``str``, etc.
    - ``Annotated[python_type, QType(...)]``: explicit q type metadata.

    Parameters
    ----------
    annotation : Any
        A Python type annotation.

    Returns
    -------
    QType
        The inferred q type descriptor.

    Raises
    ------
    TypeError
        If the annotation cannot be mapped to a q type.
    """
    # Check for Annotated[X, QType]
    origin = get_origin(annotation)
    if origin is Annotated:
        args = get_args(annotation)
        for arg in args[1:]:
            if isinstance(arg, QType):
                return arg
        # Fall through to infer from the base type
        annotation = args[0]

    # Direct type lookup
    if isinstance(annotation, type):
        code = _PYTHON_TO_QTYPE.get(annotation)
        if code is not None:
            return get_type_by_code(code)

    raise TypeError(
        f"Cannot infer q type from annotation: {annotation!r}. "
        f"Use Annotated[type, QType(...)] for explicit mapping."
    )
