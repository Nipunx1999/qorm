"""Core type system: QType descriptor, type registry."""

from __future__ import annotations

import dataclasses
from typing import Any

from ..protocol.constants import QTypeCode


@dataclasses.dataclass(frozen=True, slots=True)
class QType:
    """Describes a q type for use in model field annotations.

    Parameters
    ----------
    code : QTypeCode
        The q type byte code.
    name : str
        Human-readable q type name (e.g. "symbol", "float").
    python_type : type
        The canonical Python type used to represent values of this q type.
    size : int
        Byte width for fixed-size types, 0 for variable-width (symbol).
    """
    code: QTypeCode
    name: str
    python_type: type
    size: int = 0


# ── Global type registry ──────────────────────────────────────────
_REGISTRY_BY_CODE: dict[QTypeCode, QType] = {}
_REGISTRY_BY_NAME: dict[str, QType] = {}


def register_type(qtype: QType) -> QType:
    """Register a QType in the global registry."""
    _REGISTRY_BY_CODE[qtype.code] = qtype
    _REGISTRY_BY_NAME[qtype.name] = qtype
    return qtype


def get_type_by_code(code: QTypeCode | int) -> QType:
    """Look up a QType by its type code."""
    return _REGISTRY_BY_CODE[QTypeCode(code)]


def get_type_by_name(name: str) -> QType:
    """Look up a QType by its q name."""
    return _REGISTRY_BY_NAME[name]


def all_types() -> list[QType]:
    """Return all registered QTypes."""
    return list(_REGISTRY_BY_CODE.values())
