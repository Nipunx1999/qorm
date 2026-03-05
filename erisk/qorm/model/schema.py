"""DDL generation: create/drop/inspect tables in q."""

from __future__ import annotations

from typing import Any, TYPE_CHECKING

from ..protocol.constants import ATTR_NONE

if TYPE_CHECKING:
    from .base import Model


def _attr_prefix(attr: int) -> str:
    """Return q attribute prefix string."""
    _ATTR_MAP = {
        0: '',
        1: '`s#',
        2: '`u#',
        3: '`p#',
        5: '`g#',
    }
    return _ATTR_MAP.get(attr, '')


def create_table_q(model: type[Model]) -> str:
    """Generate the q expression to create a table from a Model class.

    Returns a string like:
        trade:([] sym:`symbol$(); price:`float$(); size:`long$(); time:`timestamp$())

    For keyed models:
        daily_price:([sym:`symbol$()] close:`float$(); volume:`long$())
    """
    tablename = model.__tablename__
    fields = model.__fields__
    key_fields = getattr(model, '__key_fields__', [])

    key_parts = []
    val_parts = []

    for name, fld in fields.items():
        attr_str = _attr_prefix(fld.attr)
        type_char = fld.q_type_char
        if type_char == ' ':
            # Mixed/general list column — use empty list literal
            col_def = f"{name}:{attr_str}()"
        else:
            col_def = f"{name}:{attr_str}`{type_char}$()"

        if name in key_fields:
            key_parts.append(col_def)
        else:
            val_parts.append(col_def)

    if key_parts:
        key_section = f"[{'; '.join(key_parts)}]"
    else:
        key_section = "[]"

    val_section = '; '.join(val_parts)

    return f"{tablename}:({key_section} {val_section})"


def drop_table_q(model: type[Model]) -> str:
    """Generate q expression to drop (delete) a table."""
    return f"delete {model.__tablename__} from `."


def table_meta_q(model: type[Model]) -> str:
    """Generate q expression to get table metadata."""
    return f"meta {model.__tablename__}"


def table_count_q(model: type[Model]) -> str:
    """Generate q expression to count table rows."""
    return f"count {model.__tablename__}"


def table_exists_q(model: type[Model]) -> str:
    """Generate q expression to check if a table exists."""
    return f"`{model.__tablename__} in tables[]"
