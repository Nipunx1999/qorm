"""InsertQuery: batch insert with row-to-column transpose."""

from __future__ import annotations

import datetime
import uuid
from typing import Any, TYPE_CHECKING

from ..types.nulls import QNull
from ..types.temporal import (
    datetime_to_timestamp, python_to_date, python_to_month,
    python_to_datetime, timedelta_to_timespan, time_to_minute,
    time_to_second, python_to_time,
)
from ..protocol.constants import QTypeCode
from .expressions import Expr

if TYPE_CHECKING:
    from ..model.base import Model


class InsertQuery:
    """Batch insert query builder.

    Transposes row-oriented model instances into column-oriented data
    for efficient kdb+ insertion.

    Usage::

        query = Trade.insert([
            Trade(sym="AAPL", price=150.25, size=100, time=now),
            Trade(sym="GOOG", price=2800.0, size=50, time=now),
        ])
    """

    def __init__(self, model: type[Model], rows: list[Any]) -> None:
        self.model = model
        self.rows = rows

    def compile(self) -> str:
        """Compile to a q insert expression.

        Returns something like:
            `trade insert ((`AAPL;`GOOG);(150.25 2800.0);(100 50);(...))
        """
        tablename = self.model.__tablename__
        fields = self.model.__fields__

        if not self.rows:
            return f'`{tablename} insert ()'

        # Transpose: row-oriented -> column-oriented
        columns: dict[str, list[Any]] = {name: [] for name in fields}
        for row in self.rows:
            for name in fields:
                val = getattr(row, name, None)
                columns[name].append(val)

        # Build q column vectors
        col_strs = []
        for name, values in columns.items():
            fld = fields[name]
            col_str = _compile_column_vector(values, fld.type_code)
            col_strs.append(col_str)

        cols_q = ';'.join(col_strs)
        return f'`{tablename} insert ({cols_q})'

    def __repr__(self) -> str:
        return f"InsertQuery({self.model.__tablename__}, {len(self.rows)} rows)"


def _compile_column_vector(values: list[Any], type_code: QTypeCode) -> str:
    """Compile a list of Python values into a q vector literal."""
    if type_code == QTypeCode.SYMBOL:
        syms = [f'`{_q_escape(v)}' if v is not None else '`' for v in values]
        return '(' + ';'.join(syms) + ')' if len(values) > 1 else syms[0]

    if type_code == QTypeCode.FLOAT:
        parts = [_format_float(v) for v in values]
        return ' '.join(parts)

    if type_code == QTypeCode.LONG:
        parts = [_format_long(v) for v in values]
        return ' '.join(parts)

    if type_code == QTypeCode.INT:
        parts = [f'{v}i' if v is not None else '0Ni' for v in values]
        return ' '.join(parts)

    if type_code == QTypeCode.SHORT:
        parts = [f'{v}h' if v is not None else '0Nh' for v in values]
        return ' '.join(parts)

    if type_code == QTypeCode.BOOLEAN:
        bits = ''.join('1' if v else '0' for v in values)
        return f'{bits}b'

    if type_code == QTypeCode.TIMESTAMP:
        parts = []
        for v in values:
            if v is None or isinstance(v, QNull):
                parts.append('0Np')
            elif isinstance(v, datetime.datetime):
                parts.append(f'{v.isoformat()}')
            else:
                parts.append(str(v))
        return '(' + ';'.join(parts) + ')'

    if type_code == QTypeCode.DATE:
        parts = []
        for v in values:
            if v is None or isinstance(v, QNull):
                parts.append('0Nd')
            elif isinstance(v, datetime.date):
                parts.append(f'{v.isoformat()}')
            else:
                parts.append(str(v))
        return '(' + ';'.join(parts) + ')'

    if type_code == QTypeCode.TIME:
        parts = []
        for v in values:
            if v is None or isinstance(v, QNull):
                parts.append('0Nt')
            elif isinstance(v, datetime.time):
                parts.append(f'{v.isoformat()}')
            else:
                parts.append(str(v))
        return '(' + ';'.join(parts) + ')'

    if type_code == QTypeCode.CHAR:
        chars = ''.join(v if v else ' ' for v in values)
        return f'"{chars}"'

    if type_code == QTypeCode.GUID:
        parts = []
        for v in values:
            if v is None or isinstance(v, QNull):
                parts.append('0Ng')
            elif isinstance(v, uuid.UUID):
                parts.append(f'"{v}"')
            else:
                parts.append(str(v))
        return '(' + ';'.join(parts) + ')'

    # Fallback: generic list
    parts = [_compile_value(v) for v in values]
    return '(' + ';'.join(parts) + ')'


def _format_float(v: Any) -> str:
    if v is None or isinstance(v, QNull):
        return '0n'
    return f'{v}f' if isinstance(v, (int, float)) else str(v)


def _format_long(v: Any) -> str:
    if v is None or isinstance(v, QNull):
        return '0N'
    return str(v)


def _q_escape(s: str) -> str:
    """Escape a string for use as a q symbol."""
    return s.replace('`', '')


def _compile_value(v: Any) -> str:
    """Compile a single Python value to q literal."""
    if v is None or isinstance(v, QNull):
        return '(::)'
    if isinstance(v, bool):
        return '1b' if v else '0b'
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        return f'{v}f'
    if isinstance(v, str):
        return f'`{v}'
    return str(v)
