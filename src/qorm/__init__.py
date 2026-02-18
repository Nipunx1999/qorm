"""qorm — Python ORM for q/kdb+.

Usage::

    from qorm import Model, Engine, Session, Symbol, Float, Long, Timestamp, avg_, aj

    class Trade(Model):
        __tablename__ = 'trade'
        sym: Symbol
        price: Float
        size: Long
        time: Timestamp

    engine = Engine(host="localhost", port=5000)

    with Session(engine) as session:
        session.create_table(Trade)
        result = session.exec(
            Trade.select(Trade.sym, avg_price=avg_(Trade.price))
                 .where(Trade.price > 100)
                 .by(Trade.sym)
        )
        for row in result:
            print(row.sym, row.avg_price)
"""

from .types import (
    Boolean, Guid, Byte, Short, Int, Long,
    Real, Float, Char, Symbol,
    Timestamp, Month, Date, DateTime,
    Timespan, Minute, Second, Time,
    QType, QTypeCode, QNull,
    q_boolean, q_guid, q_byte, q_short, q_int, q_long,
    q_real, q_float, q_char, q_symbol, q_timestamp, q_month,
    q_date, q_datetime, q_timespan, q_minute, q_second, q_time,
    infer_qtype, is_null,
)
from .model.base import Model
from .model.keyed import KeyedModel
from .model.fields import Field, field
from .model.reflect import build_model_from_meta
from .engine import Engine
from .session import Session, AsyncSession, ModelResultSet
from .registry import EngineRegistry, EngineGroup
from .rpc import QFunction, q_api
from .query.expressions import (
    Expr, Column, Literal, BinOp, AggFunc,
    avg_, sum_, min_, max_, count_, first_, last_, med_, dev_, var_,
)
from .query.select import SelectQuery
from .query.update import UpdateQuery
from .query.delete import DeleteQuery
from .query.insert import InsertQuery
from .query.joins import aj, lj, ij, wj
from .connection.sync_conn import SyncConnection
from .connection.async_conn import AsyncConnection
from .connection.pool import SyncPool, AsyncPool
from .exc import (
    QormError, ConnectionError, HandshakeError, AuthenticationError,
    SerializationError, DeserializationError, QueryError, QError,
    ModelError, SchemaError, PoolError, PoolExhaustedError,
    EngineNotFoundError, ReflectionError,
)

__version__ = "0.1.0"

__all__ = [
    # Core
    'Model', 'KeyedModel', 'Field', 'field',
    'Engine', 'Session', 'AsyncSession', 'ModelResultSet',
    'EngineRegistry', 'EngineGroup',
    'QFunction', 'q_api',
    'build_model_from_meta',
    # Types
    'Boolean', 'Guid', 'Byte', 'Short', 'Int', 'Long',
    'Real', 'Float', 'Char', 'Symbol',
    'Timestamp', 'Month', 'Date', 'DateTime',
    'Timespan', 'Minute', 'Second', 'Time',
    'QType', 'QTypeCode', 'QNull',
    'infer_qtype', 'is_null',
    # Queries
    'Expr', 'Column', 'Literal', 'BinOp', 'AggFunc',
    'SelectQuery', 'UpdateQuery', 'DeleteQuery', 'InsertQuery',
    'avg_', 'sum_', 'min_', 'max_', 'count_', 'first_', 'last_',
    'med_', 'dev_', 'var_',
    'aj', 'lj', 'ij', 'wj',
    # Connections
    'SyncConnection', 'AsyncConnection', 'SyncPool', 'AsyncPool',
    # Exceptions
    'QormError', 'ConnectionError', 'HandshakeError', 'AuthenticationError',
    'SerializationError', 'DeserializationError', 'QueryError', 'QError',
    'ModelError', 'SchemaError', 'PoolError', 'PoolExhaustedError',
    'EngineNotFoundError', 'ReflectionError',
]
