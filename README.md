# qorm

**A modern Python ORM for q/kdb+.**

qorm brings the declarative, model-based workflow popularised by SQLAlchemy to the world of kdb+. Define tables as Python classes, build queries with a chainable API, and let the ORM handle serialisation, type mapping, and IPC transport — in both sync and async flavours.

```python
from qorm import Model, Engine, Session, Symbol, Float, Long, Timestamp, avg_

class Trade(Model):
    __tablename__ = "trade"
    sym: Symbol
    price: Float
    size: Long
    time: Timestamp

engine = Engine(host="localhost", port=5000)

with Session(engine) as s:
    s.create_table(Trade)
    s.exec(Trade.insert([
        Trade(sym="AAPL", price=150.25, size=100, time=datetime.now()),
        Trade(sym="GOOG", price=2800.0,  size=50,  time=datetime.now()),
    ]))

    result = s.exec(
        Trade.select(Trade.sym, avg_price=avg_(Trade.price))
             .where(Trade.price > 100)
             .by(Trade.sym)
    )
    for row in result:
        print(row.sym, row.avg_price)
```

---

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Type System](#type-system)
  - [Type Aliases](#type-aliases)
  - [Plain Python Types](#plain-python-types)
  - [Null Handling](#null-handling)
- [Models](#models)
  - [Defining a Model](#defining-a-model)
  - [Model Instances](#model-instances)
  - [Field Options](#field-options)
  - [Keyed Models](#keyed-models)
- [Connections](#connections)
  - [Synchronous Connection](#synchronous-connection)
  - [Asynchronous Connection](#asynchronous-connection)
- [Engine](#engine)
  - [Constructor](#constructor)
  - [DSN Strings](#dsn-strings)
- [Sessions](#sessions)
  - [Synchronous Session](#synchronous-session)
  - [Asynchronous Session](#asynchronous-session)
  - [Raw Queries](#raw-queries)
- [Query Builder](#query-builder)
  - [Select](#select)
  - [Where Clauses](#where-clauses)
  - [Group By](#group-by)
  - [Aggregates](#aggregates)
  - [Limit](#limit)
  - [Update](#update)
  - [Delete](#delete)
  - [Insert](#insert)
- [Expressions](#expressions)
  - [Column References](#column-references)
  - [Comparison Operators](#comparison-operators)
  - [Arithmetic Operators](#arithmetic-operators)
  - [Logical Operators](#logical-operators)
  - [Built-in Methods](#built-in-methods)
- [Joins](#joins)
  - [As-of Join (aj)](#as-of-join-aj)
  - [Left Join (lj)](#left-join-lj)
  - [Inner Join (ij)](#inner-join-ij)
  - [Window Join (wj)](#window-join-wj)
- [Result Sets](#result-sets)
  - [Iterating Rows](#iterating-rows)
  - [Column Access](#column-access)
  - [DataFrame Export](#dataframe-export)
- [Table Reflection](#table-reflection)
  - [Listing Tables](#listing-tables)
  - [Reflecting a Table](#reflecting-a-table)
  - [Reflecting All Tables](#reflecting-all-tables)
  - [Using Reflected Models](#using-reflected-models)
- [Remote Function Calls (RPC)](#remote-function-calls-rpc)
  - [Ad-hoc Calls](#ad-hoc-calls)
  - [QFunction Wrapper](#qfunction-wrapper)
  - [Typed Decorator (q_api)](#typed-decorator-q_api)
- [Multi-Instance Registry](#multi-instance-registry)
  - [EngineRegistry](#engineregistry)
  - [EngineGroup](#enginegroup)
  - [Configuration Methods](#configuration-methods)
- [Connection Pools](#connection-pools)
  - [Sync Pool](#sync-pool)
  - [Async Pool](#async-pool)
- [Schema Management](#schema-management)
- [Error Handling](#error-handling)
- [Testing Your Code](#testing-your-code)
- [API Reference](#api-reference)

---

## Installation

```bash
pip install qorm
```

With optional pandas support for DataFrame export:

```bash
pip install qorm[pandas]
```

For development (includes pytest):

```bash
pip install qorm[dev]
```

**Requirements:** Python 3.10+. No other dependencies — qorm is pure Python.

---

## Quick Start

### 1. Define a model

```python
from qorm import Model, Symbol, Float, Long, Timestamp

class Trade(Model):
    __tablename__ = "trade"
    sym: Symbol
    price: Float
    size: Long
    time: Timestamp
```

### 2. Connect and create the table

```python
from qorm import Engine, Session

engine = Engine(host="localhost", port=5000)

with Session(engine) as s:
    s.create_table(Trade)
```

### 3. Insert data

```python
from datetime import datetime

trades = [
    Trade(sym="AAPL", price=150.25, size=100, time=datetime.now()),
    Trade(sym="GOOG", price=2800.0,  size=50,  time=datetime.now()),
]

with Session(engine) as s:
    s.exec(Trade.insert(trades))
```

### 4. Query data

```python
from qorm import avg_

with Session(engine) as s:
    result = s.exec(
        Trade.select(Trade.sym, avg_price=avg_(Trade.price))
             .where(Trade.price > 100)
             .by(Trade.sym)
    )
    for row in result:
        print(row.sym, row.avg_price)
```

### 5. Raw q fallback

```python
with Session(engine) as s:
    result = s.raw("select count i by sym from trade")
```

---

## Type System

qorm maps every q type to a Python type alias that carries metadata via `typing.Annotated`. The model layer reads this metadata to generate correct DDL and serialise values over IPC.

### Type Aliases

Use these in model annotations. Each alias encodes both the Python representation type and the q wire type.

| qorm alias    | Python type          | q type      | q type char | q type code |
|---------------|----------------------|-------------|-------------|-------------|
| `Boolean`     | `bool`               | boolean     | `b`         | 1           |
| `Guid`        | `uuid.UUID`          | guid        | `g`         | 2           |
| `Byte`        | `int`                | byte        | `x`         | 4           |
| `Short`       | `int`                | short       | `h`         | 5           |
| `Int`         | `int`                | int         | `i`         | 6           |
| `Long`        | `int`                | long        | `j`         | 7           |
| `Real`        | `float`              | real        | `e`         | 8           |
| `Float`       | `float`              | float       | `f`         | 9           |
| `Char`        | `str`                | char        | `c`         | 10          |
| `Symbol`      | `str`                | symbol      | `s`         | 11          |
| `Timestamp`   | `datetime.datetime`  | timestamp   | `p`         | 12          |
| `Month`       | `datetime.date`      | month       | `m`         | 13          |
| `Date`        | `datetime.date`      | date        | `d`         | 14          |
| `DateTime`    | `datetime.datetime`  | datetime    | `z`         | 15          |
| `Timespan`    | `datetime.timedelta` | timespan    | `n`         | 16          |
| `Minute`      | `datetime.time`      | minute      | `u`         | 17          |
| `Second`      | `datetime.time`      | second      | `v`         | 18          |
| `Time`        | `datetime.time`      | time        | `t`         | 19          |

```python
from qorm import Symbol, Float, Long, Timestamp, Date, Guid
```

### Plain Python Types

If you use a plain Python type instead of a qorm alias, the ORM infers a default q type:

| Python type          | Default q type |
|----------------------|----------------|
| `bool`               | boolean        |
| `int`                | long           |
| `float`              | float          |
| `str`                | symbol         |
| `bytes`              | byte           |
| `datetime.datetime`  | timestamp      |
| `datetime.date`      | date           |
| `datetime.time`      | time           |
| `datetime.timedelta` | timespan       |
| `uuid.UUID`          | guid           |

```python
# These two are equivalent:
class Trade(Model):
    __tablename__ = "trade"
    sym: str          # inferred as symbol
    price: float      # inferred as float

class Trade(Model):
    __tablename__ = "trade"
    sym: Symbol       # explicit symbol
    price: Float      # explicit float
```

Use the explicit aliases when you need a specific q type that differs from the default (e.g. `Short` or `Int` instead of the default `Long` for `int`).

### Null Handling

q has typed nulls — a long null and a date null are different values. qorm preserves this with `QNull`:

```python
from qorm import QNull, QTypeCode, is_null

# Create a typed null
null_price = QNull(QTypeCode.FLOAT)
null_date  = QNull(QTypeCode.DATE)

# They are distinguishable
null_price == null_date  # False

# Check if a value is null
is_null(null_price, QTypeCode.FLOAT)  # True
is_null(42, QTypeCode.LONG)           # False

# QNull is falsy
if not null_price:
    print("it's null")
```

When the deserialiser encounters a q null value (e.g. `0N`, `0Nd`), it returns a `QNull` with the appropriate type code rather than Python `None`. This ensures correct round-trip serialisation.

---

## Models

### Defining a Model

Subclass `Model` and add type-annotated fields. The `__tablename__` class variable sets the q table name.

```python
from qorm import Model, Symbol, Float, Long, Timestamp

class Trade(Model):
    __tablename__ = "trade"
    sym: Symbol
    price: Float
    size: Long
    time: Timestamp
```

The `__init_subclass__` hook automatically:
- Introspects annotations to build `Field` descriptors
- Infers the q type for each field
- Registers the model in a global registry (used for result mapping)

### Model Instances

Models generate `__init__`, `__repr__`, and `__eq__` automatically:

```python
t = Trade(sym="AAPL", price=150.25, size=100, time=datetime.now())

print(t)          # Trade(sym='AAPL', price=150.25, size=100, time=...)
print(t.sym)      # AAPL
print(t.to_dict()) # {'sym': 'AAPL', 'price': 150.25, ...}

# Create from dict
t2 = Trade.from_dict({"sym": "GOOG", "price": 2800.0, "size": 50})

# Equality
t == t2  # False
```

Unspecified fields default to `None`:

```python
t = Trade(sym="AAPL")
print(t.price)  # None
```

### Field Options

Use `field()` to set metadata on individual columns:

```python
from qorm import Model, Symbol, Float, Long, field
from qorm.protocol.constants import ATTR_SORTED

class Trade(Model):
    __tablename__ = "trade"
    sym: Symbol = field(attr=ATTR_SORTED)       # `s# attribute
    price: Float
    size: Long = field(default=0)               # default value
    active: Long = field(nullable=False)        # not nullable
```

**Parameters:**

| Parameter     | Type   | Default      | Description                           |
|---------------|--------|--------------|---------------------------------------|
| `primary_key` | `bool` | `False`      | Mark as key column (for keyed tables) |
| `attr`        | `int`  | `ATTR_NONE`  | q vector attribute (`s#`, `u#`, etc.) |
| `default`     | `Any`  | `None`       | Default value for new instances       |
| `nullable`    | `bool` | `True`       | Whether the field accepts nulls       |

**Available attributes:**

```python
from qorm.protocol.constants import ATTR_NONE, ATTR_SORTED, ATTR_UNIQUE, ATTR_PARTED, ATTR_GROUPED

# ATTR_NONE    = 0  (no attribute)
# ATTR_SORTED  = 1  (`s#)
# ATTR_UNIQUE  = 2  (`u#)
# ATTR_PARTED  = 3  (`p#)
# ATTR_GROUPED = 5  (`g#)
```

### Keyed Models

Use `KeyedModel` for keyed tables. Mark key columns with `field(primary_key=True)`:

```python
from qorm import KeyedModel, Symbol, Date, Float, Long, field

class DailyPrice(KeyedModel):
    __tablename__ = "daily_price"
    sym: Symbol = field(primary_key=True)
    date: Date = field(primary_key=True)
    close: Float
    volume: Long
```

This generates the keyed table DDL:

```q
daily_price:([sym:`s$(); date:`d$()] close:`f$(); volume:`j$())
```

Utility methods:

```python
DailyPrice.key_columns()    # ['sym', 'date']
DailyPrice.value_columns()  # ['close', 'volume']
```

---

## Connections

### Synchronous Connection

For direct, low-level access:

```python
from qorm import SyncConnection

conn = SyncConnection(host="localhost", port=5000)
conn.open()

result = conn.query("select from trade")
result = conn.query("select from trade where sym=`AAPL")

conn.close()
```

Or as a context manager:

```python
with SyncConnection(host="localhost", port=5000) as conn:
    result = conn.query("2+3")
    print(result)  # 5
```

**Constructor parameters:**

| Parameter  | Type             | Default       | Description                |
|------------|------------------|---------------|----------------------------|
| `host`     | `str`            | `"localhost"` | kdb+ host                  |
| `port`     | `int`            | `5000`        | kdb+ port                  |
| `username` | `str`            | `""`          | Authentication username    |
| `password` | `str`            | `""`          | Authentication password    |
| `timeout`  | `float \| None`  | `None`        | Socket timeout in seconds  |

### Asynchronous Connection

```python
import asyncio
from qorm import AsyncConnection

async def main():
    conn = AsyncConnection(host="localhost", port=5000)
    await conn.open()
    result = await conn.query("select from trade")
    await conn.close()

asyncio.run(main())
```

Or as an async context manager:

```python
async with AsyncConnection(host="localhost", port=5000) as conn:
    result = await conn.query("2+3")
```

---

## Engine

The `Engine` is the central configuration point. It stores connection parameters and acts as a factory for connections and sessions.

### Constructor

```python
from qorm import Engine

engine = Engine(
    host="localhost",
    port=5000,
    username="user",
    password="pass",
    timeout=30.0,
)
```

### DSN Strings

Parse a connection string:

```python
engine = Engine.from_dsn("kdb://user:pass@localhost:5000")
engine = Engine.from_dsn("kdb://localhost:5000")          # no auth
```

**Format:** `kdb://[user:pass@]host:port`

### Creating Connections

```python
sync_conn  = engine.connect()         # SyncConnection
async_conn = engine.async_connect()   # AsyncConnection
```

---

## Sessions

Sessions wrap a connection and provide the high-level ORM interface.

### Synchronous Session

```python
from qorm import Session

with Session(engine) as s:
    # Execute ORM queries
    result = s.exec(Trade.select().where(Trade.price > 100))

    # Raw q expressions
    result = s.raw("select from trade")

    # DDL operations
    s.create_table(Trade)
    s.drop_table(Trade)
    exists = s.table_exists(Trade)
```

### Asynchronous Session

```python
from qorm import AsyncSession

async with AsyncSession(engine) as s:
    result = await s.exec(Trade.select())
    result = await s.raw("select from trade")
    await s.create_table(Trade)
```

### Raw Queries

When the query builder doesn't cover your use case, fall back to raw q:

```python
with Session(engine) as s:
    # Simple expression
    s.raw("2+3")

    # Table query
    s.raw("select count i by sym from trade")

    # With arguments (sent as a q function call)
    s.raw("{select from trade where sym=x}", "AAPL")

    # System commands
    s.raw("\\t select from trade")
```

---

## Query Builder

All queries compile to q functional form (`?[t;c;b;a]` for select, `![t;c;b;a]` for update/delete). You can inspect the compiled q at any time with `.compile()`.

### Select

```python
# Select all columns
Trade.select()

# Select specific columns
Trade.select(Trade.sym, Trade.price)

# Select with aliases (named columns)
Trade.select(avg_price=avg_(Trade.price))

# Combine positional and named
Trade.select(Trade.sym, avg_price=avg_(Trade.price))
```

Inspect the compiled q:

```python
query = Trade.select(Trade.sym).where(Trade.price > 100)
print(query.compile())
# ?[trade;enlist ((price>100));0b;([] sym:sym)]
```

### Where Clauses

Chain `.where()` calls — multiple conditions are ANDed:

```python
Trade.select().where(Trade.price > 100)
Trade.select().where(Trade.price > 100).where(Trade.size > 50)
Trade.select().where(Trade.price > 100, Trade.size > 50)  # same thing
```

### Group By

```python
Trade.select(Trade.sym, avg_price=avg_(Trade.price)).by(Trade.sym)

# Multiple group-by columns
Trade.select(total=sum_(Trade.size)).by(Trade.sym, Trade.date)
```

### Aggregates

qorm provides these aggregate functions:

| Function    | q equivalent | Description             |
|-------------|--------------|-------------------------|
| `avg_(col)` | `avg`        | Average                 |
| `sum_(col)` | `sum`        | Sum                     |
| `min_(col)` | `min`        | Minimum                 |
| `max_(col)` | `max`        | Maximum                 |
| `count_()`  | `count i`    | Count rows              |
| `count_(c)` | `count`      | Count non-null in column|
| `first_(c)` | `first`      | First value             |
| `last_(c)`  | `last`       | Last value              |
| `med_(col)` | `med`        | Median                  |
| `dev_(col)` | `dev`        | Standard deviation      |
| `var_(col)` | `var`        | Variance                |

```python
from qorm import avg_, sum_, min_, max_, count_, first_, last_

Trade.select(
    Trade.sym,
    avg_price=avg_(Trade.price),
    total_size=sum_(Trade.size),
    trade_count=count_(),
    high=max_(Trade.price),
    low=min_(Trade.price),
).by(Trade.sym)
```

### Limit

```python
Trade.select().limit(10)    # first 10 rows
Trade.select().where(Trade.price > 100).limit(5)
```

### Update

```python
# Set a literal value
Trade.update().set(price=100.0)

# Set an expression
Trade.update().set(price=Trade.price * 1.1)

# With conditions
Trade.update().set(price=Trade.price * 1.1).where(Trade.sym == "AAPL")

# Multiple assignments
Trade.update().set(price=100.0, size=50)

# With group-by
Trade.update().set(price=avg_(Trade.price)).by(Trade.sym)
```

### Delete

```python
# Delete rows matching a condition
Trade.delete().where(Trade.sym == "AAPL")

# Delete specific columns from a table (rare)
Trade.delete().columns("price", "size")
```

### Insert

Insert takes a list of model instances and transposes them into column-oriented data for efficient kdb+ ingestion:

```python
from datetime import datetime

trades = [
    Trade(sym="AAPL", price=150.25, size=100, time=datetime.now()),
    Trade(sym="GOOG", price=2800.0,  size=50,  time=datetime.now()),
    Trade(sym="MSFT", price=380.0,   size=75,  time=datetime.now()),
]

Trade.insert(trades)
# Compiles to: `trade insert ((`AAPL;`GOOG;`MSFT);150.25 2800.0 380.0;...)
```

Execute with a session:

```python
with Session(engine) as s:
    s.exec(Trade.insert(trades))
```

---

## Expressions

The expression tree supports operator overloading so that Python comparisons produce q-compilable AST nodes.

### Column References

Access model columns as expression objects via class attributes:

```python
Trade.sym     # Column('sym')
Trade.price   # Column('price')
```

### Comparison Operators

| Python          | q      | Example                          |
|-----------------|--------|----------------------------------|
| `col > val`     | `>`    | `Trade.price > 100`              |
| `col >= val`    | `>=`   | `Trade.price >= 100`             |
| `col < val`     | `<`    | `Trade.price < 200`              |
| `col <= val`    | `<=`   | `Trade.price <= 200`             |
| `col == val`    | `=`    | `Trade.sym == "AAPL"`            |
| `col != val`    | `<>`   | `Trade.sym != "AAPL"`            |

### Arithmetic Operators

| Python          | q      | Example                          |
|-----------------|--------|----------------------------------|
| `col + val`     | `+`    | `Trade.price + 10`               |
| `col - val`     | `-`    | `Trade.price - 5`                |
| `col * val`     | `*`    | `Trade.price * 1.1`              |
| `col / val`     | `%`    | `Trade.price / 2`  (q uses `%`)  |
| `col % val`     | `mod`  | `Trade.size % 10`                |
| `-col`          | `neg`  | `-Trade.price`                   |

### Logical Operators

| Python          | q      | Example                          |
|-----------------|--------|----------------------------------|
| `a & b`         | `&`    | `(Trade.price > 100) & (Trade.size > 50)` |
| `a \| b`        | `\|`   | `(Trade.sym == "AAPL") \| (Trade.sym == "GOOG")` |
| `~expr`         | `not`  | `~(Trade.price > 100)`           |

### Built-in Methods

```python
# within — range check
Trade.price.within(100, 200)     # price within (100; 200)

# like — pattern matching
Trade.sym.like("A*")             # sym like "A*"

# in_ — membership
Trade.sym.in_(["AAPL", "GOOG"])  # sym in (`AAPL;`GOOG)

# asc / desc — sorting
Trade.price.asc()
Trade.price.desc()
```

---

## Joins

qorm supports all four q join types. Each takes a list of join columns, a left table, and a right table.

### As-of Join (aj)

The most common join in kdb+ — matches each left row with the most recent right row by time:

```python
from qorm import aj

class Quote(Model):
    __tablename__ = "quote"
    sym: Symbol
    bid: Float
    ask: Float
    time: Timestamp

# Join trades with most recent quotes
query = aj([Trade.sym, Trade.time], Trade, Quote)
# Compiles to: aj[`sym`time;trade;quote]

with Session(engine) as s:
    result = s.exec(query)
```

You can also pass column names as strings:

```python
aj(["sym", "time"], Trade, Quote)
```

### Left Join (lj)

```python
from qorm import lj

query = lj([Trade.sym], Trade, Quote)
# Compiles to: trade lj `sym xkey quote

with Session(engine) as s:
    result = s.exec(query)
```

### Inner Join (ij)

```python
from qorm import ij

query = ij([Trade.sym], Trade, Quote)
# Compiles to: trade ij `sym xkey quote
```

### Window Join (wj)

Join within a time window. Useful for aggregating quotes around trade times:

```python
from qorm import wj

query = wj(
    windows=(-2000000000, 0),            # 2-second lookback window (nanos)
    on=[Trade.sym, Trade.time],
    left=Trade,
    right=Quote,
    aggs={"bid": "avg", "ask": "avg"},   # aggregate functions for right cols
)
```

---

## Result Sets

When a session query returns a q table, it is wrapped in a `ModelResultSet` — a lazy, column-oriented container.

### Iterating Rows

```python
result = s.exec(Trade.select())

# Iterate as model instances
for trade in result:
    print(trade.sym, trade.price)

# Length
len(result)  # number of rows

# Index a single row
trade = result[0]
print(trade.sym)
```

### Column Access

Access raw column vectors by name (preserves kdb+'s column-oriented layout):

```python
syms   = result["sym"]     # list of all sym values
prices = result["price"]   # list of all price values

result.columns  # ['sym', 'price', 'size', 'time']
```

### DataFrame Export

Convert to a pandas DataFrame (requires `pip install qorm[pandas]`):

```python
df = result.to_dataframe()
print(df.head())
#     sym    price  size                time
# 0  AAPL   150.25   100 2024-06-15 10:30:00
# 1  GOOG  2800.00    50 2024-06-15 10:30:01
```

Or get the raw column dict:

```python
data = result.to_dict()
# {'sym': ['AAPL', 'GOOG'], 'price': [150.25, 2800.0], ...}
```

---

## Table Reflection

When connecting to existing kdb+ processes, you don't need to pre-define Model classes. qorm can reflect table schemas at runtime.

### Listing Tables

```python
with Session(engine) as s:
    tables = s.tables()
    print(tables)  # ['trade', 'quote', 'order']
```

### Reflecting a Table

`reflect()` queries the kdb+ process with `meta`, parses the column names and types, and builds a fully functional Model class dynamically:

```python
with Session(engine) as s:
    Trade = s.reflect("trade")

    # Trade is now a real Model class with the correct fields
    print(Trade.__fields__)  # {'sym': Field(sym, symbol), 'price': Field(price, float), ...}
```

### Reflecting All Tables

```python
with Session(engine) as s:
    models = s.reflect_all()
    # {'trade': Trade, 'quote': Quote, 'order': Order}

    Trade = models['trade']
    Quote = models['quote']
```

### Using Reflected Models

Reflected models support the full ORM API — select, where, by, aggregates, insert, update, delete, and joins:

```python
with Session(engine) as s:
    Trade = s.reflect("trade")

    # Query with full ORM features
    result = s.exec(
        Trade.select(Trade.sym, avg_price=avg_(Trade.price))
             .where(Trade.price > 100)
             .by(Trade.sym)
    )
    for row in result:
        print(row.sym, row.avg_price)

    df = result.to_dataframe()
```

Reflected models also support instantiation, equality, `to_dict()`, and `repr()`:

```python
t = Trade(sym="AAPL", price=150.0, size=100)
print(t)            # Trade(sym='AAPL', price=150.0, size=100)
print(t.to_dict())  # {'sym': 'AAPL', 'price': 150.0, 'size': 100}
```

Async sessions support the same reflection API:

```python
async with AsyncSession(engine) as s:
    tables = await s.tables()
    Trade = await s.reflect("trade")
    models = await s.reflect_all()
```

---

## Remote Function Calls (RPC)

Call q functions that are already deployed on a kdb+ process without writing raw q strings.

### Ad-hoc Calls

Use `session.call()` to invoke a named q function:

```python
with Session(engine) as s:
    result = s.call("getTradesByDate", "2024.01.15")
    vwap = s.call("calcVWAP", "AAPL", "2024.01.15")
    status = s.call("getStatus")  # no args
```

### QFunction Wrapper

For reusable function references:

```python
from qorm import QFunction

get_trades = QFunction("getTradesByDate")

with Session(engine) as s:
    result = get_trades(s, "2024.01.15")
    result = get_trades(s, "2024.01.16")
```

### Typed Decorator (q_api)

Use `q_api` to document the expected signature of a q function. The function body is never called — all calls are routed through IPC:

```python
from qorm import q_api

@q_api("getTradesByDate")
def get_trades_by_date(session, date: str): ...

@q_api("calcVWAP")
def calc_vwap(session, sym: str, date: str): ...

with Session(engine) as s:
    trades = get_trades_by_date(s, "2024.01.15")
    vwap = calc_vwap(s, "AAPL", "2024.01.15")
```

Async sessions also support `call()`:

```python
async with AsyncSession(engine) as s:
    result = await s.call("getTradesByDate", "2024.01.15")
```

---

## Multi-Instance Registry

Manage connections to multiple kdb+ processes — organized by data domain (equities, FX) and instance type (RDB, HDB, gateway).

### EngineRegistry

A named collection of engines for a single domain:

```python
from qorm import EngineRegistry

equities = EngineRegistry()
equities.register("rdb", Engine(host="eq-rdb", port=5010))
equities.register("hdb", Engine(host="eq-hdb", port=5012))
equities.register("gw",  Engine(host="eq-gw",  port=5000))

# The first registered engine becomes the default
with equities.session() as s:        # uses default (rdb)
    ...

with equities.session("hdb") as s:   # explicit
    Trade = s.reflect("trade")
    result = s.exec(Trade.select().where(Trade.price > 100))
```

Change the default:

```python
equities.set_default("gw")
equities.names     # ['rdb', 'hdb', 'gw']
equities.default   # 'gw'
```

### EngineGroup

A two-level registry — domains containing instances:

```python
from qorm import EngineGroup

group = EngineGroup()
group.register("equities", equities)
group.register("fx", EngineRegistry.from_config({
    "rdb": {"host": "fx-rdb", "port": 5020},
    "hdb": {"host": "fx-hdb", "port": 5022},
}))

with group.session("equities", "rdb") as s:
    result = s.call("getSnapshot", "AAPL")

with group.session("fx", "hdb") as s:
    result = s.raw("select from fxrate")

# Attribute-style access
group.equities.get("rdb")  # Engine(host='eq-rdb', port=5010)
```

### Configuration Methods

Build registries from dicts, DSN strings, or environment variables:

```python
# From config dicts
equities = EngineRegistry.from_config({
    "rdb": {"host": "eq-rdb", "port": 5010},
    "hdb": {"host": "eq-hdb", "port": 5012},
})

# From DSN strings
equities = EngineRegistry.from_dsn({
    "rdb": "kdb://eq-rdb:5010",
    "hdb": "kdb://user:pass@eq-hdb:5012",
})

# From environment variables
# Reads QORM_EQ_RDB_HOST, QORM_EQ_RDB_PORT, QORM_EQ_RDB_USER, QORM_EQ_RDB_PASS
equities = EngineRegistry.from_env(names=["rdb", "hdb"], prefix="QORM_EQ")

# Two-level config for EngineGroup
group = EngineGroup.from_config({
    "equities": {
        "rdb": {"host": "eq-rdb", "port": 5010},
        "hdb": {"host": "eq-hdb", "port": 5012},
    },
    "fx": {
        "rdb": {"host": "fx-rdb", "port": 5020},
    },
})
```

---

## Connection Pools

For applications that need multiple concurrent connections.

### Sync Pool

Thread-safe, queue-based pool:

```python
from qorm import SyncPool

with SyncPool(engine, min_size=2, max_size=10, timeout=30.0) as pool:
    conn = pool.acquire()
    try:
        result = conn.query("select from trade")
    finally:
        pool.release(conn)
```

### Async Pool

```python
from qorm import AsyncPool

async with AsyncPool(engine, min_size=2, max_size=10) as pool:
    conn = await pool.acquire()
    try:
        result = await conn.query("select from trade")
    finally:
        await pool.release(conn)
```

**Pool parameters:**

| Parameter  | Type    | Default | Description                             |
|------------|---------|---------|------------------------------------------|
| `min_size` | `int`   | `1`     | Connections created on startup           |
| `max_size` | `int`   | `10`    | Maximum pool size                        |
| `timeout`  | `float` | `30.0`  | Seconds to wait for a connection         |

If the pool is exhausted, `acquire()` raises `PoolExhaustedError` after `timeout` seconds.

---

## Schema Management

Generate and execute DDL from model definitions:

```python
with Session(engine) as s:
    # Create table
    s.create_table(Trade)
    # Generates: trade:([] sym:`s$(); price:`f$(); size:`j$(); time:`p$())

    # Check existence
    if s.table_exists(Trade):
        print("table exists")

    # Drop table
    s.drop_table(Trade)
```

For more control, use the schema functions directly:

```python
from qorm.model.schema import create_table_q, drop_table_q, table_meta_q

print(create_table_q(Trade))
# trade:([] sym:`s$(); price:`f$(); size:`j$(); time:`p$())

print(table_meta_q(Trade))
# meta trade
```

---

## Error Handling

qorm defines a structured exception hierarchy:

```
QormError                       # Base for all qorm errors
├── ConnectionError             # Failed to connect
│   ├── HandshakeError          # IPC handshake failed
│   │   └── AuthenticationError # Auth rejected by kdb+
│   └── PoolError               # Connection pool error
│       └── PoolExhaustedError  # No connections available
├── SerializationError          # Python -> q binary failed
├── DeserializationError        # q binary -> Python failed
├── QueryError                  # Query execution error
│   └── QError                  # Error returned by kdb+ (e.g. `'type)
├── ModelError                  # Model definition error
├── SchemaError                 # DDL error
├── EngineNotFoundError         # Named engine not found in registry
└── ReflectionError             # Error reflecting table metadata
```

### Catching q-level errors

```python
from qorm import QError

try:
    s.raw("select from nonexistent")
except QError as e:
    print(e.q_message)  # the error string from kdb+
```

### Catching connection errors

```python
from qorm.exc import ConnectionError, PoolExhaustedError

try:
    conn.open()
except ConnectionError:
    print("cannot reach kdb+")
```

---

## Testing Your Code

qorm ships with a `MockKdbServer` for testing without a live kdb+ process.

### Using MockKdbServer

```python
from tests.conftest import MockKdbServer

server = MockKdbServer()
port = server.start()

# Configure responses
server.set_default_response(42)
server.set_response("select from trade", [1, 2, 3])

# Connect and test
from qorm import SyncConnection
with SyncConnection(host="127.0.0.1", port=port) as conn:
    assert conn.query("1+1") == 42
    assert conn.query("select from trade") == [1, 2, 3]

server.stop()
```

### With pytest

qorm's test suite provides ready-made fixtures:

```python
# conftest.py
from tests.conftest import MockKdbServer
import pytest

@pytest.fixture
def mock_server():
    server = MockKdbServer()
    server.start()
    yield server
    server.stop()

# test_my_app.py
def test_my_query(mock_server):
    mock_server.set_default_response({"__table__": True, "sym": ["AAPL"], "price": [150.0]})

    engine = Engine(host="127.0.0.1", port=mock_server.port)
    with Session(engine) as s:
        result = s.raw("select from trade")
        # assert on result...
```

### Running qorm's own tests

```bash
# All tests (no kdb+ needed)
pytest tests/ -v

# Unit tests only
pytest tests/unit/ -v

# Integration tests (uses mock server)
pytest tests/integration/ -v
```

---

## API Reference

### Top-level imports

Everything you need is available from the `qorm` package:

```python
from qorm import (
    # Models
    Model, KeyedModel, Field, field,
    build_model_from_meta,

    # Engine & Sessions
    Engine, Session, AsyncSession, ModelResultSet,

    # Multi-instance
    EngineRegistry, EngineGroup,

    # RPC
    QFunction, q_api,

    # Type aliases
    Boolean, Guid, Byte, Short, Int, Long,
    Real, Float, Char, Symbol,
    Timestamp, Month, Date, DateTime,
    Timespan, Minute, Second, Time,

    # Type system
    QType, QTypeCode, QNull, infer_qtype, is_null,

    # Expressions & Aggregates
    Expr, Column, Literal, BinOp, AggFunc,
    avg_, sum_, min_, max_, count_, first_, last_, med_, dev_, var_,

    # Query builders
    SelectQuery, UpdateQuery, DeleteQuery, InsertQuery,

    # Joins
    aj, lj, ij, wj,

    # Connections & Pools
    SyncConnection, AsyncConnection, SyncPool, AsyncPool,

    # Exceptions
    QormError, ConnectionError, HandshakeError, AuthenticationError,
    SerializationError, DeserializationError, QueryError, QError,
    ModelError, SchemaError, PoolError, PoolExhaustedError,
    EngineNotFoundError, ReflectionError,
)
```

---

## License

MIT
