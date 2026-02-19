# qorm Examples

End-to-end examples for connecting to existing kdb+ processes and working with tables you don't own.

---

## Setup

Every example assumes a kdb+ process is already running with tables deployed by someone else. You need the host, port, and (usually) credentials.

```python
from qorm import Engine, Session, AsyncSession, avg_, sum_, min_, max_, count_, first_, last_
from qorm import aj, lj, ij, wj
from qorm import EngineRegistry, EngineGroup, QFunction, q_api, Subscriber
from qorm import xbar_, today_, now_, fby_, each_, peach_
from qorm import ExecQuery, paginate, async_paginate, RetryPolicy
from qorm import engines_from_config, group_from_config, load_config
from qorm import QNS, ServiceInfo
```

---

## 1. Connect and discover

```python
# With authentication (most production setups)
engine = Engine(host="kdb-prod", port=5010, username="myuser", password="mypass")

# Or from a DSN string
engine = Engine.from_dsn("kdb://myuser:mypass@kdb-prod:5010")

# No auth (local dev, or process started without -u/-U)
engine = Engine(host="localhost", port=5010)

with Session(engine) as s:
    # What tables exist on this process?
    print(s.tables())
    # ['trade', 'quote', 'order', 'refdata']
```

---

## 2. Reflect a single table

```python
with Session(engine) as s:
    Trade = s.reflect("trade")

    # Inspect what we got
    print(Trade.__name__)        # 'Trade'
    print(Trade.__tablename__)   # 'trade'

    for name, field in Trade.__fields__.items():
        print(f"  {name}: {field.qtype.name} (code={field.qtype.code})")
    # sym: symbol (code=11)
    # price: float (code=9)
    # size: long (code=7)
    # time: timestamp (code=12)
```

---

## 3. Reflect all tables at once

```python
with Session(engine) as s:
    models = s.reflect_all()
    # {'trade': Trade, 'quote': Quote, 'order': Order, 'refdata': Refdata}

    Trade = models["trade"]
    Quote = models["quote"]
    Order = models["order"]
```

From here on, every example assumes `Trade`, `Quote`, and `Order` have been reflected like this.

---

## 4. Select all rows

```python
with Session(engine) as s:
    Trade = s.reflect("trade")

    result = s.exec(Trade.select())

    print(len(result))       # number of rows
    print(result.columns)    # ['sym', 'price', 'size', 'time']

    for row in result:
        print(row.sym, row.price, row.size)
```

---

## 5. Select specific columns

```python
with Session(engine) as s:
    Trade = s.reflect("trade")

    result = s.exec(
        Trade.select(Trade.sym, Trade.price)
    )
    for row in result:
        print(row.sym, row.price)
```

---

## 6. Where clauses

```python
with Session(engine) as s:
    Trade = s.reflect("trade")

    # Single condition
    result = s.exec(
        Trade.select().where(Trade.price > 100)
    )

    # Multiple conditions (ANDed together)
    result = s.exec(
        Trade.select().where(
            Trade.sym == "AAPL",
            Trade.price > 100,
            Trade.size > 50,
        )
    )

    # Chain .where() calls — same effect
    result = s.exec(
        Trade.select()
             .where(Trade.sym == "AAPL")
             .where(Trade.price > 100)
    )
```

---

## 7. Pattern matching and membership

```python
with Session(engine) as s:
    Trade = s.reflect("trade")

    # like — pattern match on symbols
    result = s.exec(
        Trade.select().where(Trade.sym.like("AA*"))
    )

    # in_ — membership test
    result = s.exec(
        Trade.select().where(Trade.sym.in_(["AAPL", "GOOG", "MSFT"]))
    )

    # within — range check
    result = s.exec(
        Trade.select().where(Trade.price.within(100, 200))
    )
```

---

## 8. Group by with aggregates

```python
with Session(engine) as s:
    Trade = s.reflect("trade")

    # Average price by symbol
    result = s.exec(
        Trade.select(Trade.sym, avg_price=avg_(Trade.price))
             .by(Trade.sym)
    )
    for row in result:
        print(f"{row.sym}: avg_price={row.avg_price:.2f}")

    # Multiple aggregates
    result = s.exec(
        Trade.select(
            Trade.sym,
            avg_price=avg_(Trade.price),
            total_size=sum_(Trade.size),
            trade_count=count_(),
            high=max_(Trade.price),
            low=min_(Trade.price),
            first_trade=first_(Trade.time),
            last_trade=last_(Trade.time),
        ).by(Trade.sym)
    )
    for row in result:
        print(f"{row.sym}: {row.trade_count} trades, "
              f"range {row.low:.2f}-{row.high:.2f}, "
              f"avg {row.avg_price:.2f}")
```

---

## 9. Limit

```python
with Session(engine) as s:
    Trade = s.reflect("trade")

    # Top 10 most expensive trades
    result = s.exec(
        Trade.select().where(Trade.sym == "AAPL").limit(10)
    )
```

---

## 10. Arithmetic expressions in select

```python
with Session(engine) as s:
    Trade = s.reflect("trade")

    # Compute notional = price * size
    result = s.exec(
        Trade.select(
            Trade.sym,
            Trade.price,
            Trade.size,
            notional=Trade.price * Trade.size,
        )
    )
    for row in result:
        print(f"{row.sym}: {row.notional:.2f}")
```

---

## 11. As-of join (trades with latest quotes)

The most common join in kdb+. For each trade, find the most recent quote at or before the trade time.

```python
with Session(engine) as s:
    models = s.reflect_all()
    Trade = models["trade"]
    Quote = models["quote"]

    # aj joins on sym and time — for each trade row, pulls the
    # most recent quote row where quote.time <= trade.time
    result = s.exec(
        aj([Trade.sym, Trade.time], Trade, Quote)
    )
    for row in result:
        print(f"{row.sym} traded at {row.price}, "
              f"bid={row.bid}, ask={row.ask}")
```

You can also pass column names as strings:

```python
    result = s.exec(aj(["sym", "time"], Trade, Quote))
```

---

## 12. Left join (enrich trades with reference data)

```python
with Session(engine) as s:
    models = s.reflect_all()
    Trade = models["trade"]
    Refdata = models["refdata"]

    # Left join on sym — every trade row gets refdata columns appended
    # (nulls where no match)
    result = s.exec(
        lj([Trade.sym], Trade, Refdata)
    )
    for row in result:
        print(f"{row.sym}: sector={row.sector}, exchange={row.exchange}")
```

---

## 13. Inner join

```python
with Session(engine) as s:
    models = s.reflect_all()
    Trade = models["trade"]
    Order = models["order"]

    # Only rows where both tables have a matching sym
    result = s.exec(
        ij([Trade.sym], Trade, Order)
    )
```

---

## 14. Window join (aggregate quotes around each trade)

```python
with Session(engine) as s:
    models = s.reflect_all()
    Trade = models["trade"]
    Quote = models["quote"]

    # For each trade, average the bid and ask from quotes
    # within a 2-second window before the trade
    result = s.exec(
        wj(
            windows=(-2000000000, 0),       # 2 seconds in nanos
            on=[Trade.sym, Trade.time],
            left=Trade,
            right=Quote,
            aggs={"bid": "avg", "ask": "avg"},
        )
    )
    for row in result:
        print(f"{row.sym}: trade_price={row.price}, "
              f"avg_bid={row.bid:.4f}, avg_ask={row.ask:.4f}")
```

---

## 15. DataFrame export

```python
# pip install qorm[pandas]

with Session(engine) as s:
    Trade = s.reflect("trade")

    result = s.exec(
        Trade.select(Trade.sym, avg_price=avg_(Trade.price))
             .by(Trade.sym)
    )

    df = result.to_dataframe()
    print(df)
    #     sym  avg_price
    # 0  AAPL     152.30
    # 1  GOOG    2815.50
    # 2  MSFT     382.10

    # Or get the raw column dict
    data = result.to_dict()
    # {'sym': ['AAPL', 'GOOG', 'MSFT'], 'avg_price': [152.3, 2815.5, 382.1]}
```

---

## 16. Column access on result sets

```python
with Session(engine) as s:
    Trade = s.reflect("trade")
    result = s.exec(Trade.select())

    # Column-oriented access (no row iteration overhead)
    all_syms = result["sym"]       # list of all sym values
    all_prices = result["price"]   # list of all price values

    # Single row by index
    first_trade = result[0]
    print(first_trade.sym, first_trade.price)

    # Metadata
    print(result.columns)   # ['sym', 'price', 'size', 'time']
    print(len(result))      # row count
```

---

## 17. Call deployed q functions (RPC)

```python
with Session(engine) as s:
    # Ad-hoc calls
    snapshot = s.call("getSnapshot", "AAPL")
    vwap = s.call("calcVWAP", "AAPL", "2024.01.15")
    status = s.call("getStatus")

    # Reusable wrapper
    get_trades = QFunction("getTradesByDate")
    trades_jan = get_trades(s, "2024.01.15")
    trades_feb = get_trades(s, "2024.02.15")

    # Typed decorator — body is never called, signature is documentation
    @q_api("getTradesByDate")
    def get_trades_by_date(session, date: str): ...

    @q_api("calcVWAP")
    def calc_vwap(session, sym: str, date: str): ...

    trades = get_trades_by_date(s, "2024.01.15")
    vwap = calc_vwap(s, "AAPL", "2024.01.15")
```

---

## 18. Raw q fallback

When the query builder doesn't cover your use case:

```python
with Session(engine) as s:
    # Free-form q
    result = s.raw("select count i by sym from trade")

    # With arguments (sent as a q lambda call)
    result = s.raw("{select from trade where sym=x}", "AAPL")

    # System commands
    s.raw("\\t select from trade")  # time a query
```

---

## 19. Multiple kdb+ instances (EngineRegistry)

```python
# One domain, multiple instances (with auth)
equities = EngineRegistry.from_config({
    "rdb": {"host": "eq-rdb", "port": 5010, "username": "svc_eq", "password": "secret"},
    "hdb": {"host": "eq-hdb", "port": 5012, "username": "svc_eq", "password": "secret"},
    "gw":  {"host": "eq-gw",  "port": 5000, "username": "svc_eq", "password": "secret"},
})

# Or from DSN strings (credentials in the URL)
equities = EngineRegistry.from_dsn({
    "rdb": "kdb://svc_eq:secret@eq-rdb:5010",
    "hdb": "kdb://svc_eq:secret@eq-hdb:5012",
    "gw":  "kdb://svc_eq:secret@eq-gw:5000",
})

# Today's data from the RDB
with equities.session("rdb") as s:
    Trade = s.reflect("trade")
    today = s.exec(
        Trade.select(Trade.sym, avg_price=avg_(Trade.price))
             .by(Trade.sym)
    )

# Historical data from the HDB
with equities.session("hdb") as s:
    Trade = s.reflect("trade")
    historical = s.exec(
        Trade.select().where(Trade.sym == "AAPL").limit(1000)
    )

# Gateway call
with equities.session("gw") as s:
    result = s.call("getTradesByDateRange", "AAPL", "2024.01.01", "2024.06.30")
```

---

## 20. Multiple domains (EngineGroup)

```python
group = EngineGroup.from_config({
    "equities": {
        "rdb": {"host": "eq-rdb", "port": 5010, "username": "svc_eq", "password": "secret"},
        "hdb": {"host": "eq-hdb", "port": 5012, "username": "svc_eq", "password": "secret"},
    },
    "fx": {
        "rdb": {"host": "fx-rdb", "port": 5020, "username": "svc_fx", "password": "secret"},
        "hdb": {"host": "fx-hdb", "port": 5022, "username": "svc_fx", "password": "secret"},
    },
    "rates": {
        "rdb": {"host": "rates-rdb", "port": 5030, "username": "svc_rates", "password": "secret"},
    },
})

# Equities RDB
with group.session("equities", "rdb") as s:
    Trade = s.reflect("trade")
    result = s.exec(
        Trade.select(Trade.sym, last_price=last_(Trade.price))
             .by(Trade.sym)
    )

# FX HDB
with group.session("fx", "hdb") as s:
    tables = s.tables()
    FxRate = s.reflect("fxrate")
    result = s.exec(
        FxRate.select().where(FxRate.ccy == "EURUSD").limit(100)
    )

# Rates
with group.session("rates") as s:
    result = s.call("getYieldCurve", "USD", "2024.06.15")
```

---

## 21. Environment-based configuration

```bash
# Set these in your shell or .env file
export QORM_EQ_RDB_HOST=eq-rdb-prod
export QORM_EQ_RDB_PORT=5010
export QORM_EQ_RDB_USER=svc_eq
export QORM_EQ_RDB_PASS=secret
export QORM_EQ_HDB_HOST=eq-hdb-prod
export QORM_EQ_HDB_PORT=5012
export QORM_EQ_HDB_USER=svc_eq
export QORM_EQ_HDB_PASS=secret
```

```python
equities = EngineRegistry.from_env(names=["rdb", "hdb"], prefix="QORM_EQ")

with equities.session("rdb") as s:
    Trade = s.reflect("trade")
    result = s.exec(Trade.select().limit(10))
```

---

## 22. Full workflow — reflect, query, join, export

Putting it all together: connect to a production gateway, discover what's there, run analytics, and export to pandas.

```python
from qorm import (
    Engine, Session, EngineRegistry,
    avg_, sum_, max_, min_, count_, first_, last_,
    aj, lj,
)

equities = EngineRegistry.from_config({
    "rdb": {"host": "eq-rdb", "port": 5010, "username": "svc_eq", "password": "secret"},
    "hdb": {"host": "eq-hdb", "port": 5012, "username": "svc_eq", "password": "secret"},
})

with equities.session("rdb") as s:
    # --- Discover ---
    print("Tables:", s.tables())

    models = s.reflect_all()
    Trade = models["trade"]
    Quote = models["quote"]

    # --- Filter and aggregate ---
    summary = s.exec(
        Trade.select(
            Trade.sym,
            trade_count=count_(),
            avg_price=avg_(Trade.price),
            total_volume=sum_(Trade.size),
            high=max_(Trade.price),
            low=min_(Trade.price),
        )
        .where(Trade.sym.in_(["AAPL", "GOOG", "MSFT"]))
        .by(Trade.sym)
    )

    for row in summary:
        print(f"{row.sym}: {row.trade_count} trades, "
              f"avg={row.avg_price:.2f}, vol={row.total_volume}")

    # --- Join trades with quotes ---
    enriched = s.exec(
        aj([Trade.sym, Trade.time], Trade, Quote)
    )

    # --- Export to pandas ---
    df = enriched.to_dataframe()
    df["spread"] = df["ask"] - df["bid"]
    df["mid"] = (df["ask"] + df["bid"]) / 2
    print(df[["sym", "price", "bid", "ask", "spread", "mid"]].head(10))

    # --- Call a deployed function ---
    vwap_data = s.call("calcVWAP", "AAPL")
    print("AAPL VWAP:", vwap_data)
```

---

## 23. TLS/SSL connections

```python
from qorm import Engine, Session

# Enable TLS with system CA verification
engine = Engine(host="kdb-prod", port=5000, tls=True)

# From a DSN string with the kdb+tls:// scheme
engine = Engine.from_dsn("kdb+tls://svc_user:secret@kdb-prod:5000")

# Disable certificate verification (self-signed certs, dev environments)
engine = Engine(host="kdb-dev", port=5000, tls=True, tls_verify=False)

# Custom SSL context (client certificates, custom CA bundle)
import ssl
ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
ctx.load_cert_chain("client.crt", "client.key")
ctx.load_verify_locations("ca-bundle.crt")

engine = Engine(host="kdb-prod", port=5000, tls=True, tls_context=ctx)

with Session(engine) as s:
    Trade = s.reflect("trade")
    result = s.exec(Trade.select().limit(10))
```

---

## 24. Keyed table reflection

```python
with Session(engine) as s:
    # Reflect a keyed table — automatically detects key columns
    DailyPrice = s.reflect("daily_price")

    # Check if it's a keyed model
    from qorm import KeyedModel
    print(issubclass(DailyPrice, KeyedModel))  # True

    # Inspect keys vs value columns
    print(DailyPrice.key_columns())    # ['sym', 'date']
    print(DailyPrice.value_columns())  # ['close', 'volume']

    # Query it like any other model
    result = s.exec(
        DailyPrice.select(
            DailyPrice.sym,
            avg_close=avg_(DailyPrice.close),
        ).by(DailyPrice.sym)
    )
    for row in result:
        print(f"{row.sym}: avg close = {row.avg_close:.2f}")
```

---

## 25. Connection health checks

```python
from qorm import Engine, SyncConnection

engine = Engine(host="kdb-prod", port=5010)

# Manual health check on a connection
conn = engine.connect()
conn.open()

if conn.ping():
    print("Connection is alive")
    result = conn.query("select from trade where i<5")
else:
    print("Connection is stale, reconnecting...")
    conn.close()
    conn = engine.connect()
    conn.open()

conn.close()
```

---

## 26. Pool with health checks

```python
from qorm import SyncPool, AsyncPool

# Pools automatically check connection health on acquire (default)
with SyncPool(engine, min_size=2, max_size=10, check_on_acquire=True) as pool:
    # Dead connections are replaced transparently
    conn = pool.acquire()
    print(f"Pool size: {pool.size}")
    try:
        result = conn.query("select from trade where i<10")
    finally:
        pool.release(conn)

# Disable health checks for performance-critical paths
with SyncPool(engine, min_size=5, max_size=20, check_on_acquire=False) as pool:
    conn = pool.acquire()
    # ...
    pool.release(conn)
```

---

## 27. Pool from registry

```python
equities = EngineRegistry.from_config({
    "rdb": {"host": "eq-rdb", "port": 5010},
    "hdb": {"host": "eq-hdb", "port": 5012},
})

# Create a connection pool directly from the registry
with equities.pool("rdb", min_size=2, max_size=10) as pool:
    conn = pool.acquire()
    try:
        result = conn.query("select from trade where i<100")
    finally:
        pool.release(conn)

# Async pool from registry
# async with equities.async_pool("rdb", min_size=2, max_size=10) as pool:
#     conn = await pool.acquire()
#     ...
```

---

## 28. Debug / Explain mode

```python
with Session(engine) as s:
    Trade = s.reflect("trade")

    # Inspect the generated q before executing
    query = (
        Trade.select(Trade.sym, avg_price=avg_(Trade.price))
             .where(Trade.price > 100)
             .by(Trade.sym)
    )
    print(query.explain())
    # -- SelectQuery on `trade
    # ?[trade;enlist ((price>100));(enlist `sym)!enlist `sym;`avg_price`sym!(`avg;`price)]

    # Works on all query types
    print(Trade.update().set(price=100.0).where(Trade.sym == "AAPL").explain())
    print(Trade.delete().where(Trade.sym == "OLD").explain())

    # Joins too
    Quote = s.reflect("quote")
    print(aj([Trade.sym, Trade.time], Trade, Quote).explain())
```

---

## 29. Logging

```python
import logging

# Enable debug logging to see all qorm activity
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(name)s %(levelname)s %(message)s"
)

# Or selectively enable subsystems
logging.getLogger("qorm").setLevel(logging.DEBUG)           # session ops + timing
logging.getLogger("qorm.connection").setLevel(logging.DEBUG) # connect/disconnect
logging.getLogger("qorm.pool").setLevel(logging.DEBUG)       # pool acquire/release
logging.getLogger("qorm.subscription").setLevel(logging.DEBUG)  # pub-sub events

engine = Engine(host="kdb-prod", port=5010)

with Session(engine) as s:
    # Each operation logs the query text and execution time
    result = s.raw("select from trade where i<5")
    # DEBUG qorm: raw: select from trade where i<5
    # DEBUG qorm: raw completed in 1.234ms

    Trade = s.reflect("trade")
    result = s.exec(Trade.select().limit(10))
    # DEBUG qorm: exec: ?[trade;();0b;()]
    # DEBUG qorm: exec completed in 0.892ms

    result = s.call("getStatus")
    # DEBUG qorm: call: getStatus()
    # DEBUG qorm: call completed in 0.456ms
```

---

## 30. Subscription / Pub-sub (real-time data)

```python
import asyncio
from qorm import Engine, Subscriber

engine = Engine(host="tickerplant", port=5010)

# Define a callback — called for each incoming update
async def on_trade(table_name, data):
    print(f"[{table_name}] received {len(data)} rows")
    # data is the raw kdb+ table data (dict of columns)

async def main():
    # Using context manager
    async with Subscriber(engine, callback=on_trade) as sub:
        # Subscribe to specific symbols
        await sub.subscribe("trade", ["AAPL", "MSFT", "GOOG"])

        # Subscribe to all quotes
        await sub.subscribe("quote")

        # Listen for updates (blocks until sub.stop() is called)
        await sub.listen()

asyncio.run(main())
```

Stopping a subscriber from another coroutine:

```python
async def main():
    sub = Subscriber(engine, callback=on_trade)
    await sub.connect()
    await sub.subscribe("trade")

    # Run listener in background
    listen_task = asyncio.create_task(sub.listen())

    # Stop after 60 seconds
    await asyncio.sleep(60)
    sub.stop()
    await listen_task
    await sub.close()
```

---

## 31. TLS with registry and pools

```python
# TLS connections work across all features
equities = EngineRegistry.from_dsn({
    "rdb": "kdb+tls://svc_eq:secret@eq-rdb:5010",
    "hdb": "kdb+tls://svc_eq:secret@eq-hdb:5012",
})

# Session over TLS
with equities.session("rdb") as s:
    Trade = s.reflect("trade")
    result = s.exec(Trade.select().limit(10))

# Pool over TLS (health checks also work over TLS)
with equities.pool("rdb", min_size=2, max_size=5) as pool:
    conn = pool.acquire()
    try:
        print(conn.ping())  # True
        result = conn.query("select from trade where i<5")
    finally:
        pool.release(conn)
```

---

## 32. Time bucketing with xbar

Bucket timestamps into intervals for time-series aggregation — the most common kdb+ pattern.

```python
from qorm import xbar_, avg_, sum_, count_

with Session(engine) as s:
    Trade = s.reflect("trade")

    # 5-minute VWAP bars
    result = s.exec(
        Trade.select(
            Trade.sym,
            vwap=avg_(Trade.price),
            volume=sum_(Trade.size),
            trade_count=count_(),
        )
        .by(Trade.sym, bucket=xbar_(5, Trade.time))
    )
    for row in result:
        print(f"{row.sym} @ {row.bucket}: vwap={row.vwap:.2f}, "
              f"vol={row.volume}, trades={row.trade_count}")

    # 1-minute bars for a single symbol
    result = s.exec(
        Trade.select(
            high=max_(Trade.price),
            low=min_(Trade.price),
            close=last_(Trade.price),
        )
        .where(Trade.sym == "AAPL")
        .by(t=xbar_(1, Trade.time))
    )
```

---

## 33. Today and now helpers

Filter by the current date or timestamp without hardcoding values.

```python
from qorm import today_, now_

with Session(engine) as s:
    Trade = s.reflect("trade")

    # All trades from today
    result = s.exec(
        Trade.select().where(Trade.date == today_())
    )

    # Trades in the last timestamp range
    result = s.exec(
        Trade.select().where(Trade.time > now_())
    )

    # Combine xbar with today
    result = s.exec(
        Trade.select(Trade.sym, avg_price=avg_(Trade.price))
             .where(Trade.date == today_())
             .by(Trade.sym, t=xbar_(5, Trade.time))
    )
```

---

## 34. fby (filter by) — group-level WHERE conditions

Filter rows based on an aggregate computed per group, without a separate groupby query.

```python
from qorm import fby_

with Session(engine) as s:
    Trade = s.reflect("trade")

    # Trades where price equals the max price for that symbol
    # q: select from trade where price = (max;price) fby sym
    result = s.exec(
        Trade.select().where(
            Trade.price == fby_("max", Trade.price, Trade.sym)
        )
    )
    print(f"Found {len(result)} trades at their symbol's high")

    # Trades with above-average size for their symbol
    result = s.exec(
        Trade.select().where(
            Trade.size > fby_("avg", Trade.size, Trade.sym)
        )
    )

    # Combine fby with other WHERE conditions
    result = s.exec(
        Trade.select()
             .where(Trade.sym.in_(["AAPL", "GOOG"]))
             .where(Trade.price == fby_("min", Trade.price, Trade.sym))
    )
```

---

## 35. each / peach adverbs

Apply a function element-wise (`each`) or in parallel (`peach`) — useful for nested/list columns.

```python
from qorm import count_, avg_, each_, peach_

with Session(engine) as s:
    Trade = s.reflect("trade")

    # Count tags per row (tags is a list column)
    # q: select sym, tag_count:count each tags from trade
    result = s.exec(
        Trade.select(Trade.sym, tag_count=count_(Trade.tags).each())
    )

    # Standalone form
    result = s.exec(
        Trade.select(Trade.sym, lengths=each_("count", Trade.tags))
    )

    # Parallel execution with peach
    result = s.exec(
        Trade.select(Trade.sym, avg_prices=avg_(Trade.prices).peach())
    )
```

---

## 36. Exec query — get vectors instead of tables

Use `exec_` when you want raw column values (vectors or dicts) instead of a table result set.

```python
with Session(engine) as s:
    Trade = s.reflect("trade")

    # Single column → returns a list (vector)
    all_prices = s.exec(Trade.exec_(Trade.price))
    print(f"Got {len(all_prices)} prices")
    print(f"Average: {sum(all_prices) / len(all_prices):.2f}")

    # Multiple columns → returns a dict of vectors
    data = s.exec(Trade.exec_(Trade.sym, Trade.price))
    print(data.keys())  # dict_keys(['sym', 'price'])

    # With filtering
    aapl_prices = s.exec(
        Trade.exec_(Trade.price).where(Trade.sym == "AAPL")
    )

    # Named columns with aggregates
    result = s.exec(
        Trade.exec_(avg_price=avg_(Trade.price)).by(Trade.sym)
    )

    # Inspect the compiled q
    print(Trade.exec_(Trade.price).explain())
    # -- ExecQuery on `trade
    # ?[trade;();0b;`price]
```

---

## 37. Pagination — iterate over large result sets

Process data in manageable pages instead of loading everything into memory.

```python
from qorm import paginate

with Session(engine) as s:
    Trade = s.reflect("trade")

    # Page through all AAPL trades, 1000 rows at a time
    query = Trade.select().where(Trade.sym == "AAPL")

    total_rows = 0
    for page in paginate(s, query, page_size=1000):
        df = page.to_dataframe()
        total_rows += len(page)
        print(f"Processing page: {len(page)} rows (total so far: {total_rows})")
        # process(df)

    # Manual offset/limit for one-off paging
    page_3 = s.exec(
        Trade.select().offset(200).limit(100)
    )
```

Async version:

```python
from qorm import async_paginate, AsyncSession

async with AsyncSession(engine) as s:
    Trade = await s.reflect("trade")

    async for page in async_paginate(s, Trade.select(), page_size=1000):
        print(f"Got {len(page)} rows")
```

---

## 38. Retry / reconnection policy

Automatically retry on connection failures with exponential backoff.

```python
from qorm import Engine, Session, RetryPolicy

# Configure a retry policy
policy = RetryPolicy(
    max_retries=3,       # retry up to 3 times
    base_delay=0.5,      # start with 0.5 second delay
    backoff_factor=2.0,  # double delay each retry: 0.5s, 1.0s, 2.0s
    max_delay=30.0,      # never wait more than 30 seconds
)

# Attach the policy to an engine
engine = Engine(host="kdb-prod", port=5000, retry=policy)

# Session automatically retries raw(), exec(), and call() on ConnectionError
with Session(engine) as s:
    # If the connection drops, qorm will:
    # 1. Close the stale connection
    # 2. Open a fresh connection
    # 3. Wait (with backoff)
    # 4. Retry the query
    result = s.exec(Trade.select())

# Works with registries too
equities = EngineRegistry.from_config({
    "rdb": {"host": "eq-rdb", "port": 5010, "retry": None},  # no retry
})

# Or build the engine manually with retry
from qorm import Engine
engine_with_retry = Engine(host="eq-rdb", port=5010, retry=policy)
```

---

## 39. Config files (YAML / TOML / JSON)

Load engine configurations from files instead of hardcoding.

```python
from qorm import engines_from_config, group_from_config, load_config

# --- JSON (always available, no extra deps) ---
equities = engines_from_config("config/equities.json")
# equities.json:
# {
#   "rdb": {"host": "eq-rdb", "port": 5010},
#   "hdb": {"host": "eq-hdb", "port": 5012}
# }

with equities.session("rdb") as s:
    Trade = s.reflect("trade")
    result = s.exec(Trade.select().limit(10))

# --- TOML (built-in on Python 3.11+, or pip install qorm[toml]) ---
equities = engines_from_config("config/equities.toml")
# equities.toml:
# [rdb]
# host = "eq-rdb"
# port = 5010
#
# [hdb]
# host = "eq-hdb"
# port = 5012

# --- YAML (pip install qorm[yaml]) ---
equities = engines_from_config("config/equities.yaml")
# equities.yaml:
# rdb:
#   host: eq-rdb
#   port: 5010
# hdb:
#   host: eq-hdb
#   port: 5012

# --- Two-level config for EngineGroup ---
group = group_from_config("config/engines.yaml")
# engines.yaml:
# equities:
#   rdb:
#     host: eq-rdb
#     port: 5010
# fx:
#   rdb:
#     host: fx-rdb
#     port: 5020

with group.session("equities", "rdb") as s:
    result = s.exec(Trade.select().limit(10))

with group.session("fx") as s:
    tables = s.tables()

# --- Low-level: load raw dict ---
config = load_config("config/equities.toml")
print(config)  # {"rdb": {"host": "eq-rdb", "port": 5010}, ...}
```

---

## 40. Full workflow with new features

Combining temporal helpers, fby, exec, pagination, retry, and config files.

```python
from qorm import (
    Engine, Session, RetryPolicy,
    engines_from_config, paginate,
    avg_, sum_, max_, min_, count_, first_, last_,
    xbar_, today_, fby_,
)

# Load config with retry
policy = RetryPolicy(max_retries=3, base_delay=0.5)
equities = engines_from_config("config/equities.json")

with equities.session("rdb") as s:
    Trade = s.reflect("trade")

    # --- Today's 5-minute VWAP bars ---
    bars = s.exec(
        Trade.select(
            Trade.sym,
            vwap=avg_(Trade.price),
            volume=sum_(Trade.size),
        )
        .where(Trade.date == today_())
        .by(Trade.sym, bucket=xbar_(5, Trade.time))
    )
    df_bars = bars.to_dataframe()
    print(df_bars.head())

    # --- Find trades at the day's high per symbol ---
    highs = s.exec(
        Trade.select()
             .where(Trade.date == today_())
             .where(Trade.price == fby_("max", Trade.price, Trade.sym))
    )
    print(f"Found {len(highs)} trades at daily highs")

    # --- Get all unique symbols (exec, not select) ---
    all_syms = s.exec(Trade.exec_(Trade.sym).where(Trade.date == today_()))
    unique_syms = list(set(all_syms))
    print(f"Active symbols today: {len(unique_syms)}")

    # --- Page through historical data ---
    for page in paginate(s, Trade.select().where(Trade.sym == "AAPL"), page_size=5000):
        df = page.to_dataframe()
        # process(df)
        print(f"  Processed {len(page)} rows")
```

---

## 41. Service discovery with QNS — one-liner

Connect to a named kdb+ service without knowing its host:port. The QNS client reads registry nodes from a CSV, queries the registry, and resolves the endpoint.

> The registry query depends on the market: FX uses `.qns.getRegistry[]` (function call), all other markets use `.qns.registry` (direct table access). FX registry responses are often large and arrive IPC-compressed — qorm decompresses transparently.

```python
from qorm import Engine, Session

# All you need is the service name, market, and environment
engine = Engine.from_service(
    "EMRATESCV.SERVICE.HDB.1",
    market="fx",
    env="prod",
    username="svc_user",
    password="secret",
)

with Session(engine) as s:
    # You're connected — use all the usual ORM features
    Trade = s.reflect("trade")
    result = s.exec(Trade.select().limit(10))
    for row in result:
        print(row.sym, row.price)
```

---

## 42. QNS — browse and discover services

Use the `QNS` client to explore what services are available before connecting.

```python
from qorm import QNS

qns = QNS(market="fx", env="prod", username="svc_user", password="secret")

# Browse all services matching a prefix
services = qns.lookup("EMR", "SER", "H")

for svc in services:
    print(f"{svc.fqn}  →  {svc.host}:{svc.port}  tls={svc.tls}")
    # EMRATESCV.SERVICE.HDB.1  →  host1.example.com:5010  tls=True
    # EMRATESCV.SERVICE.HDB.2  →  host2.example.com:5011  tls=True
```

---

## 43. QNS — resolve a single service to an Engine

```python
from qorm import QNS, Session

qns = QNS(market="fx", env="prod", username="svc_user", password="secret")

engine = qns.engine("EMRATESCV.SERVICE.HDB.1")
print(engine)  # Engine(host='host1.example.com', port=5010, tls=True)

with Session(engine) as s:
    Trade = s.reflect("trade")
    result = s.exec(
        Trade.select(Trade.sym, avg_price=avg_(Trade.price))
             .by(Trade.sym)
    )
    df = result.to_dataframe()
    print(df.head())
```

---

## 44. QNS — multi-node engines for failover

Resolve all nodes of a service cluster to build a failover or round-robin pool.

```python
from qorm import QNS, Session

qns = QNS(market="fx", env="prod", username="svc_user", password="secret")

# Get engines for all HDB nodes in the EMRATESCV.SERVICE cluster
engines = qns.engines("EMRATESCV", "SERVICE", "HDB")

print(f"Found {len(engines)} nodes:")
for eng in engines:
    print(f"  {eng}")
    # Engine(host='host1.example.com', port=5010, tls=True)
    # Engine(host='host2.example.com', port=5011, tls=True)

# Use the first available node
for eng in engines:
    try:
        with Session(eng) as s:
            result = s.raw("select count i from trade")
            print(f"Connected to {eng.host}:{eng.port}, rows: {result}")
            break
    except Exception:
        print(f"Node {eng.host}:{eng.port} unavailable, trying next...")
```

---

## 45. QNS — custom CSV directory

Point to your own directory of registry CSV files instead of the bundled defaults.

```python
from qorm import QNS

# CSV files are named {market}_{env}.csv, e.g. fx_prod.csv
# Expected format:
#   dataset,cluster,dbtype,node,host,port,port_env,env
#   EMRATESCV,SERVICE,HDB,1,host1.example.com,5010,QNS_PORT,prod

qns = QNS(
    market="fx",
    env="prod",
    username="svc_user",
    password="secret",
    data_dir="/etc/qns/registries",
)

services = qns.lookup("EMR")
for svc in services:
    print(svc.fqn, svc.host, svc.port)
```

---

## 46. QNS — error handling

```python
from qorm import QNS, Engine
from qorm.exc import QNSConfigError, QNSRegistryError, QNSServiceNotFoundError

# --- Missing or malformed CSV ---
try:
    qns = QNS(market="nonexistent", env="prod")
except QNSConfigError as e:
    print(f"Config error: {e}")
    # Config error: Registry CSV not found in package data: nonexistent_prod.csv

# --- Bad service name format ---
try:
    qns = QNS(market="fx", env="prod", data_dir="/path/to/csvs")
    qns.engine("ONLY.TWO.PARTS")
except QNSConfigError as e:
    print(f"Config error: {e}")
    # Config error: Service name must be DATASET.CLUSTER.DBTYPE.NODE, got 3 part(s)

# --- All registry nodes unreachable ---
try:
    qns = QNS(market="fx", env="prod", data_dir="/path/to/csvs")
    services = qns.lookup("EMR")
except QNSRegistryError as e:
    print(f"Registry error: {e}")
    # Registry error: All 2 registry node(s) unreachable: ...

# --- Service not found ---
try:
    qns = QNS(market="fx", env="prod", data_dir="/path/to/csvs")
    engine = qns.engine("NONEXISTENT.SERVICE.HDB.1")
except QNSServiceNotFoundError as e:
    print(f"Not found: {e}")
    # Not found: Service not found: 'NONEXISTENT.SERVICE.HDB.1'
```

---

## 47. IPC compression — transparent handling

kdb+ automatically compresses large IPC responses. qorm detects and decompresses them transparently across all connection types — you don't need to do anything.

```python
from qorm import Engine, Session, Subscriber

# Sync — compressed responses are decompressed automatically
engine = Engine(host="kdb-prod", port=5010, username="user", password="pass")

with Session(engine) as s:
    # Large result sets may arrive compressed — handled transparently
    result = s.raw("select from trade")
    print(f"Got {len(result)} rows")  # works regardless of compression

# Async — same transparent decompression
from qorm import AsyncSession

async with AsyncSession(engine) as s:
    result = await s.raw("select from trade")

# Pub-sub — compressed updates are decompressed in the listener
async def on_update(table_name, data):
    print(f"Got {len(data)} rows for {table_name}")

sub = Subscriber(engine, callback=on_update)
# sub.listen() handles compressed messages automatically
```

If you need to work with the compression layer directly (e.g. for testing):

```python
from qorm.protocol.compress import compress, decompress

# Round-trip: compress then decompress
original_msg = b"\x01\x02\x00\x00..." # an IPC message (header + body)
compressed = compress(original_msg, level=1)
restored = decompress(compressed)
assert restored == original_msg
```

---

## 48. Code generation — generate typed models from a live kdb+ process

Instead of defining models by hand, introspect existing tables and generate model files with full IDE autocomplete.

```bash
# Generate models for specific tables
qorm generate --host kdb-prod --port 5010 --tables trade,quote --user myuser --password mypass

# Output:
#   wrote models/trade.py
#   wrote models/quote.py
#   wrote models/__init__.py
#
# Generated 3 file(s) in ./models/
```

The generated files are ready to import:

```python
from models import Trade, Quote  # full IDE autocomplete

engine = Engine(host="kdb-prod", port=5010, username="myuser", password="mypass")

with Session(engine) as s:
    # IDE knows Trade.sym, Trade.price, Trade.size, Trade.time
    result = s.exec(
        Trade.select(Trade.sym, avg_price=avg_(Trade.price))
             .where(Trade.price > 100)
             .by(Trade.sym)
    )
    for row in result:
        print(row.sym, row.avg_price)
```

---

## 49. Code generation — via QNS service name

```bash
# Resolve the service via QNS, then introspect and generate
qorm generate \
    --service EMRATESCV.SERVICE.HDB.1 \
    --market fx \
    --env prod \
    --tables fxtrades,daily_price \
    --output ./src/models \
    --user svc_user \
    --password secret
```

---

## 50. Code generation — keyed tables

Keyed tables are detected automatically and generate `KeyedModel` subclasses with `field(primary_key=True)`.

```bash
qorm generate --host kdb-prod --port 5010 --tables daily_price
```

Generated `models/daily_price.py`:

```python
"""Auto-generated by qorm. Do not edit."""
from qorm import KeyedModel
from qorm.types import Date, Float, Long, Symbol
from qorm import field

class DailyPrice(KeyedModel):
    __tablename__ = 'daily_price'
    sym: Symbol = field(primary_key=True)
    date: Date = field(primary_key=True)
    close: Float
    volume: Long
```

---

## 51. Code generation — programmatic API

Call the code generation functions directly from Python instead of the CLI.

```python
from qorm import Engine
from qorm.codegen import generate_models, generate_model_source
from qorm.protocol.constants import QTypeCode

# --- Full generation (connects to kdb+) ---
engine = Engine(host="kdb-prod", port=5010)
files = generate_models(engine, "./models", ["trade", "quote"])
print(files)
# ['models/trade.py', 'models/quote.py', 'models/__init__.py']

# --- Generate source without connecting ---
source = generate_model_source(
    "trade",
    fields=[
        ("sym", QTypeCode.SYMBOL),
        ("price", QTypeCode.FLOAT),
        ("size", QTypeCode.LONG),
        ("time", QTypeCode.TIMESTAMP),
    ],
)
print(source)
```

---

## 52. Introspection — discover namespaces, functions, and tables

Explore a kdb+ process before writing any queries.

```python
with Session(engine) as s:
    # What namespaces exist?
    print(s.namespaces())   # ['.', '.myapi', '.utils']

    # What tables exist?
    print(s.tables())       # ['trade', 'quote', 'bondrate']

    # What functions are available?
    print(s.functions())    # ['getPrice', 'calcVWAP', 'submitOrder']

    # Functions in a specific namespace
    print(s.functions(".myapi"))  # ['getData', 'runCalc']

    # Full discovery
    for ns in s.namespaces():
        funcs = s.functions(ns) if ns != '.' else s.functions()
        if funcs:
            print(f"{ns}: {funcs}")
```

---

## 53. Function-only kdb+ APIs — no tables, just RPC

Some kdb+ processes expose only functions, not tables. Use `q_api` or `s.call()` directly.

```python
from qorm import Engine, Session, q_api, QFunction

engine = Engine(host="api-server", port=5000, username="user", password="pass")

# Discover what's available
with Session(engine) as s:
    print("Namespaces:", s.namespaces())
    print("Functions:", s.functions())

# Define typed wrappers for the remote API
@q_api("getPrice")
def get_price(session, sym: str, date: str): ...

@q_api("getRiskReport")
def get_risk(session, portfolio: str): ...

@q_api("submitOrder")
def submit_order(session, sym: str, side: str, qty: int, price: float): ...

with Session(engine) as s:
    price = get_price(s, "AAPL", "2026.02.18")
    report = get_risk(s, "EQUITY_BOOK")
    order_id = submit_order(s, "AAPL", "BUY", 100, 150.25)
```

---

## 54. q_api with lambdas — define and call q logic inline

When the server has no pre-built functions, define q lambdas in Python:

```python
from qorm import q_api, QFunction

# Typed decorator — IDE sees the Python signature, q runs on the server
@q_api("{[dt;isin] select from bondrate where date=dt, isin=isin}")
def get_bond_rate(session, date: str, isin: str): ...

@q_api("{[isin] select avg price by date from bondrate where isin=isin}")
def avg_rate_by_date(session, isin: str): ...

@q_api("{[s;px] select from trade where sym=s, price>px}")
def get_filtered(session, sym: str, min_price: float): ...

# Or use QFunction for quick inline wrappers
bond_count = QFunction("{[i] count select from bondrate where isin=i}")
latest = QFunction("{[i] select last price from bondrate where isin=i}")

with Session(engine) as s:
    rates = get_bond_rate(s, "2026.02.18", "XS1969787396")
    avgs = avg_rate_by_date(s, "XS1969787396")
    trades = get_filtered(s, "AAPL", 150.0)
    n = bond_count(s, "XS1969787396")
    last_price = latest(s, "XS1969787396")
```
