# qorm Examples

End-to-end examples for connecting to existing kdb+ processes and working with tables you don't own.

---

## Setup

Every example assumes a kdb+ process is already running with tables deployed by someone else. You need the host, port, and (usually) credentials.

```python
from qorm import Engine, Session, AsyncSession, avg_, sum_, min_, max_, count_, first_, last_
from qorm import aj, lj, ij, wj
from qorm import EngineRegistry, EngineGroup, QFunction, q_api, Subscriber
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
