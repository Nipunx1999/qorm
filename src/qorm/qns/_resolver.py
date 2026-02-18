"""Registry querying, response parsing, file-based caching, and failover."""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any

from ..engine import Engine
from ..exc import QNSRegistryError
from ..session import Session
from ._registry import RegistryNode

log = logging.getLogger("qorm.qns")

CACHE_DIR = Path.home() / ".qorm" / "cache"
DEFAULT_CACHE_TTL = 7 * 24 * 3600  # 7 days in seconds


def _parse_service_rows(raw: Any) -> list[dict]:
    """Parse the raw kdb+ response into a list of service dicts.

    Handles three kdb+ response shapes:
    - Column-oriented dict (table): ``{'col': [...], ...}``
    - Keyed table: ``{'keys': {table}, 'values': {table}}``
    - Row-oriented list: ``[{'col': val, ...}, ...]``

    Raises
    ------
    QNSRegistryError
        If the response format is unexpected.
    """
    if isinstance(raw, list):
        if not raw:
            return []
        if isinstance(raw[0], dict):
            return [{k: v for k, v in row.items() if k != "__table__"} for row in raw]
        raise QNSRegistryError(f"Unexpected QNS list element type: {type(raw[0]).__name__}")

    if isinstance(raw, dict):
        # Keyed table: merge key columns and value columns
        if "keys" in raw and "values" in raw and isinstance(raw["keys"], dict) and isinstance(raw["values"], dict):
            keys_t = {k: v for k, v in raw["keys"].items() if k != "__table__"}
            vals_t = {k: v for k, v in raw["values"].items() if k != "__table__"}
            data = {**keys_t, **vals_t}
        else:
            data = {k: v for k, v in raw.items() if k != "__table__"}
        if not data:
            return []
        cols = list(data.keys())
        length = len(next(iter(data.values())))
        return [
            {col: data[col][i] for col in cols}
            for i in range(length)
        ]

    raise QNSRegistryError(f"Unexpected QNS response type: {type(raw).__name__}")


def _cache_path(market: str, env: str) -> Path:
    return CACHE_DIR / f"{market.lower()}_{env.lower()}.json"


def _load_cache(market: str, env: str, cache_ttl: float) -> list[dict] | None:
    """Return cached service rows if the cache file exists and is fresh."""
    path = _cache_path(market, env)
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        ts = data.get("timestamp", 0)
        if (time.time() - ts) > cache_ttl:
            log.debug("QNS cache expired for %s_%s", market, env)
            return None
        log.debug("QNS cache hit for %s_%s (%d services)", market, env, len(data["services"]))
        return data["services"]
    except (json.JSONDecodeError, KeyError, TypeError):
        log.debug("QNS cache corrupt for %s_%s, ignoring", market, env)
        return None


def _save_cache(market: str, env: str, rows: list[dict]) -> None:
    """Write service rows to the cache file."""
    path = _cache_path(market, env)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"timestamp": time.time(), "services": rows}
        path.write_text(json.dumps(payload), encoding="utf-8")
        log.debug("QNS cache saved for %s_%s (%d services)", market, env, len(rows))
    except OSError as exc:
        log.warning("Failed to write QNS cache: %s", exc)


def _fetch_from_registry(
    registry_nodes: list[RegistryNode],
    username: str,
    password: str,
    timeout: float,
    market: str = "",
) -> list[dict]:
    """Query QNS registry with failover across registry nodes.

    For FX markets, uses ``.qns.getRegistry[]``; all others use
    ``.qns.registry`` (direct table access).

    Raises
    ------
    QNSRegistryError
        If all registry nodes are unreachable.
    """
    query = ".qns.getRegistry[]" if market.lower() == "fx" else ".qns.registry"
    errors: list[str] = []

    for node in registry_nodes:
        engine = Engine(
            host=node.host,
            port=node.port,
            username=username,
            password=password,
            timeout=timeout,
        )
        try:
            with Session(engine) as sess:
                raw = sess.raw(query)
            return _parse_service_rows(raw)
        except Exception as exc:
            msg = f"{node.host}:{node.port} — {exc}"
            log.warning("QNS registry node failed: %s", msg)
            errors.append(msg)

    raise QNSRegistryError(
        f"All {len(registry_nodes)} registry node(s) unreachable:\n"
        + "\n".join(f"  - {e}" for e in errors)
    )


def resolve_services(
    registry_nodes: list[RegistryNode],
    username: str,
    password: str,
    timeout: float,
    market: str,
    env: str,
    cache_ttl: float = DEFAULT_CACHE_TTL,
) -> list[dict]:
    """Return all services from ``.qns.registry``, using a file cache.

    Checks ``~/.qorm/cache/{market}_{env}.json`` first.  If the cache is
    missing or stale, queries the registry nodes with failover and writes
    the result back to cache.
    """
    cached = _load_cache(market, env, cache_ttl)
    if cached is not None:
        return cached

    rows = _fetch_from_registry(registry_nodes, username, password, timeout, market)
    _save_cache(market, env, rows)
    return rows


def filter_by_prefix(rows: list[dict], prefixes: tuple[str, ...]) -> list[dict]:
    """Filter service rows by (dataset, cluster, dbtype) prefix.

    Prefix matching is case-insensitive and positional:
    - 0 prefixes → return all
    - 1 prefix  → match dataset
    - 2 prefixes → match dataset + cluster
    - 3 prefixes → match dataset + cluster + dbtype
    """
    if not prefixes:
        return rows

    fields = ("dataset", "cluster", "dbtype")
    result = []
    for row in rows:
        match = True
        for i, prefix in enumerate(prefixes):
            if i >= len(fields):
                break
            val = str(row.get(fields[i], ""))
            if not val.upper().startswith(prefix.upper()):
                match = False
                break
        if match:
            result.append(row)
    return result
