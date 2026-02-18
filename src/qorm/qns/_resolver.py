"""Registry query building, response parsing, and failover logic."""

from __future__ import annotations

import logging
from typing import Any

from ..engine import Engine
from ..exc import QNSRegistryError
from ..session import Session
from ._registry import RegistryNode

log = logging.getLogger("qorm.qns")


def _build_svcs_query(prefixes: tuple[str, ...]) -> str:
    """Build the q expression to query QNS services.

    Parameters
    ----------
    prefixes:
        Zero or more prefix filters (dataset, cluster, dbtype).

    Returns
    -------
    str
        A q expression like ``".qns.svcs`EMR`SER`H"`` or ``".qns.registry"``.
    """
    if not prefixes:
        return ".qns.registry"
    syms = "".join(f"`{p}" for p in prefixes)
    return f".qns.svcs{syms}"


def _parse_service_rows(raw: Any) -> list[dict]:
    """Parse the raw kdb+ response into a list of service dicts.

    Expects a column-oriented dict (table) with keys including:
    dataset, cluster, dbtype, node, host, port, ssl, ip, env.

    Raises
    ------
    QNSRegistryError
        If the response format is unexpected.
    """
    if isinstance(raw, dict):
        # Column-oriented table (possibly with __table__ marker)
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


def resolve_services(
    registry_nodes: list[RegistryNode],
    prefixes: tuple[str, ...],
    username: str,
    password: str,
    timeout: float,
) -> list[dict]:
    """Query registry nodes with failover and return service dicts.

    Tries each registry node in order.  On success, returns the parsed
    service list.  On failure, logs a warning and tries the next node.

    Raises
    ------
    QNSRegistryError
        If all registry nodes are unreachable.
    """
    query = _build_svcs_query(prefixes)
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
