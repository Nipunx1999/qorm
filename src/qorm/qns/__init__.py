"""QNS (Q Name Service) — service discovery for kdb+ environments."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from ..exc import QNSConfigError, QNSServiceNotFoundError
from ._registry import load_registry_nodes
from ._resolver import resolve_services

if TYPE_CHECKING:
    from ..engine import Engine


@dataclass(frozen=True)
class ServiceInfo:
    """Discovered kdb+ service endpoint."""

    dataset: str
    cluster: str
    dbtype: str
    node: str
    host: str
    port: int
    ssl: str
    ip: str
    env: str

    @property
    def tls(self) -> bool:
        """Whether the service uses TLS."""
        return self.ssl.lower() == "tls"

    @property
    def fqn(self) -> str:
        """Fully-qualified service name: ``DATASET.CLUSTER.DBTYPE.NODE``."""
        return f"{self.dataset}.{self.cluster}.{self.dbtype}.{self.node}"


class QNS:
    """Q Name Service client — discovers kdb+ service endpoints.

    Usage::

        qns = QNS(market="fx", env="prod", username="user", password="pass")
        services = qns.lookup("EMR", "SER", "H")
        engine = qns.engine("EMRATESCV.SERVICE.HDB.1")
    """

    def __init__(
        self,
        market: str,
        env: str,
        username: str = "",
        password: str = "",
        timeout: float = 10.0,
        data_dir: str | Path | None = None,
    ) -> None:
        self._market = market
        self._env = env
        self._username = username
        self._password = password
        self._timeout = timeout
        self._registry_nodes = load_registry_nodes(market, env, data_dir=data_dir)

    def lookup(self, *prefixes: str) -> list[ServiceInfo]:
        """Query QNS registry and return matching services.

        Parameters
        ----------
        *prefixes:
            Zero or more prefix filters (dataset, cluster, dbtype).
            With no arguments, returns all services.

        Returns
        -------
        list[ServiceInfo]

        Raises
        ------
        QNSServiceNotFoundError
            If no services match the given prefixes.
        """
        rows = resolve_services(
            self._registry_nodes,
            prefixes,
            self._username,
            self._password,
            self._timeout,
        )
        services = [_row_to_service_info(r) for r in rows]
        if not services:
            raise QNSServiceNotFoundError(
                f"No services match prefix: {'.'.join(prefixes) or '(all)'}"
            )
        return services

    def engine(self, service_name: str) -> Engine:
        """Resolve an exact service name to an :class:`Engine`.

        Parameters
        ----------
        service_name:
            Fully-qualified name: ``DATASET.CLUSTER.DBTYPE.NODE``.

        Raises
        ------
        QNSConfigError
            If *service_name* is not exactly 4 dot-separated parts.
        QNSServiceNotFoundError
            If the service is not found.
        """
        parts = service_name.split(".")
        if len(parts) != 4:
            raise QNSConfigError(
                f"Service name must be DATASET.CLUSTER.DBTYPE.NODE, "
                f"got {len(parts)} part(s): {service_name!r}"
            )
        services = self.lookup(*parts[:3])
        for svc in services:
            if svc.fqn == service_name:
                return self._build_engine(svc)
        raise QNSServiceNotFoundError(f"Service not found: {service_name!r}")

    def engines(self, *prefixes: str) -> list[Engine]:
        """Resolve all matching services to a list of :class:`Engine` instances.

        Useful for building failover or round-robin pools.
        """
        return [self._build_engine(svc) for svc in self.lookup(*prefixes)]

    def _build_engine(self, svc: ServiceInfo) -> Engine:
        from ..engine import Engine

        return Engine(
            host=svc.host,
            port=svc.port,
            username=self._username,
            password=self._password,
            tls=svc.tls,
        )


def _row_to_service_info(row: dict) -> ServiceInfo:
    return ServiceInfo(
        dataset=str(row.get("dataset", "")),
        cluster=str(row.get("cluster", "")),
        dbtype=str(row.get("dbtype", "")),
        node=str(row.get("node", "")),
        host=str(row.get("host", "")),
        port=int(row.get("port", 0)),
        ssl=str(row.get("ssl", "")),
        ip=str(row.get("ip", "")),
        env=str(row.get("env", "")),
    )
