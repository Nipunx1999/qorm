"""Multi-instance engine registry for managing connections to multiple kdb+ processes."""

from __future__ import annotations

import os
from typing import Any

from .engine import Engine
from .exc import EngineNotFoundError


class EngineRegistry:
    """Named collection of Engine instances for a single domain.

    Usage::

        equities = EngineRegistry()
        equities.register("rdb", Engine(host="eq-rdb", port=5010))
        equities.register("hdb", Engine(host="eq-hdb", port=5012))
        equities.set_default("rdb")

        with equities.session() as s:       # uses default (rdb)
            ...
        with equities.session("hdb") as s:  # explicit
            ...
    """

    def __init__(self) -> None:
        self._engines: dict[str, Engine] = {}
        self._default: str | None = None

    def register(self, name: str, engine: Engine) -> None:
        """Register an engine under a name."""
        self._engines[name] = engine
        if self._default is None:
            self._default = name

    def get(self, name: str | None = None) -> Engine:
        """Get an engine by name, or the default."""
        key = name if name is not None else self._default
        if key is None:
            raise EngineNotFoundError("No engines registered")
        try:
            return self._engines[key]
        except KeyError:
            available = ", ".join(sorted(self._engines)) or "(none)"
            raise EngineNotFoundError(
                f"Engine {key!r} not found. Available: {available}"
            )

    def set_default(self, name: str) -> None:
        """Set the default engine name."""
        if name not in self._engines:
            raise EngineNotFoundError(f"Engine {name!r} not registered")
        self._default = name

    @property
    def default(self) -> str | None:
        """Return the name of the default engine."""
        return self._default

    @property
    def names(self) -> list[str]:
        """Return all registered engine names."""
        return list(self._engines)

    def session(self, name: str | None = None) -> Any:
        """Create a Session for the named (or default) engine."""
        from .session import Session
        return Session(self.get(name))

    def async_session(self, name: str | None = None) -> Any:
        """Create an AsyncSession for the named (or default) engine."""
        from .session import AsyncSession
        return AsyncSession(self.get(name))

    def pool(self, name: str | None = None, **kwargs: Any) -> Any:
        """Create a SyncPool for the named (or default) engine.

        Extra keyword arguments are passed to SyncPool (min_size, max_size, etc.).
        """
        from .connection.pool import SyncPool
        return SyncPool(self.get(name), **kwargs)

    def async_pool(self, name: str | None = None, **kwargs: Any) -> Any:
        """Create an AsyncPool for the named (or default) engine.

        Extra keyword arguments are passed to AsyncPool (min_size, max_size, etc.).
        """
        from .connection.pool import AsyncPool
        return AsyncPool(self.get(name), **kwargs)

    @classmethod
    def from_config(cls, config: dict[str, dict[str, Any]]) -> EngineRegistry:
        """Build a registry from a config dict.

        Each key is an engine name, each value is a dict of Engine kwargs::

            EngineRegistry.from_config({
                "rdb": {"host": "eq-rdb", "port": 5010},
                "hdb": {"host": "eq-hdb", "port": 5012},
            })
        """
        registry = cls()
        for name, params in config.items():
            registry.register(name, Engine(**params))
        return registry

    @classmethod
    def from_dsn(cls, dsns: dict[str, str]) -> EngineRegistry:
        """Build a registry from a dict of DSN strings.

        Each key is an engine name, each value is a DSN string::

            EngineRegistry.from_dsn({
                "rdb": "kdb://eq-rdb:5010",
                "hdb": "kdb://eq-hdb:5012",
            })
        """
        registry = cls()
        for name, dsn in dsns.items():
            registry.register(name, Engine.from_dsn(dsn))
        return registry

    @classmethod
    def from_env(
        cls,
        names: list[str],
        prefix: str = "QORM",
    ) -> EngineRegistry:
        """Build a registry from environment variables.

        For each name, reads ``{PREFIX}_{NAME}_HOST`` and ``{PREFIX}_{NAME}_PORT``::

            # QORM_EQ_RDB_HOST=eq-rdb  QORM_EQ_RDB_PORT=5010
            EngineRegistry.from_env(names=["rdb", "hdb"], prefix="QORM_EQ")
        """
        registry = cls()
        for name in names:
            upper = name.upper()
            host = os.environ.get(f"{prefix}_{upper}_HOST", "localhost")
            port_str = os.environ.get(f"{prefix}_{upper}_PORT", "5000")
            username = os.environ.get(f"{prefix}_{upper}_USER", "")
            password = os.environ.get(f"{prefix}_{upper}_PASS", "")
            registry.register(name, Engine(
                host=host,
                port=int(port_str),
                username=username,
                password=password,
            ))
        return registry

    def __repr__(self) -> str:
        names = ", ".join(self._engines)
        return f"EngineRegistry([{names}], default={self._default!r})"


class EngineGroup:
    """Named collection of EngineRegistries (domains or environments).

    Usage::

        group = EngineGroup()
        group.register("equities", equities_registry)
        group.register("fx", fx_registry)

        with group.session("equities", "rdb") as s:
            ...
    """

    def __init__(self) -> None:
        self._registries: dict[str, EngineRegistry] = {}

    def register(self, name: str, registry: EngineRegistry) -> None:
        """Register a domain/environment registry."""
        self._registries[name] = registry

    def get(self, name: str) -> EngineRegistry:
        """Get a registry by name."""
        try:
            return self._registries[name]
        except KeyError:
            available = ", ".join(sorted(self._registries)) or "(none)"
            raise EngineNotFoundError(
                f"Registry {name!r} not found. Available: {available}"
            )

    def __getattr__(self, name: str) -> EngineRegistry:
        if name.startswith('_'):
            raise AttributeError(name)
        return self.get(name)

    @property
    def names(self) -> list[str]:
        """Return all registered registry names."""
        return list(self._registries)

    def session(self, domain: str, instance: str | None = None) -> Any:
        """Shortcut: ``group.session("equities", "rdb")``."""
        return self.get(domain).session(instance)

    def async_session(self, domain: str, instance: str | None = None) -> Any:
        """Shortcut: ``group.async_session("equities", "rdb")``."""
        return self.get(domain).async_session(instance)

    @classmethod
    def from_config(cls, config: dict[str, dict[str, dict[str, Any]]]) -> EngineGroup:
        """Build from a two-level config dict.

        Top-level keys are domain names, values are EngineRegistry configs::

            EngineGroup.from_config({
                "equities": {
                    "rdb": {"host": "eq-rdb", "port": 5010},
                    "hdb": {"host": "eq-hdb", "port": 5012},
                },
                "fx": {
                    "rdb": {"host": "fx-rdb", "port": 5020},
                },
            })
        """
        group = cls()
        for domain, registry_config in config.items():
            group.register(domain, EngineRegistry.from_config(registry_config))
        return group

    def __repr__(self) -> str:
        names = ", ".join(self._registries)
        return f"EngineGroup([{names}])"
