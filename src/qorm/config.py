"""Load engine configurations from YAML, TOML, or JSON files."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .registry import EngineRegistry, EngineGroup


def load_config(path: str | Path) -> dict[str, Any]:
    """Load a configuration dict from a file, dispatched by extension.

    Supported extensions: ``.json``, ``.toml``, ``.yaml`` / ``.yml``.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    suffix = path.suffix.lower()

    if suffix == '.json':
        with open(path) as f:
            return json.load(f)

    if suffix == '.toml':
        return _load_toml(path)

    if suffix in ('.yaml', '.yml'):
        return _load_yaml(path)

    raise ValueError(
        f"Unsupported config file extension {suffix!r}. "
        "Use .json, .toml, .yaml, or .yml."
    )


def engines_from_config(path: str | Path) -> EngineRegistry:
    """Load an :class:`EngineRegistry` from a config file."""
    data = load_config(path)
    return EngineRegistry.from_config(data)


def group_from_config(path: str | Path) -> EngineGroup:
    """Load an :class:`EngineGroup` from a config file."""
    data = load_config(path)
    return EngineGroup.from_config(data)


# ── Internal loaders ─────────────────────────────────────────────

def _load_toml(path: Path) -> dict[str, Any]:
    """Load TOML using ``tomllib`` (3.11+) or ``tomli``."""
    try:
        import tomllib  # type: ignore[import-not-found]
    except ModuleNotFoundError:
        try:
            import tomli as tomllib  # type: ignore[no-redef]
        except ModuleNotFoundError:
            raise ImportError(
                "TOML support requires Python 3.11+ (built-in tomllib) "
                "or the 'tomli' package. Install with: pip install qorm[toml]"
            )
    with open(path, 'rb') as f:
        return tomllib.load(f)


def _load_yaml(path: Path) -> dict[str, Any]:
    """Load YAML using ``pyyaml``."""
    try:
        import yaml
    except ModuleNotFoundError:
        raise ImportError(
            "YAML support requires the 'pyyaml' package. "
            "Install with: pip install qorm[yaml]"
        )
    with open(path) as f:
        return yaml.safe_load(f)
