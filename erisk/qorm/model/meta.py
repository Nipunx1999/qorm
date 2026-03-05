"""Model registry: tracks all registered Model subclasses."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .base import Model

# Global registry: tablename -> Model class
_MODEL_REGISTRY: dict[str, type[Model]] = {}


def register_model(cls: type[Model]) -> None:
    """Register a model class in the global registry."""
    tablename = getattr(cls, '__tablename__', None)
    if tablename:
        _MODEL_REGISTRY[tablename] = cls


def get_model(tablename: str) -> type[Model] | None:
    """Look up a model class by table name."""
    return _MODEL_REGISTRY.get(tablename)


def all_models() -> dict[str, type[Model]]:
    """Return all registered models."""
    return dict(_MODEL_REGISTRY)


def clear_registry() -> None:
    """Clear the model registry (useful for testing)."""
    _MODEL_REGISTRY.clear()
