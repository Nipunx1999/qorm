"""Model base class with __init_subclass__ registration."""

from __future__ import annotations

from typing import Any, ClassVar, get_type_hints

from ..exc import ModelError
from ..types.coerce import infer_qtype
from .fields import Field
from .meta import register_model


class ModelMeta(type):
    """Metaclass for Model that enables Trade.sym -> Column('sym') syntax."""

    def __getattr__(cls, name: str) -> Any:
        fields = cls.__dict__.get('__fields__')
        if fields and name in fields:
            from ..query.expressions import Column
            return Column(name, cls)
        raise AttributeError(f"type object '{cls.__name__}' has no attribute '{name}'")


class Model(metaclass=ModelMeta):
    """Base class for qorm models.

    Subclasses declare fields using type annotations::

        class Trade(Model):
            __tablename__ = 'trade'
            sym: Symbol
            price: Float
            size: Long
            time: Timestamp

    The ``__init_subclass__`` hook automatically:
    - Introspects annotations to build Field descriptors
    - Registers the model in the global registry
    - Generates ``__init__``, ``__repr__``, and ``__eq__``
    """

    __tablename__: ClassVar[str] = ''
    __fields__: ClassVar[dict[str, Field]] = {}
    __key_fields__: ClassVar[list[str]] = []

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)

        if not cls.__dict__.get('__tablename__'):
            return

        # Skip annotation processing for reflected models
        # (they already have __fields__ set directly)
        if '__fields__' in cls.__dict__ and cls.__dict__['__fields__']:
            register_model(cls)
            return

        fields: dict[str, Field] = {}
        key_fields: list[str] = []

        try:
            hints = get_type_hints(cls, include_extras=True)
        except Exception:
            hints = cls.__annotations__

        for attr_name, annotation in hints.items():
            if attr_name.startswith('_'):
                continue

            default_val = cls.__dict__.get(attr_name)
            if isinstance(default_val, Field):
                fld = default_val
            else:
                fld = Field(default=default_val)

            fld.name = attr_name

            try:
                fld.qtype = infer_qtype(annotation)
            except TypeError as e:
                raise ModelError(
                    f"Cannot determine q type for {cls.__name__}.{attr_name}: {e}"
                ) from e

            fields[attr_name] = fld

            if fld.primary_key:
                key_fields.append(attr_name)

        cls.__fields__ = fields
        cls.__key_fields__ = key_fields
        register_model(cls)

    def __init__(self, **kwargs: Any) -> None:
        for name, fld in self.__fields__.items():
            if name in kwargs:
                setattr(self, name, kwargs[name])
            elif fld.default is not None:
                setattr(self, name, fld.default)
            else:
                setattr(self, name, None)
        # Allow extra kwargs (for result mapping with computed columns)
        for key, value in kwargs.items():
            if key not in self.__fields__:
                setattr(self, key, value)

    def __repr__(self) -> str:
        parts = []
        for name in self.__fields__:
            val = getattr(self, name, None)
            parts.append(f"{name}={val!r}")
        return f"{type(self).__name__}({', '.join(parts)})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, type(self)):
            return NotImplemented
        return all(
            getattr(self, n, None) == getattr(other, n, None)
            for n in self.__fields__
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert model instance to a dictionary."""
        return {name: getattr(self, name, None) for name in self.__fields__}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Model:
        """Create a model instance from a dictionary."""
        return cls(**{k: v for k, v in data.items() if k in cls.__fields__})

    # ── Query shortcuts ────────────────────────────────────────────

    @classmethod
    def select(cls, *columns: Any, **named: Any) -> Any:
        """Start a SELECT query on this model."""
        from ..query.select import SelectQuery
        return SelectQuery(cls, columns=columns, named_columns=named)

    @classmethod
    def insert(cls, rows: list[Model]) -> Any:
        """Create an INSERT query for a list of model instances."""
        from ..query.insert import InsertQuery
        return InsertQuery(cls, rows)

    @classmethod
    def update(cls) -> Any:
        """Start an UPDATE query on this model."""
        from ..query.update import UpdateQuery
        return UpdateQuery(cls)

    @classmethod
    def delete(cls) -> Any:
        """Start a DELETE query on this model."""
        from ..query.delete import DeleteQuery
        return DeleteQuery(cls)
