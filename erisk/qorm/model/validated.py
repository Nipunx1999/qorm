"""ValidatedModel: qorm Model with pydantic validation.

Requires ``pydantic`` as an optional dependency::

    pip install qorm[pydantic]

Usage::

    from qorm import ValidatedModel, Symbol, Float, Long, field

    class Trade(ValidatedModel):
        __tablename__ = 'trade'
        sym: Symbol = field(min_length=1, max_length=10)
        price: Float = field(gt=0)
        size: Long = field(ge=0)

    Trade(sym="AAPL", price=150.0, size=100)   # OK
    Trade(sym="", price=-1, size=100)           # ValidationError
"""

from __future__ import annotations

from typing import Any, ClassVar, get_type_hints, get_origin, get_args, Annotated

from ..exc import ValidationError
from ..types.base import QType
from .base import Model
from .fields import Field


def _build_pydantic_model(cls: type) -> type:
    """Build a pydantic BaseModel dynamically from a ValidatedModel's __fields__."""
    try:
        import pydantic
    except ImportError:
        raise ImportError(
            "pydantic is required for ValidatedModel. "
            "Install it with: pip install qorm[pydantic]"
        )

    pydantic_fields: dict[str, Any] = {}

    # Get the original type hints to extract the base Python types
    try:
        hints = get_type_hints(cls, include_extras=True)
    except Exception:
        hints = getattr(cls, '__annotations__', {})

    for name, fld in cls.__fields__.items():
        # Determine the Python type from the annotation
        annotation = hints.get(name)
        python_type: Any = Any
        if annotation is not None:
            origin = get_origin(annotation)
            if origin is Annotated:
                args = get_args(annotation)
                python_type = args[0]
            elif isinstance(annotation, type):
                python_type = annotation

        # Allow None since qorm fields default to None
        if fld.nullable:
            python_type = python_type | None

        # Build pydantic FieldInfo with constraints from our Field
        field_kwargs: dict[str, Any] = {}
        if fld.default is not None:
            field_kwargs['default'] = fld.default
        elif fld.nullable:
            field_kwargs['default'] = None

        # Numeric constraints
        if fld.gt is not None:
            field_kwargs['gt'] = fld.gt
        if fld.ge is not None:
            field_kwargs['ge'] = fld.ge
        if fld.lt is not None:
            field_kwargs['lt'] = fld.lt
        if fld.le is not None:
            field_kwargs['le'] = fld.le

        # String constraints
        if fld.min_length is not None:
            field_kwargs['min_length'] = fld.min_length
        if fld.max_length is not None:
            field_kwargs['max_length'] = fld.max_length
        if fld.pattern is not None:
            field_kwargs['pattern'] = fld.pattern

        pydantic_fields[name] = (python_type, pydantic.Field(**field_kwargs))

    # Create a dynamic pydantic model
    validator_model = pydantic.create_model(
        f'{cls.__name__}Validator',
        **pydantic_fields,
    )

    # Attach custom field-level validators
    for name, fld in cls.__fields__.items():
        if fld.validator is not None:
            # We'll call custom validators separately in __init__
            pass

    return validator_model


class ValidatedModel(Model):
    """Model subclass that validates field values using pydantic.

    Add constraints to fields using the ``field()`` helper::

        class Trade(ValidatedModel):
            __tablename__ = 'trade'
            sym: Symbol = field(min_length=1, pattern=r'^[A-Z]+$')
            price: Float = field(gt=0)
            size: Long = field(ge=0, le=1_000_000)

    Custom validators::

        def check_sym(value):
            if value and value not in VALID_SYMS:
                raise ValueError(f"Unknown symbol: {value}")
            return value

        class Trade(ValidatedModel):
            __tablename__ = 'trade'
            sym: Symbol = field(validator=check_sym)
    """

    __pydantic_model__: ClassVar[type | None] = None
    __validate__: ClassVar[bool] = True

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)

        if not cls.__dict__.get('__tablename__'):
            return

        # Build the pydantic validator model (lazily on first use
        # if pydantic is not installed at class definition time)
        cls.__pydantic_model__ = None  # reset for rebuild

    def __init__(self, *, _validate: bool | None = None, **kwargs: Any) -> None:
        should_validate = _validate if _validate is not None else self.__validate__

        if should_validate:
            self._run_validation(kwargs)

        super().__init__(**kwargs)

    def _run_validation(self, kwargs: dict[str, Any]) -> None:
        """Validate kwargs using pydantic and custom validators."""
        try:
            import pydantic
        except ImportError:
            raise ImportError(
                "pydantic is required for ValidatedModel. "
                "Install it with: pip install qorm[pydantic]"
            )

        # Lazily build the pydantic model on first validation
        if self.__pydantic_model__ is None:
            type(self).__pydantic_model__ = _build_pydantic_model(type(self))

        # Run pydantic validation
        try:
            self.__pydantic_model__.model_validate(kwargs)
        except pydantic.ValidationError as e:
            raise ValidationError(str(e), errors=e.errors()) from e

        # Run custom per-field validators
        for name, fld in self.__fields__.items():
            if fld.validator is not None and name in kwargs:
                value = kwargs[name]
                if value is not None:
                    try:
                        result = fld.validator(value)
                        if result is not None:
                            kwargs[name] = result
                    except (ValueError, TypeError) as e:
                        raise ValidationError(
                            f"Validation error for field '{name}': {e}",
                            errors=[{
                                'loc': (name,),
                                'msg': str(e),
                                'type': 'value_error',
                            }],
                        ) from e
