"""Unit tests for ValidatedModel with pydantic validation."""

import pytest

from qorm import ValidatedModel, Symbol, Float, Long, Int, field, ValidationError
from qorm.model.meta import clear_registry


class TestValidatedModel:
    def setup_method(self):
        clear_registry()

    def test_valid_instance(self):
        class Trade(ValidatedModel):
            __tablename__ = 'v_trade1'
            sym: Symbol = field(min_length=1)
            price: Float = field(gt=0)
            size: Long = field(ge=0)

        t = Trade(sym="AAPL", price=150.0, size=100)
        assert t.sym == "AAPL"
        assert t.price == 150.0
        assert t.size == 100

    def test_gt_violation(self):
        class Trade(ValidatedModel):
            __tablename__ = 'v_trade2'
            price: Float = field(gt=0)

        with pytest.raises(ValidationError):
            Trade(price=-1.0)

    def test_ge_violation(self):
        class Trade(ValidatedModel):
            __tablename__ = 'v_trade3'
            size: Long = field(ge=0)

        with pytest.raises(ValidationError):
            Trade(size=-1)

    def test_lt_violation(self):
        class Trade(ValidatedModel):
            __tablename__ = 'v_trade4'
            size: Long = field(lt=100)

        with pytest.raises(ValidationError):
            Trade(size=100)

    def test_le_violation(self):
        class Trade(ValidatedModel):
            __tablename__ = 'v_trade5'
            size: Long = field(le=100)

        with pytest.raises(ValidationError):
            Trade(size=101)

    def test_min_length_violation(self):
        class Trade(ValidatedModel):
            __tablename__ = 'v_trade6'
            sym: Symbol = field(min_length=1)

        with pytest.raises(ValidationError):
            Trade(sym="")

    def test_max_length_violation(self):
        class Trade(ValidatedModel):
            __tablename__ = 'v_trade7'
            sym: Symbol = field(max_length=5)

        with pytest.raises(ValidationError):
            Trade(sym="VERYLONGSYMBOL")

    def test_pattern_violation(self):
        class Trade(ValidatedModel):
            __tablename__ = 'v_trade8'
            sym: Symbol = field(pattern=r'^[A-Z]+$')

        with pytest.raises(ValidationError):
            Trade(sym="aapl")

    def test_pattern_pass(self):
        class Trade(ValidatedModel):
            __tablename__ = 'v_trade9'
            sym: Symbol = field(pattern=r'^[A-Z]+$')

        t = Trade(sym="AAPL")
        assert t.sym == "AAPL"

    def test_nullable_default_none(self):
        class Trade(ValidatedModel):
            __tablename__ = 'v_trade10'
            sym: Symbol
            price: Float = field(gt=0)

        # None should be allowed for nullable fields (default)
        t = Trade()
        assert t.sym is None
        assert t.price is None

    def test_custom_validator(self):
        valid_syms = {"AAPL", "GOOG", "MSFT"}

        def check_sym(value):
            if value not in valid_syms:
                raise ValueError(f"Unknown symbol: {value}")
            return value

        class Trade(ValidatedModel):
            __tablename__ = 'v_trade11'
            sym: Symbol = field(validator=check_sym)

        t = Trade(sym="AAPL")
        assert t.sym == "AAPL"

        with pytest.raises(ValidationError, match="Unknown symbol"):
            Trade(sym="INVALID")

    def test_custom_validator_transforms_value(self):
        def uppercase(value):
            return value.upper()

        class Trade(ValidatedModel):
            __tablename__ = 'v_trade12'
            sym: Symbol = field(validator=uppercase)

        t = Trade(sym="aapl")
        assert t.sym == "AAPL"

    def test_skip_validation(self):
        class Trade(ValidatedModel):
            __tablename__ = 'v_trade13'
            price: Float = field(gt=0)

        # Should not raise when validation is skipped
        t = Trade(price=-1.0, _validate=False)
        assert t.price == -1.0

    def test_class_level_validate_flag(self):
        class Trade(ValidatedModel):
            __tablename__ = 'v_trade14'
            __validate__ = False
            price: Float = field(gt=0)

        # Should not raise when class-level validation is off
        t = Trade(price=-1.0)
        assert t.price == -1.0

    def test_validation_error_has_errors(self):
        class Trade(ValidatedModel):
            __tablename__ = 'v_trade15'
            price: Float = field(gt=0)

        with pytest.raises(ValidationError) as exc_info:
            Trade(price=-1.0)

        assert len(exc_info.value.errors) > 0

    def test_is_still_a_model(self):
        class Trade(ValidatedModel):
            __tablename__ = 'v_trade16'
            sym: Symbol
            price: Float

        t = Trade(sym="AAPL", price=150.0)
        assert t.to_dict() == {'sym': 'AAPL', 'price': 150.0}
        assert 'sym' in Trade.__fields__
        assert 'price' in Trade.__fields__

    def test_combined_constraints(self):
        class Trade(ValidatedModel):
            __tablename__ = 'v_trade17'
            sym: Symbol = field(min_length=1, max_length=10, pattern=r'^[A-Z.]+$')
            price: Float = field(gt=0, lt=1_000_000)
            size: Long = field(ge=1, le=1_000_000)

        t = Trade(sym="AAPL", price=150.0, size=100)
        assert t.sym == "AAPL"

        with pytest.raises(ValidationError):
            Trade(sym="", price=150.0, size=100)

        with pytest.raises(ValidationError):
            Trade(sym="AAPL", price=0, size=100)

        with pytest.raises(ValidationError):
            Trade(sym="AAPL", price=150.0, size=0)

    def test_field_with_default_and_constraint(self):
        class Trade(ValidatedModel):
            __tablename__ = 'v_trade18'
            size: Long = field(default=100, ge=0)

        t = Trade()
        assert t.size == 100

        with pytest.raises(ValidationError):
            Trade(size=-1)
