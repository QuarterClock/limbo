"""Domain tests for structured interpolation value specs."""

from __future__ import annotations

import datetime as dt
from typing import Any

import pytest

from limbo_core.application.parsers.common import InvalidValueSpecError
from limbo_core.application.parsers.value_spec_parser import (
    DataTypeInferenceError,
    InvalidLookupValueSpecError,
    InvalidReferenceValueSpecError,
    InvalidTypedValueSpecError,
    infer_data_type,
    parse_lookup_value,
    parse_value_spec,
)
from limbo_core.domain.entities import DataType
from limbo_core.domain.entities.values import (
    LiteralValue,
    LookupValue,
    ReferenceValue,
)


class TestInferDataType:
    """Tests for infer_data_type mapping logic."""

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            ("x", DataType.STRING),
            (True, DataType.BOOLEAN),
            (1, DataType.INTEGER),
            (1.5, DataType.FLOAT),
            (
                dt.datetime(2024, 1, 2, 3, 4, 5, tzinfo=dt.UTC),
                DataType.DATETIME,
            ),
            (dt.date(2024, 1, 2), DataType.DATE),
        ],
    )
    def test_supported_values(self, value: Any, expected: DataType) -> None:
        """infer_data_type maps supported Python values to DataType."""
        assert infer_data_type(value) == expected

    def test_unsupported_value_raises(self) -> None:
        """Unsupported values raise DataTypeInferenceError."""
        with pytest.raises(
            DataTypeInferenceError, match="Cannot infer data type for: object"
        ):
            infer_data_type(object())


class TestParseValueSpec:
    """Tests for parse_value_spec dispatch logic."""

    def test_literal_scalar(self) -> None:
        """Scalar values map to LiteralValue with inferred data type."""
        spec = parse_value_spec(5)
        assert spec == LiteralValue(value=5, data_type=DataType.INTEGER)

    def test_reference_mapping(self) -> None:
        """`{ref: ...}` maps to a ReferenceValue with no data_type."""
        spec = parse_value_spec({"ref": "users.id"})
        assert spec == ReferenceValue(ref="users.id")
        assert isinstance(spec, ReferenceValue)
        assert spec.data_type is None

    def test_lookup_mapping(self) -> None:
        """`{value_from: ...}` maps to LookupValue with no data_type."""
        spec = parse_value_spec({
            "value_from": {"reader": "env", "key": "DB_HOST", "default": "db"}
        })
        assert spec == LookupValue(reader="env", key="DB_HOST", default="db")
        assert isinstance(spec, LookupValue)
        assert spec.data_type is None

    def test_rejects_invalid_mapping_shape(self) -> None:
        """Unknown mapping shape fails fast with explicit error."""
        with pytest.raises(
            InvalidValueSpecError,
            match=r"expects `ref`, `value`, or `value_from`",
        ):
            parse_value_spec({"literal": "x"})

    def test_rejects_invalid_ref(self) -> None:
        """Reference mapping must use string `ref` value."""
        with pytest.raises(
            InvalidReferenceValueSpecError, match=r"expects a string"
        ):
            parse_value_spec({"ref": 123})

    def test_returns_existing_objects(self) -> None:
        """Value spec parser is identity for already-parsed specs."""
        literal = LiteralValue(value=1, data_type=DataType.INTEGER)
        reference = ReferenceValue(ref="users.id")
        lookup = LookupValue(reader="env", key="DB_HOST")
        assert parse_value_spec(literal) is literal
        assert parse_value_spec(reference) is reference
        assert parse_value_spec(lookup) is lookup


class TestParseLookupValue:
    """Tests for parse_lookup_value validation and parsing."""

    def test_rejects_invalid_payload(self) -> None:
        """Lookup parser validates required keys and string types."""
        with pytest.raises(
            InvalidLookupValueSpecError,
            match=r"`value_from\.reader` expects a string",
        ):
            parse_lookup_value({"value_from": {"key": "DB_HOST"}})

    def test_returns_existing_object(self) -> None:
        """Lookup parser is identity for already-parsed lookup values."""
        lookup = LookupValue(reader="env", key="DB_HOST")
        assert parse_lookup_value(lookup) is lookup

    def test_rejects_non_mapping_input(self) -> None:
        """Lookup parser requires mapping-shaped payloads."""
        with pytest.raises(
            InvalidLookupValueSpecError, match=r"expects a mapping payload"
        ):
            parse_lookup_value("DB_HOST")

    def test_rejects_non_mapping_value_from(self) -> None:
        """Nested value_from payload must be a mapping."""
        with pytest.raises(
            InvalidLookupValueSpecError, match=r"`value_from` expects a mapping"
        ):
            parse_lookup_value({"value_from": "env:DB_HOST"})

    def test_rejects_non_string_key(self) -> None:
        """Lookup key must be provided as a string."""
        with pytest.raises(
            InvalidLookupValueSpecError,
            match=r"`value_from\.key` expects a string",
        ):
            parse_lookup_value({"value_from": {"reader": "env", "key": 123}})

    def test_rejects_non_string_default(self) -> None:
        """Lookup default must be a string when present."""
        with pytest.raises(
            InvalidLookupValueSpecError,
            match=r"`value_from\.default` expects a string",
        ):
            parse_lookup_value({
                "value_from": {"reader": "env", "key": "DB_HOST", "default": 1}
            })


class TestExplicitTypedValueSpec:
    """Tests for explicit ``data_type`` annotations on value specs."""

    def test_typed_literal(self) -> None:
        """Explicit data_type overrides inference for literals."""
        spec = parse_value_spec({
            "data_type": "datetime",
            "value": "2024-01-01",
        })
        assert spec == LiteralValue(
            value="2024-01-01", data_type=DataType.DATETIME
        )

    def test_typed_literal_infers_when_no_data_type(self) -> None:
        """``{value: ...}`` without data_type still infers the type."""
        spec = parse_value_spec({"value": 42})
        assert spec == LiteralValue(value=42, data_type=DataType.INTEGER)

    def test_typed_reference(self) -> None:
        """Explicit data_type is stored on ReferenceValue."""
        spec = parse_value_spec({"data_type": "integer", "ref": "users.id"})
        assert spec == ReferenceValue(
            ref="users.id", data_type=DataType.INTEGER
        )

    def test_typed_lookup(self) -> None:
        """Explicit data_type is stored on LookupValue."""
        spec = parse_value_spec({
            "data_type": "string",
            "value_from": {"reader": "env", "key": "DB_HOST"},
        })
        assert spec == LookupValue(
            reader="env", key="DB_HOST", data_type=DataType.STRING
        )

    def test_rejects_invalid_data_type_value(self) -> None:
        """Unknown data_type string raises InvalidTypedValueSpecError."""
        with pytest.raises(
            InvalidTypedValueSpecError,
            match=r"`data_type` unknown value: 'unsupported'",
        ):
            parse_value_spec({"data_type": "unsupported", "value": "x"})

    def test_rejects_non_string_data_type(self) -> None:
        """Non-string data_type raises InvalidTypedValueSpecError."""
        with pytest.raises(
            InvalidTypedValueSpecError, match=r"`data_type` expects a string"
        ):
            parse_value_spec({"data_type": 123, "value": "x"})

    def test_rejects_data_type_without_value_key(self) -> None:
        """data_type alone without value/ref/value_from is rejected."""
        with pytest.raises(
            InvalidTypedValueSpecError,
            match=r"requires a `value`, `ref`, or `value_from` key",
        ):
            parse_value_spec({"data_type": "integer"})
