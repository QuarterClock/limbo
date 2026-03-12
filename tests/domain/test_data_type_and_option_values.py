"""Domain tests for data types and option value specs."""

from __future__ import annotations

import datetime as dt

import pytest

from limbo_core.application.parsers.common import InvalidValueSpecError
from limbo_core.application.parsers.value_spec_parser import parse_value_spec
from limbo_core.domain.entities import (
    DataType,
    LiteralValue,
    LookupValue,
    ReferenceValue,
)


class TestDataType:
    """Tests for the DataType enum."""

    def test_roundtrip_values(self) -> None:
        """Known data types convert from and to canonical string values."""
        expected = [
            "string",
            "integer",
            "float",
            "boolean",
            "date",
            "datetime",
            "timestamp",
        ]
        assert [DataType(value).value for value in expected] == expected

    def test_invalid_value_raises(self) -> None:
        """Unknown data type strings are rejected."""
        with pytest.raises(
            ValueError, match="'unsupported' is not a valid DataType"
        ):
            DataType("unsupported")


class TestOptionValueSpec:
    """Tests for option value spec parsing via parse_value_spec."""

    def test_from_plain_string(self) -> None:
        """Plain strings parse to literal value specs."""
        option = parse_value_spec("hello")
        assert option == LiteralValue(value="hello", data_type=DataType.STRING)

    def test_from_scalar_values(self) -> None:
        """Scalar option values preserve type inference."""
        integer = parse_value_spec(42)
        date = parse_value_spec(dt.date(2024, 1, 2))
        assert integer == LiteralValue(value=42, data_type=DataType.INTEGER)
        assert date == LiteralValue(
            value=dt.date(2024, 1, 2), data_type=DataType.DATE
        )

    def test_from_reference(self) -> None:
        """Reference options parse to reference value specs."""
        option = parse_value_spec({"ref": "users.id"})
        assert option == ReferenceValue(ref="users.id")

    def test_from_lookup(self) -> None:
        """Lookup options parse to lookup value specs."""
        option = parse_value_spec({
            "value_from": {"reader": "env", "key": "LENGTH", "default": "5"}
        })
        assert option == LookupValue(reader="env", key="LENGTH", default="5")

    def test_from_explicit_typed_literal(self) -> None:
        """Explicit data_type on literal overrides inference."""
        option = parse_value_spec({
            "data_type": "datetime",
            "value": "2024-01-01",
        })
        assert option == LiteralValue(
            value="2024-01-01", data_type=DataType.DATETIME
        )

    def test_from_typed_reference(self) -> None:
        """Explicit data_type is carried on reference specs."""
        option = parse_value_spec({"data_type": "integer", "ref": "users.id"})
        assert option == ReferenceValue(
            ref="users.id", data_type=DataType.INTEGER
        )

    def test_from_typed_lookup(self) -> None:
        """Explicit data_type is carried on lookup specs."""
        option = parse_value_spec({
            "data_type": "boolean",
            "value_from": {"reader": "env", "key": "FLAG"},
        })
        assert option == LookupValue(
            reader="env", key="FLAG", data_type=DataType.BOOLEAN
        )

    def test_invalid_mapping_shape_raises(self) -> None:
        """Unknown mapping shapes fail with explicit spec errors."""
        with pytest.raises(InvalidValueSpecError):
            parse_value_spec({"literal": 1})
