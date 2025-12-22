import datetime as dt
from datetime import datetime
from typing import Any

import pytest

from limbo_core.yaml_schema.artifacts.data_types import DataType
from limbo_core.yaml_schema.tables.option import (
    ColumnOptionBase,
    ColumnOptionPrimitiveValue,
    ColumnOptionReferenceValue,
)


class TestColumnOptionFromRaw:
    def test_plain_string(self) -> None:
        opt = ColumnOptionBase.from_raw("hello")
        assert isinstance(opt, ColumnOptionPrimitiveValue)
        assert opt.value == "hello"
        assert opt.data_type is DataType.STRING

    @pytest.mark.parametrize(
        ("value", "expected_type"),
        [
            ("hello", DataType.STRING),
            (123, DataType.INTEGER),
            (12.5, DataType.FLOAT),
            (True, DataType.BOOLEAN),
            (dt.date(2024, 1, 2), DataType.DATE),
            (
                dt.datetime(2024, 1, 2, 3, 4, 5, tzinfo=dt.UTC),
                DataType.DATETIME,
            ),
        ],
    )
    def test_non_string_primitives(
        self, value: Any, expected_type: DataType
    ) -> None:
        opt = ColumnOptionBase.from_raw(value)
        assert opt.data_type is expected_type

    @pytest.mark.parametrize(
        ("expr", "expected_type", "expected_value"),
        [
            ("${string:hello}", DataType.STRING, "hello"),
            ("${integer:42}", DataType.INTEGER, 42),
            ("${float:3.2}", DataType.FLOAT, 3.2),
            ("${boolean:yes}", DataType.BOOLEAN, True),
            ("${boolean:NO}", DataType.BOOLEAN, False),
            ("${date:2024-01-02}", DataType.DATE, dt.date(2024, 1, 2)),
            (
                "${datetime:2024-01-02T03:04:05}",
                DataType.DATETIME,
                dt.datetime(2024, 1, 2, 3, 4, 5, tzinfo=dt.UTC),
            ),
            (
                "${datetime:2024-01-02T03:04:05+00:00}",
                DataType.DATETIME,
                dt.datetime(2024, 1, 2, 3, 4, 5, tzinfo=dt.UTC),
            ),
        ],
    )
    def test_prefixed_primitives(
        self, expr: str, expected_type: DataType, expected_value: Any
    ) -> None:
        opt = ColumnOptionBase.from_raw(expr)
        assert isinstance(opt, ColumnOptionPrimitiveValue)
        assert opt.data_type is expected_type
        assert opt.value == expected_value

    def test_boolean_invalid_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid boolean value"):
            ColumnOptionBase.from_raw("${boolean:unknown}")

    def test_timestamp_integer(self) -> None:
        opt = ColumnOptionBase.from_raw("${timestamp:1710000000}")
        assert isinstance(opt, ColumnOptionPrimitiveValue)
        assert opt.data_type is DataType.TIMESTAMP
        assert opt.value == 1710000000

    def test_timestamp_iso(self) -> None:
        iso = "2024-01-02T03:04:05"
        expected = int(datetime.fromisoformat(iso).timestamp())
        opt = ColumnOptionBase.from_raw(f"${{timestamp:{iso}}}")
        assert opt.value == expected

    def test_reference(self) -> None:
        opt = ColumnOptionBase.from_raw("${ref:users.id}")
        assert isinstance(opt, ColumnOptionReferenceValue)
        assert opt.ref == "users.id"
        assert opt.serialize() == "${ref:users.id}"

    def test_multiple_prefixes_treated_as_string(self) -> None:
        """Multiple prefixes in a string are treated as a plain string."""
        opt = ColumnOptionBase.from_raw("${string:a}${string:b}")
        assert isinstance(opt, ColumnOptionPrimitiveValue)
        assert opt.data_type is DataType.STRING
        assert opt.value == "${string:a}${string:b}"

    def test_invalid_prefix_raises(self) -> None:
        with pytest.raises(ValueError, match="Unknown option prefix"):
            ColumnOptionBase.from_raw("${unknown:value}")

    def test_serialize_roundtrip(self) -> None:
        opt = ColumnOptionBase.from_raw("${integer:5}")
        assert opt.serialize() == "${integer:5}"
