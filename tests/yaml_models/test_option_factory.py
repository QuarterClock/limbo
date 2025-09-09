import datetime as dt
from datetime import datetime
from typing import Any

import pytest

from limbo_core.context import Context
from limbo_core.yaml_schema.artifacts.data_types import DataType
from limbo_core.yaml_schema.tables.option import (
    ColumnOptionPrimitiveValue,
    ColumnOptionReferenceValue,
)
from limbo_core.yaml_schema.tables.option_factory import OptionFactory


@pytest.fixture
def context() -> Context:
    return Context(generators={}, paths={})


def test_from_raw_primitive_str(context: Context) -> None:
    factory = OptionFactory(context)
    opt = factory.from_raw("hello")
    assert isinstance(opt, ColumnOptionPrimitiveValue)
    assert opt.value == "hello"
    assert opt.data_type is DataType.STRING


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("hello", DataType.STRING),
        (123, DataType.INTEGER),
        (12.5, DataType.FLOAT),
        (True, DataType.BOOLEAN),
        (dt.date(2024, 1, 2), DataType.DATE),
        (dt.datetime(2024, 1, 2, 3, 4, 5, tzinfo=dt.UTC), DataType.DATETIME),
    ],
)
def test_from_raw_non_str_primitives(
    context: Context, value: Any, expected: DataType
) -> None:
    factory = OptionFactory(context)
    assert factory.from_raw(value).data_type is expected


@pytest.mark.parametrize(
    ("expr", "expected"),
    [
        ("${string:hello}", (DataType.STRING, "hello")),
        ("${integer:42}", (DataType.INTEGER, 42)),
        ("${float:3.2}", (DataType.FLOAT, 3.2)),
        ("${boolean:yes}", (DataType.BOOLEAN, True)),
        ("${boolean:NO}", (DataType.BOOLEAN, False)),
        ("${date:2024-01-02}", (DataType.DATE, dt.date(2024, 1, 2))),
        (
            "${datetime:2024-01-02T03:04:05}",
            (
                DataType.DATETIME,
                dt.datetime(2024, 1, 2, 3, 4, 5, tzinfo=dt.UTC),
            ),
        ),
        (
            "${datetime:2024-01-02T03:04:05+00:00}",
            (
                DataType.DATETIME,
                dt.datetime(2024, 1, 2, 3, 4, 5, tzinfo=dt.UTC),
            ),
        ),
    ],
)
def test_from_raw_with_primitive_prefix(context: Context, expr: str, expected):
    factory = OptionFactory(context)
    opt = factory.from_raw(expr)
    assert isinstance(opt, ColumnOptionPrimitiveValue)
    assert opt.data_type is expected[0]
    # For date/datetime we only check type due to tz/system differences
    if expected[1] is not None:
        assert opt.value == expected[1]


def test_boolean_unknown_value_raises(context: Context) -> None:
    factory = OptionFactory(context)
    with pytest.raises(
        ValueError, match="Value unknown is not a valid boolean"
    ):
        factory.from_raw("${boolean:unknown}")


def test_from_raw_timestamp_variants(context: Context) -> None:
    factory = OptionFactory(context)
    ts_opt = factory.from_raw("${timestamp:1710000000}")
    assert isinstance(ts_opt, ColumnOptionPrimitiveValue)
    assert ts_opt.data_type is DataType.TIMESTAMP
    assert ts_opt.value == 1710000000

    iso = "2024-01-02T03:04:05"
    expected = int(datetime.fromisoformat(iso).timestamp())
    ts_from_iso = factory.from_raw(f"${{timestamp:{iso}}}")
    assert ts_from_iso.value == expected


def test_from_raw_reference(context: Context) -> None:
    factory = OptionFactory(context)
    opt = factory.from_raw("${ref:users.id}")
    assert isinstance(opt, ColumnOptionReferenceValue)
    assert opt.ref == "users.id"
    assert opt.serialize() == "${ref:users.id}"


def test_from_raw_multiple_prefixes_unsupported(context: Context) -> None:
    factory = OptionFactory(context)
    with pytest.raises(NotImplementedError):
        factory.from_raw("${string:a}${string:b}")


def test_from_raw_invalid_prefix_raises(context: Context) -> None:
    factory = OptionFactory(context)
    with pytest.raises(
        ValueError, match="Option prefix unknown is not supported"
    ):
        factory.from_raw("${unknown:value}")


def test_serialize_primitive_roundtrip(context: Context) -> None:
    factory = OptionFactory(context)
    opt = factory.from_raw("${integer:5}")
    assert opt.serialize() == "${integer:5}"
