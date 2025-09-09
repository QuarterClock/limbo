from unittest.mock import MagicMock

import pytest

from limbo_core.context import Context
from limbo_core.errors import ContextMissingError
from limbo_core.yaml_schema.artifacts.data_types import DataType
from limbo_core.yaml_schema.tables.column import TableColumn


@pytest.fixture
def context() -> Context:
    return Context(
        generators={
            "pii.full_name": object(),
            "primary_key.incrementing_id": object(),
        },
        paths={},
    )


def test_table_column_validates_generator(context: Context) -> None:
    column = TableColumn.model_validate(
        {
            "name": "full_name",
            "data_type": DataType.STRING,
            "generator": "pii.full_name",
            "options": {"length": "${integer:5}"},
        },
        context=context,
    )
    assert column.generator == "pii.full_name"
    assert column.options is not None
    assert "length" in column.options


def test_table_column_generator_missing_context_raises() -> None:
    with pytest.raises(ContextMissingError):
        TableColumn.model_validate({
            "name": "full_name",
            "data_type": DataType.STRING,
            "generator": "pii.full_name",
        })


def test_table_column_invalid_generator_raises(context: Context) -> None:
    gen = "unknown.gen"
    with pytest.raises(
        ValueError, match=f"Generator {gen} is not in the context"
    ):
        TableColumn.model_validate(
            {
                "name": "full_name",
                "data_type": DataType.STRING,
                "generator": gen,
            },
            context=context,
        )


def test_table_column_validates_options(context: Context) -> None:
    column = TableColumn.model_validate(
        {
            "name": "full_name",
            "data_type": DataType.STRING,
            "generator": "pii.full_name",
            "options": {"length": "${integer:5}"},
        },
        context=context,
    )
    assert column.options is not None
    assert "length" in column.options
    assert column.options["length"].value == 5


def test_table_column_validates_none_options(context: Context) -> None:
    column = TableColumn.model_validate(
        {
            "name": "full_name",
            "data_type": DataType.STRING,
            "generator": "pii.full_name",
        },
        context=context,
    )
    assert column.options is None


def test_table_column_options_missing_context_raises() -> None:
    mock = MagicMock()
    mock.context = None
    with pytest.raises(ContextMissingError):
        TableColumn.validate_options({"length": "${int:5}"}, mock)
