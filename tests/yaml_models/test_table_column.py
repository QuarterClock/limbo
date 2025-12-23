"""Tests for TableColumn model validation."""

from unittest.mock import MagicMock

import pytest

from limbo_core.context import Context
from limbo_core.errors import ContextMissingError
from limbo_core.yaml_schema.artifacts.data_types import DataType
from limbo_core.yaml_schema.tables.column import TableColumn


class TestTableColumnGeneratorValidation:
    """Test cases for TableColumn generator validation."""

    @pytest.fixture
    def context(self) -> Context:
        """Create a context with test generators."""
        return Context(
            generators={
                "pii.full_name": object(),
                "primary_key.incrementing_id": object(),
            },
            paths={},
        )

    def test_validates_generator(self, context: Context) -> None:
        """Verify valid generator is accepted."""
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

    def test_missing_context_raises(self) -> None:
        """Verify missing context raises ContextMissingError."""
        with pytest.raises(ContextMissingError):
            TableColumn.model_validate({
                "name": "full_name",
                "data_type": DataType.STRING,
                "generator": "pii.full_name",
            })

    def test_invalid_generator_raises(self, context: Context) -> None:
        """Verify invalid generator raises ValueError."""
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


class TestTableColumnOptionsValidation:
    """Test cases for TableColumn options validation."""

    @pytest.fixture
    def context(self) -> Context:
        """Create a context with test generators."""
        return Context(
            generators={
                "pii.full_name": object(),
                "primary_key.incrementing_id": object(),
            },
            paths={},
        )

    def test_validates_options(self, context: Context) -> None:
        """Verify options are parsed correctly."""
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

    def test_validates_none_options(self, context: Context) -> None:
        """Verify column without options has None options."""
        column = TableColumn.model_validate(
            {
                "name": "full_name",
                "data_type": DataType.STRING,
                "generator": "pii.full_name",
            },
            context=context,
        )
        assert column.options is None

    def test_options_missing_context_raises(self) -> None:
        """Verify options validation fails without context."""
        mock = MagicMock()
        mock.context = None

        with pytest.raises(ContextMissingError):
            TableColumn.validate_options({"length": "${int:5}"}, mock)
