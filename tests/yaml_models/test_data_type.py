"""Tests for DataType enum validation."""

import pytest

from limbo_core.yaml_schema.artifacts import DataType


class TestDataType:
    """Test cases for DataType enum."""

    @pytest.mark.parametrize(
        ("data_type", "expected"), [(enum, enum.value) for enum in DataType]
    )
    def test_data_type_returns_correct_value(
        self, data_type: DataType, expected: str
    ) -> None:
        """Verify that DataType enum returns correct value."""
        assert DataType(data_type) == expected

    @pytest.mark.parametrize(
        ("data_type", "expected"), [(enum, enum.value) for enum in DataType]
    )
    def test_data_type_creation_from_value(
        self, data_type: DataType, expected: str
    ) -> None:
        """Verify that DataType can be created from its value."""
        assert DataType(data_type) == expected

    @pytest.mark.parametrize(
        "invalid_type", ["invalid", "123", "true", "false"]
    )
    def test_data_type_creation_with_invalid_value_raises(
        self, invalid_type: str
    ) -> None:
        """Verify that invalid data types raise ValueError."""
        with pytest.raises(ValueError):  # noqa: PT011
            DataType(invalid_type)
