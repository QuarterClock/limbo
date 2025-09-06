import pytest

from limbo_core.yaml_schema.artifacts import DataType


@pytest.mark.parametrize(
    ("data_type", "expected"), [(enum, enum.value) for enum in DataType]
)
def test_data_type(data_type, expected):
    assert DataType(data_type) == expected


@pytest.mark.parametrize(
    ("data_type", "expected"), [(enum, enum.value) for enum in DataType]
)
def test_data_type_creation(data_type, expected):
    assert DataType(data_type) == expected


@pytest.mark.parametrize("data_type", ["invalid", "123", "true", "false"])
def test_data_type_creation_invalid(data_type):
    with pytest.raises(ValueError):  # noqa: PT011
        DataType(data_type)
