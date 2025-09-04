import pytest

from limbo_core.yaml_schema import DataType


@pytest.mark.parametrize(
    ("data_type", "expected"),
    [
        (DataType.STRING, "string"),
        (DataType.INTEGER, "integer"),
        (DataType.FLOAT, "float"),
        (DataType.BOOLEAN, "boolean"),
        (DataType.DATE, "date"),
        (DataType.DATETIME, "datetime"),
        (DataType.TIMESTAMP, "timestamp"),
    ],
)
def test_data_type(data_type, expected):
    assert DataType(data_type) == expected


@pytest.mark.parametrize(
    ("data_type", "expected"),
    [
        (DataType.STRING, "string"),
        (DataType.INTEGER, "integer"),
        (DataType.FLOAT, "float"),
        (DataType.BOOLEAN, "boolean"),
        (DataType.DATE, "date"),
        (DataType.DATETIME, "datetime"),
        (DataType.TIMESTAMP, "timestamp"),
    ],
)
def test_data_type_creation(data_type, expected):
    assert DataType(data_type) == expected


@pytest.mark.parametrize("data_type", ["invalid", "123", "true", "false"])
def test_data_type_creation_invalid(data_type):
    with pytest.raises(ValueError):  # noqa: PT011
        DataType(data_type)
