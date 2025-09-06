import datetime as dt
import re
from typing import Any, ClassVar

from limbo_core.context import Context
from limbo_core.yaml_schema.artifacts.data_types import DataType

from .option import (
    ColumnOptionBase,
    ColumnOptionPrimitiveValue,
    ColumnOptionReferenceValue,
)


class OptionFactory:
    """Factory for creating Option instances from raw YAML values."""

    _PREFIX_RE: ClassVar[re.Pattern] = re.compile(r"\{([^:}]+):([^}]+)\}")
    _BOOL_TRUE_VALUES: ClassVar[set[str]] = {"true", "1", "yes", "y", "on"}
    _BOOL_FALSE_VALUES: ClassVar[set[str]] = {"false", "0", "no", "n", "off"}

    def __init__(self, context: Context):
        self._context = context

    def from_raw(self, raw: Any) -> ColumnOptionBase:
        # TODO(Vlad): Maybe use a chain of responsibility pattern here.
        if not isinstance(raw, str):
            return ColumnOptionPrimitiveValue(
                value=raw, data_type=self._get_data_type(raw)
            )

        match = self._PREFIX_RE.findall(raw)
        if not match:
            return ColumnOptionPrimitiveValue(
                value=raw, data_type=self._get_data_type(raw)
            )
        if len(match) > 1:
            raise NotImplementedError

        prefix, content = match[0].group(1).strip().lower(), match[0].group(2)

        option = self._process_special_types(prefix, content)
        if option is not None:
            return option

        option = self._process_primitive_type(prefix, content)
        if option is not None:
            return option

        raise ValueError

    def _process_special_types(
        self, prefix: str, content: str
    ) -> ColumnOptionBase | None:
        match prefix:
            case "ref":
                return ColumnOptionReferenceValue(ref=content)
            case _:
                return None

    def _process_primitive_type(
        self, prefix: str, content: str
    ) -> ColumnOptionBase | None:
        if prefix not in DataType:
            return None
        data_type = DataType(prefix)
        return ColumnOptionPrimitiveValue(
            value=self._cast_by_datatype(data_type, content),
            data_type=data_type,
        )

    def _get_data_type(self, raw: Any) -> DataType:
        match raw:
            case str():
                return DataType.STRING
            case bool():
                return DataType.BOOLEAN
            case int():
                return DataType.INTEGER
            case float():
                return DataType.FLOAT
            case dt.date():
                return DataType.DATE
            case dt.datetime():
                return DataType.DATETIME
            case _:
                return DataType.STRING

    def _cast_by_datatype(self, data_type: DataType, raw: str) -> Any:
        match data_type:
            case DataType.STRING:
                return raw
            case DataType.INTEGER:
                return int(raw)
            case DataType.FLOAT:
                return float(raw)
            case DataType.BOOLEAN:
                return self._parse_bool(raw)
            case DataType.DATE:
                return dt.date.fromisoformat(raw)
            case DataType.DATETIME:
                return dt.datetime.fromisoformat(raw)
            case DataType.TIMESTAMP:
                coerced = raw.strip()
                if coerced.isdigit():
                    return int(coerced)
                return int(dt.datetime.fromisoformat(coerced).timestamp())
            case _:
                raise ValueError

    def _parse_bool(self, value: str) -> bool:
        lowered = value.strip().lower()
        if lowered in self._BOOL_TRUE_VALUES:
            return True
        if lowered in self._BOOL_FALSE_VALUES:
            return False
        raise ValueError
