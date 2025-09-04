import re
from datetime import date, datetime
from typing import Any, ClassVar

from limbo_core.yaml_schema import DataType

from .option import OptionBase, OptionPrimitiveValue, OptionReferenceValue


class OptionFactory:
    """Factory for creating Option instances from raw YAML values."""

    _PREFIX_RE: ClassVar[re.Pattern] = re.compile(
        r"^\$\{([^:}]+):(.*)\}$", re.DOTALL
    )
    _BOOL_TRUE_VALUES: ClassVar[set[str]] = {"true", "1", "yes", "y", "on"}
    _BOOL_FALSE_VALUES: ClassVar[set[str]] = {"false", "0", "no", "n", "off"}

    def from_raw(self, raw: Any) -> OptionBase:
        if not isinstance(raw, str):
            return OptionPrimitiveValue(
                value=raw, data_type=self._get_data_type(raw)
            )

        match = self._PREFIX_RE.match(raw)
        if not match:
            return OptionPrimitiveValue(
                value=raw, data_type=self._get_data_type(raw)
            )

        prefix, content = match.group(1).strip().lower(), match.group(2)
        if prefix == "ref":
            return OptionReferenceValue(ref=content)

        dt = DataType(prefix)
        return OptionPrimitiveValue(
            value=self._cast_by_datatype(dt, content), data_type=dt
        )

    def _get_data_type(self, raw: Any) -> DataType:
        if isinstance(raw, str):
            return DataType.STRING
        if isinstance(raw, bool):
            return DataType.BOOLEAN
        if isinstance(raw, int):
            return DataType.INTEGER
        if isinstance(raw, float):
            return DataType.FLOAT
        if isinstance(raw, date):
            return DataType.DATE
        if isinstance(raw, datetime):
            return DataType.DATETIME
        return DataType.STRING

    def _cast_by_datatype(self, data_type: DataType, raw: str) -> Any:
        if data_type is DataType.STRING:
            return raw
        if data_type is DataType.INTEGER:
            return int(raw)
        if data_type is DataType.FLOAT:
            return float(raw)
        if data_type is DataType.BOOLEAN:
            return self._parse_bool(raw)
        if data_type is DataType.DATE:
            return date.fromisoformat(raw)
        if data_type is DataType.DATETIME:
            return datetime.fromisoformat(raw)
        if data_type is DataType.TIMESTAMP:
            coerced = raw.strip()
            if coerced.isdigit():
                return int(coerced)
            return int(datetime.fromisoformat(coerced).timestamp())
        raise ValueError

    def _parse_bool(self, value: str) -> bool:
        lowered = value.strip().lower()
        if lowered in self._BOOL_TRUE_VALUES:
            return True
        if lowered in self._BOOL_FALSE_VALUES:
            return False
        raise ValueError
