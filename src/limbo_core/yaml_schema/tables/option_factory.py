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

    _PREFIX_RE: ClassVar[re.Pattern] = re.compile(r"\$\{([^:}]+):([^}]+)\}")
    _BOOL_TRUE_VALUES: ClassVar[set[str]] = {"true", "1", "yes", "y", "on"}
    _BOOL_FALSE_VALUES: ClassVar[set[str]] = {"false", "0", "no", "n", "off"}

    def __init__(self, context: Context) -> None:
        """Initialize the option factory.

        Args:
            context: The context to use for the option factory.
        """
        self._context = context

    def from_raw(self, raw: Any) -> ColumnOptionBase:
        """Create an option from a raw YAML value.

        Args:
            raw: The raw YAML value to create an option from.

        Returns:
            The created option.

        Raises:
            ValueError: If the raw YAML value is not a valid option.
            NotImplementedError: If the raw YAML value is not supported.
        """
        # TODO(Vlad): Maybe use a chain of responsibility pattern here.
        if not isinstance(raw, str):
            return ColumnOptionPrimitiveValue(
                value=raw, data_type=self._get_data_type(raw)
            )

        match = list(self._PREFIX_RE.finditer(raw))
        if not match:
            return ColumnOptionPrimitiveValue(
                value=raw, data_type=self._get_data_type(raw)
            )
        if len(match) > 1:
            raise NotImplementedError("Multiple option prefixes found")

        prefix, content = match[0].group(1).strip().lower(), match[0].group(2)

        option = self._process_special_types(prefix, content)
        if option is not None:
            return option

        option = self._process_primitive_type(prefix, content)
        if option is not None:
            return option

        raise ValueError(f"Option prefix {prefix} is not supported")

    def _process_special_types(
        self, prefix: str, content: str
    ) -> ColumnOptionBase | None:
        """Process a special type option.

        Args:
            prefix: The prefix of the option.
            content: The content of the option.

        Returns:
            The processed option.
        """
        match prefix:
            case "ref":
                return ColumnOptionReferenceValue(ref=content)
            case _:
                return None

    def _process_primitive_type(
        self, prefix: str, content: str
    ) -> ColumnOptionBase | None:
        """Process a primitive type option.

        Args:
            prefix: The prefix of the option.
            content: The content of the option.

        Returns:
            The processed option.
        """
        # TODO(Vlad): Consider raising a specific error here.
        try:
            data_type = DataType(prefix)
        except ValueError:
            return None
        return ColumnOptionPrimitiveValue(
            value=self._cast_by_datatype(data_type, content),
            data_type=data_type,
        )

    def _get_data_type(self, raw: Any) -> DataType:
        """Get the data type of a raw YAML value.

        Args:
            raw: The raw YAML value to get the data type of.

        Returns:
            The data type of the raw YAML value.
        """
        match raw:
            case str():
                return DataType.STRING
            case bool():
                return DataType.BOOLEAN
            case int():
                return DataType.INTEGER
            case float():
                return DataType.FLOAT
            case dt.datetime():
                return DataType.DATETIME
            case dt.date():
                return DataType.DATE
        raise NotImplementedError(f"Data type {type(raw)} is not supported")

    def _cast_by_datatype(self, data_type: DataType, raw: str) -> Any:
        """Cast a raw YAML value to a specific data type.

        Args:
            data_type: The data type to cast the raw YAML value to.
            raw: The raw YAML value to cast.

        Returns:
            The casted value.

        Raises:
            NotImplementedError: If the value could not be cast to the given
            data type.
        """
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
                parsed = dt.datetime.fromisoformat(raw)
                if parsed.tzinfo is None:
                    return parsed.replace(tzinfo=dt.UTC)
                return parsed
            case DataType.TIMESTAMP:
                coerced = raw.strip()
                if coerced.isdigit():
                    return int(coerced)
                return int(dt.datetime.fromisoformat(coerced).timestamp())
        raise NotImplementedError(
            f"Data type {data_type} is not supported yet."
        )

    def _parse_bool(self, value: str) -> bool:
        """Parse a string value to a boolean.

        Args:
            value: The string value to parse.

        Returns:
            The parsed boolean value.

        Raises:
            ValueError: If the string value could not be parsed to a boolean.
        """
        lowered = value.strip().lower()
        if lowered in self._BOOL_TRUE_VALUES:
            return True
        if lowered in self._BOOL_FALSE_VALUES:
            return False
        raise ValueError(f"Value {value} is not a valid boolean")
