"""Typed value interpolation using ${type:value} syntax."""

import datetime as dt
import re
from typing import Any, ClassVar

from limbo_core.yaml_schema.artifacts.data_types import DataType


class ValueInterpolator:
    """Interpolates typed values using ${type:value} syntax.

    Supports prefixes for type coercion:
    - ${string:value} - String value
    - ${integer:42} - Integer value
    - ${float:3.14} - Float value
    - ${boolean:true} - Boolean value
    - ${date:2024-01-01} - Date value
    - ${datetime:2024-01-01T12:00:00} - Datetime value
    - ${timestamp:1234567890} - Unix timestamp
    - ${ref:table.column} - Reference to another column
    """

    _PREFIX_RE: ClassVar[re.Pattern[str]] = re.compile(
        r"^\$\{([^:}]+):([^}]+)\}$"
    )
    _BOOL_TRUE: ClassVar[frozenset[str]] = frozenset({
        "true",
        "1",
        "yes",
        "y",
        "on",
    })
    _BOOL_FALSE: ClassVar[frozenset[str]] = frozenset({
        "false",
        "0",
        "no",
        "n",
        "off",
    })

    @classmethod
    def parse(cls, value: str) -> tuple[str, str] | None:
        """Parse a prefixed value string.

        Args:
            value: The string to parse.

        Returns:
            Tuple of (prefix, content) if the string has a valid prefix,
            None otherwise.
        """
        match = cls._PREFIX_RE.match(value)
        if not match:
            return None
        return match.group(1).strip().lower(), match.group(2)

    @classmethod
    def has_prefix(cls, value: str) -> bool:
        """Check if a string has a type prefix.

        Args:
            value: The string to check.

        Returns:
            True if the string has a type prefix.
        """
        return cls._PREFIX_RE.match(value) is not None

    @classmethod
    def cast(cls, data_type: DataType, raw: str) -> Any:
        """Cast a string value to a specific data type.

        Args:
            data_type: The target data type.
            raw: The string value to cast.

        Returns:
            The casted value.

        Raises:
            ValueError: If the data type is unsupported or casting fails.
        """
        match data_type:
            case DataType.STRING:
                return raw
            case DataType.INTEGER:
                return int(raw)
            case DataType.FLOAT:
                return float(raw)
            case DataType.BOOLEAN:
                return cls._parse_bool(raw)
            case DataType.DATE:
                return dt.date.fromisoformat(raw)
            case DataType.DATETIME:
                return cls._parse_datetime(raw)
            case DataType.TIMESTAMP:
                return cls._parse_timestamp(raw)
        raise ValueError(f"Unsupported data type: {data_type}")

    @classmethod
    def infer_type(cls, value: Any) -> DataType:
        """Infer the data type from a Python value.

        Args:
            value: The value to infer the type from.

        Returns:
            The inferred data type.

        Raises:
            ValueError: If the type cannot be inferred.
        """
        match value:
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
        raise ValueError(f"Cannot infer data type for: {type(value).__name__}")

    @classmethod
    def _parse_bool(cls, value: str) -> bool:
        """Parse a string to boolean.

        Args:
            value: The string to parse.

        Returns:
            The parsed boolean.

        Raises:
            ValueError: If the value is not a valid boolean string.
        """
        lowered = value.strip().lower()
        if lowered in cls._BOOL_TRUE:
            return True
        if lowered in cls._BOOL_FALSE:
            return False
        raise ValueError(f"Invalid boolean value: {value}")

    @classmethod
    def _parse_datetime(cls, value: str) -> dt.datetime:
        """Parse a string to datetime with UTC default.

        Args:
            value: The string to parse.

        Returns:
            The parsed datetime with timezone.
        """
        parsed = dt.datetime.fromisoformat(value)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=dt.UTC)
        return parsed

    @classmethod
    def _parse_timestamp(cls, value: str) -> int:
        """Parse a string to unix timestamp.

        Args:
            value: The string to parse (integer or ISO datetime).

        Returns:
            The unix timestamp as integer.
        """
        coerced = value.strip()
        if coerced.isdigit():
            return int(coerced)
        return int(dt.datetime.fromisoformat(coerced).timestamp())
