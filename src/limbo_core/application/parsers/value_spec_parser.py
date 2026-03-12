"""Value spec parsing helpers."""

from __future__ import annotations

import datetime as dt
from collections.abc import Mapping
from typing import Any

from limbo_core.domain.entities.artifacts.data_types import DataType
from limbo_core.domain.entities.values.value import (
    LiteralValue,
    LookupValue,
    ReferenceValue,
    ValueSpec,
)
from limbo_core.domain.errors import DomainValidationError

from .common import InvalidValueSpecError


class DataTypeInferenceError(DomainValidationError):
    """Raised when a value type cannot be mapped to DataType."""

    def __init__(self, value: Any) -> None:
        """Initialize the DataTypeInferenceError."""
        super().__init__(f"Cannot infer data type for: {type(value).__name__}")


class InvalidReferenceValueSpecError(InvalidValueSpecError):
    """Raised when a ``{ref: ...}`` payload is malformed."""

    def __init__(self) -> None:
        """Initialize the InvalidReferenceValueSpecError."""
        super().__init__("`ref` value spec expects a string")


class InvalidLookupValueSpecError(InvalidValueSpecError):
    """Raised when a ``{value_from: ...}`` payload is malformed."""

    def __init__(self, message: str) -> None:
        """Initialize the InvalidLookupValueSpecError."""
        super().__init__(message)


class InvalidTypedValueSpecError(InvalidValueSpecError):
    """Raised when a ``{data_type: ...}`` typed payload is malformed."""

    def __init__(self, message: str) -> None:
        """Initialize the InvalidTypedValueSpecError."""
        super().__init__(message)


def _parse_explicit_data_type(raw: Any) -> DataType:
    """Validate and convert a raw ``data_type`` key to a ``DataType``.

    Returns:
        Parsed domain data type.

    Raises:
        InvalidTypedValueSpecError: If the value is not a valid DataType.
    """
    if not isinstance(raw, str):
        raise InvalidTypedValueSpecError("`data_type` expects a string")
    try:
        return DataType(raw)
    except ValueError:
        raise InvalidTypedValueSpecError(
            f"`data_type` unknown value: {raw!r}"
        ) from None


def infer_data_type(value: Any) -> DataType:
    """Infer a domain ``DataType`` from Python value.

    Returns:
        Inferred domain data type.

    Raises:
        DataTypeInferenceError: If input value type is unsupported.
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
    raise DataTypeInferenceError(value)


def parse_lookup_value(
    raw: Any, *, data_type: DataType | None = None
) -> LookupValue:
    """Parse a lookup value spec.

    Args:
        raw: Raw YAML payload to parse.
        data_type: Optional explicit data type declared via
            ``data_type`` key.

    Returns:
        Parsed lookup value.

    Raises:
        InvalidLookupValueSpecError: If lookup payload is malformed.
    """
    if isinstance(raw, LookupValue):
        return raw
    if not isinstance(raw, Mapping):
        raise InvalidLookupValueSpecError(
            "`value_from` expects a mapping payload"
        )

    source: Any = raw.get("value_from", raw)
    if not isinstance(source, Mapping):
        raise InvalidLookupValueSpecError("`value_from` expects a mapping")

    reader = source.get("reader")
    if not isinstance(reader, str):
        raise InvalidLookupValueSpecError(
            "`value_from.reader` expects a string"
        )
    key = source.get("key")
    if not isinstance(key, str):
        raise InvalidLookupValueSpecError("`value_from.key` expects a string")

    default = source.get("default")
    if default is not None and not isinstance(default, str):
        raise InvalidLookupValueSpecError(
            "`value_from.default` expects a string when provided"
        )

    return LookupValue(
        reader=reader, key=key, default=default, data_type=data_type
    )


def parse_value_spec(raw: Any) -> ValueSpec:
    """Parse raw YAML value into structured value spec.

    Supports an optional ``data_type`` key for explicit typing::

        {data_type: "datetime", value: "2024-01-01"}
        {data_type: "integer", ref: "users.id"}
        {data_type: "string", value_from: {reader: "env", key: "X"}}

    Returns:
        Parsed structured value spec.

    Raises:
        InvalidValueSpecError: If mapping payload has unsupported shape.
        InvalidReferenceValueSpecError: If ``ref`` payload is malformed.
        InvalidTypedValueSpecError: If ``data_type`` payload is malformed.
    """
    if isinstance(raw, (LiteralValue, LookupValue, ReferenceValue)):
        return raw
    if isinstance(raw, Mapping):
        explicit_type: DataType | None = None
        if "data_type" in raw:
            explicit_type = _parse_explicit_data_type(raw["data_type"])

        if "value" in raw:
            return LiteralValue(
                value=raw["value"],
                data_type=(
                    explicit_type
                    if explicit_type is not None
                    else infer_data_type(raw["value"])
                ),
            )
        if "ref" in raw:
            ref = raw.get("ref")
            if not isinstance(ref, str):
                raise InvalidReferenceValueSpecError
            return ReferenceValue(ref=ref, data_type=explicit_type)
        if "value_from" in raw:
            return parse_lookup_value(raw, data_type=explicit_type)

        if explicit_type is not None:
            raise InvalidTypedValueSpecError(
                "`data_type` requires a `value`, `ref`, or `value_from` key"
            )
        raise InvalidValueSpecError(
            "mapping value spec expects `ref`, `value`, or `value_from`"
        )

    return LiteralValue(value=raw, data_type=infer_data_type(raw))
