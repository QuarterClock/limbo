"""Interpolation-specific errors."""

from typing import Any

from limbo_core.errors import LimboValidationError


class InterpolationError(LimboValidationError):
    """Base error for interpolation failures."""


class EnvironmentVariableNotSetError(InterpolationError):
    """Raised when an env placeholder has no value and no default."""

    def __init__(self, variable_name: str) -> None:
        """Initialize the EnvironmentVariableNotSetError."""
        super().__init__(
            f"Environment variable '{variable_name}' is not set "
            f"and no default provided"
        )


class UnsupportedDataTypeError(InterpolationError):
    """Raised when value interpolation receives an unsupported data type."""

    def __init__(self, data_type: Any) -> None:
        """Initialize the UnsupportedDataTypeError."""
        super().__init__(f"Unsupported data type: {data_type}")


class DataTypeInferenceError(InterpolationError):
    """Raised when a value type cannot be mapped to DataType."""

    def __init__(self, value: Any) -> None:
        """Initialize the DataTypeInferenceError."""
        super().__init__(f"Cannot infer data type for: {type(value).__name__}")


class InvalidBooleanValueError(InterpolationError):
    """Raised when a boolean string cannot be parsed."""

    def __init__(self, value: str) -> None:
        """Initialize the InvalidBooleanValueError."""
        super().__init__(f"Invalid boolean value: {value}")
