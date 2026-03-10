"""Table schema-specific errors."""

from limbo_core.errors import LimboValidationError


class TableSchemaError(LimboValidationError):
    """Base error for table schema validation failures."""


class GeneratorNotFoundError(TableSchemaError):
    """Raised when a configured generator does not exist in context."""

    def __init__(self, generator_name: str) -> None:
        """Initialize the GeneratorNotFoundError."""
        super().__init__(f"Generator {generator_name} is not in the context")


class UnknownOptionPrefixError(TableSchemaError):
    """Raised when an option interpolation prefix is unknown."""

    def __init__(self, prefix: str) -> None:
        """Initialize the UnknownOptionPrefixError."""
        super().__init__(f"Unknown option prefix: {prefix}")
