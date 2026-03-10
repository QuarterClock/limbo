"""Source schema-specific errors."""

from limbo_core.errors import LimboValidationError


class SourceConfigValidationError(LimboValidationError):
    """Base error for source configuration validation."""


class UnknownSourceConnectionError(SourceConfigValidationError):
    """Raised when a source references an unknown connection."""

    def __init__(self, connection_name: str) -> None:
        """Initialize the UnknownSourceConnectionError."""
        super().__init__(f"Connection '{connection_name}' not found in context")
