from collections.abc import Iterable


class LimboError(Exception):
    """Base exception for all limbo_core errors."""


class LimboValidationError(LimboError, ValueError):
    """Base exception for validation-related limbo_core errors."""


class ContextMissingError(LimboError):
    """Exception raised when a validation context is missing."""

    def __init__(self) -> None:
        """Initialize the ContextMissingError."""
        super().__init__("Context is missing")


class ConnectionNotFoundError(LimboError):
    """Exception raised when a named connection is missing from context."""

    def __init__(
        self, connection_name: str, available_connections: Iterable[str]
    ) -> None:
        """Initialize the ConnectionNotFoundError."""
        self.connection_name = connection_name
        self.available_connections = tuple(available_connections)
        available = ", ".join(self.available_connections) or "none"
        super().__init__(
            f"Connection '{connection_name}' not found. "
            f"Available connections: {available}"
        )
