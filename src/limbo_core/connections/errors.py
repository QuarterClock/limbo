from limbo_core.errors import LimboError, LimboValidationError


class MissingPackageError(LimboError):
    """Exception raised when a package is missing."""

    def __init__(self, package_name: str) -> None:
        """Initialize the MissingPackageError."""
        super().__init__(
            f"Package {package_name} is required for this connection type."
        )


class ConnectionDefinitionError(LimboValidationError):
    """Base error for invalid connection class definitions."""


class MissingConnectionTypeFieldError(ConnectionDefinitionError):
    """Raised when a connection class does not define `type`."""

    def __init__(self, connection_class_name: str) -> None:
        """Initialize the MissingConnectionTypeFieldError."""
        super().__init__(f"{connection_class_name}: missing 'type' field")


class MissingConnectionTypeDefaultError(ConnectionDefinitionError):
    """Raised when a connection class defines `type` without a default."""

    def __init__(self, connection_class_name: str) -> None:
        """Initialize the MissingConnectionTypeDefaultError."""
        super().__init__(
            f"{connection_class_name}: missing 'type' field default"
        )
