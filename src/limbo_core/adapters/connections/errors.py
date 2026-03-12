"""Connection adapter errors."""

from limbo_core.domain.errors import DomainValidationError
from limbo_core.errors import LimboError


class MissingPackageError(LimboError):
    """Exception raised when a package is missing."""

    def __init__(self, package_name: str) -> None:
        """Initialize the MissingPackageError."""
        super().__init__(
            f"Package {package_name} is required for this connection type."
        )


class UnknownConnectionBackendError(DomainValidationError):
    """Raised when no connection backend is registered for a backend key."""

    def __init__(self, backend_key: str) -> None:
        """Initialize the UnknownConnectionBackendError."""
        super().__init__(f"Unknown connection backend: {backend_key}")
