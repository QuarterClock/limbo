"""Persistence adapter errors."""

from limbo_core.domain.errors import DomainValidationError


class UnknownPathBackendError(DomainValidationError):
    """Raised when no persistence backend is registered for a backend name."""

    def __init__(self, backend: str) -> None:
        """Initialize the UnknownPathBackendError."""
        super().__init__(f"Backend {backend} is not supported")
