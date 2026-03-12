"""Value reader adapter errors."""

from limbo_core.domain.errors import DomainValidationError


class UnknownValueReaderError(DomainValidationError):
    """Raised when lookup resolver has no reader for backend name."""

    def __init__(self, reader: str) -> None:
        """Initialize the UnknownValueReaderError."""
        super().__init__(f"Unknown value reader: {reader}")


class LookupValueNotFoundError(DomainValidationError):
    """Raised when lookup key does not exist and default is missing."""

    def __init__(self, *, reader: str, key: str) -> None:
        """Initialize the LookupValueNotFoundError."""
        super().__init__(
            f"Lookup value not found for reader='{reader}', key='{key}'"
        )
