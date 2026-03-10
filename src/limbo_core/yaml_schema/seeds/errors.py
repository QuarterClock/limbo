"""Seed configuration-specific errors."""

from limbo_core.errors import LimboValidationError


class SeedPathError(LimboValidationError):
    """Base error for seed path validation and parsing issues."""


class InvalidSeedPathTypeError(SeedPathError):
    """Raised when a seed path value is not a string."""

    def __init__(self) -> None:
        """Initialize the InvalidSeedPathTypeError."""
        super().__init__("Raw YAML value is not a string")


class AbsoluteSeedPathNotAllowedError(SeedPathError):
    """Raised when a plain seed path is absolute."""

    def __init__(self) -> None:
        """Initialize the AbsoluteSeedPathNotAllowedError."""
        super().__init__(
            "Path is absolute. Must be relative to the project root."
        )


class UnsupportedSeedPathPrefixError(SeedPathError):
    """Raised when a path expression uses unsupported prefix."""

    def __init__(self, prefix: str) -> None:
        """Initialize the UnsupportedSeedPathPrefixError."""
        super().__init__(f"Prefix {prefix} is not supported")
