"""Top-level limbo_core error hierarchy."""

from limbo_core.domain.errors import DomainError, DomainValidationError


class LimboError(DomainError):
    """Base exception for all limbo_core errors."""


class LimboValidationError(LimboError, DomainValidationError):
    """Base exception for validation-related limbo_core errors."""
