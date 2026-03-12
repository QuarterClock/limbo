"""Domain-level base errors.

All domain error hierarchies derive from these classes, keeping the
domain layer self-contained and free from outer-layer imports.
"""


class DomainError(Exception):
    """Base exception for all domain-level errors."""


class DomainValidationError(DomainError, ValueError):
    """Base exception for domain validation failures."""
