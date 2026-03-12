"""Generator registry errors."""

from __future__ import annotations

from limbo_core.domain.errors import DomainValidationError


class InvalidGeneratorNamespaceError(DomainValidationError):
    """Raised when a generator registration uses an invalid namespace."""

    def __init__(self) -> None:
        """Initialize the InvalidGeneratorNamespaceError."""
        super().__init__("Generator namespace cannot be empty")


class DuplicateGeneratorHookError(DomainValidationError):
    """Raised when a fully qualified generator hook is registered twice."""

    def __init__(self, qualified_hook: str, existing_cls: str) -> None:
        """Initialize the DuplicateGeneratorHookError."""
        super().__init__(
            f"Hook '{qualified_hook}' already registered by {existing_cls}"
        )


class UnknownGeneratorHookError(DomainValidationError):
    """Raised when resolving an unknown fully qualified generator hook."""

    def __init__(self, qualified_hook: str) -> None:
        """Initialize the UnknownGeneratorHookError."""
        super().__init__(f"Generator hook '{qualified_hook}' is not registered")
