"""Context-related domain errors."""

from __future__ import annotations

from typing import TYPE_CHECKING

from limbo_core.domain.errors import DomainError

if TYPE_CHECKING:
    from collections.abc import Iterable


class ConnectionNotFoundError(DomainError):
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
