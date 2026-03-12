"""Runtime and parsing contexts used by the application layer."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterable

    from limbo_core.application.interfaces import ConnectionBackend

from limbo_core.domain.errors import DomainError


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


class ReferenceResolver(ABC):
    """Resolve runtime reference expressions like ``table.column``."""

    @abstractmethod
    def resolve(self, reference: str) -> Any:
        """Resolve a reference string into a concrete runtime value."""


@dataclass(slots=True)
class ParsingContext:
    """Context for parsing and static validation operations."""

    paths: dict[str, Any]


@dataclass(slots=True)
class RuntimeContext:
    """Context for runtime checks and generation operations."""

    generators: set[str]
    paths: dict[str, Any]
    connections: dict[str, ConnectionBackend] = field(default_factory=dict)
    reference_resolver: ReferenceResolver | None = None

    def resolve_reference(self, reference: str) -> Any:
        """Resolve a reference via configured runtime collaborator.

        Returns:
            Concrete resolved value for the provided reference.

        Raises:
            RuntimeError: If no reference resolver is configured.
        """
        if self.reference_resolver is None:
            raise RuntimeError("Reference resolver is not configured")
        return self.reference_resolver.resolve(reference)

    def get_connection(self, name: str) -> ConnectionBackend:
        """Get a connection by name.

        Returns:
            Connection object stored under the provided name.

        Raises:
            ConnectionNotFoundError: If the connection does not exist.
        """
        if name not in self.connections:
            raise ConnectionNotFoundError(
                connection_name=name,
                available_connections=self.connections.keys(),
            )
        return self.connections[name]
