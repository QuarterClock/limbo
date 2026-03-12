"""Connection backend contract."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Self

if TYPE_CHECKING:
    from limbo_core.domain.entities import ConnectionBackendSpec


class ConnectionBackend(ABC):
    """Runtime backend created from project connection specs."""

    @classmethod
    @abstractmethod
    def from_spec(cls, spec: ConnectionBackendSpec) -> Self:
        """Create a backend instance from a parsed connection spec."""

    @abstractmethod
    def connect(self) -> Any:
        """Establish the underlying connection."""
