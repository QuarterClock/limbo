"""Generic base registry port for backend class/instance management."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar

from limbo_core.domain.entities.backends.backend_spec import BackendSpec

BackendT = TypeVar("BackendT")
SpecT = TypeVar("SpecT", bound=BackendSpec)


class BaseRegistryPort(ABC, Generic[BackendT, SpecT]):
    """Registry contract shared by all backend registries."""

    @abstractmethod
    def register(self, backend_key: str, backend_class: type[BackendT]) -> None:
        """Register one backend class under a key."""

    @abstractmethod
    def get_types(self) -> dict[str, type[BackendT]]:
        """Get all registered backend classes."""

    @abstractmethod
    def create(
        self, backend_key: str, *, config: dict[str, Any] | None = None
    ) -> BackendT:
        """Create one backend instance by key."""

    @abstractmethod
    def create_many(self, specs: list[SpecT]) -> list[BackendT]:
        """Create many backend instances from specs."""

    @abstractmethod
    def configure(self, spec: SpecT) -> None:
        """Configure one named backend instance from a spec."""

    @abstractmethod
    def get_instances(self) -> dict[str, BackendT]:
        """Get all configured named backend instances."""

    @abstractmethod
    def clear_instances(self) -> None:
        """Clear all configured named backend instances."""

    @abstractmethod
    def clear_types(self) -> None:
        """Clear all registered backend classes."""
