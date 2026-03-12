"""Generator registry interface."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .generator import Generator
    from .registration import GeneratorRegistration


class GeneratorRegistryPort(ABC):
    """Registry for generator classes and their fully qualified hooks."""

    @abstractmethod
    def register(self, registration: GeneratorRegistration) -> None:
        """Register a generator class under a namespace."""

    @abstractmethod
    def get_hooks(self) -> frozenset[str]:
        """Return all fully-qualified hooks (e.g. ``pii.email``)."""

    @abstractmethod
    def resolve(self, qualified_hook: str) -> tuple[type[Generator], str]:
        """Resolve a fully-qualified hook to (generator_class, local_hook)."""

    @abstractmethod
    def clear(self) -> None:
        """Clear all registered generators."""
