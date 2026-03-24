"""Narrow tabular persistence interface (by configured backend + name)."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from limbo_core.domain.value_objects import TabularBatch


class DataPersistenceResolverPort(ABC):
    """Read and write persisted tabular data through configured backends."""

    @abstractmethod
    def save(self, backend_key: str, name: str, data: TabularBatch) -> None:
        """Write data to a configured backend instance."""

    @abstractmethod
    def load(self, backend_key: str, name: str) -> TabularBatch:
        """Load previously saved data from a configured backend."""

    @abstractmethod
    def exists(self, backend_key: str, name: str) -> bool:
        """Check whether named data exists in a configured backend."""

    @abstractmethod
    def cleanup(self, backend_key: str, name: str) -> None:
        """Remove named data from a configured backend."""
