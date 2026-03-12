"""Backend interface for persistence write operations."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class PersistenceWriteBackend(ABC):
    """Write and manage persisted resources in a backend."""

    @abstractmethod
    def save(self, name: str, data: Any) -> None:
        """Write data to storage under the given name."""

    @abstractmethod
    def load(self, name: str) -> Any:
        """Load previously saved data by name."""

    @abstractmethod
    def exists(self, name: str) -> bool:
        """Check whether named data exists in storage."""

    @abstractmethod
    def cleanup(self, name: str) -> None:
        """Remove named data from storage."""
