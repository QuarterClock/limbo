"""Backend interface for tabular data persistence via storage references."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from limbo_core.domain.value_objects import ResolvedStorageRef, TabularBatch


class DataPersistenceBackend(ABC):
    """Serialize and deserialize TabularBatch through resolved storage refs."""

    @abstractmethod
    def storage_object_name(self, logical_name: str) -> str:
        """Return storage id for the artifact (e.g. filename with suffix)."""

    @abstractmethod
    def save(self, ref: ResolvedStorageRef, data: TabularBatch) -> None:
        """Write data to the given ref."""

    @abstractmethod
    def load(self, ref: ResolvedStorageRef) -> TabularBatch:
        """Load data from the given ref."""

    @abstractmethod
    def exists(self, ref: ResolvedStorageRef) -> bool:
        """Return True if the ref points to existing data."""

    @abstractmethod
    def cleanup(self, ref: ResolvedStorageRef) -> None:
        """Remove data at the ref if present."""
