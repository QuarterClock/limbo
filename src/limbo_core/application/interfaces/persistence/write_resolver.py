"""Narrow persistence write interface.

Services that only need to persist data (e.g. ``Persistor``)
should depend on this port, not the full ``PersistenceWriteRegistryPort``.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from limbo_core.domain.value_objects import TabularBatch


class PersistenceWriteResolverPort(ABC):
    """Write and read persisted resources through configured backends."""

    @abstractmethod
    def save(self, backend_key: str, name: str, data: TabularBatch) -> None:
        """Write data to a configured backend instance.

        Args:
            backend_key: Named backend instance to delegate to.
            name: Logical name of the data artifact.
            data: Data payload to persist.
        """

    @abstractmethod
    def load(self, backend_key: str, name: str) -> TabularBatch:
        """Load previously saved data from a configured backend.

        Args:
            backend_key: Named backend instance to delegate to.
            name: Logical name of the data artifact.

        Returns:
            Previously saved data payload.
        """

    @abstractmethod
    def exists(self, backend_key: str, name: str) -> bool:
        """Check whether named data exists in a configured backend.

        Args:
            backend_key: Named backend instance to delegate to.
            name: Logical name of the data artifact.

        Returns:
            True if the artifact exists, False otherwise.
        """

    @abstractmethod
    def cleanup(self, backend_key: str, name: str) -> None:
        """Remove named data from a configured backend.

        Args:
            backend_key: Named backend instance to delegate to.
            name: Logical name of the data artifact.
        """
