"""Persistor abstraction backed by persistence write backends."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from limbo_core.domain.value_objects import TabularBatch


class Persistor(ABC):
    """Coordinate materialization and caching of generated data."""

    @abstractmethod
    def save(
        self, name: str, data: TabularBatch, *, materialize: bool = True
    ) -> None:
        """Save data under a logical name.

        Args:
            name: Logical table or artifact name.
            data: Tabular batch to persist.
            materialize: When True, write to permanent storage. When False,
                cache for intermediate reuse by downstream dependents.
        """

    @abstractmethod
    def load(self, name: str) -> TabularBatch:
        """Load previously saved or cached data."""

    @abstractmethod
    def exists(self, name: str) -> bool:
        """Check whether data for the given name exists."""

    @abstractmethod
    def cleanup(self, name: str) -> None:
        """Remove saved or cached data for the given name."""
