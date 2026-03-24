"""Default persistor coordinating materialization and caching."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from limbo_core.application.interfaces.persistence import Persistor

if TYPE_CHECKING:
    from limbo_core.application.interfaces.persistence import (
        DataPersistenceResolverPort,
    )
    from limbo_core.domain.value_objects import TabularBatch


@dataclass(slots=True)
class DefaultPersistor(Persistor):
    """Coordinate materialization and in-memory caching of generated data.

    When ``materialize`` is True the data is written to the configured
    data persistence backend **and** cached locally. When False the data is
    only held in the local cache for downstream dependents.

    ``backend_key`` must name a configured data persistence backend instance
    (no default).
    """

    data_resolver: DataPersistenceResolverPort
    backend_key: str
    _cache: dict[str, TabularBatch] = field(default_factory=dict)

    def save(
        self, name: str, data: TabularBatch, *, materialize: bool = True
    ) -> None:
        """Save data, optionally materializing to permanent storage."""
        self._cache[name] = data
        if materialize:
            self.data_resolver.save(self.backend_key, name, data)

    def load(self, name: str) -> TabularBatch:
        """Load data from cache first, falling back to data backend.

        Returns:
            The tabular batch for ``name``.
        """
        if name in self._cache:
            return self._cache[name]
        return self.data_resolver.load(self.backend_key, name)

    def exists(self, name: str) -> bool:
        """Check cache first, then data backend.

        Returns:
            True if the name is cached or persisted.
        """
        if name in self._cache:
            return True
        return self.data_resolver.exists(self.backend_key, name)

    def cleanup(self, name: str) -> None:
        """Remove data from both cache and data backend."""
        self._cache.pop(name, None)
        self.data_resolver.cleanup(self.backend_key, name)
