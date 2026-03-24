"""Default persistor coordinating materialization and caching."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from limbo_core.application.interfaces.persistence import Persistor

if TYPE_CHECKING:
    from limbo_core.application.interfaces.persistence import (
        PersistenceWriteResolverPort,
    )
    from limbo_core.domain.value_objects import TabularBatch


@dataclass(slots=True)
class DefaultPersistor(Persistor):
    """Coordinate materialization and in-memory caching of generated data.

    When ``materialize`` is True the data is written to the configured
    write backend **and** cached locally.  When False the data is only
    held in the local cache for downstream dependents.
    """

    write_resolver: PersistenceWriteResolverPort
    default_backend_key: str = "file"
    _cache: dict[str, TabularBatch] = field(default_factory=dict)

    def save(
        self, name: str, data: TabularBatch, *, materialize: bool = True
    ) -> None:
        """Save data, optionally materializing to permanent storage."""
        self._cache[name] = data
        if materialize:
            self.write_resolver.save(self.default_backend_key, name, data)

    def load(self, name: str) -> TabularBatch:
        """Load data from cache first, falling back to write backend.

        Returns:
            Previously saved or cached data payload.
        """
        if name in self._cache:
            return self._cache[name]
        return self.write_resolver.load(self.default_backend_key, name)

    def exists(self, name: str) -> bool:
        """Check cache first, then write backend.

        Returns:
            True if the artifact is cached or persisted.
        """
        if name in self._cache:
            return True
        return self.write_resolver.exists(self.default_backend_key, name)

    def cleanup(self, name: str) -> None:
        """Remove data from both cache and write backend."""
        self._cache.pop(name, None)
        self.write_resolver.cleanup(self.default_backend_key, name)
