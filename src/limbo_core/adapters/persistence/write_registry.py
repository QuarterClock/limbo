"""Persistence write backend registry implementation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from limbo_core.adapters.base_registry import BaseRegistry
from limbo_core.application.interfaces import (
    PersistenceWriteBackend,
    PersistenceWriteRegistryPort,
)
from limbo_core.domain.entities.backends.destination_backend_spec import (
    DestinationBackendSpec,
)
from limbo_core.validation import ValidationError

if TYPE_CHECKING:
    from limbo_core.domain.value_objects import TabularBatch


@dataclass(slots=True)
class PersistenceWriteRegistry(
    BaseRegistry[PersistenceWriteBackend, DestinationBackendSpec],
    PersistenceWriteRegistryPort,
):
    """Registry managing persistence write backend types and instances."""

    _backend_label: str = "persistence write backend"

    def save(self, backend_key: str, name: str, data: TabularBatch) -> None:
        """Convenience helper to save via a configured backend instance."""
        backend = self._get_instance(backend_key)
        backend.save(name, data)

    def load(self, backend_key: str, name: str) -> TabularBatch:
        """Convenience helper to load via a configured backend instance.

        Returns:
            Loaded tabular batch for the given backend key and name.
        """
        backend = self._get_instance(backend_key)
        return backend.load(name)

    def exists(self, backend_key: str, name: str) -> bool:
        """Check if a value exists via a configured backend instance.

        Returns:
            True if the value exists, False otherwise.
        """
        backend = self._get_instance(backend_key)
        return backend.exists(name)

    def cleanup(self, backend_key: str, name: str) -> None:
        """Remove a value via a configured backend instance."""
        backend = self._get_instance(backend_key)
        backend.cleanup(name)

    def _get_instance(self, backend_key: str) -> PersistenceWriteBackend:
        normalized = self._normalize_name(backend_key)
        try:
            backend = self._instances[normalized]
        except KeyError as err:
            raise ValidationError(
                f"Persistence write backend '{backend_key}' is not configured"
            ) from err
        return backend
