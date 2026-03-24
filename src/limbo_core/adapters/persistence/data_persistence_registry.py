"""Data persistence registry implementation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from limbo_core.adapters.base_registry import BaseRegistry
from limbo_core.application.interfaces import (
    DataPersistenceBackend,
    DataPersistenceRegistryPort,
)
from limbo_core.domain.entities.backends.destination_backend_spec import (
    DestinationBackendSpec,
)
from limbo_core.validation import ValidationError

if TYPE_CHECKING:
    from limbo_core.domain.value_objects import TabularBatch


@dataclass(slots=True)
class DataPersistenceRegistry(
    BaseRegistry[DataPersistenceBackend, DestinationBackendSpec],
    DataPersistenceRegistryPort,
):
    """Registry for tabular data persistence backend types and instances."""

    _backend_label: str = "data persistence backend"

    def save(self, backend_key: str, name: str, data: TabularBatch) -> None:
        """Serialize ``data`` for logical ``name`` via the backend."""
        backend = self._get_instance(backend_key)
        ref = backend.ref_for_name(name)
        backend.save(ref, data)

    def load(self, backend_key: str, name: str) -> TabularBatch:
        """Load a batch for ``name`` from the backend.

        Returns:
            The deserialized tabular batch.
        """
        backend = self._get_instance(backend_key)
        ref = backend.ref_for_name(name)
        return backend.load(ref)

    def exists(self, backend_key: str, name: str) -> bool:
        """Return True if persisted data exists for ``name``."""
        backend = self._get_instance(backend_key)
        ref = backend.ref_for_name(name)
        return backend.exists(ref)

    def cleanup(self, backend_key: str, name: str) -> None:
        """Remove persisted data for ``name`` if present."""
        backend = self._get_instance(backend_key)
        ref = backend.ref_for_name(name)
        backend.cleanup(ref)

    def _get_instance(self, backend_key: str) -> DataPersistenceBackend:
        normalized = self._normalize_name(backend_key)
        try:
            backend = self._instances[normalized]
        except KeyError as err:
            raise ValidationError(
                f"Data persistence backend '{backend_key}' is not configured"
            ) from err
        return backend
