"""Data persistence registry implementation."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from limbo_core.adapters.base_registry import BaseRegistry
from limbo_core.application.interfaces import (
    DataPersistenceBackend,
    DataPersistenceRegistryPort,
    PathResolverRegistryPort,
)
from limbo_core.domain.entities.backends.destination_backend_spec import (
    DestinationBackendSpec,
)
from limbo_core.domain.entities.resources.path_spec import PathSpec
from limbo_core.domain.value_objects import LocalFilesystemStorageRef
from limbo_core.validation import ValidationError

if TYPE_CHECKING:
    from limbo_core.domain.value_objects import ResolvedStorageRef, TabularBatch


@dataclass(slots=True)
class DataPersistenceRegistry(
    BaseRegistry[DataPersistenceBackend, DestinationBackendSpec],
    DataPersistenceRegistryPort,
):
    """Registry for tabular data persistence backend types and instances."""

    path_resolver_registry: PathResolverRegistryPort | None = None
    _backend_label: str = "data persistence backend"

    def _resolve_storage_ref(
        self, backend: DataPersistenceBackend, artifact: str
    ) -> ResolvedStorageRef:
        root = getattr(backend, "directory", None)
        if root is None:
            raise ValidationError(
                f"Data persistence backend {type(backend).__name__!r} "
                "has no output directory; cannot resolve storage location"
            )
        root_path = Path(root) if not isinstance(root, Path) else root
        path_spec = PathSpec(backend="file", location=artifact, base=None)
        if self.path_resolver_registry is not None:
            return self.path_resolver_registry.resolve_spec(
                path_spec, base=root_path, allow_missing=True
            )
        resolved = root_path / artifact
        return LocalFilesystemStorageRef(
            backend="file", uri=str(resolved), local_path=resolved
        )

    def save(self, backend_key: str, name: str, data: TabularBatch) -> None:
        """Serialize ``data`` for logical ``name`` via the backend."""
        backend = self._get_instance(backend_key)
        artifact = backend.storage_object_name(name)
        ref = self._resolve_storage_ref(backend, artifact)
        backend.save(ref, data)

    def load(self, backend_key: str, name: str) -> TabularBatch:
        """Load a batch for ``name`` from the backend.

        Returns:
            The deserialized ``TabularBatch``.
        """
        backend = self._get_instance(backend_key)
        artifact = backend.storage_object_name(name)
        ref = self._resolve_storage_ref(backend, artifact)
        return backend.load(ref)

    def exists(self, backend_key: str, name: str) -> bool:
        """Return True if persisted data exists for ``name``."""
        backend = self._get_instance(backend_key)
        artifact = backend.storage_object_name(name)
        ref = self._resolve_storage_ref(backend, artifact)
        return backend.exists(ref)

    def cleanup(self, backend_key: str, name: str) -> None:
        """Remove persisted data for ``name`` if present."""
        backend = self._get_instance(backend_key)
        artifact = backend.storage_object_name(name)
        ref = self._resolve_storage_ref(backend, artifact)
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
