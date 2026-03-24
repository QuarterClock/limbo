"""Tests for the DefaultPersistor adapter."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import pytest

from limbo_core.adapters.persistence import (
    DataPersistenceRegistry,
    DefaultPersistor,
    PathResolverRegistry,
)
from limbo_core.application.interfaces.persistence import DataPersistenceBackend
from limbo_core.domain.entities.backends import DestinationBackendSpec
from limbo_core.domain.value_objects import ResolvedStorageRef, TabularBatch
from limbo_core.plugins.builtin.persistence import FilesystemPathResolver


def _batch_id(value: int) -> TabularBatch:
    return TabularBatch(column_names=("id",), rows=({"id": value},))


@dataclass(slots=True)
class _MemoryWriteBackend(DataPersistenceBackend):
    """In-memory persistence backend used for tests."""

    store: dict[str, TabularBatch] = field(default_factory=dict)
    _root: Path = field(default_factory=lambda: Path("/__limbo_memory__"))

    @property
    def directory(self) -> Path:
        return self._root

    def storage_object_name(self, logical_name: str) -> str:
        return logical_name

    def save(self, ref: ResolvedStorageRef, data: TabularBatch) -> None:
        key = ref.as_local_path().name
        self.store[key] = data

    def load(self, ref: ResolvedStorageRef) -> TabularBatch:
        return self.store[ref.as_local_path().name]

    def exists(self, ref: ResolvedStorageRef) -> bool:
        return ref.as_local_path().name in self.store

    def cleanup(self, ref: ResolvedStorageRef) -> None:
        self.store.pop(ref.as_local_path().name, None)


@pytest.fixture
def data_registry() -> DataPersistenceRegistry:
    """Registry with an in-memory backend configured."""
    path_reg = PathResolverRegistry()
    path_reg.register("file", FilesystemPathResolver)
    registry = DataPersistenceRegistry(path_resolver_registry=path_reg)
    registry.register("memory", _MemoryWriteBackend)
    registry.configure(DestinationBackendSpec(name="memory", type="memory"))
    return registry


@pytest.fixture
def persistor(data_registry: DataPersistenceRegistry) -> DefaultPersistor:
    """Persistor wired to an in-memory data persistence registry."""
    return DefaultPersistor(data_resolver=data_registry, backend_key="memory")


class TestDefaultPersistorMaterialize:
    """Tests for save with materialize=True."""

    def test_save_materialize_writes_to_backend(
        self,
        persistor: DefaultPersistor,
        data_registry: DataPersistenceRegistry,
    ) -> None:
        """Materialized save delegates to the data backend."""
        batch = _batch_id(1)
        persistor.save("users", batch, materialize=True)

        assert data_registry.exists("memory", "users")
        assert data_registry.load("memory", "users") == batch

    def test_save_materialize_also_caches(
        self, persistor: DefaultPersistor
    ) -> None:
        """Materialized save populates the local cache."""
        batch = _batch_id(1)
        persistor.save("users", batch, materialize=True)

        assert persistor.exists("users")
        assert persistor.load("users") == batch

    def test_load_returns_cached_value(
        self, persistor: DefaultPersistor
    ) -> None:
        """Load serves from cache when available."""
        persistor.save("users", _batch_id(1))
        persistor.save("users", _batch_id(2))

        assert persistor.load("users") == _batch_id(2)


class TestDefaultPersistorCache:
    """Tests for save with materialize=False (cache-only)."""

    def test_save_no_materialize_does_not_write_to_backend(
        self,
        persistor: DefaultPersistor,
        data_registry: DataPersistenceRegistry,
    ) -> None:
        """Non-materialized save only caches, does not hit the backend."""
        persistor.save("users", _batch_id(1), materialize=False)

        assert not data_registry.exists("memory", "users")

    def test_save_no_materialize_caches_locally(
        self, persistor: DefaultPersistor
    ) -> None:
        """Non-materialized save is still loadable from the persistor."""
        batch = _batch_id(1)
        persistor.save("users", batch, materialize=False)

        assert persistor.exists("users")
        assert persistor.load("users") == batch


class TestDefaultPersistorLoad:
    """Tests for load fall-through behaviour."""

    def test_load_falls_back_to_backend(
        self,
        persistor: DefaultPersistor,
        data_registry: DataPersistenceRegistry,
    ) -> None:
        """Load falls through to data backend when not cached."""
        backend_batch = _batch_id(99)
        data_registry.save("memory", "users", backend_batch)

        assert persistor.load("users") == backend_batch

    def test_load_prefers_cache(
        self,
        persistor: DefaultPersistor,
        data_registry: DataPersistenceRegistry,
    ) -> None:
        """Cache takes precedence over backend value."""
        data_registry.save("memory", "users", _batch_id(99))
        persistor.save("users", _batch_id(1), materialize=False)

        assert persistor.load("users") == _batch_id(1)


class TestDefaultPersistorExists:
    """Tests for exists checking."""

    def test_exists_returns_false_when_empty(
        self, persistor: DefaultPersistor
    ) -> None:
        """Exists returns False for unknown artifacts."""
        assert not persistor.exists("users")

    def test_exists_from_cache(self, persistor: DefaultPersistor) -> None:
        """Exists returns True for cache-only artifacts."""
        persistor.save("users", _batch_id(1), materialize=False)
        assert persistor.exists("users")

    def test_exists_falls_back_to_backend(
        self,
        persistor: DefaultPersistor,
        data_registry: DataPersistenceRegistry,
    ) -> None:
        """Exists falls through to backend when not cached."""
        data_registry.save("memory", "users", _batch_id(99))
        assert persistor.exists("users")


class TestDefaultPersistorCleanup:
    """Tests for cleanup behaviour."""

    def test_cleanup_removes_from_cache_and_backend(
        self,
        persistor: DefaultPersistor,
        data_registry: DataPersistenceRegistry,
    ) -> None:
        """Cleanup removes data from both cache and backend."""
        persistor.save("users", _batch_id(1), materialize=True)
        assert persistor.exists("users")

        persistor.cleanup("users")

        assert not persistor.exists("users")
        assert not data_registry.exists("memory", "users")

    def test_cleanup_cache_only_artifact(
        self, persistor: DefaultPersistor
    ) -> None:
        """Cleanup removes cache-only artifacts gracefully."""
        persistor.save("users", _batch_id(1), materialize=False)

        persistor.cleanup("users")
        assert not persistor.exists("users")

    def test_cleanup_nonexistent_is_noop(
        self, persistor: DefaultPersistor
    ) -> None:
        """Cleanup of unknown artifact does not raise."""
        persistor.cleanup("nonexistent")
