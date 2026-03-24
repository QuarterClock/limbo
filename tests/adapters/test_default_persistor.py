"""Tests for the DefaultPersistor adapter."""

from __future__ import annotations

from dataclasses import dataclass, field

import pytest

from limbo_core.adapters.persistence import (
    DefaultPersistor,
    PersistenceWriteRegistry,
)
from limbo_core.application.interfaces.persistence import (
    PersistenceWriteBackend,
)
from limbo_core.domain.entities.backends import DestinationBackendSpec
from limbo_core.domain.value_objects import TabularBatch


def _batch_id(value: int) -> TabularBatch:
    return TabularBatch(column_names=("id",), rows=({"id": value},))


@dataclass(slots=True)
class _MemoryWriteBackend(PersistenceWriteBackend):
    """In-memory persistence backend used for tests."""

    store: dict[str, TabularBatch] = field(default_factory=dict)

    def save(self, name: str, data: TabularBatch) -> None:
        self.store[name] = data

    def load(self, name: str) -> TabularBatch:
        return self.store[name]

    def exists(self, name: str) -> bool:
        return name in self.store

    def cleanup(self, name: str) -> None:
        self.store.pop(name, None)


@pytest.fixture
def write_registry() -> PersistenceWriteRegistry:
    """Registry with an in-memory backend configured as default."""
    registry = PersistenceWriteRegistry()
    registry.register("memory", _MemoryWriteBackend)
    registry.configure(DestinationBackendSpec(name="memory", type="memory"))
    return registry


@pytest.fixture
def persistor(write_registry: PersistenceWriteRegistry) -> DefaultPersistor:
    """Persistor wired to an in-memory write registry."""
    return DefaultPersistor(
        write_resolver=write_registry, default_backend_key="memory"
    )


class TestDefaultPersistorMaterialize:
    """Tests for save with materialize=True."""

    def test_save_materialize_writes_to_backend(
        self,
        persistor: DefaultPersistor,
        write_registry: PersistenceWriteRegistry,
    ) -> None:
        """Materialized save delegates to the write backend."""
        batch = _batch_id(1)
        persistor.save("users", batch, materialize=True)

        assert write_registry.exists("memory", "users")
        assert write_registry.load("memory", "users") == batch

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
        write_registry: PersistenceWriteRegistry,
    ) -> None:
        """Non-materialized save only caches, does not hit the backend."""
        persistor.save("users", _batch_id(1), materialize=False)

        assert not write_registry.exists("memory", "users")

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
        write_registry: PersistenceWriteRegistry,
    ) -> None:
        """Load falls through to write backend when not cached."""
        backend_batch = _batch_id(99)
        write_registry.save("memory", "users", backend_batch)

        assert persistor.load("users") == backend_batch

    def test_load_prefers_cache(
        self,
        persistor: DefaultPersistor,
        write_registry: PersistenceWriteRegistry,
    ) -> None:
        """Cache takes precedence over backend value."""
        write_registry.save("memory", "users", _batch_id(99))
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
        write_registry: PersistenceWriteRegistry,
    ) -> None:
        """Exists falls through to backend when not cached."""
        write_registry.save("memory", "users", _batch_id(99))
        assert persistor.exists("users")


class TestDefaultPersistorCleanup:
    """Tests for cleanup behaviour."""

    def test_cleanup_removes_from_cache_and_backend(
        self,
        persistor: DefaultPersistor,
        write_registry: PersistenceWriteRegistry,
    ) -> None:
        """Cleanup removes data from both cache and backend."""
        persistor.save("users", _batch_id(1), materialize=True)
        assert persistor.exists("users")

        persistor.cleanup("users")

        assert not persistor.exists("users")
        assert not write_registry.exists("memory", "users")

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
