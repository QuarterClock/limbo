"""Tests for the DataPersistenceRegistry adapter."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import pytest

from limbo_core.adapters.persistence.data_persistence_registry import (
    DataPersistenceRegistry,
)
from limbo_core.application.interfaces.persistence import DataPersistenceBackend
from limbo_core.domain.entities.backends import DestinationBackendSpec
from limbo_core.domain.value_objects import (
    LocalFilesystemStorageRef,
    ResolvedStorageRef,
    TabularBatch,
)
from limbo_core.validation import ValidationError


@dataclass(slots=True)
class _MemoryWriteBackend(DataPersistenceBackend):
    """In-memory persistence backend used for tests."""

    store: dict[str, TabularBatch] = field(default_factory=dict)
    _root: Path = field(default_factory=lambda: Path("/__limbo_memory__"))

    def ref_for_name(self, name: str) -> LocalFilesystemStorageRef:
        p = self._root / name
        return LocalFilesystemStorageRef(
            backend="memory", uri=f"memory://{name}", local_path=p
        )

    def save(self, ref: ResolvedStorageRef, data: TabularBatch) -> None:
        key = ref.as_local_path().name
        self.store[key] = data

    def load(self, ref: ResolvedStorageRef) -> TabularBatch:
        return self.store[ref.as_local_path().name]

    def exists(self, ref: ResolvedStorageRef) -> bool:
        return ref.as_local_path().name in self.store

    def cleanup(self, ref: ResolvedStorageRef) -> None:
        self.store.pop(ref.as_local_path().name, None)


def test_data_persistence_registry_save_load_exists_cleanup_roundtrip() -> None:
    """Registry delegates save/load/exists/cleanup to configured backend."""
    registry = DataPersistenceRegistry()
    registry.register("memory", _MemoryWriteBackend)
    registry.configure(DestinationBackendSpec(name="mem", type="memory"))

    payload = TabularBatch(column_names=("id",), rows=({"id": 1},))
    registry.save("mem", "users", payload)
    assert registry.exists("mem", "users")
    assert registry.load("mem", "users") == payload

    registry.cleanup("mem", "users")
    assert not registry.exists("mem", "users")


def test_data_persistence_registry_unknown_backend_raises() -> None:
    """Using an unknown backend key results in a validation error."""
    registry = DataPersistenceRegistry()
    batch = TabularBatch(column_names=("x",), rows=())

    with pytest.raises(
        ValidationError,
        match="Data persistence backend 'missing' is not configured",
    ):
        registry.save("missing", "name", batch)


def test_create_unknown_type_uses_base_registry_error() -> None:
    """``create`` for an unregistered type raises the base registry error."""
    registry = DataPersistenceRegistry()
    with pytest.raises(ValueError, match="Unknown data persistence backend"):
        registry.create("nonexistent", config={})
