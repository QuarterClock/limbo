"""Tests for bootstrap container wiring."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from limbo_core.application.interfaces.persistence import DataPersistenceBackend
from limbo_core.bootstrap import Container, get_container
from limbo_core.domain.value_objects import (
    LocalFilesystemStorageRef,
    ResolvedStorageRef,
    TabularBatch,
)


@dataclass(slots=True)
class _StubWriteBackend(DataPersistenceBackend):
    store: dict[str, TabularBatch] = field(default_factory=dict)
    _root: Path = field(default_factory=lambda: Path("/__limbo_stub__"))

    def ref_for_name(self, name: str) -> LocalFilesystemStorageRef:
        return LocalFilesystemStorageRef(
            backend="memory",
            uri=f"memory://{name}",
            local_path=self._root / name,
        )

    def save(self, ref: ResolvedStorageRef, data: TabularBatch) -> None:
        self.store[ref.as_local_path().name] = data

    def load(self, ref: ResolvedStorageRef) -> TabularBatch:
        return self.store[ref.as_local_path().name]

    def exists(self, ref: ResolvedStorageRef) -> bool:
        return ref.as_local_path().name in self.store

    def cleanup(self, ref: ResolvedStorageRef) -> None:
        self.store.pop(ref.as_local_path().name, None)


class TestContainer:
    """Tests for the bootstrap dependency container."""

    def test_get_container_returns_singleton_instance(self) -> None:
        """Ensure bootstrap returns the same default container."""
        first = get_container()
        second = get_container()
        assert first is second

    def test_load_project_delegates_to_loader_service(self) -> None:
        """Container.load_project returns parsed project from wired service."""
        container = Container()
        container.data_persistence_registry.register(
            "memory", _StubWriteBackend
        )
        payload = {
            "connections": [],
            "destinations": [{"name": "default", "type": "memory"}],
            "tables": [
                {
                    "name": "users",
                    "columns": [
                        {
                            "name": "id",
                            "data_type": "integer",
                            "generator": "primary_key.incrementing_id",
                        }
                    ],
                    "config": {},
                }
            ],
        }
        project = container.load_project(payload)
        assert project.tables[0].name == "users"
