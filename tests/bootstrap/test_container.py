"""Tests for bootstrap container wiring."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from limbo_core.application.interfaces.persistence import (
    PersistenceWriteBackend,
)
from limbo_core.bootstrap import Container, get_container


@dataclass(slots=True)
class _StubWriteBackend(PersistenceWriteBackend):
    store: dict[str, Any] = field(default_factory=dict)

    def save(self, name: str, data: Any) -> None:
        self.store[name] = data

    def load(self, name: str) -> Any:
        return self.store[name]

    def exists(self, name: str) -> bool:
        return name in self.store

    def cleanup(self, name: str) -> None:
        self.store.pop(name, None)


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
        container.persistence_write_registry.register(
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
