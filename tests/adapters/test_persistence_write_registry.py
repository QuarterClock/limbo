"""Tests for the PersistenceWriteRegistry adapter."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

from limbo_core.adapters.persistence.write_registry import (
    PersistenceWriteRegistry,
)
from limbo_core.application.interfaces.persistence import (
    PersistenceWriteBackend,
)
from limbo_core.domain.entities.backends import DestinationBackendSpec
from limbo_core.validation import ValidationError


@dataclass(slots=True)
class _MemoryWriteBackend(PersistenceWriteBackend):
    """In-memory persistence backend used for tests."""

    store: dict[str, Any] = field(default_factory=dict)

    def save(self, name: str, data: Any) -> None:
        self.store[name] = data

    def load(self, name: str) -> Any:
        return self.store[name]

    def exists(self, name: str) -> bool:
        return name in self.store

    def cleanup(self, name: str) -> None:
        self.store.pop(name, None)


def test_persistence_write_registry_save_load_exists_cleanup_roundtrip() -> (
    None
):
    """Registry delegates save/load/exists/cleanup to configured backend."""
    registry = PersistenceWriteRegistry()
    registry.register("memory", _MemoryWriteBackend)
    registry.configure(DestinationBackendSpec(name="mem", type="memory"))

    payload = {"id": 1}
    registry.save("mem", "users", payload)
    assert registry.exists("mem", "users")
    assert registry.load("mem", "users") == payload

    registry.cleanup("mem", "users")
    assert not registry.exists("mem", "users")


def test_persistence_write_registry_unknown_backend_raises() -> None:
    """Using an unknown backend key results in a validation error."""
    registry = PersistenceWriteRegistry()

    with pytest.raises(
        ValidationError,
        match="Persistence write backend 'missing' is not configured",
    ):
        registry.save("missing", "name", 1)
