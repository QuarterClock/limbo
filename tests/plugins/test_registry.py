"""Tests for instance-based ConnectionRegistry."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from limbo_core.adapters.connections import (
    ConnectionBackend,
    ConnectionRegistry,
)
from limbo_core.adapters.connections.errors import UnknownConnectionBackendError
from limbo_core.application.context import ConnectionNotFoundError
from limbo_core.domain.entities import ConnectionBackendSpec
from limbo_core.validation import ValidationError

if TYPE_CHECKING:
    from limbo_core.application.interfaces import ConnectionRegistryPort


class MockConnectionBackend(ConnectionBackend):
    """Mock connection backend for testing."""

    host: str

    def __init__(self, *, host: str = "localhost") -> None:
        self.host = host

    @classmethod
    def from_spec(cls, spec: ConnectionBackendSpec) -> MockConnectionBackend:
        host = spec.config.get("host", "localhost")
        if not isinstance(host, str):
            raise ValidationError(
                "MockConnectionBackend: `host` expects string"
            )
        return cls(host=host)

    def connect(self) -> str:
        """Return a sentinel value to verify connect dispatch."""
        return "connected"


class AnotherMockConnectionBackend(ConnectionBackend):
    """Another mock connection backend for testing."""

    port: int

    def __init__(self, *, port: int = 8080) -> None:
        self.port = port

    @classmethod
    def from_spec(
        cls, spec: ConnectionBackendSpec
    ) -> AnotherMockConnectionBackend:
        port = spec.config.get("port", 8080)
        if not isinstance(port, int):
            raise ValidationError(
                "AnotherMockConnectionBackend: `port` expects int"
            )
        return cls(port=port)

    def connect(self) -> str:
        """Return a sentinel value to verify connect dispatch."""
        return "another-connected"


@pytest.fixture
def registry() -> ConnectionRegistryPort:
    """Create a fresh registry instance per test."""
    return ConnectionRegistry()


class TestConnectionRegistryRegister:
    """Tests for ConnectionRegistry.register method."""

    def test_register_connection_type(
        self, registry: ConnectionRegistryPort
    ) -> None:
        """Adding a type registers discriminator mapping."""
        registry.register("mock", MockConnectionBackend)
        types = registry.get_types()
        assert "mock" in types
        assert types["mock"] is MockConnectionBackend

    def test_register_empty_key_raises(
        self, registry: ConnectionRegistryPort
    ) -> None:
        """Backend keys must be non-empty strings after normalization."""
        with pytest.raises(
            ValidationError, match="Connection backend key cannot be empty"
        ):
            registry.register("   ", MockConnectionBackend)


class TestConnectionRegistryCreate:
    """Tests for ConnectionRegistry.create methods."""

    def test_create_single_connection(
        self, registry: ConnectionRegistryPort
    ) -> None:
        """Single payload creates expected connection instance."""
        registry.register("mock", MockConnectionBackend)
        result = registry.create("mock", config={"host": "myhost"})
        assert isinstance(result, MockConnectionBackend)
        assert result.host == "myhost"

    def test_create_many(self, registry: ConnectionRegistryPort) -> None:
        """List creation preserves order and concrete classes."""
        registry.register("mock", MockConnectionBackend)
        registry.register("another_mock", AnotherMockConnectionBackend)
        specs = [
            ConnectionBackendSpec(
                name="conn1", type="mock", config={"host": "host1"}
            ),
            ConnectionBackendSpec(
                name="conn2", type="another_mock", config={"port": 3000}
            ),
        ]
        results = registry.create_many(specs)
        assert len(results) == 2
        assert isinstance(results[0], MockConnectionBackend)
        assert isinstance(results[1], AnotherMockConnectionBackend)

    def test_create_rejects_unknown_backend(
        self, registry: ConnectionRegistryPort
    ) -> None:
        """Unknown backend key raises UnknownConnectionBackendError."""
        with pytest.raises(
            UnknownConnectionBackendError,
            match="Unknown connection backend: missing",
        ):
            registry.create("missing")

    def test_create_normalizes_backend_key(
        self, registry: ConnectionRegistryPort
    ) -> None:
        """Backend lookup is case-insensitive via normalized keys."""
        registry.register("mock", MockConnectionBackend)
        result = registry.create(" MOCK ")
        assert isinstance(result, MockConnectionBackend)

    def test_create_with_invalid_config_raises(
        self, registry: ConnectionRegistryPort
    ) -> None:
        """Passing unsupported config kwargs raises ValidationError."""
        registry.register("mock", MockConnectionBackend)
        with pytest.raises(ValidationError, match="config is invalid"):
            registry.create("mock", config={"not_a_real_kwarg": 42})

    def test_create_many_with_empty_list(
        self, registry: ConnectionRegistryPort
    ) -> None:
        """An empty spec list produces an empty result list."""
        results = registry.create_many([])
        assert results == []


class TestConnectionRegistryInstances:
    """Tests for named instance lifecycle operations."""

    def test_configure_instance(self, registry: ConnectionRegistryPort) -> None:
        """Configure stores named backend instance."""
        registry.register("mock", MockConnectionBackend)
        registry.configure(
            ConnectionBackendSpec(
                name="main", type="mock", config={"host": "runtime-host"}
            )
        )
        instances = registry.get_instances()
        assert "main" in instances
        assert isinstance(instances["main"], MockConnectionBackend)
        assert instances["main"].host == "runtime-host"

    def test_configure_from_spec(
        self, registry: ConnectionRegistryPort
    ) -> None:
        """configure stores a named backend instance from spec."""
        registry.register("mock", MockConnectionBackend)
        registry.configure(
            ConnectionBackendSpec(name="analytics", type="mock", config={})
        )
        instances = registry.get_instances()
        assert "analytics" in instances

    def test_clear_instances(self, registry: ConnectionRegistryPort) -> None:
        """clear_instances removes configured runtime instances."""
        registry.register("mock", MockConnectionBackend)
        registry.configure(ConnectionBackendSpec(name="main", type="mock"))
        assert registry.get_instances()
        registry.clear_instances()
        assert registry.get_instances() == {}

    def test_clear_types_removes_all_registered_classes(
        self, registry: ConnectionRegistryPort
    ) -> None:
        """Clearing types drops all backend key mappings."""
        registry.register("mock", MockConnectionBackend)
        registry.register("another_mock", AnotherMockConnectionBackend)
        assert len(registry.get_types()) == 2
        registry.clear_types()
        assert len(registry.get_types()) == 0

    def test_configure_empty_name_raises(
        self, registry: ConnectionRegistryPort
    ) -> None:
        """Configuring with a blank name raises ValidationError."""
        registry.register("mock", MockConnectionBackend)
        with pytest.raises(
            ValidationError, match="Connection backend name cannot be empty"
        ):
            registry.configure(
                ConnectionBackendSpec(name="   ", type="mock", config={})
            )

    def test_get_instances_returns_copy(
        self, registry: ConnectionRegistryPort
    ) -> None:
        """Returned mapping is a copy and does not expose internals."""
        registry.register("mock", MockConnectionBackend)
        registry.configure(
            ConnectionBackendSpec(name="db", type="mock", config={})
        )
        instances1 = registry.get_instances()
        instances1["injected"] = MockConnectionBackend()
        instances2 = registry.get_instances()
        assert "injected" not in instances2


class TestConnectionRegistryGetTypes:
    """Tests for ConnectionRegistry.get_types method."""

    def test_get_types_returns_copy(
        self, registry: ConnectionRegistryPort
    ) -> None:
        """Returned mapping is a copy and does not expose internals."""
        registry.register("mock", MockConnectionBackend)
        types1 = registry.get_types()
        types1["fake"] = MockConnectionBackend
        types2 = registry.get_types()
        assert "fake" not in types2


class TestConnectionRegistryConnect:
    """Tests for ConnectionRegistry.connect method."""

    def test_connect_returns_backend_connection_result(
        self, registry: ConnectionRegistryPort
    ) -> None:
        """connect() delegates to backend and returns its result."""
        registry.register("mock", MockConnectionBackend)
        registry.configure(
            ConnectionBackendSpec(
                name="primary", type="mock", config={"host": "db.example.com"}
            )
        )
        result = registry.connect("primary")
        assert result == "connected"

    def test_connect_unknown_name_raises(
        self, registry: ConnectionRegistryPort
    ) -> None:
        """Connecting to an unconfigured name raises ConnectionNotFoundError."""
        with pytest.raises(ConnectionNotFoundError, match="missing_db"):
            registry.connect("missing_db")

    def test_connect_strips_whitespace(
        self, registry: ConnectionRegistryPort
    ) -> None:
        """connect() strips whitespace but preserves original casing."""
        registry.register("mock", MockConnectionBackend)
        registry.configure(
            ConnectionBackendSpec(name="Main", type="mock", config={})
        )
        result = registry.connect(" Main ")
        assert result == "connected"
