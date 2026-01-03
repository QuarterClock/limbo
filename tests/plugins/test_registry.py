"""Tests for ConnectionRegistry."""

from typing import Literal

import pytest

from limbo_core.connections import Connection, ConnectionRegistry


class MockConnection(Connection):
    """Mock connection for testing."""

    type: Literal["mock"] = "mock"
    name: str
    host: str = "localhost"

    def connect(self) -> None:
        """Mock connect method."""


class AnotherMockConnection(Connection):
    """Another mock connection for testing discriminated unions."""

    type: Literal["another_mock"] = "another_mock"
    name: str
    port: int = 8080

    def connect(self) -> None:
        """Mock connect method."""


class NoTypeFieldConnection(Connection):
    """Connection that doesn't override the type field properly."""

    # Inherits type: str from base (no default = PydanticUndefined)
    name: str

    def connect(self) -> None:
        """Mock connect method."""


@pytest.fixture(autouse=True)
def reset_registry() -> None:
    """Reset registry before and after each test."""
    # Store original state
    original_types = ConnectionRegistry._types.copy()
    original_adapter = ConnectionRegistry._adapter

    yield

    # Restore original state
    ConnectionRegistry._types = original_types
    ConnectionRegistry._adapter = original_adapter


class TestConnectionRegistryAdd:
    """Tests for ConnectionRegistry.add method."""

    def test_add_connection_type(self) -> None:
        """Test adding a connection type."""
        ConnectionRegistry.clear()
        ConnectionRegistry.add(MockConnection)

        types = ConnectionRegistry.get_types()
        assert "mock" in types
        assert types["mock"] is MockConnection

    def test_add_invalidates_adapter_cache(self) -> None:
        """Test that adding a type invalidates the adapter cache."""
        ConnectionRegistry.clear()
        ConnectionRegistry.add(MockConnection)

        # Force adapter creation
        adapter1 = ConnectionRegistry.get_adapter()

        # Add another type
        ConnectionRegistry.add(AnotherMockConnection)

        # Adapter should be recreated
        adapter2 = ConnectionRegistry.get_adapter()
        assert adapter1 is not adapter2

    def test_add_connection_without_type_default_raises(self) -> None:
        """Test that adding a connection without type default raises."""
        ConnectionRegistry.clear()
        with pytest.raises(ValueError, match="missing 'type' field default"):
            ConnectionRegistry.add(NoTypeFieldConnection)


class TestConnectionRegistryGetAdapter:
    """Tests for ConnectionRegistry.get_adapter method."""

    def test_empty_registry_returns_base_adapter(self) -> None:
        """Test adapter for empty registry."""
        ConnectionRegistry.clear()
        adapter = ConnectionRegistry.get_adapter()
        assert adapter is not None

    def test_single_type_adapter(self) -> None:
        """Test adapter with single registered type."""
        ConnectionRegistry.clear()
        ConnectionRegistry.add(MockConnection)

        adapter = ConnectionRegistry.get_adapter()
        result = adapter.validate_python({
            "name": "test",
            "host": "localhost",
            "type": "mock",
        })
        assert isinstance(result, MockConnection)

    def test_multiple_types_discriminated_union(self) -> None:
        """Test adapter with multiple types uses discriminated union."""
        ConnectionRegistry.clear()
        ConnectionRegistry.add(MockConnection)
        ConnectionRegistry.add(AnotherMockConnection)

        adapter = ConnectionRegistry.get_adapter()

        # Validate mock connection
        result1 = adapter.validate_python({
            "name": "test1",
            "host": "localhost",
            "type": "mock",
        })
        assert isinstance(result1, MockConnection)

        # Validate another_mock connection
        result2 = adapter.validate_python({
            "name": "test2",
            "port": 9000,
            "type": "another_mock",
        })
        assert isinstance(result2, AnotherMockConnection)

    def test_adapter_is_cached(self) -> None:
        """Test that adapter is cached after first call."""
        ConnectionRegistry.clear()
        ConnectionRegistry.add(MockConnection)

        adapter1 = ConnectionRegistry.get_adapter()
        adapter2 = ConnectionRegistry.get_adapter()
        assert adapter1 is adapter2


class TestConnectionRegistryValidation:
    """Tests for ConnectionRegistry.validate methods."""

    def test_validate_single_connection(self) -> None:
        """Test validating a single connection."""
        ConnectionRegistry.clear()
        ConnectionRegistry.add(MockConnection)

        result = ConnectionRegistry.validate({
            "name": "test",
            "host": "myhost",
            "type": "mock",
        })
        assert isinstance(result, MockConnection)
        assert result.name == "test"
        assert result.host == "myhost"

    def test_validate_list(self) -> None:
        """Test validating a list of connections."""
        ConnectionRegistry.clear()
        ConnectionRegistry.add(MockConnection)
        ConnectionRegistry.add(AnotherMockConnection)

        data = [
            {"name": "conn1", "host": "host1", "type": "mock"},
            {"name": "conn2", "port": 3000, "type": "another_mock"},
        ]
        results = ConnectionRegistry.validate_list(data)

        assert len(results) == 2
        assert isinstance(results[0], MockConnection)
        assert isinstance(results[1], AnotherMockConnection)


class TestConnectionRegistryClear:
    """Tests for ConnectionRegistry.clear method."""

    def test_clear_removes_all_types(self) -> None:
        """Test that clear removes all registered types."""
        ConnectionRegistry.clear()
        ConnectionRegistry.add(MockConnection)
        ConnectionRegistry.add(AnotherMockConnection)

        assert len(ConnectionRegistry.get_types()) == 2

        ConnectionRegistry.clear()
        assert len(ConnectionRegistry.get_types()) == 0

    def test_clear_invalidates_adapter(self) -> None:
        """Test that clear invalidates the adapter cache."""
        ConnectionRegistry.clear()
        ConnectionRegistry.add(MockConnection)
        _ = ConnectionRegistry.get_adapter()  # Create adapter

        ConnectionRegistry.clear()
        assert ConnectionRegistry._adapter is None


class TestConnectionRegistryGetTypes:
    """Tests for ConnectionRegistry.get_types method."""

    def test_get_types_returns_copy(self) -> None:
        """Test that get_types returns a copy, not the original dict."""
        ConnectionRegistry.clear()
        ConnectionRegistry.add(MockConnection)

        types1 = ConnectionRegistry.get_types()
        types1["fake"] = None  # Modify the returned dict

        types2 = ConnectionRegistry.get_types()
        assert "fake" not in types2  # Original should be unchanged
