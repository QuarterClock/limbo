"""Tests for PluginManager."""

from typing import Literal

import pytest

from limbo_core.connections import Connection, ConnectionRegistry
from limbo_core.plugins import PluginManager, get_plugin_manager, hookimpl


class PluginTestConnection(Connection):
    """Connection for plugin tests."""

    type: Literal["test_plugin_conn"] = "test_plugin_conn"
    name: str

    def connect(self) -> None:
        """Mock connect."""


class SamplePlugin:
    """Sample plugin implementing connection hook."""

    @hookimpl
    def limbo_register_connections(self) -> list[type[Connection]]:
        """Register test connection."""
        return [PluginTestConnection]


class EmptyPlugin:
    """Plugin that returns empty list."""

    @hookimpl
    def limbo_register_connections(self) -> list[type[Connection]]:
        """Return empty list."""
        return []


class NonePlugin:
    """Plugin that returns None (edge case)."""

    @hookimpl
    def limbo_register_connections(self) -> None:
        """Return None."""
        return


@pytest.fixture
def fresh_plugin_manager() -> PluginManager:
    """Create a fresh PluginManager instance without singleton."""
    return PluginManager()


@pytest.fixture(autouse=True)
def reset_state() -> None:
    """Reset singleton and registry state after each test."""
    # Store original registry state
    original_types = ConnectionRegistry._types.copy()
    original_adapter = ConnectionRegistry._adapter

    yield

    # Reset singleton
    PluginManager.reset_instance()

    # Restore registry state
    ConnectionRegistry._types = original_types
    ConnectionRegistry._adapter = original_adapter


class TestPluginManagerSingleton:
    """Tests for PluginManager singleton pattern."""

    def test_get_instance_returns_same_instance(self) -> None:
        """Test that get_instance returns the same instance."""
        PluginManager.reset_instance()
        instance1 = PluginManager.get_instance()
        instance2 = PluginManager.get_instance()
        assert instance1 is instance2

    def test_reset_instance_clears_singleton(self) -> None:
        """Test that reset_instance clears the singleton."""
        PluginManager.reset_instance()
        instance1 = PluginManager.get_instance()
        PluginManager.reset_instance()
        instance2 = PluginManager.get_instance()
        assert instance1 is not instance2

    def test_get_plugin_manager_function(self) -> None:
        """Test the convenience function get_plugin_manager."""
        PluginManager.reset_instance()
        pm = get_plugin_manager()
        assert pm is PluginManager.get_instance()


class TestPluginManagerRegistration:
    """Tests for plugin registration."""

    def test_register_plugin(self, fresh_plugin_manager: PluginManager) -> None:
        """Test registering a plugin."""
        plugin = SamplePlugin()
        name = fresh_plugin_manager.register(plugin, name="test_plugin")
        assert name == "test_plugin"

    def test_register_plugin_without_name(
        self, fresh_plugin_manager: PluginManager
    ) -> None:
        """Test registering a plugin without explicit name."""
        plugin = SamplePlugin()
        name = fresh_plugin_manager.register(plugin)
        assert name is not None

    def test_is_registered(self, fresh_plugin_manager: PluginManager) -> None:
        """Test checking if plugin is registered."""
        plugin = SamplePlugin()
        assert not fresh_plugin_manager.is_registered(plugin)

        fresh_plugin_manager.register(plugin)
        assert fresh_plugin_manager.is_registered(plugin)

    def test_unregister_plugin(
        self, fresh_plugin_manager: PluginManager
    ) -> None:
        """Test unregistering a plugin."""
        plugin = SamplePlugin()
        fresh_plugin_manager.register(plugin, name="test_plugin")

        unregistered = fresh_plugin_manager.unregister(name="test_plugin")
        assert unregistered is plugin
        assert not fresh_plugin_manager.is_registered(plugin)

    def test_unregister_by_instance(
        self, fresh_plugin_manager: PluginManager
    ) -> None:
        """Test unregistering by plugin instance."""
        plugin = SamplePlugin()
        fresh_plugin_manager.register(plugin)

        unregistered = fresh_plugin_manager.unregister(plugin=plugin)
        assert unregistered is plugin


class TestPluginManagerPluginListing:
    """Tests for plugin listing methods."""

    def test_get_plugins(self, fresh_plugin_manager: PluginManager) -> None:
        """Test getting list of plugins."""
        plugin1 = SamplePlugin()
        plugin2 = EmptyPlugin()

        fresh_plugin_manager.register(plugin1, name="plugin1")
        fresh_plugin_manager.register(plugin2, name="plugin2")

        plugins = fresh_plugin_manager.get_plugins()
        assert plugin1 in plugins
        assert plugin2 in plugins

    def test_get_plugin_names(
        self, fresh_plugin_manager: PluginManager
    ) -> None:
        """Test getting list of plugin names."""
        fresh_plugin_manager.register(SamplePlugin(), name="test_plugin")
        fresh_plugin_manager.register(EmptyPlugin(), name="empty_plugin")

        names = fresh_plugin_manager.get_plugin_names()
        assert "test_plugin" in names
        assert "empty_plugin" in names


class TestPluginManagerHooks:
    """Tests for hook invocation."""

    def test_hook_property(self, fresh_plugin_manager: PluginManager) -> None:
        """Test accessing hook relay."""
        hook = fresh_plugin_manager.hook
        assert hook is not None
        assert hasattr(hook, "limbo_register_connections")

    def test_register_connections_hook(
        self, fresh_plugin_manager: PluginManager
    ) -> None:
        """Test that connection hook is called during load_plugins."""
        ConnectionRegistry.clear()
        fresh_plugin_manager.register(SamplePlugin(), name="test")
        fresh_plugin_manager.load_plugins()

        types = ConnectionRegistry.get_types()
        assert "test_plugin_conn" in types

    def test_empty_plugin_hook_result(
        self, fresh_plugin_manager: PluginManager
    ) -> None:
        """Test plugin returning empty list."""
        ConnectionRegistry.clear()
        fresh_plugin_manager.register(EmptyPlugin(), name="empty")
        fresh_plugin_manager.load_plugins()

        # Should not crash, just no connections added
        assert ConnectionRegistry.get_types() == {}


class TestPluginManagerLoadPlugins:
    """Tests for load_plugins method."""

    def test_load_plugins_only_once(
        self, fresh_plugin_manager: PluginManager
    ) -> None:
        """Test that load_plugins only runs once."""
        ConnectionRegistry.clear()
        fresh_plugin_manager.register(SamplePlugin(), name="test")

        fresh_plugin_manager.load_plugins()
        assert "test_plugin_conn" in ConnectionRegistry.get_types()

        # Modify registry to detect if load_plugins runs again
        ConnectionRegistry.clear()
        fresh_plugin_manager.load_plugins()

        # Registry should still be empty because load_plugins didn't run again
        assert ConnectionRegistry.get_types() == {}

    def test_load_plugins_marks_loaded(
        self, fresh_plugin_manager: PluginManager
    ) -> None:
        """Test that _plugins_loaded is set after loading."""
        assert not fresh_plugin_manager._plugins_loaded
        fresh_plugin_manager.load_plugins()
        assert fresh_plugin_manager._plugins_loaded
