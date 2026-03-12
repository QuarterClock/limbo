"""Tests for instance-based PluginManager."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from limbo_core.adapters.connections import ConnectionRegistry
from limbo_core.adapters.filesystem import PathBackendRegistry
from limbo_core.adapters.value_reader import ValueReaderRegistry
from limbo_core.application.interfaces import (
    BackendRegistration,
    ConnectionBackend,
    PathBackend,
    ValueReaderBackend,
)
from limbo_core.domain.entities import ResolvedResource
from limbo_core.plugins import PluginManager, hookimpl

if TYPE_CHECKING:
    from limbo_core.domain.entities import ConnectionBackendSpec


class PluginTestConnectionBackend(ConnectionBackend):
    """Connection backend used for plugin tests."""

    name: str

    def __init__(self, *, name: str) -> None:
        self.name = name

    @classmethod
    def from_spec(
        cls, spec: ConnectionBackendSpec
    ) -> PluginTestConnectionBackend:
        return cls(name=spec.name)

    def connect(self) -> None:
        """Mock connect."""


class SamplePlugin:
    """Sample plugin implementing connection hook."""

    @hookimpl
    def limbo_register_connections(
        self,
    ) -> list[BackendRegistration[ConnectionBackend]]:
        """Register test connection."""
        return [
            BackendRegistration(
                key="test_plugin_conn",
                backend_class=PluginTestConnectionBackend,
            )
        ]


class EmptyPlugin:
    """Plugin that returns an empty connection list."""

    @hookimpl
    def limbo_register_connections(
        self,
    ) -> list[BackendRegistration[ConnectionBackend]]:
        """Return empty list."""
        return []


class _StaticReader(ValueReaderBackend):
    def get(self, key: str, default: str | None = None) -> str | None:
        return f"value:{key}"


class _MockPathBackend(PathBackend):
    def resolve(
        self, path_spec: object, *, base: object | None = None
    ) -> ResolvedResource:
        return ResolvedResource(backend="mock", uri="mock://resource")


class ReaderPlugin:
    """Plugin that contributes value readers."""

    @hookimpl
    def limbo_register_value_readers(
        self,
    ) -> list[BackendRegistration[ValueReaderBackend]]:
        return [BackendRegistration(key="static", backend_class=_StaticReader)]


class PathPlugin:
    """Plugin that contributes path backends."""

    @hookimpl
    def limbo_register_path_backends(
        self,
    ) -> list[BackendRegistration[PathBackend]]:
        return [BackendRegistration(key="mock", backend_class=_MockPathBackend)]


@pytest.fixture
def connection_registry() -> ConnectionRegistry:
    """Provide isolated connection registry per test."""
    return ConnectionRegistry()


@pytest.fixture
def value_reader_registry() -> ValueReaderRegistry:
    """Provide isolated value reader registry per test."""
    return ValueReaderRegistry()


@pytest.fixture
def path_backend_registry() -> PathBackendRegistry:
    """Provide isolated path backend registry per test."""
    return PathBackendRegistry()


@pytest.fixture
def fresh_plugin_manager(
    connection_registry: ConnectionRegistry,
    value_reader_registry: ValueReaderRegistry,
    path_backend_registry: PathBackendRegistry,
) -> PluginManager:
    """Create a fresh PluginManager instance."""
    return PluginManager(
        connection_registry=connection_registry,
        value_reader_registry=value_reader_registry,
        path_backend_registry=path_backend_registry,
    )


class TestPluginManagerInitialization:
    """Tests for manager initialization behavior."""

    def test_builtin_plugin_registered(
        self, fresh_plugin_manager: PluginManager
    ) -> None:
        """Built-in plugin is registered at initialization time."""
        assert "limbo_builtin" in fresh_plugin_manager.get_plugin_names()

    def test_managers_are_independent(self) -> None:
        """Separate manager instances maintain separate plugin state."""
        first = PluginManager(
            connection_registry=ConnectionRegistry(),
            value_reader_registry=ValueReaderRegistry(),
            path_backend_registry=PathBackendRegistry(),
        )
        second = PluginManager(
            connection_registry=ConnectionRegistry(),
            value_reader_registry=ValueReaderRegistry(),
            path_backend_registry=PathBackendRegistry(),
        )
        first.register(SamplePlugin(), name="first")
        assert "first" in first.get_plugin_names()
        assert "first" not in second.get_plugin_names()

    def test_get_plugins_includes_builtin(
        self, fresh_plugin_manager: PluginManager
    ) -> None:
        """Built-in plugin appears in get_plugins listing."""
        plugins = fresh_plugin_manager.get_plugins()
        assert plugins
        assert "limbo_builtin" in fresh_plugin_manager.get_plugin_names()

    def test_ensure_builtin_registered_is_idempotent(
        self, fresh_plugin_manager: PluginManager
    ) -> None:
        """Re-running builtin registration does not duplicate plugin entry."""
        before = fresh_plugin_manager.get_plugin_names().count("limbo_builtin")
        fresh_plugin_manager._ensure_builtin_registered()
        after = fresh_plugin_manager.get_plugin_names().count("limbo_builtin")
        assert before == 1
        assert after == 1


class TestPluginManagerRegistration:
    """Tests for plugin registration lifecycle."""

    def test_register_plugin(self, fresh_plugin_manager: PluginManager) -> None:
        """Registering plugin with explicit name succeeds."""
        plugin = SamplePlugin()
        name = fresh_plugin_manager.register(plugin, name="test_plugin")
        assert name == "test_plugin"

    def test_register_plugin_without_name(
        self, fresh_plugin_manager: PluginManager
    ) -> None:
        """Registering plugin without explicit name returns generated name."""
        name = fresh_plugin_manager.register(SamplePlugin())
        assert name is not None

    def test_is_registered(self, fresh_plugin_manager: PluginManager) -> None:
        """is_registered reflects plugin registration status."""
        plugin = SamplePlugin()
        assert not fresh_plugin_manager.is_registered(plugin)
        fresh_plugin_manager.register(plugin)
        assert fresh_plugin_manager.is_registered(plugin)

    def test_unregister_plugin(
        self, fresh_plugin_manager: PluginManager
    ) -> None:
        """Unregistering by name removes plugin."""
        plugin = SamplePlugin()
        fresh_plugin_manager.register(plugin, name="test_plugin")
        unregistered = fresh_plugin_manager.unregister(name="test_plugin")
        assert unregistered is plugin
        assert not fresh_plugin_manager.is_registered(plugin)

    def test_unregister_by_instance(
        self, fresh_plugin_manager: PluginManager
    ) -> None:
        """Unregistering by plugin instance removes plugin."""
        plugin = SamplePlugin()
        fresh_plugin_manager.register(plugin)
        unregistered = fresh_plugin_manager.unregister(plugin=plugin)
        assert unregistered is plugin


class TestPluginManagerLoadPlugins:
    """Tests for plugin load flow and registry updates."""

    def test_register_connections_hook(
        self,
        fresh_plugin_manager: PluginManager,
        connection_registry: ConnectionRegistry,
    ) -> None:
        """Connection hooks register connection classes into registry."""
        fresh_plugin_manager.register(SamplePlugin(), name="test")
        fresh_plugin_manager.load_plugins()
        types = connection_registry.get_types()
        assert "test_plugin_conn" in types

    def test_empty_plugin_hook_result(
        self,
        fresh_plugin_manager: PluginManager,
        connection_registry: ConnectionRegistry,
    ) -> None:
        """Empty hook results are handled without side effects."""
        fresh_plugin_manager.register(EmptyPlugin(), name="empty")
        fresh_plugin_manager.load_plugins()
        assert set(connection_registry.get_types()) == {"sqlalchemy"}

    def test_load_plugins_only_once(
        self,
        fresh_plugin_manager: PluginManager,
        connection_registry: ConnectionRegistry,
    ) -> None:
        """load_plugins is idempotent for one manager instance."""
        fresh_plugin_manager.register(SamplePlugin(), name="test")
        fresh_plugin_manager.load_plugins()
        assert "test_plugin_conn" in connection_registry.get_types()

        connection_registry.clear_types()
        fresh_plugin_manager.load_plugins()
        assert connection_registry.get_types() == {}

    def test_load_plugins_marks_loaded(
        self, fresh_plugin_manager: PluginManager
    ) -> None:
        """Manager tracks that plugins were loaded."""
        assert not fresh_plugin_manager._plugins_loaded
        fresh_plugin_manager.load_plugins()
        assert fresh_plugin_manager._plugins_loaded

    def test_register_value_readers_hook(self) -> None:
        """Value-reader hook populates the value reader registry."""
        value_reader_registry = ValueReaderRegistry()
        manager = PluginManager(
            connection_registry=ConnectionRegistry(),
            value_reader_registry=value_reader_registry,
            path_backend_registry=PathBackendRegistry(),
        )
        manager.register(ReaderPlugin(), name="reader_plugin")
        manager.load_plugins()
        assert "static" in value_reader_registry.get_types()
        assert "env" in value_reader_registry.get_types()

    def test_register_path_backends_hook(self) -> None:
        """Path-backend hook populates path backend registry."""
        path_backend_registry = PathBackendRegistry()
        manager = PluginManager(
            connection_registry=ConnectionRegistry(),
            value_reader_registry=ValueReaderRegistry(),
            path_backend_registry=path_backend_registry,
        )
        manager.register(PathPlugin(), name="path_plugin")
        manager.load_plugins()
        resolved = path_backend_registry.resolve({
            "path_from": {"backend": "mock", "location": "mock://x"}
        })
        assert resolved.backend == "mock"


class TestNoImportSideEffects:
    """Tests for import-time behavior."""

    def test_importing_connection_adapters_does_not_load_plugins(self) -> None:
        """Importing adapters must not auto-register plugin connections."""
        script = """
import sys
sys.path.insert(0, {src!r})
from limbo_core.adapters.connections import ConnectionRegistry
types = ConnectionRegistry().get_types()
sys.exit(0 if "sqlalchemy" not in types else 1)
"""
        src = Path(__file__).resolve().parent.parent.parent / "src"
        proc = subprocess.run(
            [sys.executable, "-c", script.format(src=str(src))],
            capture_output=True,
            text=True,
            timeout=10,
        )
        assert proc.returncode == 0, (
            "connection adapters import must not load plugins; "
            f"stderr: {proc.stderr}"
        )
