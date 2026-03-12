"""Plugin manager for limbo plugins."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pluggy

from .builtin import BuiltinPlugin
from .hookspecs import LimboHookSpec
from .markers import PROJECT_NAME

if TYPE_CHECKING:
    from limbo_core.application.interfaces import (
        BackendRegistration,
        ConnectionBackend,
        ConnectionRegistryPort,
        PathBackend,
        PathBackendRegistryPort,
        ValueReaderBackend,
        ValueReaderRegistryPort,
    )

BUILTIN_PLUGIN_NAME = "limbo_builtin"


class PluginManager:
    """Instance-based plugin manager for discovery and registration."""

    def __init__(
        self,
        connection_registry: ConnectionRegistryPort,
        value_reader_registry: ValueReaderRegistryPort,
        path_backend_registry: PathBackendRegistryPort,
    ) -> None:
        """Initialize plugin manager with explicit dependencies."""
        self._pm = pluggy.PluginManager(PROJECT_NAME)
        self._pm.add_hookspecs(LimboHookSpec)
        self._connection_registry = connection_registry
        self._value_reader_registry = value_reader_registry
        self._path_backend_registry = path_backend_registry
        self._plugins_loaded = False
        self._ensure_builtin_registered()

    @property
    def hook(self) -> pluggy.HookRelay:
        """Access the hook relay for calling hooks.

        Returns:
            The pluggy HookRelay for invoking hooks.
        """
        return self._pm.hook

    def register(self, plugin: object, name: str | None = None) -> str | None:
        """Register a plugin instance.

        Returns:
            Registered plugin name if successful.
        """
        return self._pm.register(plugin, name)

    def unregister(
        self, plugin: object | None = None, name: str | None = None
    ) -> object | None:
        """Unregister a plugin.

        Returns:
            The unregistered plugin, or None if not found.
        """
        return self._pm.unregister(plugin, name)

    def is_registered(self, plugin: object) -> bool:
        """Check whether plugin is registered.

        Returns:
            True if plugin is currently registered in this manager.
        """
        return self._pm.is_registered(plugin)

    def load_setuptools_plugins(self) -> int:
        """Load plugins from setuptools entry points.

        Returns:
            Number of plugins loaded from configured entry-point group.
        """
        return self._pm.load_setuptools_entrypoints(PROJECT_NAME)

    def load_plugins(self) -> None:
        """Load external plugins and register provided components."""
        if self._plugins_loaded:
            return

        self.load_setuptools_plugins()
        self._register_value_readers()
        self._register_path_backends()
        self._register_connections()
        self._plugins_loaded = True

    def get_plugins(self) -> list[object]:
        """Get all registered plugins.

        Returns:
            List of registered plugin instances.
        """
        return list(self._pm.get_plugins())

    def get_plugin_names(self) -> list[str]:
        """Get names of all registered plugins.

        Returns:
            List of plugin names.
        """
        return [
            name for name, _ in self._pm.list_name_plugin() if name is not None
        ]

    def _ensure_builtin_registered(self) -> None:
        """Register built-in plugin exactly once per manager instance."""
        if BUILTIN_PLUGIN_NAME not in self.get_plugin_names():
            self.register(BuiltinPlugin(), name=BUILTIN_PLUGIN_NAME)

    def _register_connections(self) -> None:
        """Register all connection classes contributed by plugins."""
        results: list[list[BackendRegistration[ConnectionBackend]]] = (
            self.hook.limbo_register_connections()
        )
        for registrations in results:
            for registration in registrations:
                self._connection_registry.register(
                    registration.key, registration.backend_class
                )

    def _register_value_readers(self) -> None:
        """Register value readers contributed by plugins."""
        results: list[list[BackendRegistration[ValueReaderBackend]]] = (
            self.hook.limbo_register_value_readers()
        )
        for registrations in results:
            for registration in registrations:
                self._value_reader_registry.register(
                    registration.key, registration.backend_class
                )

    def _register_path_backends(self) -> None:
        """Register path backends contributed by plugins."""
        results: list[list[BackendRegistration[PathBackend]]] = (
            self.hook.limbo_register_path_backends()
        )
        for registrations in results:
            for registration in registrations:
                self._path_backend_registry.register(
                    registration.key, registration.backend_class
                )
