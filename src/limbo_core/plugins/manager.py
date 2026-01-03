"""Plugin manager for the limbo plugin system.

This module provides the central plugin manager that handles:
- Plugin discovery via setuptools entry points
- Plugin registration and lifecycle management
- Hook invocation and result collection

The plugin manager uses pluggy under the hood and supports automatic
discovery of plugins installed as Python packages.

Example:
    Loading plugins and registering components::

        from limbo_core.plugins import PluginManager

        # Create and load plugins
        pm = PluginManager()
        pm.load_plugins()

        # Or use the convenience function
        from limbo_core.plugins import load_plugins
        load_plugins()
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pluggy

from .hookspecs import LimboHookSpec
from .markers import PROJECT_NAME

if TYPE_CHECKING:
    from limbo_core.connections import Connection


class PluginManager:
    """Central plugin manager for limbo.

    Handles plugin discovery, registration, and hook invocation.
    Plugins can be registered manually or discovered automatically
    via setuptools entry points.

    Attributes:
        pm: The underlying pluggy PluginManager instance.

    Example:
        >>> manager = PluginManager()
        >>> manager.register(MyPlugin())
        >>> manager.load_setuptools_plugins()
        >>> manager.load_plugins()
    """

    _instance: PluginManager | None = None

    def __init__(self) -> None:
        """Initialize the plugin manager."""
        self._pm = pluggy.PluginManager(PROJECT_NAME)
        self._pm.add_hookspecs(LimboHookSpec)
        self._plugins_loaded = False

    @classmethod
    def get_instance(cls) -> PluginManager:
        """Get the singleton plugin manager instance.

        Returns:
            The global PluginManager instance.
        """
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    @classmethod
    def reset_instance(cls) -> None:
        """Reset the singleton instance.

        Primarily useful for testing.
        """
        cls._instance = None

    @property
    def hook(self) -> pluggy.HookRelay:
        """Access the hook relay for calling hooks.

        Returns:
            The pluggy HookRelay for invoking hooks.
        """
        return self._pm.hook

    def register(self, plugin: object, name: str | None = None) -> str | None:
        """Register a plugin instance.

        Args:
            plugin: The plugin instance to register.
            name: Optional name for the plugin.

        Returns:
            The plugin name if registration succeeded, None otherwise.
        """
        return self._pm.register(plugin, name)

    def unregister(
        self, plugin: object | None = None, name: str | None = None
    ) -> object | None:
        """Unregister a plugin.

        Args:
            plugin: The plugin instance to unregister.
            name: The name of the plugin to unregister.

        Returns:
            The unregistered plugin, or None if not found.
        """
        return self._pm.unregister(plugin, name)

    def is_registered(self, plugin: object) -> bool:
        """Check if a plugin is registered.

        Args:
            plugin: The plugin instance to check.

        Returns:
            True if the plugin is registered, False otherwise.
        """
        return self._pm.is_registered(plugin)

    def load_setuptools_plugins(self) -> int:
        """Load plugins from setuptools entry points.

        Discovers and loads plugins registered under the 'limbo'
        entry point group.

        Returns:
            Number of plugins loaded.
        """
        return self._pm.load_setuptools_entrypoints(PROJECT_NAME)

    def load_plugins(self) -> None:
        """Load all plugins and register their components.

        This method:
        1. Loads plugins from setuptools entry points
        2. Calls all registration hooks
        3. Registers discovered components with their registries

        Should be called once during application startup.
        """
        if self._plugins_loaded:
            return

        # Load external plugins from entry points
        self.load_setuptools_plugins()

        self._register_all_components()

        self._plugins_loaded = True

    def _register_connections(self) -> None:
        """Register connections from all plugins."""
        from limbo_core.connections import ConnectionRegistry

        # Collect connection types from all plugins
        results: list[list[type[Connection]]] = (
            self.hook.limbo_register_connections()
        )
        for connection_list in results:
            if connection_list:
                for connection_class in connection_list:
                    ConnectionRegistry.add(connection_class)

    def _register_all_components(self) -> None:
        """Register all components from all plugins."""
        # Register connections from all plugins
        self._register_connections()

        # NOTE: Future component registration (generators, persistors)
        # will be added here as new hooks are implemented

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


# Module-level convenience functions


def get_plugin_manager() -> PluginManager:
    """Get the global plugin manager instance.

    Returns:
        The singleton PluginManager instance.
    """
    return PluginManager.get_instance()


def load_plugins() -> None:
    """Load all plugins and register their components.

    Convenience function that gets the global plugin manager
    and loads all plugins.
    """
    get_plugin_manager().load_plugins()
