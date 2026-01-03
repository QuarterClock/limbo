"""Limbo plugin system.

This module provides the plugin infrastructure for extending limbo
with custom connections, generators, persistors, and other components.

Quick Start:
    Loading plugins in your application::

        from limbo_core.plugins import load_plugins

        # Load all plugins (including built-in and external)
        load_plugins()

    Creating a plugin::

        from limbo_core.plugins import hookimpl
        from limbo_core.connections import Connection

        class MyPlugin:
            @hookimpl
            def limbo_register_connections(self) -> list[type[Connection]]:
                return [MyCustomConnection]

    Registering a plugin package (in pyproject.toml)::

        [project.entry-points.limbo]
        my_plugin = "my_plugin:MyPlugin"

Public API:
    - hookspec: Decorator for defining hook specifications
    - hookimpl: Decorator for implementing hooks
    - PluginManager: Central plugin manager class
    - get_plugin_manager: Get the global plugin manager instance
    - load_plugins: Load all plugins and register components
"""

from .builtin import BuiltinPlugin
from .manager import PluginManager, get_plugin_manager, load_plugins
from .markers import hookimpl, hookspec

__all__ = [
    "BuiltinPlugin",
    "PluginManager",
    "get_plugin_manager",
    "hookimpl",
    "hookspec",
    "load_plugins",
]

# Register the built-in plugin automatically when this module is imported
_pm = get_plugin_manager()
_pm.register(BuiltinPlugin(), name="limbo_builtin")
