"""Limbo plugin system.

This module provides the plugin infrastructure for extending limbo
with custom connections, generators, persistors, and other components.

Quick Start:
    Loading plugins in your application::

        from limbo_core.plugins import PluginManager

        manager = PluginManager()
        manager.load_plugins()

    Creating a plugin::

        from limbo_core.application.interfaces import (
            BackendRegistration,
        )
        from limbo_core.plugins import hookimpl

        class MyPlugin:
            @hookimpl
            def limbo_register_connections(
                self,
            ) -> list[BackendRegistration[ConnectionBackend]]:
                return [
                    BackendRegistration(
                        key="my_custom_connection",
                        backend_class=MyCustomConnection,
                    )
                ]

            @hookimpl
            def limbo_register_value_readers(
                self,
            ) -> list[BackendRegistration[ValueReaderBackend]]:
                return [
                    BackendRegistration(
                        key="env",
                        backend_class=OsEnvReader,
                    )
                ]

            @hookimpl
            def limbo_register_path_backends(
                self,
            ) -> list[BackendRegistration[PathBackendPort]]:
                return [
                    BackendRegistration(
                        key="file",
                        backend_class=FilesystemPathBackend,
                    )
                ]

    Registering a plugin package (in pyproject.toml)::

        [project.entry-points.limbo]
        my_plugin = "my_plugin:MyPlugin"

Public API:
    - hookspec: Decorator for defining hook specifications
    - hookimpl: Decorator for implementing hooks
    - PluginManager: Central plugin manager class
"""

from .builtin import BuiltinPlugin
from .markers import hookimpl, hookspec
from .plugin_manager import PluginManager

__all__ = ["BuiltinPlugin", "PluginManager", "hookimpl", "hookspec"]
