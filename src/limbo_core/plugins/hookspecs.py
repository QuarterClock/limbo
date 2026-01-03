"""Hook specifications for the limbo plugin system.

This module defines all hook specifications that plugins can implement
to extend limbo's functionality.

Hook Categories:
    - Connections: Register custom database connection types
    - (Future) Generators: Register custom field generators
    - (Future) Persistors: Register custom data persistors

Example:
    Implementing hooks in a plugin::

        from limbo_core.plugins.markers import hookimpl
        from limbo_core.connections import Connection

        class MyPlugin:
            @hookimpl
            def limbo_register_connections(self) -> list[type[Connection]]:
                from my_plugin.connections import MyCustomConnection
                return [MyCustomConnection]
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from .markers import hookspec

if TYPE_CHECKING:
    from limbo_core.connections import Connection


class LimboHookSpec:
    """Hook specifications for limbo plugins.

    Plugins implement these hooks to extend limbo's functionality.
    Each hook is called during the plugin loading phase, and results
    from all plugins are collected and processed.
    """

    # =========================================================================
    # Connection Hooks
    # =========================================================================

    @hookspec
    def limbo_register_connections(  # type: ignore[empty-body]
        self,
    ) -> list[type[Connection]]:
        """Register custom connection types.

        Implement this hook to register custom database connection types
        that can be used in project configurations.

        Returns:
            List of Connection subclasses to register. Each class must have
            a `type` field with a unique Literal default value.

        Example:
            >>> @hookimpl
            ... def limbo_register_connections(self) -> list[type[Connection]]:
            ...     return [PostgreSQLConnection, MySQLConnection]
        """

    # =========================================================================
    # Future Hook Categories
    # =========================================================================
    # The following hooks will be added as the plugin system expands:
    # - limbo_register_generators: Register custom field generators
    # - limbo_register_persistors: Register custom data persistors
    # - limbo_startup / limbo_shutdown: Lifecycle hooks
