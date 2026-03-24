"""Hook specifications for the limbo plugin system.

This module defines all hook specifications that plugins can implement
to extend limbo's functionality.

Hook Categories:
    - Connections: Register custom database connection types
    - (Future) Generators: Register custom field generators
    - (Future) Persistors: Register custom data persistors

Example:
    Implementing hooks in a plugin::

        from limbo_core.application.interfaces import (
            BackendRegistration,
        )
        from limbo_core.plugins.markers import hookimpl

        class MyPlugin:
            @hookimpl
            def limbo_register_connections(
                self,
            ) -> list[BackendRegistration[ConnectionBackend]]:
                from my_plugin.connections import MyCustomConnection
                return [
                    BackendRegistration(
                        key="my_custom",
                        backend_class=MyCustomConnection,
                    )
                ]
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from .markers import hookspec

if TYPE_CHECKING:
    from limbo_core.application.interfaces import (
        BackendRegistration,
        ConnectionBackend,
        DataPersistenceBackend,
        GeneratorRegistration,
        PathResolverBackend,
        ValueReaderBackend,
    )


class LimboHookSpec:
    """Hook specifications for limbo plugins.

    Plugins implement these hooks to extend limbo's functionality.
    Each hook is called during the plugin loading phase, and results
    from all plugins are collected and processed.
    """

    @hookspec
    def limbo_register_connections(  # type: ignore[empty-body]
        self,
    ) -> list[BackendRegistration[ConnectionBackend]]:
        """Register custom connection types.

        Implement this hook to register custom database connection types
        that can be used in project configurations.

        Returns:
            List of explicit connection backend registrations.
        """

    @hookspec
    def limbo_register_value_readers(  # type: ignore[empty-body]
        self,
    ) -> list[BackendRegistration[ValueReaderBackend]]:
        """Register value readers for lookup-backed config resolution.

        Returns:
            List of explicit value reader backend registrations.
        """

    @hookspec
    def limbo_register_path_resolver_backends(  # type: ignore[empty-body]
        self,
    ) -> list[BackendRegistration[PathResolverBackend]]:
        """Register path resolver backends (filesystem, cloud, etc.).

        Returns:
            List of explicit path resolver backend registrations.
        """

    @hookspec
    def limbo_register_data_persistence_backends(  # type: ignore[empty-body]
        self,
    ) -> list[BackendRegistration[DataPersistenceBackend]]:
        """Register tabular data persistence backends (csv, json, …).

        Returns:
            List of explicit data persistence backend registrations.
        """

    @hookspec
    def limbo_register_generators(  # type: ignore[empty-body]
        self,
    ) -> list[GeneratorRegistration]:
        """Register generator classes for hook-based value generation."""

    # =========================================================================
    # Future Hook Categories
    # =========================================================================
    # The following hooks will be added as the plugin system expands:
    # - limbo_startup / limbo_shutdown: Lifecycle hooks
