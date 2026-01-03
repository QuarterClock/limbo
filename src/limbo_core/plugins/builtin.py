"""Built-in plugin providing default limbo components.

This plugin is automatically registered and provides the core
functionality that ships with limbo, including:
- SQLAlchemy connection type
- (Future) Default field generators
- (Future) Default persistors

The built-in plugin is always loaded first, ensuring that default
components are available before any external plugins are loaded.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from .markers import hookimpl

if TYPE_CHECKING:
    from limbo_core.connections import Connection


class BuiltinPlugin:
    """Built-in plugin providing default limbo functionality.

    This plugin registers the core components that ship with limbo.
    It is automatically registered by the plugin manager.
    """

    @hookimpl
    def limbo_register_connections(self) -> list[type[Connection]]:
        """Register built-in connection types.

        Returns:
            List containing SQLAlchemyConnection.
        """
        from limbo_core.connections.sqlalchemy import SQLAlchemyConnection

        return [SQLAlchemyConnection]

    # NOTE: Future hooks for generators, persistors, etc. will be added here
    # as new hook specifications are defined in hookspecs.py
