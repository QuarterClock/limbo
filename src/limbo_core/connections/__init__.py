from .base import Connection
from .registry import ConnectionRegistry
from .sqlalchemy import SQLAlchemyConnection

__all__ = ["Connection", "ConnectionRegistry", "SQLAlchemyConnection"]


def _ensure_plugins_loaded() -> None:
    """Ensure plugins are loaded and connections are registered.

    This is called automatically when the connections module is imported.
    It loads all plugins (including built-in and external) which triggers
    the registration of all connection types.
    """
    from limbo_core.plugins import load_plugins

    load_plugins()


# Load plugins when this module is imported
_ensure_plugins_loaded()
