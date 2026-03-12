"""Connection registry for storing and validating connection types."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from limbo_core.adapters.base_registry import BaseRegistry
from limbo_core.application.context.errors import ConnectionNotFoundError
from limbo_core.application.interfaces import (
    ConnectionBackend,
    ConnectionRegistryPort,
)
from limbo_core.domain.entities.backends.connection_spec import (
    ConnectionBackendSpec,
)

from .errors import UnknownConnectionBackendError


@dataclass(slots=True)
class ConnectionRegistry(
    BaseRegistry[ConnectionBackend, ConnectionBackendSpec],
    ConnectionRegistryPort,
):
    """Registry for keyed connection backend classes and named instances."""

    _backend_label: str = "connection backend"

    def connect(self, name: str) -> Any:
        """Establish the underlying connection for a named backend.

        Returns:
            Engine or session object from the connection backend.

        Raises:
            ConnectionNotFoundError: If no backend is configured under *name*.
        """
        normalized = self._normalize_name(name)
        backend = self._instances.get(normalized)
        if backend is None:
            raise ConnectionNotFoundError(
                connection_name=name,
                available_connections=self._instances.keys(),
            )
        return backend.connect()

    def _unknown_backend_error(self, backend_key: str) -> Exception:
        return UnknownConnectionBackendError(backend_key)
