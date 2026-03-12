"""Connection backend registry interface."""

from __future__ import annotations

from limbo_core.application.interfaces.base_registry import BaseRegistryPort
from limbo_core.domain.entities.backends.connection_spec import (
    ConnectionBackendSpec,
)

from .connection_backend import ConnectionBackend
from .connection_provider import ConnectionProviderPort


class ConnectionRegistryPort(
    BaseRegistryPort[ConnectionBackend, ConnectionBackendSpec],
    ConnectionProviderPort,
):
    """Registry contract for connection backend classes and instances."""
