"""Data persistence registry interface."""

from __future__ import annotations

from limbo_core.application.interfaces.base_registry import BaseRegistryPort
from limbo_core.domain.entities.backends.destination_backend_spec import (
    DestinationBackendSpec,
)

from .data_persistence_backend import DataPersistenceBackend
from .data_persistence_resolver_port import DataPersistenceResolverPort


class DataPersistenceRegistryPort(
    BaseRegistryPort[DataPersistenceBackend, DestinationBackendSpec],
    DataPersistenceResolverPort,
):
    """Registry contract for tabular data persistence backends."""
