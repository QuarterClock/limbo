"""Persistence write backend registry interface."""

from __future__ import annotations

from limbo_core.application.interfaces.base_registry import BaseRegistryPort
from limbo_core.domain.entities.backends.destination_backend_spec import (
    DestinationBackendSpec,
)

from .write_backend import PersistenceWriteBackend
from .write_resolver import PersistenceWriteResolverPort


class PersistenceWriteRegistryPort(
    BaseRegistryPort[PersistenceWriteBackend, DestinationBackendSpec],
    PersistenceWriteResolverPort,
):
    """Registry contract for persistence write backend classes and instances."""
