"""Persistence read backend registry interface."""

from __future__ import annotations

from limbo_core.application.interfaces.base_registry import BaseRegistryPort
from limbo_core.domain.entities.backends.path_backend_spec import (
    PathBackendSpec,
)

from .read_backend import PersistenceReadBackend
from .read_resolver import PersistenceReadResolverPort


class PersistenceReadRegistryPort(
    BaseRegistryPort[PersistenceReadBackend, PathBackendSpec],
    PersistenceReadResolverPort,
):
    """Registry contract for persistence read backend classes and instances."""
