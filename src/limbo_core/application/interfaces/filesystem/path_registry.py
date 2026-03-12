"""Path backend registry interface."""

from __future__ import annotations

from limbo_core.application.interfaces.base_registry import BaseRegistryPort
from limbo_core.domain.entities.backends.path_backend_spec import (
    PathBackendSpec,
)

from .path_backend import PathBackend
from .path_resolver import PathResolverPort


class PathBackendRegistryPort(
    BaseRegistryPort[PathBackend, PathBackendSpec], PathResolverPort
):
    """Registry contract for path backend classes and instances."""
