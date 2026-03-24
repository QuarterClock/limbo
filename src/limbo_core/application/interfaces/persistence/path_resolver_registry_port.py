"""Path resolver registry interface."""

from __future__ import annotations

from limbo_core.application.interfaces.base_registry import BaseRegistryPort
from limbo_core.domain.entities.backends.path_backend_spec import (
    PathBackendSpec,
)

from .path_resolver_backend import PathResolverBackend
from .path_resolver_port import PathResolverPort


class PathResolverRegistryPort(
    BaseRegistryPort[PathResolverBackend, PathBackendSpec], PathResolverPort
):
    """Registry contract for path resolver backend classes and instances."""
