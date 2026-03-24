"""Path resolver registry interface."""

from __future__ import annotations

from abc import abstractmethod
from typing import TYPE_CHECKING, Any

from limbo_core.application.interfaces.base_registry import BaseRegistryPort
from limbo_core.domain.entities.backends.path_backend_spec import (
    PathBackendSpec,
)

from .path_resolver_backend import PathResolverBackend
from .path_resolver_port import PathResolverPort

if TYPE_CHECKING:
    from limbo_core.application.context import ResolutionContext
    from limbo_core.domain.entities.resources.path_spec import PathSpec
    from limbo_core.domain.value_objects import ResolvedStorageRef


class PathResolverRegistryPort(
    BaseRegistryPort[PathResolverBackend, PathBackendSpec], PathResolverPort
):
    """Registry contract for path resolver backend classes and instances."""

    @abstractmethod
    def resolve_spec(
        self,
        path_spec: PathSpec,
        *,
        base: Any | None = None,
        context: ResolutionContext | None = None,
        allow_missing: bool = False,
    ) -> ResolvedStorageRef:
        """Resolve a parsed ``PathSpec`` without re-parsing a raw payload."""
