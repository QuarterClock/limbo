"""Path backend registry with backend dispatch."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from limbo_core.adapters.base_registry import BaseRegistry
from limbo_core.application.interfaces import (
    PathBackend,
    PathBackendRegistryPort,
)
from limbo_core.application.parsers.path_spec_parser import parse_path_spec
from limbo_core.domain.entities.backends.path_backend_spec import (
    PathBackendSpec,
)

from .errors import UnknownPathBackendError

if TYPE_CHECKING:
    from limbo_core.domain.entities import ResolvedResource


@dataclass(slots=True)
class PathBackendRegistry(
    BaseRegistry[PathBackend, PathBackendSpec], PathBackendRegistryPort
):
    """Resolve path expressions through backend dispatch."""

    _backend_label: str = "path backend"

    def resolve(
        self, raw_path: Any, *, paths: dict[str, Any]
    ) -> ResolvedResource:
        """Resolve one resource path through selected backend.

        Returns:
            Backend-agnostic resolved resource descriptor.
        """
        path_spec = parse_path_spec(raw_path)
        backend_name = self._normalize_name(path_spec.backend)
        backend = self._instances.get(backend_name)
        if backend is None:
            backend = self.create(path_spec.backend)
        return backend.resolve(path_spec, paths=paths)

    def _unknown_backend_error(self, backend_key: str) -> Exception:
        return UnknownPathBackendError(backend_key)
