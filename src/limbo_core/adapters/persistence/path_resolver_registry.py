"""Path resolver registry with backend dispatch."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from limbo_core.adapters.base_registry import BaseRegistry
from limbo_core.application.interfaces import (
    PathResolverBackend,
    PathResolverRegistryPort,
)
from limbo_core.application.parsers.common import InvalidPathSpecError
from limbo_core.application.parsers.path_spec_parser import parse_path_spec
from limbo_core.domain.entities.backends.path_backend_spec import (
    PathBackendSpec,
)

from .errors import UnknownPathBackendError

if TYPE_CHECKING:
    from limbo_core.application.context import ResolutionContext
    from limbo_core.domain.entities.resources.path_spec import PathSpec
    from limbo_core.domain.value_objects import ResolvedStorageRef


@dataclass(slots=True)
class PathResolverRegistry(
    BaseRegistry[PathResolverBackend, PathBackendSpec], PathResolverRegistryPort
):
    """Resolve path expressions through backend dispatch."""

    _backend_label: str = "path resolver backend"

    def resolve(
        self, raw_path: Any, *, context: ResolutionContext | None = None
    ) -> ResolvedStorageRef:
        """Resolve one resource expression through selected backend.

        Returns:
            A storage reference for the resolved object.
        """
        path_spec = parse_path_spec(raw_path)
        backend_name = self._normalize_name(path_spec.backend)
        backend = self._instances.get(backend_name)
        if backend is None:
            backend = self.create(path_spec.backend)
        aliases = context.build_alias_map() if context is not None else {}
        base = self._resolve_base_alias(path_spec.base, aliases=aliases)
        return backend.resolve(path_spec, base=base, allow_missing=False)

    def resolve_spec(
        self,
        path_spec: PathSpec,
        *,
        base: Any | None = None,
        context: ResolutionContext | None = None,
        allow_missing: bool = False,
    ) -> ResolvedStorageRef:
        """Resolve a structured path spec to a storage reference.

        Returns:
            A ``ResolvedStorageRef`` for the resolved location.
        """
        backend_name = self._normalize_name(path_spec.backend)
        backend = self._instances.get(backend_name)
        if backend is None:
            backend = self.create(path_spec.backend)
        if path_spec.base is not None and context is not None:
            resolved_base = self._resolve_base_alias(
                path_spec.base, aliases=context.build_alias_map()
            )
        else:
            resolved_base = base
        return backend.resolve(
            path_spec, base=resolved_base, allow_missing=allow_missing
        )

    @staticmethod
    def _resolve_base_alias(
        base: str | None, *, aliases: dict[str, Any]
    ) -> Any | None:
        """Resolve a dotted base alias against the alias mapping.

        Returns:
            The alias value, or None when ``base`` is None.

        Raises:
            InvalidPathSpecError: If the base key is empty or unknown.
        """
        if base is None:
            return None

        parts = base.split(".")
        if not parts or not parts[0]:
            raise InvalidPathSpecError("`path_from.base` cannot be empty")
        if parts[0] not in aliases:
            raise InvalidPathSpecError(
                f"`path_from.base` unknown key: {parts[0]}"
            )

        value: Any = aliases[parts[0]]
        for part in parts[1:]:
            value = getattr(value, part)
        return value

    def _unknown_backend_error(self, backend_key: str) -> Exception:
        return UnknownPathBackendError(backend_key)
