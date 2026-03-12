"""Narrow path resolution interface.

Services that only need to resolve paths (e.g. ``ProjectValidatorService``)
should depend on this port, not the full ``PathBackendRegistryPort``.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from limbo_core.domain.entities import ResolvedResource


class PathResolverPort(ABC):
    """Resolve resource paths for runtime validation and IO flows."""

    @abstractmethod
    def resolve(
        self, raw_path: Any, *, paths: dict[str, Any]
    ) -> ResolvedResource:
        """Resolve and validate a resource path expression."""
