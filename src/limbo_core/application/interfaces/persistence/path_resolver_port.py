"""Narrow path resolution interface."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from limbo_core.application.context import ResolutionContext
    from limbo_core.domain.value_objects import ResolvedStorageRef


class PathResolverPort(ABC):
    """Resolve resource locations for validation and downstream IO."""

    @abstractmethod
    def resolve(
        self, raw_path: Any, *, context: ResolutionContext | None = None
    ) -> ResolvedStorageRef:
        """Resolve and validate a resource expression."""
