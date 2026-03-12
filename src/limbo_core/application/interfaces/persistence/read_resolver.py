"""Narrow persistence resolution interface.

Services that only need to resolve resources (e.g. ``ProjectValidatorService``)
should depend on this port, not the full ``PersistenceReadRegistryPort``.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from limbo_core.application.context import ResolutionContext
    from limbo_core.domain.entities import ResolvedResource


class PersistenceReadResolverPort(ABC):
    """Resolve resource locations for runtime validation and IO flows."""

    @abstractmethod
    def resolve(
        self, raw_path: Any, *, context: ResolutionContext | None = None
    ) -> ResolvedResource:
        """Resolve and validate a resource expression."""
