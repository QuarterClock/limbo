"""Backend interface for path resolution."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from limbo_core.domain.entities import PathSpec, ResolvedResource


class PathBackend(ABC):
    """Backend contract for resolving resource locations."""

    @abstractmethod
    def resolve(
        self, path_spec: PathSpec, *, paths: dict[str, Any]
    ) -> ResolvedResource:
        """Resolve one structured path spec to a backend-agnostic resource."""
