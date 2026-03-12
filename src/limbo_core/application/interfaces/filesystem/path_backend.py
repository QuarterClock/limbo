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
        self, path_spec: PathSpec, *, base: Any | None = None
    ) -> ResolvedResource:
        """Resolve one structured path spec to a backend-agnostic resource.

        Args:
            path_spec: Parsed path specification.
            base: Pre-resolved base value for relative path resolution.
                  The registry resolves the ``PathSpec.base`` alias before
                  calling this method; backends receive the concrete value.
        """
