"""Backend interface for resolving logical paths to storage references."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from limbo_core.domain.entities import PathSpec
    from limbo_core.domain.value_objects import ResolvedStorageRef


class PathResolverBackend(ABC):
    """Resolve structured path specs to storage references."""

    @abstractmethod
    def resolve(
        self,
        path_spec: PathSpec,
        *,
        base: Any | None = None,
        allow_missing: bool = False,
    ) -> ResolvedStorageRef:
        """Resolve one structured path spec to a storage reference.

        Args:
            path_spec: Parsed path specification.
            base: Pre-resolved base value for relative path resolution.
                  The registry resolves the ``PathSpec.base`` alias before
                  calling this method; backends receive the concrete value.
            allow_missing: When True and the path is relative, do not require
                the target file to exist (for tabular outputs not yet written).
        """
