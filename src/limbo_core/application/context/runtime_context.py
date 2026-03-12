"""Runtime context for checks and generation operations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from limbo_core.application.interfaces import ReferenceResolver


@dataclass(slots=True)
class RuntimeContext:
    """Context for runtime checks and generation operations."""

    generators: set[str]
    reference_resolver: ReferenceResolver | None = None

    def resolve_reference(self, reference: str) -> Any:
        """Resolve a reference via configured runtime collaborator.

        Returns:
            Concrete resolved value for the provided reference.

        Raises:
            RuntimeError: If no reference resolver is configured.
        """
        if self.reference_resolver is None:
            raise RuntimeError("Reference resolver is not configured")
        return self.reference_resolver.resolve(reference)
