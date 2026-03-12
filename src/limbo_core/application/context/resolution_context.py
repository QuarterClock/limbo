"""Per-invocation context for resource resolution."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathlib import Path


@dataclass(frozen=True, slots=True)
class ResolutionContext:
    """Immutable per-file metadata threaded through resolution calls.

    Carries the schema file's directory and optional extra aliases so that
    path backends can resolve relative locations without mutable registry
    state.
    """

    source_dir: Path | None = None
    extra_aliases: dict[str, Any] = field(default_factory=dict)

    def build_alias_map(self) -> dict[str, Any]:
        """Build the base-path alias mapping from context fields.

        Returns:
            Merged alias dict (``"this"`` points to *source_dir* when set).
        """
        aliases: dict[str, Any] = dict(self.extra_aliases)
        if self.source_dir is not None:
            aliases.setdefault("this", self.source_dir)
        return aliases
