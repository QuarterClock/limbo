"""Backend-agnostic resolved seed resource value object."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathlib import Path


@dataclass(slots=True, kw_only=True)
class ResolvedResource:
    """Represents resolved seed resource with backend metadata."""

    backend: str
    uri: str
    local_path: Path | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
