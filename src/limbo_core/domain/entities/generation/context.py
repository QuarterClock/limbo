"""Generation context shared across generators."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class GenerationContext:
    """Mutable context for a single table generation run."""

    table_name: str = ""
    row_index: int = 0
    row_data: dict[str, Any] = field(default_factory=dict)
    shared_state: dict[str, Any] = field(default_factory=dict)
