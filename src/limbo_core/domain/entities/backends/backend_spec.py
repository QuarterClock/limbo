"""Shared backend specification contract."""

from __future__ import annotations

from abc import ABC
from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True, kw_only=True)
class BackendSpec(ABC):
    """Common payload contract for project-declared backend bindings."""

    name: str
    type: str
    config: dict[str, Any] = field(default_factory=dict)
