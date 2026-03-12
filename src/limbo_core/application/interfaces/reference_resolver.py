"""Reference resolution interface."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class ReferenceResolver(ABC):
    """Resolve runtime reference expressions like ``table.column``."""

    @abstractmethod
    def resolve(self, reference: str) -> Any:
        """Resolve a reference string into a concrete runtime value."""
