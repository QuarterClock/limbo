"""Interface for lookup-backed value readers."""

from __future__ import annotations

from abc import ABC, abstractmethod


class ValueReaderBackend(ABC):
    """Read configuration/runtime values from backend sources."""

    @abstractmethod
    def get(self, key: str, default: str | None = None) -> str | None:
        """Return backend value for key, or default when missing."""
