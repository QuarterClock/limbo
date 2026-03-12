"""Narrow connection provider interface.

Services that only need to establish connections should depend on this port,
not the full ``ConnectionRegistryPort``.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class ConnectionProviderPort(ABC):
    """Establish named connections for runtime use."""

    @abstractmethod
    def connect(self, name: str) -> Any:
        """Establish the underlying connection for a named backend."""
