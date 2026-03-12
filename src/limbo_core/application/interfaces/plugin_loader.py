"""Plugin loading interface."""

from __future__ import annotations

from abc import ABC, abstractmethod


class PluginLoader(ABC):
    """Abstract plugin loader contract for orchestration services."""

    @abstractmethod
    def load_plugins(self) -> None:
        """Load plugins and register their components."""
