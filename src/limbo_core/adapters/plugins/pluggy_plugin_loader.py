"""Pluggy-backed plugin loader adapter."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from limbo_core.application.interfaces import PluginLoader

if TYPE_CHECKING:
    from limbo_core.plugins.plugin_manager import PluginManager


@dataclass(slots=True)
class PluggyPluginLoader(PluginLoader):
    """Adapter exposing plugin loading through application port."""

    manager: PluginManager

    def load_plugins(self) -> None:
        """Load plugins through pluggy manager."""
        self.manager.load_plugins()
