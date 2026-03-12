"""OS environment adapter."""

from __future__ import annotations

import os
from dataclasses import dataclass

from limbo_core.application.interfaces import ValueReaderBackend


@dataclass(slots=True)
class OsEnvReader(ValueReaderBackend):
    """Read values from process environment."""

    def get(self, key: str, default: str | None = None) -> str | None:
        """Return environment variable value."""
        return os.environ.get(key, default)
