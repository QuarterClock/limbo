"""Narrow value resolution interface.

Services that only need to resolve lookup values should depend on this port,
not the full ``ValueReaderRegistryPort``.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from limbo_core.domain.entities import LookupValue


class ValueResolverPort(ABC):
    """Resolve lookup value specs into concrete string values."""

    @abstractmethod
    def resolve(self, lookup: LookupValue) -> str:
        """Resolve one lookup payload into a string value."""
