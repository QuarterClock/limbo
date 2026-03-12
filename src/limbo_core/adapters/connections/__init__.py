"""Connection adapters."""

from limbo_core.application.interfaces import ConnectionBackend

from .errors import MissingPackageError
from .registry import ConnectionRegistry

__all__ = ["ConnectionBackend", "ConnectionRegistry", "MissingPackageError"]
