"""Persistence adapters."""

from .persistor import DefaultPersistor
from .read_registry import PersistenceReadRegistry
from .write_registry import PersistenceWriteRegistry

__all__ = [
    "DefaultPersistor",
    "PersistenceReadRegistry",
    "PersistenceWriteRegistry",
]
