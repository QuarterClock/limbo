"""Plugin-facing backend registration descriptors."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, TypeVar

from .connections import ConnectionBackend
from .persistence import DataPersistenceBackend, PathResolverBackend
from .value_reader import ValueReaderBackend

BackendType = TypeVar(
    "BackendType",
    bound=ConnectionBackend
    | ValueReaderBackend
    | PathResolverBackend
    | DataPersistenceBackend,
)


@dataclass(frozen=True, slots=True)
class BackendRegistration(Generic[BackendType]):
    """Register one connection backend class under a key."""

    key: str
    backend_class: type[BackendType]
