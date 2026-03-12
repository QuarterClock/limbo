from dataclasses import dataclass, field

from .backends import (
    ConnectionBackendSpec,
    PathBackendSpec,
    ValueReaderBackendSpec,
)
from .seeds import Seed
from .sources import Source
from .tables import Table
from .values import ValueSpec


@dataclass(slots=True, kw_only=True)
class Project:
    """Project configuration."""

    vars: dict[str, ValueSpec] | None = None
    value_readers: list[ValueReaderBackendSpec] = field(default_factory=list)
    path_backends: list[PathBackendSpec] = field(default_factory=list)
    connections: list[ConnectionBackendSpec] = field(default_factory=list)
    tables: list[Table]
    seeds: list[Seed]
    sources: list[Source]
