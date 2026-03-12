"""Project aggregate root entity."""

from dataclasses import dataclass, field

from .backends import (
    ConnectionBackendSpec,
    DestinationBackendSpec,
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
    destinations: list[DestinationBackendSpec]
    connections: list[ConnectionBackendSpec] = field(default_factory=list)
    tables: list[Table]
    seeds: list[Seed] = field(default_factory=list)
    sources: list[Source] = field(default_factory=list)
