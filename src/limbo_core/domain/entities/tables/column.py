from dataclasses import dataclass

from limbo_core.domain.entities.artifacts.column import ArtifactColumn
from limbo_core.domain.entities.values import ValueSpec


@dataclass(slots=True, kw_only=True)
class TableColumn(ArtifactColumn):
    """Column definition with flexible options."""

    generator: str
    options: dict[str, ValueSpec] | None = None
