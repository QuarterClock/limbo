from dataclasses import dataclass

from limbo_core.domain.entities.artifacts.artifact import Artifact

from .column import TableColumn
from .config import TableConfig
from .reference import TableReference


@dataclass(slots=True, kw_only=True)
class Table(Artifact[TableConfig, TableColumn]):
    """Table definition."""

    references: list[TableReference] | None = None
