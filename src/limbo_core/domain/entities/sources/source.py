from dataclasses import dataclass

from limbo_core.domain.entities.artifacts.artifact import Artifact

from .column import SourceColumn
from .config import SourceConfig


@dataclass(slots=True, kw_only=True)
class Source(Artifact[SourceConfig, SourceColumn]):
    """Source definition."""
