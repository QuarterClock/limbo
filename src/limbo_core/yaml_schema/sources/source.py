from limbo_core.yaml_schema.artifacts.artifact import Artifact

from .column import SourceColumn
from .config import SourceConfig


class Source(Artifact[SourceConfig, SourceColumn]):
    """Source definition."""
