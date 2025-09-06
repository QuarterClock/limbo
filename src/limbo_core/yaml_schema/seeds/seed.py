from limbo_core.yaml_schema.artifacts.artifact import Artifact

from .column import SeedColumn
from .config import SeedConfig
from .file import SeedFile


class Seed(Artifact[SeedConfig, SeedColumn]):
    """Seed definition."""

    # New fields
    seed_file: SeedFile
