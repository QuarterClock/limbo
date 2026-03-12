"""Seed artifact entity."""

from dataclasses import dataclass

from limbo_core.domain.entities.artifacts.artifact import Artifact

from .column import SeedColumn
from .config import SeedConfig
from .file import SeedFile


@dataclass(slots=True, kw_only=True)
class Seed(Artifact[SeedConfig, SeedColumn]):
    """Seed definition."""

    seed_file: SeedFile
