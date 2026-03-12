from dataclasses import dataclass


@dataclass(slots=True, kw_only=True)
class ArtifactConfig:
    """Artifact configuration."""

    materialize: bool = True
