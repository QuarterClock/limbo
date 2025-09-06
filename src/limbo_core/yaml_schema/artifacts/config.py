from pydantic import BaseModel


class ArtifactConfig(BaseModel):
    """Artifact configuration."""

    materialize: bool = True
