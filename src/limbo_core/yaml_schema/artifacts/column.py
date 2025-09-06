from pydantic import BaseModel

from .data_types import DataType


class ArtifactColumn(BaseModel):
    """Artifact column definition."""

    name: str
    description: str | None = None
    data_type: DataType
