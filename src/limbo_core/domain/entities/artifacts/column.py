"""Base artifact column entity."""

from dataclasses import dataclass

from .data_types import DataType


@dataclass(slots=True, kw_only=True)
class ArtifactColumn:
    """Artifact column definition."""

    name: str
    data_type: DataType
    description: str | None = None
