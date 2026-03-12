"""Base artifact entity."""

from abc import ABC
from dataclasses import dataclass
from typing import Generic, TypeVar

from limbo_core.domain.validation import ValidationError

from .column import ArtifactColumn
from .config import ArtifactConfig

ConfigType = TypeVar("ConfigType", bound=ArtifactConfig)
ColumnType = TypeVar("ColumnType", bound=ArtifactColumn)


@dataclass(slots=True, kw_only=True)
class Artifact(ABC, Generic[ConfigType, ColumnType]):
    """Artifact definition."""

    name: str
    config: ConfigType
    columns: list[ColumnType]
    description: str | None = None

    def __post_init__(self) -> None:
        """Validate minimum artifact invariants.

        Raises:
            ValidationError: If the artifact has no columns.
        """
        if not self.columns:
            raise ValidationError("Field 'columns' must have at least one item")
