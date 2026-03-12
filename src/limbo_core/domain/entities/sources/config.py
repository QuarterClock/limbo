from __future__ import annotations

from dataclasses import dataclass

from limbo_core.domain.entities.artifacts.config import ArtifactConfig
from limbo_core.domain.validation import ValidationError


@dataclass(slots=True, kw_only=True)
class SourceConfig(ArtifactConfig):
    """Source configuration."""

    connection: str = ""
    schema_name: str | None = None
    table_name: str | None = None

    def __post_init__(self) -> None:
        """Validate source config invariants.

        Raises:
            ValidationError: If connection is empty.
        """
        if not self.connection:
            raise ValidationError(
                "SourceConfig: 'connection' must be a non-empty string"
            )
