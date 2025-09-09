from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ValidationInfo, field_validator

from limbo_core.errors import ContextMissingError
from limbo_core.yaml_schema.seeds.path_factory import PathFactory


class SeedFile(BaseModel):
    """Seed file configuration."""

    type: Literal["csv", "json", "parquet", "jsonl", "infer"] = "infer"
    compression: Literal["gzip", "brotli", "zstd", "infer"] = "infer"
    path: Path

    @field_validator("path", mode="before")
    @classmethod
    def validate_path(cls, value: str, info: ValidationInfo) -> Path:
        """Validate the path.

        Args:
            value: The path to validate.
            info: The validation info.

        Returns:
            The validated path.

        Raises:
            ContextMissingError: If the context is missing.
        """
        if info.context is None:
            raise ContextMissingError
        factory = PathFactory(info.context)
        return factory.from_raw(value)
