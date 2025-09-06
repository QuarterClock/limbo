from abc import ABC
from typing import Annotated, Generic, TypeVar

from pydantic import BaseModel, Field

ConfigType = TypeVar("ConfigType")
ColumnType = TypeVar("ColumnType")


class Artifact(BaseModel, ABC, Generic[ConfigType, ColumnType]):
    """Artifact definition."""

    name: str
    description: str | None = None
    config: Annotated[ConfigType, Field(default_factory=ConfigType)]
    columns: Annotated[list[ColumnType], Field(min_length=1)]
