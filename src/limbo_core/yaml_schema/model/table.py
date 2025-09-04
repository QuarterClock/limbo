from enum import StrEnum
from typing import Annotated

from pydantic import BaseModel, Field

from .column import Column


class TableRelationship(StrEnum):
    """Table relationship."""

    ONE_TO_ONE = "one_to_one"
    ONE_TO_MANY = "one_to_many"
    MANY_TO_ONE = "many_to_one"
    MANY_TO_MANY = "many_to_many"


class TableReference(BaseModel):
    """Reference to another table."""

    table: str
    relationship: TableRelationship


class TableConfig(BaseModel):
    """Table configuration options."""

    materialize: bool = True


class Table(BaseModel):
    """Table definition."""

    name: str
    description: str | None = None
    alias: str | None = None
    config: Annotated[TableConfig, Field(default_factory=TableConfig)]
    columns: Annotated[list[Column], Field(min_length=1)]
    references: Annotated[list[TableReference], Field(min_length=1)] | None = (
        None
    )
