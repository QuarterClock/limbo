from typing import Annotated

from pydantic import Field

from limbo_core.yaml_schema.artifacts.artifact import Artifact

from .column import TableColumn
from .config import TableConfig
from .reference import TableReference


class Table(Artifact[TableConfig, TableColumn]):
    """Table definition."""

    # New fields
    references: Annotated[list[TableReference], Field(min_length=1)] | None = (
        None
    )
