from typing import Any

from pydantic import BaseModel, Field

from limbo_core.connections import SQLAlchemyConnection

from .seeds import Seed
from .sources import Source
from .tables import Table


class Project(BaseModel):
    """Project configuration."""

    vars: dict[str, Any] | None = None
    connections: list[SQLAlchemyConnection] = Field(default_factory=list)
    tables: list[Table]
    seeds: list[Seed]
    sources: list[Source]
