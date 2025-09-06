from typing import Any

from pydantic import BaseModel

from .seeds import Seed
from .sources import Source
from .tables import Table


class Project(BaseModel):
    """Project configuration."""

    vars: dict[str, Any] | None = None
    tables: list[Table]
    seeds: list[Seed]
    sources: list[Source]
