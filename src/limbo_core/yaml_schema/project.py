from typing import Any

from pydantic import BaseModel


class Project(BaseModel):
    """Project configuration."""

    vars: dict[str, Any] | None = None
