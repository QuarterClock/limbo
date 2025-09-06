from pathlib import Path
from typing import Any

from pydantic import BaseModel


class Context(BaseModel):
    """Context for data generation and validation hooks."""

    generators: dict[str, Any]
    paths: dict[str, Path]

    def resolve_reference(self, reference: str) -> Any:
        """Resolve a reference string like 'table.column'.

        The concrete resolution is provided by the integration layer.
        """
        return "not implemented"
