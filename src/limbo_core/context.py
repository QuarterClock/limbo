# TODO(Vlad): Split context into Parsing and generation contexts.
from pathlib import Path
from typing import Any

from pydantic import BaseModel


class Context(BaseModel):
    """Context for data generation and validation hooks."""

    generators: dict[str, Any]
    paths: dict[str, Path]

    def resolve_reference(self, reference: str) -> Any:
        """Resolve a reference string like 'table.column'.

        Resolves to the value of the referenced column.

        Args:
            reference: The reference string to resolve.

        Returns:
            The resolved value.
        """
        # TODO(Vlad): Implement this.
        return reference
