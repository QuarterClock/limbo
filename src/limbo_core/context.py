from typing import Any

from pydantic import BaseModel


class Context(BaseModel):
    """Context for data generation and validation hooks."""

    generators: dict[str, Any]

    def resolve_reference(self, reference: str) -> Any:  # pragma: no cover
        """Resolve a reference string like 'table.column'.

        The concrete resolution is provided by the integration layer.
        """
        raise NotImplementedError
