# TODO(Vlad): Split context into Parsing and Generation contexts.
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict

from limbo_core.errors import ConnectionNotFoundError


class Context(BaseModel):
    """Context for data generation and validation hooks."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    generators: dict[str, Any]
    paths: dict[str, Path]
    connections: dict[str, Any] = {}

    def resolve_reference(self, reference: str) -> Any:
        """Resolve a reference string like 'table.column'.

        Resolves to the value of the referenced column.

        Args:
            reference: The reference string to resolve.

        Returns:
            The resolved value.
        """
        raise NotImplementedError

    def get_connection(self, name: str) -> Any:
        """Get a connection by name.

        Args:
            name: The name of the connection.

        Returns:
            The connection object.

        Raises:
            ConnectionNotFoundError: If the connection does not exist.
        """
        if name not in self.connections:
            raise ConnectionNotFoundError(
                connection_name=name,
                available_connections=self.connections.keys(),
            )
        return self.connections[name]
