from typing import Annotated, Any

from pydantic import BaseModel, BeforeValidator, Field

from limbo_core.connections import Connection, ConnectionRegistry

from .seeds import Seed
from .sources import Source
from .tables import Table


def _validate_connections(value: Any) -> list[Connection]:
    """Validate connections using the registry.

    Ensures plugins (and thus connection types) are loaded before validation.

    Args:
        value: Raw connection data to validate.

    Returns:
        List of validated connection instances.
    """
    if not isinstance(value, list):
        return value  # type: ignore[no-any-return]
    from limbo_core.plugins import load_plugins

    load_plugins()
    return ConnectionRegistry.validate_list(value)


ConnectionList = Annotated[
    list[Connection], BeforeValidator(_validate_connections)
]


class Project(BaseModel):
    """Project configuration."""

    vars: dict[str, Any] | None = None
    connections: ConnectionList = Field(default_factory=list)
    tables: list[Table]
    seeds: list[Seed]
    sources: list[Source]
