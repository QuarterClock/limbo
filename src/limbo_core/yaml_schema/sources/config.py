from typing import Any

from pydantic import ValidationInfo, field_validator

from limbo_core.errors import ContextMissingError
from limbo_core.yaml_schema.artifacts.config import ArtifactConfig


class SourceConfig(ArtifactConfig):
    """Source configuration."""

    connection: str
    schema_name: str | None = None
    table_name: str | None = None

    @field_validator("connection", mode="after")
    @classmethod
    def validate_connection_exists(
        cls, value: str, info: ValidationInfo
    ) -> str:
        """Validate that the connection exists in the context.

        Args:
            value: The connection name to validate.
            info: The validation info containing the context.

        Returns:
            The validated connection name.

        Raises:
            ContextMissingError: If the context is missing.
            ValueError: If the connection does not exist.
        """
        if info.context is None:
            raise ContextMissingError
        try:
            info.context.get_connection(value)
        except KeyError as err:
            raise ValueError(str(err)) from err
        return value

    def get_connection(self, context: Any) -> Any:
        """Get the resolved connection object.

        Args:
            context: The context containing the connections.

        Returns:
            The resolved connection object.
        """
        return context.get_connection(self.connection)
