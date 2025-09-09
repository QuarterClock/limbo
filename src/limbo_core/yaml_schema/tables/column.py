from typing import Any

from pydantic import ValidationInfo, field_validator

from limbo_core.errors import ContextMissingError
from limbo_core.yaml_schema.artifacts.column import ArtifactColumn

from .option import ColumnOptionBase
from .option_factory import OptionFactory


class TableColumn(ArtifactColumn):
    """Column definition with flexible options."""

    generator: str
    options: dict[str, ColumnOptionBase] | None = None

    @field_validator("generator", mode="after")
    @classmethod
    def validate_generator(cls, value: str, info: ValidationInfo) -> str:
        """Validate the generator.

        Args:
            value: The generator to validate.
            info: The validation info.

        Returns:
            The validated generator.

        Raises:
            ContextMissingError: If the context is missing.
            ValueError: If the generator is invalid.
        """
        if info.context is None:
            raise ContextMissingError
        if value not in info.context.generators:
            raise ValueError(f"Generator {value} is not in the context")
        return value

    @field_validator("options", mode="before")
    @classmethod
    def validate_options(
        cls, value: dict[str, Any] | None, info: ValidationInfo
    ) -> dict[str, ColumnOptionBase] | None:
        """Validate the options.

        Args:
            value: The options to validate.
            info: The validation info.

        Returns:
            The validated options.

        Raises:
            ContextMissingError: If the context is missing.
        """
        if value is None:
            return None
        if info.context is None:
            raise ContextMissingError
        factory = OptionFactory(info.context)
        return {key: factory.from_raw(val) for key, val in value.items()}
