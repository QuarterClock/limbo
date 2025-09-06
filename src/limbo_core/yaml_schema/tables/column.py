from typing import Any

from pydantic import ValidationInfo, field_validator

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
            ValueError: If the context is missing or the generator is invalid.
        """
        if info.context is None:
            raise ValueError
        if value not in info.context.generators:
            raise ValueError
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
            ValueError: If the context is missing or the options are invalid.
        """
        if value is None:
            return None
        if info.context is None:
            raise ValueError
        factory = OptionFactory(info.context)
        return {key: factory.from_raw(val) for key, val in value.items()}
