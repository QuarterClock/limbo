from typing import Any

from pydantic import BaseModel, ValidationInfo, field_validator

from limbo_core.context import Context

from .data_types import DataType
from .option import OptionBase
from .option_factory import OptionFactory


class Column(BaseModel):
    """Column definition with flexible options."""

    name: str
    data_type: DataType
    generator: str

    options: dict[str, OptionBase] | None = None

    @field_validator("options", mode="before")
    @classmethod
    def validate_options(
        cls, value: dict[str, Any] | None, info: ValidationInfo
    ) -> dict[str, OptionBase] | None:
        if value is None:
            return None
        factory = OptionFactory()
        return {key: factory.from_raw(val) for key, val in value.items()}

    @field_validator("generator", mode="after")
    @classmethod
    def validate_generator(cls, value: str, info: ValidationInfo) -> str:
        if info.context is None:
            raise ValueError
        if not isinstance(info.context, Context):
            raise ValueError
        if value not in info.context.generators:
            raise ValueError
        return value
