"""Column option value types."""

import datetime as dt
from abc import ABC, abstractmethod
from typing import Any, Literal

from pydantic import BaseModel, model_serializer

from limbo_core.context import Context
from limbo_core.yaml_schema.artifacts.data_types import DataType
from limbo_core.yaml_schema.interpolation import ValueInterpolator

PrimitiveType = str | int | float | bool | dt.date | dt.datetime


class ColumnOptionBase(BaseModel, ABC):
    """Base class for option values with a uniform API."""

    @classmethod
    def from_raw(cls, raw: Any) -> "ColumnOptionBase":
        """Create an option from a raw YAML value.

        Args:
            raw: The raw YAML value (string, int, float, bool, date, etc.).

        Returns:
            The appropriate option subclass instance.

        Raises:
            ValueError: If the value cannot be parsed.
        """
        if not isinstance(raw, str):
            return ColumnOptionPrimitiveValue(
                value=raw, data_type=ValueInterpolator.infer_type(raw)
            )

        parsed = ValueInterpolator.parse(raw)
        if parsed is None:
            return ColumnOptionPrimitiveValue(
                value=raw, data_type=DataType.STRING
            )

        prefix, content = parsed
        if prefix == "ref":
            return ColumnOptionReferenceValue(ref=content)

        try:
            data_type = DataType(prefix)
        except ValueError as err:
            raise ValueError(f"Unknown option prefix: {prefix}") from err

        return ColumnOptionPrimitiveValue(
            value=ValueInterpolator.cast(data_type, content),
            data_type=data_type,
        )

    @abstractmethod
    def resolve(self, context: Context) -> Any:
        """Resolve the option value from context.

        Args:
            context: The context to resolve the option value from.

        Returns:
            The resolved option value.
        """

    @model_serializer
    @abstractmethod
    def serialize(self) -> Any:
        """Serialize the option value.

        Returns:
            The serialized option value.
        """
        raise NotImplementedError


class ColumnOptionPrimitiveValue(ColumnOptionBase):
    """Primitive option value, optionally annotated with a data type."""

    type: Literal["primitive"] = "primitive"
    value: PrimitiveType
    data_type: DataType

    def resolve(self, context: Context) -> PrimitiveType:
        """Return the primitive value as-is.

        Args:
            context: The context (unused for primitives).

        Returns:
            The primitive value.
        """
        return self.value

    @model_serializer
    def serialize(self) -> str:
        """Serialize to ${type:value} format.

        Returns:
            The serialized string.
        """
        return f"${{{self.data_type.value}:{self.value}}}"


class ColumnOptionReferenceValue(ColumnOptionBase):
    """Reference to another column via ${ref:...} prefix."""

    type: Literal["reference"] = "reference"
    ref: str

    def resolve(self, context: Context) -> Any:
        """Resolve the reference from context.

        Args:
            context: The context containing the referenced value.

        Returns:
            The resolved reference value.
        """
        return context.resolve_reference(self.ref)

    @model_serializer
    def serialize(self) -> str:
        """Serialize to ${ref:...} format.

        Returns:
            The serialized string.
        """
        return f"${{ref:{self.ref}}}"
