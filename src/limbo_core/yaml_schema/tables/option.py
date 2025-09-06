from abc import ABC, abstractmethod
from typing import Any, Literal

from pydantic import BaseModel, model_serializer

from limbo_core.context import Context
from limbo_core.yaml_schema.artifacts.data_types import DataType


# TODO(Vlad): Consider splitting this into a multiple files.
class ColumnOptionBase(BaseModel, ABC):
    """Base class for option values with a uniform API."""

    @abstractmethod
    def resolve(self, context: Context) -> Any:
        """Resolve the option value from context.

        Args:
            context: The context to resolve the option value from.

        Returns:
            The resolved option value.
        """
        raise NotImplementedError

    @model_serializer
    @abstractmethod
    def serialize(self) -> Any:
        """Serialize the option value to a dictionary.

        Returns:
            The serialized option value.
        """
        raise NotImplementedError


class ColumnOptionPrimitiveValue(ColumnOptionBase):
    """Primitive option value, optionally annotated with a data type."""

    type: Literal["primitive"] = "primitive"
    value: Any
    data_type: DataType

    def resolve(self, context: Context) -> Any:
        """Return the primitive value as-is.

        Args:
            context: The context to resolve the primitive value from.

        Returns:
            The resolved primitive value.
        """
        return self.value

    @model_serializer
    def serialize(self) -> str:
        """Serialize the option value to a dictionary.

        Returns:
            The serialized option value.
        """
        return f"${{{self.data_type.value}:{self.value}}}"


class ColumnOptionReferenceValue(ColumnOptionBase):
    """Reference to another column via ${ref:...} prefix."""

    type: Literal["reference"] = "reference"
    ref: str

    def resolve(self, context: Context) -> Any:
        """Resolve the reference value from context.

        Args:
            context: The context to resolve the reference value from.

        Returns:
            The resolved reference value.
        """
        return context.resolve_reference(self.ref)

    @model_serializer
    def serialize(self) -> str:
        """Serialize the option value to a dictionary.

        Returns:
            The serialized option value.
        """
        return f"${{ref:{self.ref}}}"
