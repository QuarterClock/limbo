from abc import ABC, abstractmethod
from typing import Any, Literal

from pydantic import BaseModel, model_serializer

from limbo_core.context import Context
from limbo_core.yaml_schema.artifacts.data_types import DataType


# TODO(Vlad): Consider splitting this into a separate file.
class ColumnOptionBase(BaseModel, ABC):
    """Base class for option values with a uniform API."""

    @abstractmethod
    def resolve(self, context: Context) -> Any:
        """Resolve the option value from context."""
        raise NotImplementedError

    @model_serializer
    @abstractmethod
    def serialize(self) -> Any:
        """Serialize the option value to a dictionary."""
        raise NotImplementedError


class ColumnOptionPrimitiveValue(ColumnOptionBase):
    """Primitive option value, optionally annotated with a data type."""

    type: Literal["primitive"] = "primitive"
    value: Any
    data_type: DataType

    def resolve(self, context: Context) -> Any:
        """Return the primitive value as-is."""
        return self.value

    @model_serializer
    def serialize(self) -> str:
        """Serialize the option value to a dictionary."""
        return f"${{{self.data_type.value}:{self.value}}}"


class ColumnOptionReferenceValue(ColumnOptionBase):
    """Reference to another column via ${ref:...} prefix."""

    type: Literal["reference"] = "reference"
    ref: str

    def resolve(self, context: Context) -> Any:
        """Resolve the reference value from context."""
        return context.resolve_reference(self.ref)

    @model_serializer
    def serialize(self) -> str:
        """Serialize the option value to a dictionary."""
        return f"${{ref:{self.ref}}}"
