from abc import ABC, abstractmethod
from typing import Any, Literal

from pydantic import BaseModel

from limbo_core.context import Context
from limbo_core.yaml_schema import DataType


class OptionBase(BaseModel, ABC):
    """Base class for option values with a uniform API."""

    @abstractmethod
    def resolve(self, context: Context) -> Any:  # pragma: no cover - interface
        raise NotImplementedError


class OptionPrimitiveValue(OptionBase):
    """Primitive option value, optionally annotated with a data type."""

    type: Literal["primitive"] = "primitive"
    value: Any
    data_type: DataType | None = None

    def resolve(self, context: Context) -> Any:
        """Return the primitive value as-is."""
        return self.value


class OptionReferenceValue(OptionBase):
    """Reference to another column via ${ref:...} prefix."""

    type: Literal["reference"] = "reference"
    ref: str

    def resolve(self, context: Context) -> Any:
        """Resolve the reference value from context."""
        return context.resolve_reference(self.ref)
