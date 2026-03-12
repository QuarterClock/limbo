"""Domain-level validation helpers."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .errors import DomainValidationError


class ValidationError(DomainValidationError):
    """Raised when domain model validation fails."""


def require_mapping(value: Any, *, model_name: str) -> dict[str, Any]:
    """Require a mapping payload and return it as a mutable dict.

    Returns:
        Input converted to a mutable dictionary.

    Raises:
        ValidationError: If input is not a mapping.
    """
    if not isinstance(value, Mapping):
        raise ValidationError(
            f"{model_name} expects a mapping, got {type(value).__name__}"
        )
    return dict(value)
