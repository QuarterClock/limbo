"""Validation helpers for dataclass-based models.

Re-exports from domain.validation to maintain backwards compatibility.
Application-layer and adapter-layer code should import from here.
Domain-layer code should import from ``domain.validation`` directly.
"""

from limbo_core.domain.validation import ValidationError, require_mapping

__all__ = ["ValidationError", "require_mapping"]
