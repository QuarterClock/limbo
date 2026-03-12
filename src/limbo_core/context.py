"""Backward-compatible re-exports from ``application.context``."""

from limbo_core.application.context import ResolutionContext, RuntimeContext
from limbo_core.application.interfaces import ReferenceResolver

__all__ = ["ReferenceResolver", "ResolutionContext", "RuntimeContext"]
