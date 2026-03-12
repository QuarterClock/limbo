"""Backward-compatible re-exports from ``application.context``."""

from limbo_core.application.context import (
    ParsingContext,
    ReferenceResolver,
    RuntimeContext,
)

__all__ = ["ParsingContext", "ReferenceResolver", "RuntimeContext"]
