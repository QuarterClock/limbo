"""Tests for the top-level ``limbo_core.context`` re-export module."""

from __future__ import annotations

from limbo_core import context


def test_context_module_reexports_symbols() -> None:
    """Module exposes expected context symbols."""
    # Importing the module should succeed and expose the public API.
    assert hasattr(context, "ResolutionContext")
    assert hasattr(context, "RuntimeContext")
    assert hasattr(context, "ReferenceResolver")
