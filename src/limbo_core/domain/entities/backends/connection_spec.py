"""Domain-level connection specification entity."""

from __future__ import annotations

from .backend_spec import BackendSpec


class ConnectionBackendSpec(BackendSpec):
    """Named connection backend binding declared in project payload."""
