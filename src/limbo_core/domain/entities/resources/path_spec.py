"""Structured path specs for pluggable resource backends."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class PathSpec:
    """Generic backend path specification."""

    backend: str
    location: str
    base: str | None = None
