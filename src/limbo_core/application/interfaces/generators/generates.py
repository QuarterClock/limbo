"""Decorator for declaring generator hooks."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, TypeVar

GenerateMethod = TypeVar("GenerateMethod", bound=Callable[..., Any])


def generates(hook: str) -> Callable[[GenerateMethod], GenerateMethod]:
    """Declare that a method handles a local generator hook.

    The hook name is local to the generator class; namespaces such as ``pii``
    are provided separately via ``GeneratorRegistration``.

    Args:
        hook: The local hook name.

    Returns:
        The decorated method that handles the given hook.
    """

    def decorator(method: GenerateMethod) -> GenerateMethod:
        hooks: list[str] = getattr(method, "_limbo_hooks", [])
        hooks.append(hook)
        method._limbo_hooks = hooks  # type: ignore[attr-defined]
        return method

    return decorator
