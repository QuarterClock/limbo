"""Tests for generator interfaces and registry."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from unittest.mock import Mock

import pytest

from limbo_core.adapters.generators import GeneratorRegistry
from limbo_core.adapters.generators.errors import (
    DuplicateGeneratorHookError,
    InvalidGeneratorNamespaceError,
    UnknownGeneratorHookError,
)
from limbo_core.application.interfaces.generators import (
    Generator,
    GeneratorRegistration,
    GeneratorRegistryPort,
    generates,
)
from limbo_core.domain.entities import GenerationContext


class _SampleGenerator(Generator):
    """Sample generator exposing multiple local hooks."""

    def __init__(self) -> None:
        self.spy: Mock = Mock()

    @generates("email")
    def email(self, context: GenerationContext, **options: Any) -> str:
        self.spy("email", options)
        name = context.row_data.get("name", "user")
        domain = options.get("domain", "example.com")
        return f"{name}@{domain}"

    @generates("name")
    @generates("first_name")
    def name(self, context: GenerationContext, **options: Any) -> str:
        self.spy("name", options)
        value = options.get("value")
        return value if isinstance(value, str) else "user"


def test_generates_decorator_attaches_hooks() -> None:
    """@generates decorator records local hook names on methods."""
    method = _SampleGenerator.email
    hooks = method._limbo_hooks
    assert "email" in hooks


def test_generator_collects_hooks_and_generates_values() -> None:
    """Generator discovers hooks and dispatches generate calls."""
    ctx = GenerationContext(
        table_name="users", row_index=0, row_data={"name": "alice"}
    )
    gen = _SampleGenerator()

    hooks = _SampleGenerator.get_hooks()
    assert hooks == frozenset({"email", "name", "first_name"})

    value = gen.generate("email", ctx, domain="test.com")
    assert value == "alice@test.com"
    gen.spy.assert_any_call("email", {"domain": "test.com"})


def test_generator_generate_unknown_hook_raises() -> None:
    """Generator.generate raises for an unsupported hook name."""
    ctx = GenerationContext()
    gen = _SampleGenerator()

    with pytest.raises(ValueError, match="not supported"):
        gen.generate("missing", ctx)


def test_generator_setup_and_teardown_are_noops() -> None:
    """Default setup/teardown methods are callable and side-effect free."""
    ctx = GenerationContext()
    gen = _SampleGenerator()
    gen.setup(ctx)
    gen.teardown(ctx)


@dataclass
class _LocalOnlyRegistry:
    """Minimal GeneratorRegistryPort stub for exercising RuntimeContext."""

    hooks: set[str]

    def register(self, registration: GeneratorRegistration) -> None:
        raise NotImplementedError

    def get_hooks(self) -> frozenset[str]:
        return frozenset(self.hooks)

    def resolve(self, qualified_hook: str) -> tuple[type[Generator], str]:
        raise NotImplementedError

    def clear(self) -> None:
        self.hooks.clear()


def test_generator_registry_registers_and_resolves_hooks() -> None:
    """GeneratorRegistry registers and resolves qualified hooks."""
    registry: GeneratorRegistryPort = GeneratorRegistry()

    registry.register(
        GeneratorRegistration(namespace="pii", generator_class=_SampleGenerator)
    )

    hooks = registry.get_hooks()
    assert "pii.email" in hooks
    assert "pii.name" in hooks
    assert "pii.first_name" in hooks

    generator_class, local_hook = registry.resolve("pii.email")
    assert generator_class is _SampleGenerator
    assert local_hook == "email"


def test_generator_registry_invalid_namespace_raises() -> None:
    """Empty namespace in registration raises a specific error."""
    registry = GeneratorRegistry()

    with pytest.raises(InvalidGeneratorNamespaceError):
        registry.register(
            GeneratorRegistration(
                namespace="  ", generator_class=_SampleGenerator
            )
        )


def test_generator_registry_duplicate_hook_raises() -> None:
    """Registering two generators for the same hook raises."""
    registry = GeneratorRegistry()
    registry.register(
        GeneratorRegistration(namespace="pii", generator_class=_SampleGenerator)
    )

    class _OtherGenerator(Generator):
        @generates("email")
        def email(self, context: GenerationContext, **options: Any) -> str:
            return "other@example.com"

    with pytest.raises(DuplicateGeneratorHookError):
        registry.register(
            GeneratorRegistration(
                namespace="pii", generator_class=_OtherGenerator
            )
        )


def test_generator_registry_unknown_hook_raises() -> None:
    """Resolving an unknown hook produces a dedicated error."""
    registry = GeneratorRegistry()

    with pytest.raises(UnknownGeneratorHookError):
        registry.resolve("pii.email")


def test_generator_registry_clear_removes_all_hooks() -> None:
    """Clearing the registry removes all registered hooks."""
    registry = GeneratorRegistry()
    registry.register(
        GeneratorRegistration(namespace="pii", generator_class=_SampleGenerator)
    )
    assert registry.get_hooks()  # sanity check

    registry.clear()
    assert registry.get_hooks() == frozenset()


def test_generator_class_rejects_duplicate_local_hooks() -> None:
    """Duplicate local hooks on a single generator class raise TypeError."""

    with pytest.raises(TypeError, match="Duplicate hook 'email'"):

        class _DuplicateHookGenerator(Generator):
            @generates("email")
            def first(self, context: GenerationContext, **options: Any) -> str:
                return "first@example.com"

            @generates("email")
            def second(self, context: GenerationContext, **options: Any) -> str:
                return "second@example.com"
