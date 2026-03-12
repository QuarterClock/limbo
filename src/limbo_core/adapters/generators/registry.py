"""Generator registry implementation."""

from __future__ import annotations

from dataclasses import dataclass, field

from limbo_core.application.interfaces.generators import (
    Generator,
    GeneratorRegistration,
    GeneratorRegistryPort,
)

from .errors import (
    DuplicateGeneratorHookError,
    InvalidGeneratorNamespaceError,
    UnknownGeneratorHookError,
)


@dataclass(slots=True)
class GeneratorRegistry(GeneratorRegistryPort):
    """In-memory registry for generator classes and their hooks."""

    _index: dict[str, tuple[type[Generator], str]] = field(default_factory=dict)

    def register(self, registration: GeneratorRegistration) -> None:
        """Register a generator class under a namespace.

        Raises:
            InvalidGeneratorNamespaceError: If the namespace is empty.
            DuplicateGeneratorHookError: If a fully qualified hook is
                already registered by another generator class.
        """
        namespace = registration.namespace.strip()
        if not namespace:
            raise InvalidGeneratorNamespaceError

        generator_class = registration.generator_class
        for local_hook in generator_class.get_hooks():
            qualified = f"{namespace}.{local_hook}"
            if qualified in self._index:
                existing_cls, _ = self._index[qualified]
                raise DuplicateGeneratorHookError(
                    qualified_hook=qualified, existing_cls=existing_cls.__name__
                )
            self._index[qualified] = (generator_class, local_hook)

    def get_hooks(self) -> frozenset[str]:
        """Return all fully-qualified hooks."""
        return frozenset(self._index)

    def resolve(self, qualified_hook: str) -> tuple[type[Generator], str]:
        """Resolve a fully-qualified hook to (generator_class, local_hook).

        Args:
            qualified_hook: The fully-qualified hook name.

        Returns:
            The generator class and local hook name.

        Raises:
            UnknownGeneratorHookError: If the hook is not registered.
        """
        try:
            generator_class, local_hook = self._index[qualified_hook]
        except KeyError as err:
            raise UnknownGeneratorHookError(qualified_hook) from err
        return generator_class, local_hook

    def clear(self) -> None:
        """Clear all registered generators."""
        self._index.clear()
