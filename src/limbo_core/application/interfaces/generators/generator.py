"""Abstract base class for generators."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar

if TYPE_CHECKING:
    from limbo_core.domain.entities import GenerationContext


class Generator:
    """Base class for value generators with multiple hooks."""

    _hook_registry: ClassVar[dict[str, str]]  # local_hook -> method_name

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Discover all @generates-decorated methods on subclass definition.

        Raises:
            TypeError: If the same hook is declared more than once on the
                subclass hierarchy.
        """
        super().__init_subclass__(**kwargs)
        cls._hook_registry = {}
        for attr_name in dir(cls):
            attr = getattr(cls, attr_name, None)
            if callable(attr):
                for hook in getattr(attr, "_limbo_hooks", []):
                    if hook in cls._hook_registry:
                        msg = f"Duplicate hook '{hook}' in {cls.__name__}"
                        raise TypeError(msg)
                    cls._hook_registry[hook] = attr_name

    @classmethod
    def get_hooks(cls) -> frozenset[str]:
        """Return all local hooks supported by this generator class."""
        return frozenset(cls._hook_registry)

    def generate(
        self, hook: str, context: GenerationContext, **options: Any
    ) -> Any:
        """Generate a value for the given local hook.

        Args:
            hook: Local hook name (without namespace).
            context: Shared generation context for the current row.
            **options: Generator-specific options.

        Returns:
            Generated value for the requested hook.

        Raises:
            ValueError: If the hook is not supported by this generator.
        """
        method_name = self._hook_registry.get(hook)
        if method_name is None:
            msg = f"Hook '{hook}' not supported by {type(self).__name__}"
            raise ValueError(msg)
        method = getattr(self, method_name)
        return method(context, **options)

    def setup(self, context: GenerationContext) -> None:
        """Optional lifecycle hook called before a table is generated."""

    def teardown(self, context: GenerationContext) -> None:
        """Optional lifecycle hook called after a table is generated."""
