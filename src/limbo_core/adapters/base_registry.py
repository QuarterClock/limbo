"""Generic base registry implementation for backend management."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar

from limbo_core.application.interfaces.base_registry import BaseRegistryPort
from limbo_core.domain.entities.backends.backend_spec import BackendSpec
from limbo_core.validation import ValidationError

BackendT = TypeVar("BackendT")
SpecT = TypeVar("SpecT", bound=BackendSpec)


@dataclass(slots=True)
class BaseRegistry(BaseRegistryPort[BackendT, SpecT], Generic[BackendT, SpecT]):
    """Concrete generic registry managing backend types and named instances."""

    _types: dict[str, type[BackendT]] = field(default_factory=dict)
    _instances: dict[str, BackendT] = field(default_factory=dict)
    _backend_label: str = "backend"

    def register(self, backend_key: str, backend_class: type[BackendT]) -> None:
        """Register one backend class under a key."""
        self._types[self._normalize_key(backend_key)] = backend_class

    def get_types(self) -> dict[str, type[BackendT]]:
        """Get all registered backend classes.

        Returns:
            Copy of registered backend type mapping.
        """
        return self._types.copy()

    def create(
        self, backend_key: str, *, config: dict[str, Any] | None = None
    ) -> BackendT:
        """Create one backend instance by key.

        Returns:
            Instantiated backend implementation.
        """
        normalized = self._normalize_key(backend_key)
        return self._create_backend(
            normalized, config={} if config is None else config
        )

    def create_many(self, specs: list[SpecT]) -> list[BackendT]:
        """Create many backend instances from specs.

        Returns:
            Instantiated backends in the same order as the specs.
        """
        return [self.create(spec.type, config=spec.config) for spec in specs]

    def configure(self, spec: SpecT) -> None:
        """Configure one named backend instance from a spec."""
        self._instances[self._normalize_name(spec.name)] = self.create(
            spec.type, config=spec.config
        )

    def get_instances(self) -> dict[str, BackendT]:
        """Get all configured named backend instances.

        Returns:
            Copy of configured instance mapping.
        """
        return self._instances.copy()

    def clear_instances(self) -> None:
        """Clear all configured named backend instances."""
        self._instances.clear()

    def clear_types(self) -> None:
        """Clear all registered backend classes."""
        self._types.clear()

    def _create_backend(
        self, backend_key: str, *, config: dict[str, Any]
    ) -> BackendT:
        """Instantiate backend from registry by normalized key.

        Returns:
            Instantiated backend implementation.

        Raises:
            ValidationError: If backend constructor args are invalid.
        """
        backend_class = self._types.get(backend_key)
        if backend_class is None:
            raise self._unknown_backend_error(backend_key)
        try:
            return backend_class(**config)
        except TypeError as err:
            raise ValidationError(
                f"{self._backend_label} '{backend_key}' "
                f"config is invalid: {err}"
            ) from err

    def _unknown_backend_error(self, backend_key: str) -> Exception:
        """Build error for unregistered backend key.

        Returns:
            Exception describing the unknown backend key.
        """
        return ValueError(f"Unknown {self._backend_label}: {backend_key}")

    def _normalize_key(self, backend_key: str) -> str:
        """Normalize backend key for registration and lookup.

        Returns:
            Normalized non-empty backend key.

        Raises:
            ValidationError: If the normalized key is empty.
        """
        normalized = backend_key.strip().lower()
        if not normalized:
            label = self._backend_label[0].upper() + self._backend_label[1:]
            raise ValidationError(f"{label} key cannot be empty")
        return normalized

    def _normalize_name(self, name: str) -> str:
        """Normalize configured backend alias name.

        Returns:
            Normalized non-empty backend alias.

        Raises:
            ValidationError: If the normalized name is empty.
        """
        normalized = name.strip().lower()
        if not normalized:
            label = self._backend_label[0].upper() + self._backend_label[1:]
            raise ValidationError(f"{label} name cannot be empty")
        return normalized
