"""Value reader backend registry interface."""

from __future__ import annotations

from limbo_core.application.interfaces.base_registry import BaseRegistryPort
from limbo_core.domain.entities.backends.value_reader_backend_spec import (
    ValueReaderBackendSpec,
)

from .value_backend import ValueReaderBackend
from .value_resolver import ValueResolverPort


class ValueReaderRegistryPort(
    BaseRegistryPort[ValueReaderBackend, ValueReaderBackendSpec],
    ValueResolverPort,
):
    """Registry contract for value reader backend classes and instances."""
