"""Runtime registry for lookup-based value readers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from limbo_core.adapters.base_registry import BaseRegistry
from limbo_core.application.interfaces import (
    ValueReaderBackend,
    ValueReaderRegistryPort,
)
from limbo_core.domain.entities.backends.value_reader_backend_spec import (
    ValueReaderBackendSpec,
)

from .errors import LookupValueNotFoundError, UnknownValueReaderError

if TYPE_CHECKING:
    from limbo_core.domain.entities.values import LookupValue


@dataclass(slots=True)
class ValueReaderRegistry(
    BaseRegistry[ValueReaderBackend, ValueReaderBackendSpec],
    ValueReaderRegistryPort,
):
    """Resolve ``LookupValue`` instances via registered readers."""

    _backend_label: str = "value reader"

    def resolve(self, lookup: LookupValue) -> str:
        """Resolve one lookup spec to a concrete string value.

        Returns:
            Resolved value from reader backend or lookup default.

        Raises:
            LookupValueNotFoundError: If key is missing and no default.
        """
        reader_name = self._normalize_name(lookup.reader)
        reader = self._instances.get(reader_name)
        if reader is None:
            reader = self.create(lookup.reader)
        value = reader.get(lookup.key)
        if value is not None:
            return value
        if lookup.default is not None:
            return lookup.default
        raise LookupValueNotFoundError(reader=lookup.reader, key=lookup.key)

    def _unknown_backend_error(self, backend_key: str) -> Exception:
        return UnknownValueReaderError(backend_key)
