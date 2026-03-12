"""Registration descriptor for generator classes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .generator import Generator


@dataclass(frozen=True, slots=True)
class GeneratorRegistration:
    """Register a generator class under a namespace.

    The namespace is used as the prefix (e.g. ``pii``) for all local hooks
    declared on the generator class.
    """

    namespace: str
    generator_class: type[Generator]
