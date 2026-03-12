"""Structured value specs for literals, references, and lookups."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import TYPE_CHECKING, TypeAlias

if TYPE_CHECKING:
    from limbo_core.domain.entities.artifacts.data_types import DataType

LiteralScalar: TypeAlias = str | int | float | bool | dt.date | dt.datetime


@dataclass(slots=True, frozen=True)
class LiteralValue:
    """Literal typed value parsed from YAML scalar."""

    value: LiteralScalar
    data_type: DataType


@dataclass(slots=True, frozen=True)
class ReferenceValue:
    """Reference expression value parsed from ``{ref: ...}``."""

    ref: str
    data_type: DataType | None = None


@dataclass(slots=True, frozen=True)
class LookupValue:
    """Lookup-backed value parsed from ``{value_from: ...}``."""

    reader: str
    key: str
    default: str | None = None
    data_type: DataType | None = None


ValueSpec: TypeAlias = LiteralValue | LookupValue | ReferenceValue
