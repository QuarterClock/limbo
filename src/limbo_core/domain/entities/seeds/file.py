"""Seed file specification entity."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from limbo_core.domain.entities.resources import PathSpec


@dataclass(slots=True, kw_only=True)
class SeedFile:
    """Seed file configuration."""

    type: Literal["csv", "json", "parquet", "jsonl", "infer"] = "infer"
    compression: Literal["gzip", "brotli", "zstd", "infer"] = "infer"
    path: PathSpec
