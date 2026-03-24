"""Tabular payload for persistence write boundaries."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Mapping

from limbo_core.domain.validation import ValidationError

CellValue = str | int | float | bool | date | datetime | None


@dataclass(frozen=True, slots=True)
class TabularBatch:
    """Immutable tabular payload crossing the persistence write boundary."""

    column_names: tuple[str, ...]
    rows: tuple[Mapping[str, CellValue], ...]

    def __post_init__(self) -> None:
        """Validate column names and row key alignment.

        Raises:
            ValidationError: If columns are empty, duplicated, or rows mismatch.
        """
        if not self.column_names:
            raise ValidationError("TabularBatch requires at least one column")
        col_set = set(self.column_names)
        if len(col_set) != len(self.column_names):
            raise ValidationError("column_names must be unique")
        for i, row in enumerate(self.rows):
            keys = set(row.keys())
            if keys != col_set:
                raise ValidationError(
                    f"row {i} keys {keys!r} do not match column_names"
                )
