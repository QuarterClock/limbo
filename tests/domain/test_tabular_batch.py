"""Tests for TabularBatch value object."""

from __future__ import annotations

import pytest

from limbo_core.domain.validation import ValidationError
from limbo_core.domain.value_objects import TabularBatch


def test_tabular_batch_valid() -> None:
    batch = TabularBatch(
        column_names=("id", "name"),
        rows=({"id": 1, "name": "a"}, {"id": 2, "name": "b"}),
    )
    assert batch.column_names == ("id", "name")
    assert len(batch.rows) == 2


def test_tabular_batch_rejects_empty_columns() -> None:
    with pytest.raises(ValidationError, match="at least one column"):
        TabularBatch(column_names=(), rows=())


def test_tabular_batch_rejects_duplicate_column_names() -> None:
    with pytest.raises(ValidationError, match="unique"):
        TabularBatch(column_names=("id", "id"), rows=({"id": 1},))


def test_tabular_batch_rejects_row_key_mismatch() -> None:
    with pytest.raises(ValidationError, match="row 0 keys"):
        TabularBatch(column_names=("id", "name"), rows=({"id": 1},))


def test_tabular_batch_allows_none_cell() -> None:
    batch = TabularBatch(
        column_names=("id", "note"), rows=({"id": 1, "note": None},)
    )
    assert batch.rows[0]["note"] is None
