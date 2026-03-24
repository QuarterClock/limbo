"""Tests for path spec parsing helpers."""

from __future__ import annotations

import pytest

from limbo_core.application.parsers.common import InvalidPathSpecError
from limbo_core.application.parsers.path_spec_parser import parse_path_spec


def test_parse_path_spec_rejects_non_mapping_non_string() -> None:
    """Non-mapping, non-string inputs raise InvalidPathSpecError."""
    with pytest.raises(
        InvalidPathSpecError, match="path spec expects a mapping or string"
    ):
        parse_path_spec(123)


def test_parse_path_spec_rejects_non_mapping_path_from() -> None:
    """`path_from` must be a mapping when provided."""
    with pytest.raises(
        InvalidPathSpecError, match="`path_from` expects a mapping"
    ):
        parse_path_spec({"path_from": "not-a-mapping"})


def test_parse_path_spec_rejects_non_string_backend() -> None:
    """`path_from.backend` must be a non-empty string."""
    with pytest.raises(
        InvalidPathSpecError, match=r"`path_from\.backend` expects a string"
    ):
        parse_path_spec({"path_from": {"backend": 123, "location": "x.csv"}})

    with pytest.raises(
        InvalidPathSpecError, match=r"`path_from\.backend` cannot be empty"
    ):
        parse_path_spec({"path_from": {"backend": "  ", "location": "x.csv"}})


def test_parse_path_spec_rejects_non_string_location() -> None:
    """`path_from.location` must be a string."""
    with pytest.raises(
        InvalidPathSpecError, match=r"`path_from\.location` expects a string"
    ):
        parse_path_spec({"path_from": {"backend": "file", "location": 42}})


def test_parse_path_spec_rejects_invalid_base_type() -> None:
    """`path_from.base` must be a string when provided."""
    with pytest.raises(
        InvalidPathSpecError,
        match=r"`path_from\.base` expects a string when provided",
    ):
        parse_path_spec({
            "path_from": {"backend": "file", "location": "x.csv", "base": 123}
        })
