"""Tests for backend specification parsing helpers."""

from __future__ import annotations

import pytest

from limbo_core.application.parsers.backends_parser import (
    _parse_destinations,
    _parse_path_backends,
    _parse_value_reader_backends,
    parse_backend_spec,
)
from limbo_core.application.parsers.common import ParseError
from limbo_core.domain.entities import (
    DestinationBackendSpec,
    ValueReaderBackendSpec,
)
from limbo_core.validation import ValidationError


def test_parse_backend_spec_returns_existing_instance() -> None:
    """Passing an existing spec instance returns it unchanged."""
    spec = ValueReaderBackendSpec(name="env", type="os_env", config={})
    assert parse_backend_spec(spec, spec_cls=ValueReaderBackendSpec) is spec


def test_parse_backend_spec_rejects_non_mapping() -> None:
    """Non-mapping inputs raise ValidationError."""
    with pytest.raises(
        ValidationError,
        match="ValueReaderBackendSpec expects a mapping, got int",
    ):
        parse_backend_spec(123, spec_cls=ValueReaderBackendSpec)  # type: ignore[arg-type]


def test_parse_value_reader_backends_wraps_validation_error() -> None:
    """_parse_value_reader_backends wraps ValidationError in ParseError."""
    payload = [{"config": {}}]
    with pytest.raises(ParseError, match=r"value_readers\[0\]"):
        _parse_value_reader_backends(payload, path=("value_readers",))


def test_parse_path_backends_wraps_validation_error() -> None:
    """_parse_path_backends wraps ValidationError in ParseError."""
    payload = [{"config": {}}]
    with pytest.raises(ParseError, match=r"path_backends\[0\]"):
        _parse_path_backends(payload, path=("path_backends",))


class TestParseDestinations:
    """Tests for _parse_destinations helper."""

    def test_parses_valid_destination(self) -> None:
        """A well-formed payload produces a DestinationBackendSpec."""
        payload = [{"name": "local", "type": "file"}]
        result = _parse_destinations(payload, path=("destinations",))

        assert len(result) == 1
        assert isinstance(result[0], DestinationBackendSpec)
        assert result[0].name == "local"
        assert result[0].type == "file"

    def test_parses_destination_with_config(self) -> None:
        """Destination config dict flows through to the spec."""
        payload = [
            {"name": "s3out", "type": "s3", "config": {"bucket": "my-bucket"}}
        ]
        result = _parse_destinations(payload, path=("destinations",))

        assert result[0].config == {"bucket": "my-bucket"}

    def test_wraps_validation_error(self) -> None:
        """_parse_destinations wraps ValidationError in ParseError."""
        payload = [{"config": {}}]
        with pytest.raises(ParseError, match=r"destinations\[0\]"):
            _parse_destinations(payload, path=("destinations",))

    def test_rejects_duplicate_names(self) -> None:
        """Duplicate destination names are rejected."""
        payload = [
            {"name": "out", "type": "file"},
            {"name": "out", "type": "file"},
        ]
        with pytest.raises(
            ParseError, match=r"destinations\[1\]\.name: duplicate backend name"
        ):
            _parse_destinations(payload, path=("destinations",))

    def test_rejects_empty_list(self) -> None:
        """An empty destinations list is rejected."""
        with pytest.raises(
            ParseError, match="destinations: must have at least one item"
        ):
            _parse_destinations([], path=("destinations",))

    def test_rejects_non_list(self) -> None:
        """Non-list input raises ParseError."""
        with pytest.raises(ParseError, match="destinations: expects a list"):
            _parse_destinations({}, path=("destinations",))
