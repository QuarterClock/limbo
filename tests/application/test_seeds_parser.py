"""Tests for seed payload parsing helpers."""

from __future__ import annotations

import pytest

from limbo_core.application.parsers.common import ParseError
from limbo_core.application.parsers.seeds_parser import _parse_seed_file


def test_parse_seed_file_wraps_invalid_path_spec_error() -> None:
    """_parse_seed_file wraps InvalidPathSpecError into ParseError."""
    # ``path`` value that will cause parse_path_spec
    # to raise InvalidPathSpecError.
    payload = {"type": "csv", "compression": "infer", "path": 123}

    with pytest.raises(ParseError) as excinfo:
        _parse_seed_file(payload, path=("seeds", 0, "seed_file"))

    msg = str(excinfo.value)
    # Ensure the error is wrapped with the correct path and original message.
    assert "seeds[0].seed_file.path" in msg
    assert "path spec expects a mapping or string" in msg
