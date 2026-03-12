"""Tests for the SourceConfig entity."""

from __future__ import annotations

import pytest

from limbo_core.domain.entities.sources.config import SourceConfig
from limbo_core.domain.validation import ValidationError


def test_source_config_requires_non_empty_connection() -> None:
    """SourceConfig raises when connection is an empty string."""
    with pytest.raises(
        ValidationError,
        match="SourceConfig: 'connection' must be a non-empty string",
    ):
        SourceConfig(connection="")


def test_source_config_accepts_valid_connection() -> None:
    """SourceConfig accepts a non-empty connection string."""
    config = SourceConfig(connection="default")
    assert config.connection == "default"
