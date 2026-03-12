"""Tests for environment adapter."""

from __future__ import annotations

import pytest

from limbo_core.plugins.builtin.value_readers import OsEnvReader


class TestOsEnvReader:
    """Tests for OsEnvReader value reader backend."""

    @pytest.fixture
    def reader(self) -> OsEnvReader:
        """Create a fresh OsEnvReader instance."""
        return OsEnvReader()

    def test_reads_environment(
        self, reader: OsEnvReader, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Read configured variable from os environment."""
        monkeypatch.setenv("APP_KEY", "value")
        assert reader.get("APP_KEY") == "value"

    def test_get_returns_default_when_key_missing(
        self, reader: OsEnvReader, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Return explicit default when the requested key is absent."""
        monkeypatch.delenv("MISSING_KEY", raising=False)
        assert reader.get("MISSING_KEY", default="fallback") == "fallback"

    def test_get_returns_none_when_key_missing_no_default(
        self, reader: OsEnvReader, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Return None when key is absent and no default is provided."""
        monkeypatch.delenv("MISSING_KEY", raising=False)
        assert reader.get("MISSING_KEY") is None

    def test_get_returns_none_for_nonexistent_key(
        self, reader: OsEnvReader
    ) -> None:
        """Return None for a key that was never set in the environment."""
        assert reader.get("ABSOLUTELY_NOT_SET_12345") is None
