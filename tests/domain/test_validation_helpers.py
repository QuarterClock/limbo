"""Tests for core validation helper functions."""

from __future__ import annotations

from collections import UserDict

import pytest

from limbo_core.validation import ValidationError, require_mapping


class TestRequireMapping:
    """Tests for the require_mapping helper."""

    def test_accepts_mapping_and_returns_copy(self) -> None:
        """Mapping inputs are accepted and copied into plain dicts."""
        value = UserDict({"a": 1})

        result = require_mapping(value, model_name="Demo")

        assert result == {"a": 1}
        assert result is not value

    def test_rejects_non_mapping(self) -> None:
        """Non-mapping inputs raise ValidationError with model name."""
        with pytest.raises(
            ValidationError, match="Demo expects a mapping, got list"
        ):
            require_mapping([], model_name="Demo")

    def test_rejects_list(self) -> None:
        """A list payload is rejected with an appropriate type name."""
        with pytest.raises(ValidationError, match="got list"):
            require_mapping([1, 2, 3], model_name="Config")

    def test_rejects_string(self) -> None:
        """A string payload is rejected."""
        with pytest.raises(ValidationError, match="got str"):
            require_mapping("hello", model_name="Config")

    def test_rejects_none(self) -> None:
        """None is rejected as a non-mapping value."""
        with pytest.raises(ValidationError, match="got NoneType"):
            require_mapping(None, model_name="Config")
