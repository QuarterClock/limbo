"""Tests for core errors and artifact invariants."""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from limbo_core.domain.entities.artifacts.artifact import Artifact
from limbo_core.domain.entities.artifacts.column import ArtifactColumn
from limbo_core.domain.entities.artifacts.config import ArtifactConfig
from limbo_core.domain.entities.artifacts.data_types import DataType
from limbo_core.domain.errors import DomainError, DomainValidationError
from limbo_core.errors import LimboError, LimboValidationError
from limbo_core.validation import ValidationError


@dataclass(slots=True, kw_only=True)
class _DemoArtifact(Artifact[ArtifactConfig, ArtifactColumn]):
    """Minimal concrete artifact for base-class behavior tests."""


class TestErrorHierarchy:
    """Tests for the domain/application error class hierarchy."""

    def test_domain_error_is_exception(self) -> None:
        """DomainError is a standard Exception subclass."""
        assert issubclass(DomainError, Exception)

    def test_domain_validation_error_is_value_error(self) -> None:
        """DomainValidationError inherits DomainError and ValueError."""
        assert issubclass(DomainValidationError, DomainError)
        assert issubclass(DomainValidationError, ValueError)

    def test_limbo_error_inherits_domain_error(self) -> None:
        """LimboError is a DomainError subclass."""
        assert issubclass(LimboError, DomainError)

    def test_limbo_validation_error_inherits_both(self) -> None:
        """LimboValidationError inherits both LimboError and DVE."""
        assert issubclass(LimboValidationError, LimboError)
        assert issubclass(LimboValidationError, DomainValidationError)

    def test_validation_error_inherits_domain_validation_error(self) -> None:
        """ValidationError inherits DomainValidationError."""
        assert issubclass(ValidationError, DomainValidationError)


class TestArtifactValidation:
    """Tests for Artifact base-class validation rules."""

    def test_requires_at_least_one_column(self) -> None:
        """Artifact base class rejects empty column collections."""
        with pytest.raises(
            ValidationError, match="Field 'columns' must have at least one item"
        ):
            _DemoArtifact(name="users", config=ArtifactConfig(), columns=[])

    def test_accepts_non_empty_columns(self) -> None:
        """Artifact base class accepts valid non-empty column payloads."""
        col = ArtifactColumn(name="id", data_type=DataType.INTEGER)
        artifact = _DemoArtifact(
            name="users", config=ArtifactConfig(), columns=[col]
        )
        assert artifact.columns == [col]
