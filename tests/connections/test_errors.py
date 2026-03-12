"""Tests for connection errors."""

import pytest

from limbo_core.adapters.connections.errors import (
    MissingPackageError,
    UnknownConnectionBackendError,
)
from limbo_core.domain.errors import DomainValidationError
from limbo_core.errors import LimboError


class TestMissingPackageError:
    """Tests for MissingPackageError exception."""

    def test_error_message(self) -> None:
        """Test that error message includes package name."""
        error = MissingPackageError("sqlalchemy")
        assert "sqlalchemy" in str(error)
        assert "required for this connection type" in str(error)

    def test_raises_correctly(self) -> None:
        """Test that the error can be raised and caught."""
        with pytest.raises(MissingPackageError) as exc_info:
            raise MissingPackageError("some_package")

        assert "some_package" in str(exc_info.value)

    def test_inherits_from_exception(self) -> None:
        """Test that MissingPackageError inherits from LimboError."""
        error = MissingPackageError("test")
        assert isinstance(error, LimboError)
        assert isinstance(error, Exception)


class TestUnknownConnectionBackendError:
    """Tests for UnknownConnectionBackendError exception."""

    def test_error_message_contains_backend_key(self) -> None:
        """Test that the error message includes the offending backend key."""
        error = UnknownConnectionBackendError("redis")
        assert "redis" in str(error)

    def test_inherits_from_value_error(self) -> None:
        """DomainValidationError inherits ValueError, so this error does too."""
        error = UnknownConnectionBackendError("unknown_backend")
        assert isinstance(error, DomainValidationError)
        assert isinstance(error, ValueError)
