"""Tests for connection errors."""

import pytest

from limbo_core.connections.errors import MissingPackageError


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
        """Test that MissingPackageError inherits from Exception."""
        error = MissingPackageError("test")
        assert isinstance(error, Exception)
