"""SQLAlchemy built-in connection implementation."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from limbo_core.adapters.connections.errors import MissingPackageError
from limbo_core.application.interfaces import ConnectionBackend
from limbo_core.validation import ValidationError

if TYPE_CHECKING:
    from sqlalchemy import Engine
    from sqlalchemy.engine import URL

    from limbo_core.domain.entities import ConnectionBackendSpec

SQLALCHEMY_BACKEND_KEY = "sqlalchemy"


@dataclass(slots=True)
class SQLAlchemyConnectionBackend(ConnectionBackend):
    """Runtime SQLAlchemy backend created from a parsed connection spec."""

    host: str
    user: str
    password: str
    database: str
    dialect: str = "sqlite"
    driver: str | None = None
    port: int | None = None
    connection_args: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_spec(
        cls, spec: ConnectionBackendSpec
    ) -> SQLAlchemyConnectionBackend:
        """Create a SQLAlchemy backend from a generic connection spec.

        Returns:
            Parsed SQLAlchemy runtime backend.

        Raises:
            ValidationError: If connection type or config values are invalid.
        """
        if spec.type != SQLALCHEMY_BACKEND_KEY:
            raise ValidationError(
                "SQLAlchemyConnection: expected type "
                f"'{SQLALCHEMY_BACKEND_KEY}', "
                f"got '{spec.type}'"
            )

        config = dict(spec.config)
        dialect = cls._resolve_required_string(
            config.pop("dialect", "sqlite"), field_name="dialect"
        )
        driver = cls._resolve_optional_string(
            config.pop("driver", None), field_name="driver"
        )
        host = cls._resolve_required_string(
            config.pop("host", None), field_name="host"
        )
        user = cls._resolve_required_string(
            config.pop("user", None), field_name="user"
        )
        password = cls._resolve_password(config.pop("password", None))
        database = cls._resolve_required_string(
            config.pop("database", None), field_name="database"
        )
        port = cls._resolve_port(config.pop("port", None))
        connection_args = config.pop("connection_args", {})
        if not isinstance(connection_args, dict):
            raise ValidationError(
                "SQLAlchemyConnection: `connection_args` expects a mapping"
            )
        if config:
            unknown = ", ".join(sorted(config.keys()))
            raise ValidationError(
                f"SQLAlchemyConnection: unexpected config fields: {unknown}"
            )

        return cls(
            host=host,
            user=user,
            password=password,
            database=database,
            dialect=dialect,
            driver=driver,
            port=port,
            connection_args=connection_args,
        )

    @staticmethod
    def _resolve_required_string(value: Any, *, field_name: str) -> str:
        """Resolve required string field from literal value.

        Returns:
            Resolved string value.

        Raises:
            ValidationError: If value is not a string.
        """
        if isinstance(value, str):
            return value
        raise ValidationError(
            f"SQLAlchemyConnection: `{field_name}` expects a string"
        )

    @classmethod
    def _resolve_optional_string(
        cls, value: Any, *, field_name: str
    ) -> str | None:
        """Resolve optional string field from literal value.

        Returns:
            Resolved string value, or None.
        """
        if value is None:
            return None
        return cls._resolve_required_string(value, field_name=field_name)

    @staticmethod
    def _resolve_password(value: Any) -> str:
        """Resolve password field from literal value.

        Returns:
            Resolved password value.

        Raises:
            ValidationError: If value is not a string.
        """
        if isinstance(value, str):
            return value
        raise ValidationError(
            "SQLAlchemyConnection: `password` expects a string"
        )

    @staticmethod
    def _resolve_port(value: Any) -> int | None:
        """Resolve optional integer port from scalar values.

        Returns:
            Integer port value, or None.

        Raises:
            ValidationError: If value cannot be interpreted as a port.
        """
        if value is None:
            return None
        if isinstance(value, int):
            return value
        if isinstance(value, str):
            return int(value) if value else None
        raise ValidationError(
            "SQLAlchemyConnection: `port` expects int or string"
        )

    def _build_url(self) -> URL:
        """Build the SQLAlchemy connection URL.

        Returns:
            SQLAlchemy URL object.
        """
        from sqlalchemy.engine import URL

        drivername = (
            f"{self.dialect}+{self.driver}" if self.driver else self.dialect
        )
        return URL.create(
            drivername=drivername,
            username=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.database,
        )

    def connect(self) -> Engine:
        """Establish the connection using SQLAlchemy.

        Returns:
            SQLAlchemy engine instance.

        Raises:
            MissingPackageError: If SQLAlchemy is unavailable.
        """
        try:
            from sqlalchemy import create_engine
        except ImportError:
            raise MissingPackageError("sqlalchemy") from None
        return create_engine(
            self._build_url(), connect_args=self.connection_args
        )
