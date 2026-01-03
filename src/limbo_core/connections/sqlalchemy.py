"""SQLAlchemy Generic Connector for database connections."""

from typing import TYPE_CHECKING, Any, Literal

from pydantic import Field, SecretStr, field_validator

from limbo_core.yaml_schema.interpolation import EnvInterpolator

from .base import Connection
from .errors import MissingPackageError

try:
    from sqlalchemy import create_engine
    from sqlalchemy.engine import URL

    SQLALCHEMY_AVAILABLE = True
except ImportError:
    SQLALCHEMY_AVAILABLE = False
    create_engine = None  # type: ignore[assignment]
    URL = None  # type: ignore[misc, assignment]

if TYPE_CHECKING:
    from sqlalchemy import Engine


class SQLAlchemyConnection(Connection):
    """SQLAlchemy Generic Connector for database connections.

    Supports any database that SQLAlchemy supports through dialect+driver URLs.
    Environment variables can be interpolated using ${env:VAR_NAME} syntax.
    Default values are supported with ${env:VAR_NAME:-default}.
    """

    type: Literal["sqlalchemy"] = "sqlalchemy"
    dialect: str = "sqlite"
    driver: str | None = None
    host: str
    port: int | None = None
    user: str
    password: SecretStr
    database: str
    connection_args: dict[str, Any] = Field(default_factory=dict)

    @field_validator(
        "dialect", "driver", "host", "user", "database", mode="before"
    )
    @classmethod
    def interpolate_string_fields(cls, value: Any) -> Any:
        """Interpolate environment variables in string fields.

        Args:
            value: The value to interpolate.

        Returns:
            The interpolated value.
        """
        if isinstance(value, str):
            return EnvInterpolator.interpolate(value)
        return value

    @field_validator("port", mode="before")
    @classmethod
    def interpolate_port(cls, value: Any) -> Any:
        """Interpolate environment variables in the port field.

        Args:
            value: The port value to interpolate.

        Returns:
            The interpolated port as int or None.
        """
        if isinstance(value, str):
            interpolated = EnvInterpolator.interpolate(value)
            return int(interpolated) if interpolated else None
        return value

    @field_validator("password", mode="before")
    @classmethod
    def interpolate_password(cls, value: Any) -> Any:
        """Interpolate environment variables in the password field.

        Args:
            value: The password value to interpolate.

        Returns:
            The interpolated password as SecretStr.
        """
        if isinstance(value, str):
            return SecretStr(EnvInterpolator.interpolate(value))
        return value

    def _build_url(self) -> "URL":
        """Build the SQLAlchemy connection URL.

        Returns:
            The SQLAlchemy URL object.
        """
        drivername = (
            f"{self.dialect}+{self.driver}" if self.driver else self.dialect
        )
        return URL.create(
            drivername=drivername,
            username=self.user,
            password=self.password.get_secret_value(),
            host=self.host,
            port=self.port,
            database=self.database,
        )

    def connect(self) -> "Engine":
        """Establishes the connection using SQLAlchemy.

        Returns:
            The SQLAlchemy Engine object.

        Raises:
            MissingPackageError: If sqlalchemy is not installed.
        """
        if not SQLALCHEMY_AVAILABLE:
            raise MissingPackageError("sqlalchemy")

        return create_engine(
            self._build_url(), connect_args=self.connection_args
        )
