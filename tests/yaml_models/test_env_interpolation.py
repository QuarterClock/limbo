import pytest

from limbo_core.connections import SQLAlchemyConnection
from limbo_core.yaml_schema.interpolation import EnvInterpolator


class TestEnvInterpolator:
    def test_interpolate_single_env_var(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("TEST_HOST", "myhost.example.com")
        result = EnvInterpolator.interpolate("${env:TEST_HOST}")
        assert result == "myhost.example.com"

    def test_interpolate_env_var_in_string(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("TEST_PORT", "5432")
        result = EnvInterpolator.interpolate("host:${env:TEST_PORT}/db")
        assert result == "host:5432/db"

    def test_interpolate_multiple_env_vars(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("TEST_USER", "admin")
        monkeypatch.setenv("TEST_PASS", "secret")
        result = EnvInterpolator.interpolate(
            "${env:TEST_USER}:${env:TEST_PASS}"
        )
        assert result == "admin:secret"

    def test_interpolate_with_default_value(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("NONEXISTENT_VAR", raising=False)
        result = EnvInterpolator.interpolate(
            "${env:NONEXISTENT_VAR:-default_val}"
        )
        assert result == "default_val"

    def test_interpolate_with_empty_default(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("NONEXISTENT_VAR", raising=False)
        result = EnvInterpolator.interpolate(
            "prefix_${env:NONEXISTENT_VAR:-}_suffix"
        )
        assert result == "prefix__suffix"

    def test_interpolate_env_var_overrides_default(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("TEST_VAR", "actual_value")
        result = EnvInterpolator.interpolate("${env:TEST_VAR:-default}")
        assert result == "actual_value"

    def test_interpolate_missing_env_var_no_default_raises(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("MISSING_VAR", raising=False)
        with pytest.raises(
            ValueError, match="Environment variable 'MISSING_VAR'"
        ):
            EnvInterpolator.interpolate("${env:MISSING_VAR}")

    def test_interpolate_no_env_vars_returns_unchanged(self) -> None:
        result = EnvInterpolator.interpolate("plain string without vars")
        assert result == "plain string without vars"

    def test_has_env_vars_returns_true(self) -> None:
        assert EnvInterpolator.has_env_vars("${env:SOME_VAR}") is True

    def test_has_env_vars_returns_false(self) -> None:
        assert EnvInterpolator.has_env_vars("plain string") is False


class TestSQLAlchemyConnectionEnvInterpolation:
    def test_connection_interpolates_host(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("DB_HOST", "db.example.com")
        conn = SQLAlchemyConnection(
            name="test",
            host="${env:DB_HOST}",
            user="user",
            password="pass",
            database="mydb",
        )
        assert conn.host == "db.example.com"

    def test_connection_interpolates_user(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("DB_USER", "admin_user")
        conn = SQLAlchemyConnection(
            name="test",
            host="localhost",
            user="${env:DB_USER}",
            password="pass",
            database="mydb",
        )
        assert conn.user == "admin_user"

    def test_connection_interpolates_password(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("DB_PASSWORD", "super_secret")
        conn = SQLAlchemyConnection(
            name="test",
            host="localhost",
            user="user",
            password="${env:DB_PASSWORD}",
            database="mydb",
        )
        assert conn.password.get_secret_value() == "super_secret"

    def test_connection_interpolates_database(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("DB_NAME", "production_db")
        conn = SQLAlchemyConnection(
            name="test",
            host="localhost",
            user="user",
            password="pass",
            database="${env:DB_NAME}",
        )
        assert conn.database == "production_db"

    def test_connection_interpolates_all_fields(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("DB_HOST", "db.example.com")
        monkeypatch.setenv("DB_USER", "admin")
        monkeypatch.setenv("DB_PASSWORD", "secret123")
        monkeypatch.setenv("DB_NAME", "myapp")
        conn = SQLAlchemyConnection(
            name="test",
            host="${env:DB_HOST}",
            user="${env:DB_USER}",
            password="${env:DB_PASSWORD}",
            database="${env:DB_NAME}",
        )
        assert conn.host == "db.example.com"
        assert conn.user == "admin"
        assert conn.password.get_secret_value() == "secret123"
        assert conn.database == "myapp"

    def test_connection_with_default_values(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("OPTIONAL_HOST", raising=False)
        conn = SQLAlchemyConnection(
            name="test",
            host="${env:OPTIONAL_HOST:-localhost}",
            user="user",
            password="pass",
            database="mydb",
        )
        assert conn.host == "localhost"

    def test_connection_interpolates_port(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("DB_PORT", "5432")
        conn = SQLAlchemyConnection(
            name="test",
            host="localhost",
            port="${env:DB_PORT}",
            user="user",
            password="pass",
            database="mydb",
        )
        assert conn.port == 5432

    def test_connection_port_with_default(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("DB_PORT", raising=False)
        conn = SQLAlchemyConnection(
            name="test",
            host="localhost",
            port="${env:DB_PORT:-3306}",
            user="user",
            password="pass",
            database="mydb",
        )
        assert conn.port == 3306

    def test_connection_with_connection_args(self) -> None:
        conn = SQLAlchemyConnection(
            name="test",
            host="localhost",
            user="user",
            password="pass",
            database="mydb",
            connection_args={"timeout": 30, "charset": "utf8mb4"},
        )
        assert conn.connection_args == {"timeout": 30, "charset": "utf8mb4"}
