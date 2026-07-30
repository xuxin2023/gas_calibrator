from __future__ import annotations

from dataclasses import asdict, dataclass, is_dataclass
import json
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote_plus


@dataclass
class StorageConfig:
    """Common file/database storage configuration contract."""

    enabled: Optional[bool] = None
    backend: str = "file"
    host: str = "localhost"
    port: int = 5432
    database: str = "gas_calibrator"
    user: str = "postgres"
    password: str = ""
    pool_size: int = 10
    echo: bool = False
    dsn: str = ""
    timescaledb: bool = False
    auto_import: bool = True

    @classmethod
    def from_dict(cls, payload: Optional[dict[str, Any]]) -> "StorageConfig":
        if not payload:
            return cls()
        if isinstance(payload.get("storage"), dict):
            payload = payload["storage"]
        enabled = payload.get("enabled")
        values: dict[str, Any] = {
            "enabled": None if enabled is None else bool(enabled),
            "backend": str(payload.get("backend", "file")),
            "host": str(payload.get("host", "localhost")),
            "port": int(payload.get("port", 5432)),
            "database": str(payload.get("database", "gas_calibrator")),
            "user": str(payload.get("user", "postgres")),
            "password": str(payload.get("password", "")),
            "pool_size": int(payload.get("pool_size", 10)),
            "echo": bool(payload.get("echo", False)),
            "dsn": str(payload.get("dsn", "")),
            "timescaledb": bool(payload.get("timescaledb", False)),
            "auto_import": bool(payload.get("auto_import", True)),
        }
        if "async_driver" in getattr(cls, "__dataclass_fields__", {}):
            values["async_driver"] = str(payload.get("async_driver", "asyncpg"))
        if "schema" in getattr(cls, "__dataclass_fields__", {}):
            values["schema"] = str(payload.get("schema", "public"))
        return cls(**values)

    @classmethod
    def from_config(cls, config: Any) -> "StorageConfig":
        if isinstance(config, cls):
            return config
        if is_dataclass(config):
            return cls.from_dict(asdict(config))
        if isinstance(config, dict):
            return cls.from_dict(config)
        field_names = (
            "enabled",
            "backend",
            "host",
            "port",
            "database",
            "user",
            "password",
            "pool_size",
            "echo",
            "dsn",
            "timescaledb",
            "auto_import",
            "async_driver",
            "schema",
        )
        payload = {key: getattr(config, key) for key in field_names if hasattr(config, key)}
        return cls.from_dict(payload)

    @property
    def normalized_backend(self) -> str:
        backend = str(self.backend or "").strip().lower()
        if backend in {"postgres", "postgresql", "timescaledb"}:
            return "postgresql"
        if backend in {"sqlite", "sqlite3"}:
            return "sqlite"
        return backend

    @property
    def is_enabled(self) -> bool:
        if self.enabled is not None:
            return bool(self.enabled)
        return bool(self.dsn) or self.normalized_backend in {"postgresql", "sqlite"}

    @property
    def database_enabled(self) -> bool:
        return self.is_enabled

    def sync_url(self) -> str:
        if self.dsn:
            return self.dsn
        if self.normalized_backend == "sqlite":
            database_path = str(self.database or ":memory:")
            if database_path == ":memory:":
                return "sqlite:///:memory:"
            return f"sqlite:///{Path(database_path).resolve().as_posix()}"
        user = quote_plus(self.user)
        password = quote_plus(self.password)
        auth = f"{user}:{password}@" if password else f"{user}@"
        return f"postgresql+psycopg://{auth}{self.host}:{self.port}/{self.database}"


@dataclass
class StorageSettings(StorageConfig):
    """Database-engine settings extending the common storage contract."""

    schema: str = "public"
    async_driver: str = "asyncpg"

    def async_url(self) -> str:
        if self.dsn:
            if self.dsn.startswith("postgresql+"):
                return self.dsn
            if self.dsn.startswith("postgresql://"):
                return self.dsn.replace("postgresql://", f"postgresql+{self.async_driver}://", 1)
            if self.dsn.startswith("sqlite://"):
                return self.dsn.replace("sqlite://", "sqlite+aiosqlite://", 1)
            return self.dsn
        if self.normalized_backend == "sqlite":
            database_path = str(self.database or ":memory:")
            if database_path == ":memory:":
                return "sqlite+aiosqlite:///:memory:"
            return f"sqlite+aiosqlite:///{Path(database_path).resolve().as_posix()}"
        user = quote_plus(self.user)
        password = quote_plus(self.password)
        auth = f"{user}:{password}@" if password else f"{user}@"
        return f"postgresql+{self.async_driver}://{auth}{self.host}:{self.port}/{self.database}"


def load_storage_config_file(path: str | Path) -> StorageSettings:
    file_path = Path(path)
    payload = json.loads(file_path.read_text(encoding="utf-8"))
    return StorageSettings.from_dict(payload)
