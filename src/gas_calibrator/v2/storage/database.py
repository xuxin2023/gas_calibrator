from __future__ import annotations

from contextlib import asynccontextmanager, contextmanager
from dataclasses import asdict, dataclass, is_dataclass
import json
import os
from pathlib import Path
from typing import Any, AsyncIterator, Iterator, Optional
from urllib.parse import quote_plus
from uuid import UUID, uuid5

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy import create_engine

from .models import create_schema_async, create_schema_sync


STORAGE_NAMESPACE = UUID("9b9bbd10-9180-4bcc-9fe5-bf3405209ef4")


def stable_uuid(*parts: object) -> UUID:
    token = "::".join("" if part is None else str(part) for part in parts)
    return uuid5(STORAGE_NAMESPACE, token)


def resolve_run_uuid(run_ref: str | UUID) -> UUID:
    if isinstance(run_ref, UUID):
        return run_ref
    text_value = str(run_ref).strip()
    try:
        return UUID(text_value)
    except ValueError:
        return stable_uuid("run", text_value)


@dataclass(slots=True)
class StorageSettings:
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
    schema: str = "public"
    timescaledb: bool = False
    auto_import: bool = True
    async_driver: str = "asyncpg"

    @classmethod
    def from_dict(cls, payload: Optional[dict[str, Any]]) -> "StorageSettings":
        if not payload:
            return cls()
        if isinstance(payload.get("storage"), dict):
            payload = payload["storage"]
        enabled = payload.get("enabled")
        return cls(
            enabled=None if enabled is None else bool(enabled),
            backend=str(payload.get("backend", "file")),
            host=str(payload.get("host", "localhost")),
            port=int(payload.get("port", 5432)),
            database=str(payload.get("database", "gas_calibrator")),
            user=str(payload.get("user", "postgres")),
            password=str(payload.get("password", "")),
            pool_size=int(payload.get("pool_size", 10)),
            echo=bool(payload.get("echo", False)),
            dsn=str(payload.get("dsn", "")),
            schema=str(payload.get("schema", "public")),
            timescaledb=bool(payload.get("timescaledb", False)),
            auto_import=bool(payload.get("auto_import", True)),
            async_driver=str(payload.get("async_driver", "asyncpg")),
        )

    @classmethod
    def from_config(cls, config: Any) -> "StorageSettings":
        if isinstance(config, cls):
            return config
        if is_dataclass(config):
            return cls.from_dict(asdict(config))
        if isinstance(config, dict):
            return cls.from_dict(config)
        payload = {
            key: getattr(config, key)
            for key in (
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
                "schema",
                "timescaledb",
                "auto_import",
            )
            if hasattr(config, key)
        }
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


class DatabaseManager:
    def __init__(self, settings: StorageSettings):
        self.settings = settings
        self._engine: Optional[Engine] = None
        self._async_engine: Optional[AsyncEngine] = None
        self._session_factory: Optional[sessionmaker[Session]] = None
        self._async_session_factory: Optional[async_sessionmaker[AsyncSession]] = None

    @classmethod
    def from_config(cls, config: Any) -> "DatabaseManager":
        return cls(StorageSettings.from_config(config))

    @property
    def enabled(self) -> bool:
        return self.settings.is_enabled

    @property
    def engine(self) -> Engine:
        if self._engine is None:
            kwargs = self._engine_kwargs()
            self._engine = create_engine(self.settings.sync_url(), **kwargs)
        return self._engine

    @property
    def async_engine(self) -> AsyncEngine:
        if self._async_engine is None:
            kwargs = self._engine_kwargs()
            self._async_engine = create_async_engine(self.settings.async_url(), **kwargs)
        return self._async_engine

    @property
    def session_factory(self) -> sessionmaker[Session]:
        if self._session_factory is None:
            self._session_factory = sessionmaker(
                bind=self.engine,
                autoflush=False,
                autocommit=False,
                expire_on_commit=False,
            )
        return self._session_factory

    @property
    def async_session_factory(self) -> async_sessionmaker[AsyncSession]:
        if self._async_session_factory is None:
            self._async_session_factory = async_sessionmaker(
                bind=self.async_engine,
                autoflush=False,
                expire_on_commit=False,
            )
        return self._async_session_factory

    def initialize(self) -> bool:
        if not self.enabled:
            return False
        create_schema_sync(self.engine, enable_timescaledb=self.settings.timescaledb)
        return True

    async def initialize_async(self) -> bool:
        if not self.enabled:
            return False
        await create_schema_async(self.async_engine, enable_timescaledb=self.settings.timescaledb)
        return True

    def health_check(self) -> dict[str, Any]:
        if not self.enabled:
            return {"ok": False, "reason": "storage disabled", "backend": self.settings.normalized_backend}
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return {
                "ok": True,
                "backend": self.settings.normalized_backend,
                "database": self.settings.database,
            }
        except SQLAlchemyError as exc:
            return {
                "ok": False,
                "backend": self.settings.normalized_backend,
                "database": self.settings.database,
                "error": str(exc),
            }

    @contextmanager
    def session_scope(self) -> Iterator[Session]:
        if not self.enabled:
            raise RuntimeError("database storage is disabled")
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    @asynccontextmanager
    async def async_session_scope(self) -> AsyncIterator[AsyncSession]:
        if not self.enabled:
            raise RuntimeError("database storage is disabled")
        session = self.async_session_factory()
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()

    def dispose(self) -> None:
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None
        self._session_factory = None

    async def dispose_async(self) -> None:
        if self._async_engine is not None:
            await self._async_engine.dispose()
            self._async_engine = None
        self._async_session_factory = None

    def _engine_kwargs(self) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "echo": self.settings.echo,
            "future": True,
        }
        if self.settings.normalized_backend != "sqlite":
            kwargs["pool_pre_ping"] = True
            kwargs["pool_size"] = self.settings.pool_size
            kwargs["max_overflow"] = max(2, self.settings.pool_size)
        return kwargs


_db_manager: Optional[DatabaseManager] = None
_db_settings: Optional[StorageSettings] = None


def _resolve_db_url() -> str:
    env_url = os.environ.get("GAS_CAL_DB_URL", "").strip()
    if env_url:
        url = env_url
        safe_drivers = ("psycopg", "psycopg2", "pg8000")
        if "+asyncpg" in url:
            url = url.replace("+asyncpg", "+psycopg")
        elif "://" in url and "postgresql" in url.split("://")[0] and not any(f"+{d}" in url for d in safe_drivers):
            url = url.replace("postgresql://", "postgresql+psycopg://", 1)
        return url
    try:
        import psycopg
        return "postgresql+psycopg://postgres:postgres@localhost:5432/gas_calibrator"
    except ImportError:
        return "sqlite:///gas_calibrator_index.db"


def get_engine() -> Engine:
    global _db_manager, _db_settings
    if _db_manager is not None and _db_manager._engine is not None:
        return _db_manager.engine
    dsn = _resolve_db_url()
    backend = "sqlite" if dsn.startswith("sqlite") else "postgresql"
    _db_settings = StorageSettings.from_dict({"dsn": dsn, "backend": backend})
    _db_manager = DatabaseManager(_db_settings)
    _db_manager.initialize()
    return _db_manager.engine


def get_session() -> Session:
    return get_engine_manager().session_factory()


def get_engine_manager() -> DatabaseManager:
    global _db_manager
    if _db_manager is None:
        get_engine()
    return _db_manager
