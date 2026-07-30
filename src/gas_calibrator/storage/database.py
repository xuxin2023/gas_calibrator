from __future__ import annotations

from contextlib import asynccontextmanager, contextmanager
from typing import Any, AsyncIterator, Iterator, Optional
from uuid import UUID, uuid5

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy import create_engine

from .models import create_schema_async, create_schema_sync
from .settings import StorageSettings as StorageSettings
from .settings import load_storage_config_file as load_storage_config_file


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
