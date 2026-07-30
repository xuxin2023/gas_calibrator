from __future__ import annotations

from dataclasses import asdict
import os
from pathlib import Path
import subprocess
import sys

from gas_calibrator.storage.database import (
    StorageSettings as DatabaseStorageSettings,
    load_storage_config_file as database_load_storage_config_file,
)
from gas_calibrator.storage.settings import (
    StorageConfig,
    StorageSettings,
    load_storage_config_file,
)
from gas_calibrator.v2.config.models import AppConfig, StorageConfig as V2StorageConfig


def test_storage_config_has_one_shared_common_contract() -> None:
    assert V2StorageConfig is StorageConfig
    assert type(AppConfig().storage) is StorageConfig
    assert set(asdict(AppConfig().storage)) == {
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
    }


def test_database_module_preserves_storage_settings_exports() -> None:
    assert DatabaseStorageSettings is StorageSettings
    assert database_load_storage_config_file is load_storage_config_file


def test_storage_enablement_and_urls_preserve_existing_behavior(tmp_path: Path) -> None:
    assert StorageConfig().database_enabled is False
    assert StorageConfig(backend="sqlite").database_enabled is True
    assert StorageConfig(enabled=False, backend="sqlite").database_enabled is False
    assert StorageConfig(dsn="postgresql://host/db").database_enabled is True

    sqlite_path = tmp_path / "settings.sqlite"
    settings = StorageSettings.from_dict(
        {
            "storage": {
                "backend": "sqlite",
                "database": str(sqlite_path),
                "schema": "calibration",
                "async_driver": "custom_async",
            }
        }
    )
    assert settings.sync_url() == f"sqlite:///{sqlite_path.resolve().as_posix()}"
    assert settings.async_url() == f"sqlite+aiosqlite:///{sqlite_path.resolve().as_posix()}"
    assert settings.schema == "calibration"
    assert settings.async_driver == "custom_async"

    postgres = StorageSettings.from_dict(
        {
            "backend": "postgres",
            "host": "db.example",
            "port": 5433,
            "database": "calibration",
            "user": "operator",
            "password": "p a",
            "async_driver": "custom_async",
        }
    )
    assert postgres.sync_url() == (
        "postgresql+psycopg://operator:p+a@db.example:5433/calibration"
    )
    assert postgres.async_url() == (
        "postgresql+custom_async://operator:p+a@db.example:5433/calibration"
    )


def test_storage_settings_module_is_lightweight() -> None:
    source_root = Path(__file__).resolve().parents[1] / "src"
    script = (
        "import sys\n"
        "import gas_calibrator.storage.settings\n"
        "assert not any(name == 'sqlalchemy' or name.startswith('sqlalchemy.') "
        "for name in sys.modules)\n"
    )
    env = dict(os.environ)
    env["PYTHONPATH"] = str(source_root)
    subprocess.run([sys.executable, "-c", script], check=True, env=env)
