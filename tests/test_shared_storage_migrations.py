from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("sqlalchemy")
from sqlalchemy import inspect

from gas_calibrator.storage.database import DatabaseManager, StorageSettings
from gas_calibrator.storage.models import Base

REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATION_ROOT = REPO_ROOT / "src" / "gas_calibrator" / "storage" / "migrations"
OLD_V2_MIGRATION_ROOT = REPO_ROOT / "src" / "gas_calibrator" / "v2" / "storage" / "migrations"
EXPECTED_MIGRATIONS = (
    "001_initial.sql",
    "002_sensor_dimension_and_run_metadata.sql",
    "003_sensor_sn_device_code_identity.sql",
    "004_sensor_identity_aliases.sql",
)


def test_postgresql_migration_history_is_owned_by_shared_storage() -> None:
    assert tuple(path.name for path in sorted(MIGRATION_ROOT.glob("*.sql"))) == EXPECTED_MIGRATIONS
    assert not list(OLD_V2_MIGRATION_ROOT.glob("*.sql"))
    assert not (REPO_ROOT / "src" / "gas_calibrator" / "v2" / "storage" / "__init__.py").exists()


def test_postgresql_migrations_cover_the_shared_schema_end_state() -> None:
    migration_sql = "\n".join(
        (MIGRATION_ROOT / name).read_text(encoding="utf-8")
        for name in EXPECTED_MIGRATIONS
    ).lower()

    for table_name in Base.metadata.tables:
        assert (
            f"create table if not exists {table_name}" in migration_sql
        ), table_name

    expected_columns = {
        "run_mode",
        "route_mode",
        "profile_name",
        "profile_version",
        "report_family",
        "report_templates",
        "analyzer_setup",
        "co2_group",
        "cylinder_nominal_ppm",
        "sensor_id",
        "sn_code",
        "device_code",
        "alias_type",
        "alias_value",
        "source_run_id",
        "observed_at",
        "valid_from",
        "valid_to",
    }
    for column_name in expected_columns:
        assert column_name in migration_sql

    assert "on delete set null" in migration_sql
    assert "uq_sensors_sn_code_not_null" in migration_sql
    assert "uq_sensors_device_code_not_null" in migration_sql
    assert "uq_sensor_identity_alias_source" in migration_sql


def test_known_postgresql_and_orm_ddl_differences_are_explicitly_governed() -> None:
    governance = (MIGRATION_ROOT / "README.md").read_text(encoding="utf-8")
    assert "separate P1 schema-governance work" in governance
    assert "ON DELETE SET NULL" in governance
    assert "server defaults" in governance

    runs = Base.metadata.tables["runs"]
    points = Base.metadata.tables["points"]
    run_mode_profile_index = next(
        index
        for index in runs.indexes
        if index.name == "ix_runs_mode_profile"
    )
    assert runs.c.run_mode.type.length == 32
    assert runs.c.route_mode.type.length == 32
    assert points.c.co2_group.type.length == 16
    assert tuple(run_mode_profile_index.columns.keys()) == (
        "run_mode",
        "profile_name",
    )

    for table_name in (
        "samples",
        "measurement_frames",
        "fit_results",
        "coefficient_versions",
    ):
        sensor_id = Base.metadata.tables[table_name].c.sensor_id
        assert next(iter(sensor_id.foreign_keys)).ondelete is None


def test_shared_orm_initializes_the_migration_end_state_in_temporary_sqlite(
    tmp_path: Path,
) -> None:
    database = DatabaseManager(
        StorageSettings(
            backend="sqlite",
            database=str(tmp_path / "shared_storage_schema.sqlite"),
        )
    )
    try:
        database.initialize()
        inspector = inspect(database.engine)
        assert set(Base.metadata.tables).issubset(inspector.get_table_names())
        assert {
            "sensor_id",
            "device_key",
            "sn_code",
            "device_code",
            "metadata",
        }.issubset(
            {column["name"] for column in inspector.get_columns("sensors")}
        )
        assert {
            "sensor_id",
            "alias_type",
            "alias_value",
            "source_run_id",
            "observed_at",
            "valid_from",
            "valid_to",
            "metadata",
        }.issubset(
            {
                column["name"]
                for column in inspector.get_columns("sensor_identity_aliases")
            }
        )
    finally:
        database.dispose()
