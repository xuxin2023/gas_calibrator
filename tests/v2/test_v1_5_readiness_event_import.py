from __future__ import annotations

import json
from pathlib import Path

import pytest

pytest.importorskip("sqlalchemy")

from sqlalchemy import func, select

from gas_calibrator.v2.storage.database import DatabaseManager, StorageSettings
from gas_calibrator.v2.storage.import_v1_5_initialization import run_import as run_initialization_import
from gas_calibrator.v2.storage.import_v1_5_readiness_events import EVENT_TYPE, run_import
from gas_calibrator.v2.storage.models import DeviceEventRecord

from .test_v1_5_initialization_import import _bundle_path


def _settings(db_path: Path) -> StorageSettings:
    return StorageSettings(backend="sqlite", database=str(db_path))


def _readiness_path(tmp_path: Path) -> Path:
    payload = {
        "schema_version": "v1_5_post_initialization_readiness_backfill_result_v0",
        "generated_at": "2026-06-24T17:34:38",
        "run_id": "current6_post_initialization_readiness_after_temperature_20260624",
        "status": "blocked_or_partial_next_layer",
        "gates": {
            "schema_version": "v1_5_post_initialization_readiness_gates_v0",
            "status": "blocked_or_partial_next_layer",
            "gates": [
                {
                    "name": "pressure_s9_completion_review",
                    "status": "ready",
                    "expected_device_scope": ["01260601", "01260602"],
                },
                {
                    "name": "temperature_s7_s8_review",
                    "status": "ready",
                    "expected_device_scope": ["01260601", "01260602"],
                },
                {
                    "name": "gas_route_readiness",
                    "status": "blocked_or_review_required",
                    "expected_device_scope": ["01260601", "01260602"],
                },
                {
                    "name": "water_route_readiness",
                    "status": "blocked_or_review_required",
                    "expected_device_scope": ["01260601", "01260602"],
                },
                {"name": "sampling_execution", "status": "blocked_by_readiness_gates"},
            ],
        },
        "boundary": {
            "offline_only": True,
            "opens_com": False,
            "writes_senco": False,
            "controls_gas_route": False,
            "runs_sampling": False,
        },
    }
    path = tmp_path / "readiness.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def test_readiness_event_import_preview_is_offline(tmp_path: Path) -> None:
    result = run_import(
        readiness=_readiness_path(tmp_path),
        source_run_id="v1_5_formal_initialization_test_2ch",
    )

    assert result["database_written"] is False
    assert result["event_type"] == EVENT_TYPE
    assert result["event_count"] == 2
    assert result["gate_statuses"]["pressure_s9_completion_review"] == "ready"
    assert result["gate_statuses"]["sampling_execution"] == "blocked_by_readiness_gates"
    assert result["boundary"]["opens_com"] is False
    assert result["boundary"]["runs_sampling"] is False


def test_readiness_event_import_apply_is_idempotent(tmp_path: Path) -> None:
    bundle = _bundle_path(tmp_path)
    db_path = tmp_path / "storage.sqlite"
    settings = _settings(db_path)
    run_initialization_import(
        bundle=bundle,
        settings=settings,
        init_schema=True,
        apply=True,
        acknowledge_formal_db_write=True,
        operator="qa",
    )
    readiness = _readiness_path(tmp_path)
    result = run_import(
        readiness=readiness,
        source_run_id="v1_5_formal_initialization_test_2ch",
        settings=settings,
        apply=True,
        acknowledge_formal_db_write=True,
        operator="qa_readiness",
    )
    rerun = run_import(
        readiness=readiness,
        source_run_id="v1_5_formal_initialization_test_2ch",
        settings=settings,
        apply=True,
        acknowledge_formal_db_write=True,
        operator="qa_readiness",
    )

    assert result["database_written"] is True
    assert result["events_written"] == 2
    assert rerun["events_written"] == 2

    database = DatabaseManager(settings)
    try:
        with database.session_scope() as session:
            assert (
                session.execute(
                    select(func.count(DeviceEventRecord.id)).where(DeviceEventRecord.event_type == EVENT_TYPE)
                ).scalar_one()
                == 2
            )
            row = session.execute(
                select(DeviceEventRecord).where(
                    DeviceEventRecord.event_type == EVENT_TYPE,
                    DeviceEventRecord.device_name == "01260601",
                )
            ).scalar_one()
            assert row.event_data["gate_statuses"]["temperature_s7_s8_review"] == "ready"
            assert row.event_data["gate_statuses"]["gas_route_readiness"] == "blocked_or_review_required"
            assert row.event_data["boundary"]["controls_gas_route"] is False
            assert row.event_data["boundary"]["not_calibration_acceptance_result"] is True
    finally:
        database.dispose()


def test_readiness_event_import_requires_acknowledgement(tmp_path: Path) -> None:
    with pytest.raises(PermissionError):
        run_import(
            readiness=_readiness_path(tmp_path),
            source_run_id="v1_5_formal_initialization_test_2ch",
            settings=_settings(tmp_path / "storage.sqlite"),
            apply=True,
            acknowledge_formal_db_write=False,
        )
