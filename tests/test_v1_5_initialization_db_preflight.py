from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pytest

pytest.importorskip("sqlalchemy")
from sqlalchemy import select

from gas_calibrator.tools.run_v1_5_initialization_db_preflight import main as preflight_main
from gas_calibrator.tools.run_v1_5_initialization_db_preflight import run_preflight
from gas_calibrator.v1_5.import_initialization_database import run_import
from gas_calibrator.v1_5.initialization_db_preflight import build_v1_5_initialization_db_preflight
from gas_calibrator.storage.database import DatabaseManager, StorageSettings
from gas_calibrator.storage.models import DeviceEventRecord
from v1_5_runtime_setup_support import write_runtime_setup_result


def _settings(db_path: Path) -> StorageSettings:
    return StorageSettings(backend="sqlite", database=str(db_path))


def _bundle_path(tmp_path: Path) -> Path:
    payload = {
        "schema": "v1_5_formal_db_upsert_dry_run_2ch_v0",
        "created_at": "2026-06-27T10:00:00+08:00",
        "run_id": "v1_5_init_db_preflight_getco",
        "validation": {
            "status": "ready",
            "formal_database_written": False,
            "protocol_device_id_is_primary_identity": False,
            "all_final_mode2_ready": True,
            "all_getco_epoch0_complete": True,
            "all_identity_bound_to_sn": True,
        },
        "identity_rules": {
            "primary_identity": "sn_code/device_code",
            "protocol_device_id_role": "compatibility query field only",
        },
        "devices": [
            {
                "device_key": "gas_analyzer:sn:01260601",
                "sn_code": "01260601",
                "device_code": "01260601",
                "metadata_json": {
                    "sn_code": "01260601",
                    "device_code": "01260601",
                    "protocol_device_id_current": "047",
                },
            },
            {
                "device_key": "gas_analyzer:sn:01260602",
                "sn_code": "01260602",
                "device_code": "01260602",
                "metadata_json": {
                    "sn_code": "01260602",
                    "device_code": "01260602",
                    "protocol_device_id_current": "054",
                },
            },
        ],
        "run_devices": [
            {
                "run_id": "v1_5_init_db_preflight_getco",
                "slot_id": "GA01",
                "port": "COM36",
                "sn_code": "01260601",
                "device_code": "01260601",
                "protocol_device_id_at_run": "047",
                "mode_at_run": "2",
                "status": "formal_initialization_ready",
            },
            {
                "run_id": "v1_5_init_db_preflight_getco",
                "slot_id": "GA02",
                "port": "COM37",
                "sn_code": "01260602",
                "device_code": "01260602",
                "protocol_device_id_at_run": "054",
                "mode_at_run": "2",
                "status": "formal_initialization_ready",
            },
        ],
        "identity_lookup": [],
        "coefficient_snapshots": [
            {
                "sn_code": "01260601",
                "snapshot_type": "initialization_epoch0_getco1_9",
                "getco_complete": "True",
            },
            {
                "sn_code": "01260602",
                "snapshot_type": "initialization_epoch0_getco1_9",
                "getco_complete": "True",
            },
        ],
        "evidence_manifest": [],
        "evidence_manifest_sha256": "preflight-test",
    }
    path = tmp_path / "init_bundle.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _runtime_result_path(tmp_path: Path) -> Path:
    return write_runtime_setup_result(
        tmp_path / "runtime_setup.json",
        [
            {
                "slot": "GA01",
                "port": "COM36",
                "protocol_device_id": "047",
                "sn_code": "01260601",
            },
            {
                "slot": "GA02",
                "port": "COM37",
                "protocol_device_id": "054",
                "sn_code": "01260602",
            },
        ],
        run_id="v1_5_init_db_preflight_runtime",
    )


def _import_ready_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "init.sqlite"
    settings = _settings(db_path)
    run_import(
        bundle=_bundle_path(tmp_path),
        settings=settings,
        init_schema=True,
        apply=True,
        acknowledge_formal_db_write=True,
        operator="qa",
    )
    run_import(
        runtime_setup_result=_runtime_result_path(tmp_path),
        settings=settings,
        apply=True,
        acknowledge_formal_db_write=True,
        operator="qa",
    )
    return db_path


def test_v1_5_initialization_db_preflight_ready_for_expected_devices(tmp_path: Path) -> None:
    db_path = _import_ready_db(tmp_path)
    database = DatabaseManager(_settings(db_path))
    try:
        report = build_v1_5_initialization_db_preflight(
            database,
            expected_devices=["01260601=047", "01260602=054"],
        )
    finally:
        database.dispose()

    assert report["status"] == "ready"
    assert report["ready_count"] == 2
    assert report["next_gate"] == "gas_route_allowed"
    assert report["boundary"]["opens_com"] is False
    assert report["devices"][0]["protocol_device_id"] == "047"
    assert all(check["status"] == "pass" for row in report["devices"] for check in row["checks"])


def test_v1_5_initialization_db_preflight_accepts_older_runtime_event_shape(tmp_path: Path) -> None:
    db_path = _import_ready_db(tmp_path)
    database = DatabaseManager(_settings(db_path))
    try:
        with database.session_scope() as session:
            event = session.execute(
                select(DeviceEventRecord).where(
                    DeviceEventRecord.device_name == "01260601",
                    DeviceEventRecord.event_type == "v1_5_analyzer_runtime_setup",
                )
            ).scalar_one()
            payload = dict(event.event_data)
            payload.pop("summary", None)
            event.event_data = payload

        report = build_v1_5_initialization_db_preflight(
            database,
            expected_devices=["01260601=047"],
        )
    finally:
        database.dispose()

    assert report["status"] == "ready"
    assert report["devices"][0]["status"] == "ready"


def test_v1_5_initialization_db_preflight_prefers_complete_getco_event_over_newer_incomplete_event(
    tmp_path: Path,
) -> None:
    db_path = _import_ready_db(tmp_path)
    database = DatabaseManager(_settings(db_path))
    try:
        with database.session_scope() as session:
            complete_event = session.execute(
                select(DeviceEventRecord).where(
                    DeviceEventRecord.device_name == "01260601",
                    DeviceEventRecord.event_type == "v1_5_initialization_identity_bound",
                )
            ).scalars().first()
            assert complete_event is not None
            session.add(
                DeviceEventRecord(
                    id=uuid4(),
                    run_id=complete_event.run_id,
                    device_name="01260601",
                    event_type="v1_5_initialization_identity_bound",
                    event_data={
                        "summary": {
                            "sn_code": "01260601",
                            "getco_complete": False,
                            "snapshot_type": "planned_identity_only",
                        },
                        "not_calibration_acceptance_result": True,
                    },
                    timestamp=datetime(2026, 6, 28, tzinfo=timezone.utc),
                )
            )

        report = build_v1_5_initialization_db_preflight(
            database,
            expected_devices=["01260601=047"],
        )
    finally:
        database.dispose()

    assert report["status"] == "ready"
    assert report["devices"][0]["status"] == "ready"


def test_v1_5_initialization_db_preflight_blocks_missing_runtime_setup(tmp_path: Path) -> None:
    db_path = tmp_path / "init_only.sqlite"
    run_import(
        bundle=_bundle_path(tmp_path),
        settings=_settings(db_path),
        init_schema=True,
        apply=True,
        acknowledge_formal_db_write=True,
        operator="qa",
    )
    database = DatabaseManager(_settings(db_path))
    try:
        report = build_v1_5_initialization_db_preflight(
            database,
            expected_devices=["01260601=047"],
        )
    finally:
        database.dispose()

    assert report["status"] == "blocked"
    failed = [
        check["reason"]
        for row in report["devices"]
        for check in row["checks"]
        if check["status"] == "fail"
    ]
    assert "runtime_setup_event_missing" in failed
    assert report["next_gate"] == "blocked_before_gas_route"


def test_v1_5_initialization_db_preflight_requires_postgresql18_when_requested(tmp_path: Path) -> None:
    db_path = _import_ready_db(tmp_path)
    database = DatabaseManager(_settings(db_path))
    try:
        report = build_v1_5_initialization_db_preflight(
            database,
            expected_devices=["01260601=047"],
            require_postgresql_major=18,
        )
    finally:
        database.dispose()

    assert report["status"] == "blocked"
    assert report["database"]["reason"] == "postgresql_18_required"
    assert report["devices"] == []


def test_v1_5_initialization_db_preflight_cli_writes_reports(tmp_path: Path) -> None:
    db_path = _import_ready_db(tmp_path)
    output_json = tmp_path / "preflight.json"
    output_md = tmp_path / "preflight.md"

    result = run_preflight(
        expected_devices=["01260601=047", "01260602=054"],
        settings=_settings(db_path),
        output_json=output_json,
        output_md=output_md,
    )

    assert result["report"]["status"] == "ready"
    assert output_json.exists()
    assert output_md.exists()
    assert json.loads(output_json.read_text(encoding="utf-8"))["status"] == "ready"
    assert "V1.5 Initialization DB Preflight" in output_md.read_text(encoding="utf-8")

    cli_dir = tmp_path / "cli"
    rc = preflight_main(
        [
            "--expected-device",
            "01260601=047",
            "--expected-device",
            "01260602=054",
            "--backend",
            "sqlite",
            "--database",
            str(db_path),
            "--output-json",
            str(cli_dir / "preflight.json"),
            "--output-md",
            str(cli_dir / "preflight.md"),
        ]
    )
    assert rc == 0
    assert (cli_dir / "preflight.json").exists()
    assert json.loads((cli_dir / "preflight.json").read_text(encoding="utf-8"))["status"] == "ready"
