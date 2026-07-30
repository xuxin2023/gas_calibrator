from __future__ import annotations

import json
from pathlib import Path

import pytest

pytest.importorskip("sqlalchemy")
from sqlalchemy import func, select

from gas_calibrator.storage.database import (
    DatabaseManager,
    StorageSettings,
    stable_uuid,
)
from gas_calibrator.storage.models import (
    DeviceEventRecord,
    RunRecord,
    SensorIdentityAliasRecord,
    SensorRecord,
)
from gas_calibrator.v1_5.initialization_database import (
    build_v1_5_runtime_setup_storage_preview,
    import_v1_5_runtime_setup_result,
    load_v1_5_runtime_setup_result,
)
from v1_5_runtime_setup_support import write_runtime_setup_result


def _runtime_result_path(tmp_path: Path) -> Path:
    return write_runtime_setup_result(
        tmp_path / "v1_5_analyzer_runtime_setup_result.json",
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
        run_id="v1_5_analyzer_runtime_setup_test_2ch",
    )


def _settings(db_path: Path) -> StorageSettings:
    return StorageSettings(backend="sqlite", database=str(db_path))


def test_v1_5_runtime_setup_preview_does_not_require_database(tmp_path: Path) -> None:
    result_path = _runtime_result_path(tmp_path)
    payload = load_v1_5_runtime_setup_result(result_path)

    preview = build_v1_5_runtime_setup_storage_preview(payload, result_path=result_path)

    assert preview["target_tables"] == ["sensors", "runs", "device_events"]
    assert preview["runtime_status"] == "ready"
    assert preview["notes"]["runtime_setup_writes_senco"] is False
    assert [item["device_key"] for item in preview["sensors"]] == [
        "co2_h2o_dual:01260601",
        "co2_h2o_dual:01260602",
    ]
    assert [item["event_type"] for item in preview["device_events"]] == [
        "v1_5_analyzer_runtime_setup",
        "v1_5_analyzer_runtime_setup",
    ]


def test_v1_5_runtime_setup_import_apply_is_idempotent(tmp_path: Path) -> None:
    result_path = _runtime_result_path(tmp_path)
    db_path = tmp_path / "storage.sqlite"
    database = DatabaseManager(_settings(db_path))
    try:
        database.initialize()
        result = import_v1_5_runtime_setup_result(
            database,
            result_path,
            dry_run=False,
            allow_write=True,
            operator="qa",
        )
        assert result["database_written"] is True
        assert result["sensors"] == 2
        assert result["device_events"] == 2

        rerun = import_v1_5_runtime_setup_result(
            database,
            result_path,
            dry_run=False,
            allow_write=True,
            operator="qa",
        )
        assert rerun["sensors"] == 2

        with database.session_scope() as session:
            assert session.execute(select(func.count(SensorRecord.sensor_id))).scalar_one() == 2
            assert session.execute(select(func.count(SensorIdentityAliasRecord.id))).scalar_one() >= 6
            assert session.execute(select(func.count(DeviceEventRecord.id))).scalar_one() == 2
            assert session.execute(select(func.count(RunRecord.id))).scalar_one() == 1

            sensor = session.execute(
                select(SensorRecord).where(SensorRecord.sn_code == "01260601")
            ).scalar_one()
            assert sensor.device_key == "co2_h2o_dual:01260601"
            assert sensor.sn_code == "01260601"
            assert sensor.device_code == "01260601"
            assert sensor.analyzer_serial == "01260601"
            assert sensor.metadata_json["protocol_device_id_current"] == "047"
            assert sensor.metadata_json["runtime_setup_status"] == "ready"
            assert sensor.metadata_json["runtime_setup_ready"] is True
            assert sensor.metadata_json["active_upload_rate"]["target_hz"] == 1

            run_record = session.get(RunRecord, stable_uuid("run", "v1_5_analyzer_runtime_setup_test_2ch"))
            assert run_record is not None
            assert run_record.status == "completed"
            assert run_record.run_mode == "v1_5_analyzer_runtime_setup"
            notes = json.loads(run_record.notes or "{}")
            runtime_notes = notes["v1_5_analyzer_runtime_setup"]
            assert runtime_notes["runtime_status"] == "ready"
            assert len(runtime_notes["run_devices"]) == 2
            assert runtime_notes["run_devices"][0]["active_upload_rate"]["ok"] is True

            event = session.execute(
                select(DeviceEventRecord).where(DeviceEventRecord.device_name == "01260601")
            ).scalar_one()
            assert event.event_type == "v1_5_analyzer_runtime_setup"
            assert event.event_data["summary"]["runtime_setup_ready"] is True
            assert event.event_data["summary"]["active_upload_rate_ok"] is True
            assert event.event_data["writes_senco"] is False
            assert event.event_data["runs_sampling"] is False
            assert event.event_data["run_device"]["protocol_device_id_at_run"] == "047"
            assert event.event_data["runtime_setup_result"]["active_upload_rate"]["target_hz"] == 1
    finally:
        database.dispose()


def test_v1_5_runtime_setup_import_requires_allow_write(tmp_path: Path) -> None:
    result_path = _runtime_result_path(tmp_path)
    database = DatabaseManager(_settings(tmp_path / "storage.sqlite"))
    try:
        with pytest.raises(PermissionError):
            import_v1_5_runtime_setup_result(database, result_path, dry_run=False, allow_write=False)
    finally:
        database.dispose()


def test_v1_5_runtime_setup_import_rejects_non_mature_or_unverified_ready(tmp_path: Path) -> None:
    result_path = _runtime_result_path(tmp_path)
    payload = json.loads(result_path.read_text(encoding="utf-8"))
    payload["plan"]["contract"]["ftd_hz"] = 10
    bad_path = tmp_path / "bad_ftd10.json"
    bad_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    with pytest.raises(ValueError, match="FTD=1Hz"):
        load_v1_5_runtime_setup_result(bad_path)
        build_v1_5_runtime_setup_storage_preview(payload, result_path=bad_path)

    payload["plan"]["contract"]["ftd_hz"] = 1
    payload["results"][0].pop("active_upload_rate")
    missing_rate_path = tmp_path / "missing_rate.json"
    missing_rate_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    with pytest.raises(ValueError, match="active_upload_rate"):
        loaded = load_v1_5_runtime_setup_result(missing_rate_path)
        build_v1_5_runtime_setup_storage_preview(loaded, result_path=missing_rate_path)
