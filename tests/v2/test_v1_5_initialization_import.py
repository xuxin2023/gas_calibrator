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
from gas_calibrator.storage.queries import HistoryQueryService
from gas_calibrator.v1_5.import_initialization_database import run_import


def _bundle_path(tmp_path: Path) -> Path:
    bundle = {
        "schema": "v1_5_formal_db_upsert_dry_run_6ch_v0",
        "created_at": "2026-06-23T16:44:58+08:00",
        "run_id": "v1_5_formal_initialization_test_2ch",
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
            "protocol_device_id_role": "long-lived protocol and compatibility query field, not unique primary identity",
        },
        "devices": [
            {
                "device_key": "gas_analyzer:sn:01260601",
                "sn_code": "01260601",
                "device_code": "01260601",
                "metadata_json": json.dumps(
                    {
                        "sn_code": "01260601",
                        "device_code": "01260601",
                        "protocol_device_id_current": "047",
                    }
                ),
            },
            {
                "device_key": "gas_analyzer:sn:01260602",
                "sn_code": "01260602",
                "device_code": "01260602",
                "metadata_json": json.dumps(
                    {
                        "sn_code": "01260602",
                        "device_code": "01260602",
                        "protocol_device_id_current": "054",
                    }
                ),
            },
        ],
        "run_devices": [
            {
                "run_id": "v1_5_formal_initialization_test_2ch",
                "slot_id": "GA01",
                "sn_code": "01260601",
                "device_code": "01260601",
                "protocol_device_id_at_run": "047",
                "mode_at_run": "2",
                "status": "formal_initialization_ready",
            },
            {
                "run_id": "v1_5_formal_initialization_test_2ch",
                "slot_id": "GA02",
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
        "evidence_manifest_sha256": "test-evidence",
    }
    path = tmp_path / "v1_5_bundle.json"
    path.write_text(json.dumps(bundle, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _formal_planned_bundle_path(tmp_path: Path) -> Path:
    bundle = {
        "schema": "v1_5_formal_initialization_db_bundle_v0",
        "generated_at": "2026-06-27T10:00:00+08:00",
        "run_id": "formal_demo",
        "tables": {
            "runs": [
                {
                    "id": "run_formal_demo",
                    "run_id": "formal_demo",
                    "metadata": {
                        "config_hash": "test-config",
                    },
                }
            ],
            "devices": [
                {
                    "device_key": "gas_analyzer:sn:01260601",
                    "serial_number": "01260601",
                    "sn_code": "01260601",
                    "device_code": "01260601",
                    "protocol_device_id_current": "047",
                    "metadata_json": {
                        "slot_id": "GA01",
                        "port_at_initialization": "COM36",
                        "protocol_device_id_current": "047",
                    },
                },
                {
                    "device_key": "gas_analyzer:sn:01260602",
                    "serial_number": "01260602",
                    "sn_code": "01260602",
                    "device_code": "01260602",
                    "protocol_device_id_current": "054",
                    "metadata_json": {
                        "slot_id": "GA02",
                        "port_at_initialization": "COM37",
                        "protocol_device_id_current": "054",
                    },
                },
            ],
            "run_devices": [
                {
                    "run_id": "formal_demo",
                    "slot_id": "GA01",
                    "port": "COM36",
                    "sn_code": "01260601",
                    "device_code": "01260601",
                    "protocol_device_id_at_run": "047",
                    "mode_at_run": "2",
                    "status": "formal_initialization_planned_identity_bound",
                    "metadata": {
                        "slot_id": "GA01",
                        "sn_code": "01260601",
                        "device_code": "01260601",
                        "planned_device_id": "047",
                    },
                },
                {
                    "run_id": "formal_demo",
                    "slot_id": "GA02",
                    "port": "COM37",
                    "sn_code": "01260602",
                    "device_code": "01260602",
                    "protocol_device_id_at_run": "054",
                    "mode_at_run": "2",
                    "status": "formal_initialization_planned_identity_bound",
                    "metadata": {
                        "slot_id": "GA02",
                        "sn_code": "01260602",
                        "device_code": "01260602",
                        "planned_device_id": "054",
                    },
                },
            ],
            "coefficient_snapshots": [],
            "sample_files": [],
        },
    }
    path = tmp_path / "v1_5_formal_planned_bundle.json"
    path.write_text(json.dumps(bundle, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _settings(db_path: Path) -> StorageSettings:
    return StorageSettings(backend="sqlite", database=str(db_path))


def test_v1_5_initialization_import_preview_does_not_require_database(tmp_path: Path) -> None:
    bundle_path = _bundle_path(tmp_path)

    result = run_import(bundle=bundle_path)

    assert result["dry_run"] is True
    assert result["database_written"] is False
    imported = result["imported"]
    assert imported["target_tables"] == ["sensors", "runs", "device_events"]
    assert [item["device_key"] for item in imported["sensors"]] == [
        "co2_h2o_dual:01260601",
        "co2_h2o_dual:01260602",
    ]
    assert imported["notes"]["protocol_device_id_is_primary_identity"] is False


def test_v1_5_initialization_import_can_derive_sn_subset_with_provenance(tmp_path: Path) -> None:
    bundle_path = _bundle_path(tmp_path)
    db_path = tmp_path / "subset.sqlite"

    preview = run_import(
        bundle=bundle_path,
        include_sn_codes=["01260602"],
        derived_run_id="v1_5_current4_getco_subset_test",
    )

    imported = preview["imported"]
    assert preview["dry_run"] is True
    assert preview["derived_run_id"] == "v1_5_current4_getco_subset_test"
    assert [item["sn_code"] for item in imported["sensors"]] == ["01260602"]
    assert [item["sn_code"] for item in imported["run_devices"]] == ["01260602"]
    assert imported["validation_status"] == "ready"
    event = imported["device_events"][0]
    assert event["event_data"]["summary"]["sn_code"] == "01260602"
    assert event["event_data"]["summary"]["getco_complete"] is True

    applied = run_import(
        bundle=bundle_path,
        include_sn_codes=["01260602"],
        derived_run_id="v1_5_current4_getco_subset_test",
        settings=_settings(db_path),
        init_schema=True,
        apply=True,
        acknowledge_formal_db_write=True,
        operator="qa",
    )
    assert applied["database_written"] is True
    assert applied["imported"]["sensors"] == 1
    assert applied["imported"]["device_events"] == 1

    database = DatabaseManager(_settings(db_path))
    try:
        with database.session_scope() as session:
            assert session.execute(select(func.count(SensorRecord.sensor_id))).scalar_one() == 1
            assert session.execute(select(func.count(DeviceEventRecord.id))).scalar_one() == 1
            run_record = session.get(RunRecord, stable_uuid("run", "v1_5_current4_getco_subset_test"))
            assert run_record is not None
            assert run_record.profile_version == "v1_5_formal_initialization_v0"
            assert len(run_record.profile_version) <= 64
            notes = json.loads(run_record.notes or "{}")
            assert notes["v1_5_initialization"]["bundle_schema"] == "v1_5_formal_db_upsert_dry_run_6ch_v0"
            validation = notes["v1_5_initialization"]["validation"]
            assert validation["subset_of_source_run_id"] == "v1_5_formal_initialization_test_2ch"
            assert validation["subset_sn_codes"] == ["01260602"]
            event_record = session.execute(select(DeviceEventRecord)).scalar_one()
            assert event_record.event_data["summary"]["protocol_device_id_at_run"] == "054"
    finally:
        database.dispose()


def test_v1_5_initialization_import_previews_formal_planner_bundle_with_sn_identity(tmp_path: Path) -> None:
    bundle_path = _formal_planned_bundle_path(tmp_path)

    result = run_import(bundle=bundle_path)

    imported = result["imported"]
    assert result["dry_run"] is True
    assert imported["validation_status"] == "planned_identity_index"
    assert [item["sn_code"] for item in imported["sensors"]] == ["01260601", "01260602"]
    assert [item["device_code"] for item in imported["sensors"]] == ["01260601", "01260602"]
    assert imported["run_devices"][0]["protocol_device_id_at_run"] == "047"
    assert imported["device_events"][0]["event_data"]["not_calibration_acceptance_result"] is True

    db_path = tmp_path / "formal_planned.sqlite"
    applied = run_import(
        bundle=bundle_path,
        settings=_settings(db_path),
        init_schema=True,
        apply=True,
        acknowledge_formal_db_write=True,
        operator="qa",
    )
    assert applied["database_written"] is True
    database = DatabaseManager(_settings(db_path))
    try:
        with database.session_scope() as session:
            run_record = session.get(RunRecord, stable_uuid("run", "formal_demo"))
            assert run_record is not None
            assert run_record.status == "completed"
            assert run_record.route_mode == "identity_planned"
            assert run_record.warnings == 1
            assert session.execute(select(func.count(SensorRecord.sensor_id))).scalar_one() == 2
    finally:
        database.dispose()

    query = HistoryQueryService(DatabaseManager(_settings(db_path)))
    try:
        assert query.sensors_by_identity("01260601")[0]["sn_code"] == "01260601"
        assert query.sensors_by_identity("047")[0]["sn_code"] == "01260601"
    finally:
        query.database.dispose()


def test_v1_5_initialization_import_apply_is_idempotent(tmp_path: Path) -> None:
    bundle_path = _bundle_path(tmp_path)
    db_path = tmp_path / "storage.sqlite"

    result = run_import(
        bundle=bundle_path,
        settings=_settings(db_path),
        init_schema=True,
        apply=True,
        acknowledge_formal_db_write=True,
        operator="qa",
    )
    assert result["database_written"] is True
    assert result["imported"]["sensors"] == 2
    assert result["imported"]["device_events"] == 2

    rerun = run_import(
        bundle=bundle_path,
        settings=_settings(db_path),
        apply=True,
        acknowledge_formal_db_write=True,
        operator="qa",
    )
    assert rerun["imported"]["sensors"] == 2

    database = DatabaseManager(_settings(db_path))
    try:
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
            assert sensor.metadata_json["formal_device_key"] == "gas_analyzer:sn:01260601"
            assert sensor.metadata_json["protocol_device_id_current"] == "047"

            by_device_code = session.execute(
                select(SensorRecord).where(SensorRecord.device_code == "01260602")
            ).scalar_one()
            assert by_device_code.analyzer_id == "01260602"

            protocol_alias = session.execute(
                select(SensorIdentityAliasRecord).where(
                    SensorIdentityAliasRecord.alias_type == "protocol_device_id_current",
                    SensorIdentityAliasRecord.alias_value == "047",
                )
            ).scalar_one()
            assert str(protocol_alias.sensor_id) == str(sensor.sensor_id)

            run_record = session.get(RunRecord, stable_uuid("run", "v1_5_formal_initialization_test_2ch"))
            assert run_record is not None
            notes = json.loads(run_record.notes or "{}")
            assert notes["v1_5_initialization"]["validation"]["status"] == "ready"
            assert len(notes["v1_5_initialization"]["run_devices"]) == 2
    finally:
        database.dispose()

    query = HistoryQueryService(DatabaseManager(_settings(db_path)))
    try:
        by_sn = query.sensors_by_identity("01260601")
        assert len(by_sn) == 1
        assert by_sn[0]["sn_code"] == "01260601"
        assert by_sn[0]["device_code"] == "01260601"

        by_protocol_id = query.sensors_by_identity("047")
        assert len(by_protocol_id) == 1
        assert by_protocol_id[0]["sn_code"] == "01260601"
    finally:
        query.database.dispose()


def test_v1_5_initialization_import_apply_requires_acknowledgement(tmp_path: Path) -> None:
    bundle_path = _bundle_path(tmp_path)

    with pytest.raises(PermissionError):
        run_import(
            bundle=bundle_path,
            settings=_settings(tmp_path / "storage.sqlite"),
            apply=True,
            acknowledge_formal_db_write=False,
        )


def test_v1_5_initialization_import_rejects_uninitialized_or_duplicate_sn(tmp_path: Path) -> None:
    bundle_path = _bundle_path(tmp_path)
    payload = json.loads(bundle_path.read_text(encoding="utf-8"))
    payload["devices"][0]["sn_code"] = "00000000"
    payload["devices"][0]["device_code"] = "00000000"
    payload["run_devices"][0]["sn_code"] = "00000000"
    payload["run_devices"][0]["device_code"] = "00000000"
    bad_path = tmp_path / "bad_sn_bundle.json"
    bad_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    with pytest.raises(ValueError, match="00000000"):
        run_import(bundle=bad_path)

    duplicate_payload = json.loads(bundle_path.read_text(encoding="utf-8"))
    duplicate_payload["devices"][1]["sn_code"] = "01260601"
    duplicate_payload["devices"][1]["device_code"] = "01260601"
    duplicate_payload["run_devices"][1]["sn_code"] = "01260601"
    duplicate_payload["run_devices"][1]["device_code"] = "01260601"
    duplicate_path = tmp_path / "duplicate_sn_bundle.json"
    duplicate_path.write_text(json.dumps(duplicate_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    with pytest.raises(ValueError, match="duplicate sn_code"):
        run_import(bundle=duplicate_path)
