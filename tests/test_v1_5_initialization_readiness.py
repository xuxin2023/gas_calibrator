import csv
import json

from gas_calibrator.tools.export_v1_5_initialization_readiness import main as readiness_cli
from gas_calibrator.validation.v1_5_initialization_readiness import (
    build_initialization_database_sidecar,
    build_initialization_evidence_index_rows,
    build_initialization_readiness_model,
    write_initialization_readiness_report,
)


def _config(*, unsafe=False):
    return {
        "metadata": {
            "no_write": True,
            "writes_senco": bool(unsafe),
            "writes_device_id": False,
            "startup_allowed_analyzer_commands": ["SETCOMWAY", "MODE", "AVERAGE", "FTD"],
            "startup_forbidden_analyzer_commands": [
                "ID",
                "SENCO",
                "CLEARSENCO",
                "SETPOW",
                "SETILLUM",
                "SETCO2",
            ],
        },
        "workflow": {
            "controlled_write": False,
            "startup_pressure_sensor_calibration": {"enabled": False, "apply_write": False},
            "analyzer_mode2_init": {
                "read_first_before_config": True,
                "sniff_stream_before_config": True,
                "send_active_freq": True,
                "command_gap_s": 1.2,
                "reapply_delay_s": 1.2,
            },
        },
        "devices": {
            "pressure_controller": {"enabled": True, "present": True, "port": "COM23"},
            "pressure_gauge": {"enabled": True, "present": True, "port": "COM22"},
            "gas_analyzer": {"name": "GA01", "port": "COM35", "device_id": "023", "enabled": True},
            "gas_analyzers": [
                {"name": "GA01", "port": "COM35", "device_id": "023", "enabled": True},
                {"name": "GA02", "port": "COM36", "device_id": "003", "enabled": True},
            ]
        },
    }


def _write_config(path, *, unsafe=False):
    path.write_text(json.dumps(_config(unsafe=unsafe), ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _write_getco_snapshot(path):
    path.mkdir(parents=True)
    payload = {}
    for device_id, port in (("023", "COM35"), ("003", "COM36")):
        row = {
            "analyzer_prefix": "GA",
            "analyzer_device_id": device_id,
            "configured_device_id": device_id,
            "runtime_device_id": device_id,
            "port": port,
            "source": "read_only_getco_component_snapshot",
        }
        for group in range(1, 10):
            row[f"GETCO{group}_before"] = [0.0, 1.0, 0.0, 0.0]
            row[f"GETCO{group}_before_parsed"] = {"c0": 0.0, "c1": 1.0}
            row[f"GETCO{group}_before_command"] = f"GETCO,YGAS,FFF,{group}"
        payload[device_id] = row
    (path / "old_component_coefficients_snapshot.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return path


def _write_aux_csv(path, file_name):
    with (path / file_name).open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["device_id", "status", "readback_verified"])
        writer.writeheader()
        writer.writerow({"device_id": "023", "status": "success", "readback_verified": "true"})
        writer.writerow({"device_id": "003", "status": "already_neutral", "readback_verified": "true"})


def _write_aux_neutralization(path, *, include_senco78=True):
    path.mkdir(parents=True)
    file_names = [
        "senco5_neutral_write_events.csv",
        "senco6_neutral_write_events.csv",
        "senco9_clear_write_events.csv",
    ]
    if include_senco78:
        file_names.insert(2, "senco78_neutral_write_events.csv")
    for file_name in file_names:
        _write_aux_csv(path, file_name)
    return path


def _write_temperature_review(path, *, status="reference_equivalence_required"):
    review_dir = path / "temperature_current_point_review"
    review_dir.mkdir(parents=True)
    with (review_dir / "temperature_current_point_review.csv").open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "device_id",
                "senco_group",
                "status",
                "reason",
                "current_analyzer_temp_c",
                "reference_temp_c",
            ],
        )
        writer.writeheader()
        for device_id in ("023", "003"):
            for group in ("SENCO7", "SENCO8"):
                writer.writerow(
                    {
                        "device_id": device_id,
                        "senco_group": group,
                        "status": status,
                        "reason": (
                            "temperature_reference_not_equivalent_to_analyzer_thermal_state"
                            if status == "reference_equivalence_required"
                            else "current_temperature_hard_bad_value"
                        ),
                        "current_analyzer_temp_c": "31.0" if status == "reference_equivalence_required" else "60.0",
                        "reference_temp_c": "27.0",
                    }
                )
    return review_dir


def _write_archive_confirmation(path, *, pressure_verified=True):
    archive_dir = path / "initialization_archive_confirmation_20260611"
    archive_dir.mkdir(parents=True)
    payload = {
        "schema": "v1_5_initialization_archive_confirmation_v1",
        "created_at": "2026-06-11T20:15:07",
        "run_dir": str(path),
        "source_snapshot_dir": str(path / "coefficient_epoch_0_getco_snapshot"),
        "device_count": 2,
        "device_ids": ["023", "003"],
        "conclusion_status": "pass",
        "all_identity_verified": True,
        "all_getco1_to_9_complete": True,
        "archive_decision": "initialization_not_repeated_current_batch",
        "device_rows": [
            {
                "runtime_device_id": "023",
                "port": "COM35",
                "identity_verified": True,
                "getco1_to_9_complete": True,
                "s5_epoch0": [0.0, 1.0],
                "s6_epoch0": [0.0, 1.0],
                "s7_epoch0": [0.0, 1.0, 0.0, 0.0],
                "s8_epoch0": [0.0, 1.0, 0.0, 0.0],
                "s9_epoch0": [0.0, 1.0, 0.0, 0.0],
            },
            {
                "runtime_device_id": "003",
                "port": "COM36",
                "identity_verified": True,
                "getco1_to_9_complete": True,
                "s5_epoch0": [0.0, 1.0],
                "s6_epoch0": [-1.2, 1.0],
                "s7_epoch0": [0.0, 1.0, 0.0, 0.0],
                "s8_epoch0": [0.0, 1.0, 0.0, 0.0],
                "s9_epoch0": [-8.0, 1.0, 0.0, 0.0],
            },
        ],
        "pressure_channel_status": (
            "senco9_written_and_post_write_verified"
            if pressure_verified
            else "senco9_write_pending"
        ),
        "pressure_channel_device_ids": ["023", "003"] if pressure_verified else ["023"],
    }
    path_obj = archive_dir / "v1_5_initialization_archive_confirmation.json"
    path_obj.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path_obj


def _make_ready_inputs(tmp_path, *, unsafe=False, aux=True):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    config_path = _write_config(tmp_path / "runtime.json", unsafe=unsafe)
    getco_dir = _write_getco_snapshot(run_dir / "coefficient_epoch_0_getco_snapshot")
    aux_dir = _write_aux_neutralization(run_dir / "auxiliary_senco56789_neutralization") if aux else None
    return run_dir, config_path, getco_dir, aux_dir


def test_initialization_ready_when_getco_and_auxiliary_evidence_exist(tmp_path):
    run_dir, config_path, getco_dir, aux_dir = _make_ready_inputs(tmp_path)

    model = build_initialization_readiness_model(
        run_dir=run_dir,
        config_path=config_path,
        getco_snapshot_dir=getco_dir,
        aux_neutralization_dir=aux_dir,
    )

    assert model["readiness_status"] == "initialization_ready"
    assert model["opens_com_ports"] is False
    assert model["writes_coefficients"] is False
    assert model["expected_device_ids"] == ["023", "003"]
    getco_check = next(row for row in model["checks"] if row["check"] == "getco1_to_getco9_epoch0_snapshot")
    assert getco_check["details"]["device_count"] == 2


def test_initialization_blocks_when_auxiliary_evidence_is_missing(tmp_path):
    run_dir, config_path, getco_dir, _ = _make_ready_inputs(tmp_path, aux=False)

    model = build_initialization_readiness_model(
        run_dir=run_dir,
        config_path=config_path,
        getco_snapshot_dir=getco_dir,
    )

    assert model["readiness_status"] == "initialization_blocked"
    senco5 = next(row for row in model["checks"] if row["check"] == "senco5_neutralization_evidence")
    assert senco5["status"] == "fail"
    assert "senco5_neutral_write_events.csv_missing" in senco5["reasons"]


def test_continuation_recovery_marks_missing_auxiliary_evidence_as_review_required(tmp_path):
    run_dir, config_path, getco_dir, _ = _make_ready_inputs(tmp_path, aux=False)

    model = build_initialization_readiness_model(
        run_dir=run_dir,
        config_path=config_path,
        getco_snapshot_dir=getco_dir,
        continuation_recovery=True,
    )

    assert model["readiness_status"] == "continuation_requires_review"
    assert any(row["status"] == "warning" for row in model["checks"])


def test_temperature_reference_equivalence_warning_does_not_require_senco78_neutralization(tmp_path):
    run_dir, config_path, getco_dir, _ = _make_ready_inputs(tmp_path, aux=False)
    aux_dir = _write_aux_neutralization(run_dir / "auxiliary_senco56789_neutralization", include_senco78=False)
    _write_temperature_review(aux_dir, status="reference_equivalence_required")

    model = build_initialization_readiness_model(
        run_dir=run_dir,
        config_path=config_path,
        getco_snapshot_dir=getco_dir,
        aux_neutralization_dir=aux_dir,
    )

    assert model["readiness_status"] == "initialization_ready_with_warnings"
    checks = {row["check"]: row for row in model["checks"]}
    assert "senco78_neutralization_evidence" not in checks
    temp_check = checks["senco78_temperature_current_point_review_evidence"]
    assert temp_check["status"] == "warning"
    assert "reference_equivalence_required" in temp_check["reasons"]
    assert any(
        action["action"] == "do_not_single_point_repair_temperature_reference_offset"
        for action in model["next_actions"]
    )


def test_temperature_repair_required_blocks_initialization_even_when_auxiliary_events_exist(tmp_path):
    run_dir, config_path, getco_dir, _ = _make_ready_inputs(tmp_path, aux=False)
    aux_dir = _write_aux_neutralization(run_dir / "auxiliary_senco56789_neutralization", include_senco78=False)
    _write_temperature_review(aux_dir, status="repair_required")

    model = build_initialization_readiness_model(
        run_dir=run_dir,
        config_path=config_path,
        getco_snapshot_dir=getco_dir,
        aux_neutralization_dir=aux_dir,
    )

    assert model["readiness_status"] == "initialization_blocked"
    temp_check = next(row for row in model["checks"] if row["check"] == "senco78_temperature_current_point_review_evidence")
    assert temp_check["status"] == "fail"
    assert "temperature_review_status=repair_required" in temp_check["reasons"]


def test_archive_confirmation_satisfies_s5_s6_s9_when_event_tables_are_absent(tmp_path):
    run_dir, config_path, getco_dir, _ = _make_ready_inputs(tmp_path, aux=False)
    aux_dir = run_dir / "auxiliary_senco56789_neutralization"
    _write_temperature_review(aux_dir, status="pass")
    _write_archive_confirmation(run_dir, pressure_verified=True)

    model = build_initialization_readiness_model(
        run_dir=run_dir,
        config_path=config_path,
        getco_snapshot_dir=getco_dir,
        aux_neutralization_dir=aux_dir,
    )

    assert model["readiness_status"] == "initialization_ready"
    checks = {row["check"]: row for row in model["checks"]}
    assert checks["senco5_archive_snapshot_evidence"]["status"] == "pass"
    assert checks["senco6_archive_snapshot_evidence"]["status"] == "pass"
    assert checks["senco9_archive_snapshot_evidence"]["status"] == "pass"
    assert "senco5_neutralization_evidence" not in checks
    assert checks["senco9_archive_snapshot_evidence"]["details"]["pressure_channel_status"] == (
        "senco9_written_and_post_write_verified"
    )


def test_archive_confirmation_blocks_s9_when_pressure_verification_is_not_complete(tmp_path):
    run_dir, config_path, getco_dir, _ = _make_ready_inputs(tmp_path, aux=False)
    aux_dir = run_dir / "auxiliary_senco56789_neutralization"
    _write_temperature_review(aux_dir, status="pass")
    _write_archive_confirmation(run_dir, pressure_verified=False)

    model = build_initialization_readiness_model(
        run_dir=run_dir,
        config_path=config_path,
        getco_snapshot_dir=getco_dir,
        aux_neutralization_dir=aux_dir,
    )

    assert model["readiness_status"] == "initialization_blocked"
    s9_check = next(row for row in model["checks"] if row["check"] == "senco9_archive_snapshot_evidence")
    assert s9_check["status"] == "fail"
    assert "pressure_channel_status=senco9_write_pending" in s9_check["reasons"]
    assert "pressure_channel_device_ids_missing=003" in s9_check["reasons"]


def test_initialization_blocks_when_runtime_config_allows_writes(tmp_path):
    run_dir, config_path, getco_dir, aux_dir = _make_ready_inputs(tmp_path, unsafe=True)

    model = build_initialization_readiness_model(
        run_dir=run_dir,
        config_path=config_path,
        getco_snapshot_dir=getco_dir,
        aux_neutralization_dir=aux_dir,
    )

    assert model["readiness_status"] == "initialization_blocked"
    config_check = next(row for row in model["checks"] if row["check"] == "initialization_runtime_config")
    assert "metadata_writes_senco_true" in config_check["reasons"]


def test_initialization_blocks_when_analyzer_command_gap_is_too_short(tmp_path):
    run_dir, config_path, getco_dir, aux_dir = _make_ready_inputs(tmp_path)
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    payload["workflow"]["analyzer_mode2_init"]["command_gap_s"] = 0.35
    payload["workflow"]["analyzer_mode2_init"]["reapply_delay_s"] = 0.75
    config_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    model = build_initialization_readiness_model(
        run_dir=run_dir,
        config_path=config_path,
        getco_snapshot_dir=getco_dir,
        aux_neutralization_dir=aux_dir,
    )

    assert model["readiness_status"] == "initialization_blocked"
    config_check = next(row for row in model["checks"] if row["check"] == "initialization_runtime_config")
    assert "analyzer_mode2_init_command_gap_too_short=0.35s" in config_check["reasons"]
    assert "analyzer_mode2_init_reapply_delay_too_short=0.75s" in config_check["reasons"]


def test_initialization_readiness_blocks_when_pressure_hardware_is_declared_missing(tmp_path):
    run_dir, config_path, getco_dir, aux_dir = _make_ready_inputs(tmp_path)

    model = build_initialization_readiness_model(
        run_dir=run_dir,
        config_path=config_path,
        getco_snapshot_dir=getco_dir,
        aux_neutralization_dir=aux_dir,
        pressure_hardware_missing=True,
    )

    assert model["readiness_status"] == "pressure_hardware_blocked"
    assert model["pressure_hardware_missing"] is True
    hardware_check = next(row for row in model["checks"] if row["check"] == "pressure_hardware_presence")
    assert hardware_check["status"] == "blocked"
    assert "operator_declared_pressure_controller_or_gauge_missing" in hardware_check["reasons"]


def test_initialization_readiness_blocks_when_config_marks_pressure_hardware_absent(tmp_path):
    run_dir, config_path, getco_dir, aux_dir = _make_ready_inputs(tmp_path)
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    payload["devices"]["pressure_gauge"]["present"] = False
    config_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    model = build_initialization_readiness_model(
        run_dir=run_dir,
        config_path=config_path,
        getco_snapshot_dir=getco_dir,
        aux_neutralization_dir=aux_dir,
    )

    assert model["readiness_status"] == "pressure_hardware_blocked"
    hardware_check = next(row for row in model["checks"] if row["check"] == "pressure_hardware_presence")
    assert hardware_check["status"] == "blocked"
    assert "pressure_gauge_present_false" in hardware_check["reasons"]


def test_initialization_readiness_writer_and_cli(tmp_path):
    run_dir, config_path, getco_dir, aux_dir = _make_ready_inputs(tmp_path)
    outputs = write_initialization_readiness_report(
        run_dir=run_dir,
        config_path=config_path,
        getco_snapshot_dir=getco_dir,
        aux_neutralization_dir=aux_dir,
        output_dir=tmp_path / "out",
    )

    assert outputs["json"].exists()
    assert outputs["markdown"].exists()
    assert outputs["evidence_index_csv"].exists()
    assert outputs["database_sidecar_json"].exists()
    assert "initialization_ready" in outputs["markdown"].read_text(encoding="utf-8")
    with outputs["evidence_index_csv"].open("r", encoding="utf-8-sig", newline="") as handle:
        index_rows = list(csv.DictReader(handle))
    roles = {row["artifact_role"] for row in index_rows}
    assert {
        "initialization_readiness_model",
        "initialization_readiness_report",
        "initialization_database_sidecar",
        "epoch0_getco_snapshot",
        "auxiliary_coefficient_neutralization",
    } <= roles
    assert all(row["sha256"] for row in index_rows)
    sidecar = json.loads(outputs["database_sidecar_json"].read_text(encoding="utf-8"))
    assert sidecar["schema"] == "v1_5_initialization_readiness_database_sidecar_v1"
    assert sidecar["sidecar_only"] is True
    assert sidecar["opens_com_ports"] is False
    assert sidecar["writes_coefficients"] is False
    assert any(row["db_table"] == "sample_files" for row in sidecar["suggested_rows"])
    assert any(row["db_table"] == "qc_results" for row in sidecar["suggested_rows"])

    rc = readiness_cli(
        [
            "--run-dir",
            str(run_dir),
            "--config",
            str(config_path),
            "--getco-snapshot-dir",
            str(getco_dir),
            "--aux-neutralization-dir",
            str(aux_dir),
            "--output-dir",
            str(tmp_path / "cli_out"),
            "--pressure-hardware-missing",
        ]
    )
    assert rc == 0
    model = json.loads((tmp_path / "cli_out" / "v1_5_initialization_readiness.json").read_text(encoding="utf-8"))
    assert model["readiness_status"] == "pressure_hardware_blocked"


def test_initialization_evidence_index_and_sidecar_include_archive_confirmation(tmp_path):
    run_dir, config_path, getco_dir, _ = _make_ready_inputs(tmp_path, aux=False)
    aux_dir = run_dir / "auxiliary_senco56789_neutralization"
    _write_temperature_review(aux_dir, status="pass")
    archive_path = _write_archive_confirmation(run_dir, pressure_verified=True)

    model = build_initialization_readiness_model(
        run_dir=run_dir,
        config_path=config_path,
        getco_snapshot_dir=getco_dir,
        aux_neutralization_dir=aux_dir,
    )
    readiness_json = tmp_path / "out" / "v1_5_initialization_readiness.json"
    readiness_json.parent.mkdir()
    readiness_json.write_text(json.dumps(model, ensure_ascii=False), encoding="utf-8")
    rows = build_initialization_evidence_index_rows(model, generated_paths={"json": readiness_json})

    archive_rows = [row for row in rows if row["path"] == str(archive_path.resolve())]
    assert {row["source_check"] for row in archive_rows} == {
        "senco5_archive_snapshot_evidence",
        "senco6_archive_snapshot_evidence",
        "senco9_archive_snapshot_evidence",
    }
    assert all(row["sha256"] for row in archive_rows)

    sidecar = build_initialization_database_sidecar(model, rows)
    sample_rows = [row for row in sidecar["suggested_rows"] if row["db_table"] == "sample_files"]
    assert any(row["artifact_role"] == "initialization_archive_confirmation" for row in sample_rows)
    qc_rows = [row for row in sidecar["suggested_rows"] if row["db_table"] == "qc_results"]
    assert any(
        row["metadata_json"]["rule_name"] == "senco9_archive_snapshot_evidence"
        and row["metadata_json"]["status"] == "pass"
        for row in qc_rows
    )
