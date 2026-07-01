import csv
import json

from gas_calibrator.tools.export_v1_5_pre_gas_readiness import main as pre_gas_cli
from gas_calibrator.validation.v1_5_pre_gas_readiness import (
    build_pre_gas_readiness_model,
    write_pre_gas_readiness_outputs,
)


def _contract():
    return {
        "identity": {
            "primary_key": "sn_code/device_code",
            "compatibility_alias": "protocol_device_id",
            "transport_not_identity": "COM/GA labels are transport mapping only",
        },
        "database": {
            "backend": "postgresql",
            "required_major": 18,
            "preflight_mutates_database": False,
        },
        "runtime": {
            "mode": 2,
            "ftd_hz": 1,
            "minimum_command_gap_s": 1.0,
        },
        "temperature": {
            "senco7_senco8_policy": "neutralize_in_initialization_for_classic_and_new_algorithm",
            "temperature_calibration": "disabled",
        },
        "check_monitor": {
            "command": "CHECK,YGAS,FFF",
            "minimum_command_gap_s": 1.0,
            "read_only": True,
        },
    }


def _write_inputs(tmp_path):
    run_dir = tmp_path / "flow"
    init_dir = run_dir / "formal_initialization"
    init_dir.mkdir(parents=True)
    readiness = {
        "readiness_status": "initialization_ready_with_warnings",
        "expected_device_ids": ["001", "004"],
        "initialization_contract": _contract(),
        "checks": [
            {
                "check": "getco1_to_getco9_epoch0_snapshot",
                "status": "fail",
                "reasons": "old_component_coefficients_snapshot.json_missing",
                "path": "",
                "details": {},
            },
            {
                "check": "senco78_neutralization_evidence",
                "status": "pass",
                "reasons": "",
                "path": str(init_dir / "senco78_neutral_write_events.csv"),
                "details": {"passed_device_ids": ["001", "004"]},
            },
            {
                "check": "formal_route_readiness_evidence",
                "status": "pass",
                "reasons": "",
                "path": str(init_dir / "formal_route_readiness.json"),
                "details": {"n2_prepurge_enabled": True},
            },
        ],
    }
    readiness_path = init_dir / "v1_5_initialization_readiness.json"
    readiness_path.write_text(json.dumps(readiness, ensure_ascii=False, indent=2), encoding="utf-8")
    sidecar_path = init_dir / "v1_5_initialization_database_sidecar.json"
    sidecar_path.write_text(json.dumps({"sidecar_only": True}, ensure_ascii=False), encoding="utf-8")
    config_path = tmp_path / "runtime.json"
    config_path.write_text("{}", encoding="utf-8")
    return run_dir, init_dir, readiness_path, sidecar_path, config_path


def test_pre_gas_readiness_keeps_live_gates_pending_without_touching_hardware(tmp_path):
    run_dir, init_dir, readiness_path, sidecar_path, config_path = _write_inputs(tmp_path)

    model = build_pre_gas_readiness_model(
        run_dir=run_dir,
        initialization_dir=init_dir,
        config_path=config_path,
        initialization_readiness_json=readiness_path,
        database_sidecar_json=sidecar_path,
    )

    assert model["schema"] == "v1_5_pre_gas_readiness_v1"
    assert model["overall_status"] == "ready_for_identity_gate_with_later_live_gates"
    assert model["opens_com_ports"] is False
    assert model["connects_postgresql"] is False
    assert model["controls_water_or_gas_routes"] is False
    assert model["writes_coefficients"] is False
    assert model["next_live_gate"] == "device_identity_and_getco_snapshot"
    assert "pressure_senco9_pre_open_flow_gate" in model["required_before_open_flow"]
    assert model["required_before_point_sampling_after_chamber_temperature_stable"] == [
        "check_monitor_after_chamber_temperature_stable_gate"
    ]
    checks = {row["check"]: row for row in model["checks"]}
    assert checks["postgresql18_initialization_db_preflight_contract"]["status"] == "ready"
    assert checks["sn_device_code_identity_contract"]["status"] == "ready"
    assert checks["mode2_1hz_senco78_neutral_check_contract"]["status"] == "ready"
    assert checks["getco_epoch0_snapshot_gate"]["status"] == "pending_live_gate"
    assert checks["pressure_senco9_pre_open_flow_gate"]["status"] == "pending_live_gate"
    assert checks["formal_route_readiness_before_chamber_soak_gate"]["status"] == "ready"
    assert checks["check_monitor_after_chamber_temperature_stable_gate"]["status"] == "pending_point_gate"
    assert checks["mature_co2_h2o_queue_boundary"]["details"]["touches_formal_co2_queue"] is False


def test_pre_gas_readiness_outputs_json_markdown_and_checks_csv(tmp_path):
    run_dir, init_dir, readiness_path, sidecar_path, config_path = _write_inputs(tmp_path)
    model = build_pre_gas_readiness_model(
        run_dir=run_dir,
        initialization_dir=init_dir,
        config_path=config_path,
        initialization_readiness_json=readiness_path,
        database_sidecar_json=sidecar_path,
    )

    outputs = write_pre_gas_readiness_outputs(model, tmp_path / "out")

    assert outputs["json"].exists()
    assert outputs["markdown"].exists()
    assert outputs["checks_csv"].exists()
    text = outputs["markdown"].read_text(encoding="utf-8")
    assert "CHECK,YGAS,FFF" in text
    assert "PostgreSQL" in text or "postgresql18" in text
    with outputs["checks_csv"].open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert {row["check"] for row in rows} >= {
        "postgresql18_initialization_db_preflight_contract",
        "pressure_senco9_pre_open_flow_gate",
        "check_monitor_after_chamber_temperature_stable_gate",
    }


def test_pre_gas_cli_writes_sidecar_and_does_not_fail_pending_live_gates(tmp_path):
    run_dir, init_dir, readiness_path, sidecar_path, config_path = _write_inputs(tmp_path)
    out = tmp_path / "cli_out"

    rc = pre_gas_cli(
        [
            "--run-dir",
            str(run_dir),
            "--initialization-dir",
            str(init_dir),
            "--config",
            str(config_path),
            "--initialization-readiness-json",
            str(readiness_path),
            "--database-sidecar-json",
            str(sidecar_path),
            "--output-dir",
            str(out),
            "--fail-on-review-required",
        ]
    )

    assert rc == 0
    payload = json.loads((out / "v1_5_pre_gas_readiness.json").read_text(encoding="utf-8"))
    assert payload["overall_status"] == "ready_for_identity_gate_with_later_live_gates"
    assert payload["not_real_acceptance_evidence"] is True
