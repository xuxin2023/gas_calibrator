import csv
import json

from gas_calibrator.tools.export_v1_5_getco_identity_readiness import main as export_main
from gas_calibrator.validation.v1_5_getco_identity_readiness import (
    REQUIRED_GETCO_GROUPS,
    build_getco_identity_readiness_model,
    write_getco_identity_readiness_outputs,
)


def _write_csv(path, rows):
    keys = []
    for row in rows:
        for key in row:
            if key not in keys:
                keys.append(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)


def _write_ready_getco_artifacts(getco_dir, *, device_ids=("001", "004")):
    getco_dir.mkdir(parents=True, exist_ok=True)
    groups = ",".join(str(group) for group in REQUIRED_GETCO_GROUPS)
    identity_rows = []
    snapshot = {}
    analyzers = []
    for index, device_id in enumerate(device_ids, start=1):
        port = f"COM{34 + index}"
        sn_code = f"012606{index:02d}"
        identity_rows.append(
            {
                "analyzer_name": f"ga{index:02d}",
                "configured_device_id": device_id,
                "analyzer_device_id": device_id,
                "runtime_device_id": device_id,
                "sn_code": sn_code,
                "device_code": sn_code,
                "runtime_identity_rebound": "False",
                "port": port,
                "requested_groups": groups,
                "found_groups": groups,
                "all_groups_found": "True",
                "identity_verified": "True",
                "writes_senco": "False",
                "writes_device_id": "False",
                "controls_water_or_gas_routes": "False",
                "controls_pace": "False",
            }
        )
        snapshot[device_id] = {
            "analyzer_prefix": f"ga{index:02d}",
            "analyzer_device_id": device_id,
            "configured_device_id": device_id,
            "runtime_device_id": device_id,
            "port": port,
            "source": "read_only_getco_component_snapshot",
            "sn_code": sn_code,
            "device_code": sn_code,
        }
        for group in REQUIRED_GETCO_GROUPS:
            snapshot[device_id][f"GETCO{group}_before"] = [float(group), 0.0, 0.0, 0.0]
            snapshot[device_id][f"GETCO{group}_before_command"] = f"GETCO,YGAS,FFF,{group}"
        analyzers.append(
            {
                "name": f"ga{index:02d}",
                "port": port,
                "device_id": device_id,
                "runtime_device_id": device_id,
                "sn_code": sn_code,
                "device_code": sn_code,
                "runtime_identity_bound": True,
                "identity_binding_source": "v1_5_getco_component_snapshot",
                "identity_binding_frozen": True,
            }
        )
    _write_csv(getco_dir / "getco_component_snapshot_identity.csv", identity_rows)
    _write_csv(
        getco_dir / "getco_component_snapshot_conclusion.csv",
        [
            {
                "status": "pass",
                "reason": "",
                "analyzer_count": str(len(device_ids)),
                "groups": groups,
                "all_devices_bound": "True",
                "all_identity_verified": "True",
                "writes_senco": "False",
                "writes_device_id": "False",
                "controls_water_or_gas_routes": "False",
                "controls_pace": "False",
            }
        ],
    )
    (getco_dir / "old_component_coefficients_snapshot.json").write_text(
        json.dumps(snapshot, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (getco_dir / "runtime_identity_bound_config.json").write_text(
        json.dumps(
            {
                "devices": {"gas_analyzers": analyzers},
                "workflow": {
                    "analyzer_mode2_init": {
                        "command_gap_s": 1.0,
                        "fragile_serial_contract": "minimum_1s_command_gap",
                    }
                },
                "v1_5_identity_binding": {
                    "frozen_for_run": True,
                    "writes_device_id": False,
                    "configured_ids_preserved": True,
                    "analyzer_command_gap_s": 1.0,
                    "identity_rows": identity_rows,
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def test_getco_identity_readiness_accepts_complete_read_only_epoch0_evidence(tmp_path):
    getco_dir = tmp_path / "coefficient_epoch_0_getco_snapshot"
    _write_ready_getco_artifacts(getco_dir)

    model = build_getco_identity_readiness_model(getco_dir=getco_dir)
    outputs = write_getco_identity_readiness_outputs(model, tmp_path / "out")

    assert model["overall_status"] == "identity_getco_ready_for_auxiliary_neutralization"
    assert model["active_analyzer_count"] == 2
    assert model["opens_com_ports"] is False
    assert model["controls_water_or_gas_routes"] is False
    assert model["writes_coefficients"] is False
    assert model["writes_device_id"] is False
    assert model["traceability_review_required"] is False
    assert {row["status"] for row in model["checks"]} == {"ready"}
    sn_check = next(row for row in model["checks"] if row["check"] == "sn_device_code_traceability_preserved")
    assert [item["sn_code"] for item in sn_check["details"]["runtime_identity_traces"]] == [
        "01260601",
        "01260602",
    ]
    assert outputs["json"].exists()
    assert outputs["checks_csv"].exists()
    assert "GETCO" in outputs["markdown"].read_text(encoding="utf-8")


def test_getco_identity_readiness_marks_missing_live_snapshot_as_pending(tmp_path):
    model = build_getco_identity_readiness_model(getco_dir=tmp_path / "missing")

    assert model["overall_status"] == "identity_getco_pending_live_gate"
    missing = next(row for row in model["checks"] if row["check"] == "required_artifacts_present")
    assert missing["status"] == "pending_live_gate"
    assert "snapshot_json" in missing["reasons"]
    assert model["not_real_acceptance_evidence"] is True


def test_getco_identity_readiness_blocks_incomplete_groups_and_unsafe_conclusion(tmp_path):
    getco_dir = tmp_path / "coefficient_epoch_0_getco_snapshot"
    _write_ready_getco_artifacts(getco_dir, device_ids=("001",))
    snapshot = json.loads((getco_dir / "old_component_coefficients_snapshot.json").read_text(encoding="utf-8"))
    del snapshot["001"]["GETCO9_before"]
    (getco_dir / "old_component_coefficients_snapshot.json").write_text(
        json.dumps(snapshot, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    rows = list(csv.DictReader((getco_dir / "getco_component_snapshot_conclusion.csv").open("r", encoding="utf-8-sig")))
    rows[0]["writes_senco"] = "True"
    _write_csv(getco_dir / "getco_component_snapshot_conclusion.csv", rows)
    config = json.loads((getco_dir / "runtime_identity_bound_config.json").read_text(encoding="utf-8"))
    config["workflow"]["analyzer_mode2_init"]["command_gap_s"] = 0.5
    (getco_dir / "runtime_identity_bound_config.json").write_text(json.dumps(config), encoding="utf-8")

    model = build_getco_identity_readiness_model(getco_dir=getco_dir)

    assert model["overall_status"] == "identity_getco_blocked"
    reasons = {
        reason
        for row in model["checks"]
        for reason in row["reasons"]
    }
    assert "conclusion_writes_senco_must_be_false" in reasons
    assert "001_GETCO9_before_missing" in reasons
    assert "analyzer_mode2_init_command_gap_below_1s" in reasons


def test_getco_identity_readiness_keeps_missing_sn_traceability_as_review_only(tmp_path):
    getco_dir = tmp_path / "coefficient_epoch_0_getco_snapshot"
    _write_ready_getco_artifacts(getco_dir, device_ids=("001",))
    config = json.loads((getco_dir / "runtime_identity_bound_config.json").read_text(encoding="utf-8"))
    analyzer = config["devices"]["gas_analyzers"][0]
    analyzer.pop("sn_code")
    analyzer.pop("device_code")
    (getco_dir / "runtime_identity_bound_config.json").write_text(
        json.dumps(config, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    model = build_getco_identity_readiness_model(getco_dir=getco_dir)

    assert model["overall_status"] == "identity_getco_ready_for_auxiliary_neutralization"
    assert model["traceability_review_required"] is True
    sn_check = next(row for row in model["checks"] if row["check"] == "sn_device_code_traceability_preserved")
    assert sn_check["status"] == "review_required"
    assert "COM35_runtime_sn_code_missing" in sn_check["reasons"]
    assert "COM35_runtime_device_code_missing" in sn_check["reasons"]


def test_getco_identity_readiness_reviews_invalid_or_mismatched_sn_device_code(tmp_path):
    getco_dir = tmp_path / "coefficient_epoch_0_getco_snapshot"
    _write_ready_getco_artifacts(getco_dir, device_ids=("001",))
    config = json.loads((getco_dir / "runtime_identity_bound_config.json").read_text(encoding="utf-8"))
    analyzer = config["devices"]["gas_analyzers"][0]
    analyzer["sn_code"] = "SN260601"
    analyzer["device_code"] = "01260699"
    (getco_dir / "runtime_identity_bound_config.json").write_text(
        json.dumps(config, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    model = build_getco_identity_readiness_model(getco_dir=getco_dir)

    assert model["overall_status"] == "identity_getco_ready_for_auxiliary_neutralization"
    assert model["traceability_review_required"] is True
    sn_check = next(row for row in model["checks"] if row["check"] == "sn_device_code_traceability_preserved")
    assert sn_check["status"] == "review_required"
    assert "COM35_runtime_sn_code_must_be_8_digit_numeric" in sn_check["reasons"]
    assert "COM35_runtime_sn_code_device_code_mismatch" in sn_check["reasons"]
    assert "COM35_device_code_not_preserved_from_identity_snapshot" in sn_check["reasons"]


def test_export_getco_identity_readiness_cli_can_fail_on_not_ready(tmp_path):
    out = tmp_path / "out"

    rc = export_main(
        [
            "--getco-dir",
            str(tmp_path / "missing_getco"),
            "--output-dir",
            str(out),
            "--fail-on-not-ready",
        ]
    )

    assert rc == 2
    payload = json.loads((out / "v1_5_getco_identity_readiness.json").read_text(encoding="utf-8"))
    assert payload["overall_status"] == "identity_getco_pending_live_gate"
