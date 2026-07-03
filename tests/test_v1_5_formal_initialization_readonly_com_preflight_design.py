import csv
import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_formal_initialization_readonly_com_preflight_design import (
    main as cli_main,
)
from gas_calibrator.validation.v1_5_formal_initialization_readonly_com_preflight_design import (
    build_v1_5_formal_initialization_readonly_com_preflight_design,
    write_v1_5_formal_initialization_readonly_com_preflight_design,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _controlled_design_json(tmp_path: Path) -> Path:
    return _write_json(
        tmp_path
        / "controlled_design"
        / "v1_5_formal_initialization_controlled_executor_design.json",
        {
            "schema": "v1_5_formal_initialization_controlled_executor_design_v1",
            "overall_status": "ready_for_controlled_initialization_executor_design_review",
            "blocker_count": 0,
            "review_required_count": 0,
            "production_state": "blocked_design_only",
            "execution_supported": False,
            "live_execution_allowed": False,
            "read_only_real_com_execution_allowed": False,
            "controlled_write_execution_allowed": False,
            "real_com_execution_allowed": False,
            "execute_flag_allowed": False,
            "opens_com_ports": False,
            "connects_postgresql": False,
            "controls_water_or_gas_routes": False,
            "controls_pressure": False,
            "writes_sn": False,
            "writes_device_id": False,
            "writes_coefficients": False,
            "database_written": False,
            "formal_release_allowed": False,
            "database_import_allowed": False,
            "not_real_acceptance_evidence": True,
            "minimum_serial_command_gap_s": 1.0,
        },
    )


def test_readonly_com_preflight_design_is_offline_and_locked(tmp_path: Path) -> None:
    tables = build_v1_5_formal_initialization_readonly_com_preflight_design(
        formal_initialization_controlled_executor_design_json=_controlled_design_json(tmp_path),
    )
    manifest = tables["manifest"]
    gates = {row["gate"]: row for row in tables["boundary_gates"]}

    assert manifest["schema"] == "v1_5_formal_initialization_readonly_com_preflight_design_v1"
    assert manifest["overall_status"] == "ready_for_readonly_real_com_preflight_design_review"
    assert manifest["production_state"] == "blocked_design_only"
    assert manifest["execution_supported"] is False
    assert manifest["live_execution_allowed"] is False
    assert manifest["read_only_real_com_execution_allowed"] is False
    assert manifest["controlled_write_execution_allowed"] is False
    assert manifest["opens_com_ports"] is False
    assert manifest["writes_sn"] is False
    assert manifest["writes_device_id"] is False
    assert manifest["writes_coefficients"] is False
    assert manifest["connects_postgresql"] is False
    assert manifest["database_written"] is False
    assert manifest["minimum_serial_command_gap_s"] == 1.0
    assert manifest["supported_active_analyzer_count"] == "1_to_6"
    assert manifest["required_future_read_only_real_com_flag"] == "--execute-read-only-real-com"
    assert manifest["required_future_controlled_write_flag_excluded"] == "--execute-controlled-writes"
    assert gates["design_only_no_com"]["status"] == "pass"
    assert gates["controlled_design_consumed"]["status"] == "pass"
    assert gates["future_real_com_still_locked"]["status"] == "pass"
    assert gates["no_writes"]["status"] == "pass"


def test_readonly_com_preflight_design_defines_physical_read_contracts(tmp_path: Path) -> None:
    tables = build_v1_5_formal_initialization_readonly_com_preflight_design(
        formal_initialization_controlled_executor_design_json=_controlled_design_json(tmp_path),
    )
    auth = {row["gate"]: row for row in tables["authorization_contract"]}
    serial = {row["step"]: row for row in tables["serial_preflight_contract"]}
    identity = {row["read"]: row for row in tables["identity_read_contract"]}
    getco = {row["read"]: row for row in tables["getco_read_contract"]}
    check = {row["read"]: row for row in tables["check_read_contract"]}
    hold = {row["trigger"]: row for row in tables["failure_hold_contract"]}

    assert auth["explicit_read_only_real_com_flag"]["future_flag"] == "--execute-read-only-real-com"
    assert auth["controlled_writes_stay_locked"]["future_flag_excluded"] == "--execute-controlled-writes"
    assert auth["active_device_scope"]["future_scope"] == "1_to_6_active_analyzers"
    assert serial["serial_command_spacing"]["physical_meaning"].endswith("at least 1.0s spacing.")
    assert serial["bounded_timeout_and_retry"]["physical_meaning"].endswith("retry_gap_s >= 1.0.")
    assert identity["sn_code"]["command"] == "SN,YGAS,FFF"
    assert identity["sn_code"]["expected"] == "8 numeric digits"
    assert identity["device_code"]["expected"] == "device_code equals sn_code for production identity"
    assert getco["getco_epoch0"]["required_groups"].endswith("GETCO9")
    assert getco["getco_epoch0"]["failure_policy"].startswith("incomplete snapshot blocks")
    assert check["check_monitor"]["command"] == "CHECK,YGAS,FFF"
    assert check["check_monitor"]["applies_to"] == "CHECK-capable or new-algorithm analyzers only"
    assert check["legacy_algorithm_skip"]["command"] == "none"
    assert "skip is not a failure" in check["legacy_algorithm_skip"]["expected"]
    assert hold["identity_mismatch_or_duplicate_sn"]["hold_action"].startswith("hold all writes")
    assert hold["command_gap_violation"]["release_policy"].endswith(">=1.0s command spacing")


def test_readonly_com_preflight_design_writer_and_cli_create_artifacts(
    tmp_path: Path,
    capsys,
) -> None:
    controlled_design = _controlled_design_json(tmp_path)
    outputs = write_v1_5_formal_initialization_readonly_com_preflight_design(
        tmp_path / "design",
        formal_initialization_controlled_executor_design_json=controlled_design,
    )

    manifest = json.loads(Path(outputs["manifest"]).read_text(encoding="utf-8-sig"))
    assert manifest["not_real_acceptance_evidence"] is True
    assert Path(outputs["authorization_contract"]).exists()
    assert Path(outputs["serial_preflight_contract"]).exists()
    assert Path(outputs["identity_read_contract"]).exists()
    assert Path(outputs["getco_read_contract"]).exists()
    assert Path(outputs["check_read_contract"]).exists()
    assert Path(outputs["failure_hold_contract"]).exists()
    assert "does not implement read-only COM execution" in Path(outputs["summary"]).read_text(
        encoding="utf-8"
    )
    assert _read_csv(Path(outputs["boundary_gates"]))[0]["gate"] == "design_only_no_com"

    cli_out = tmp_path / "cli"
    rc = cli_main(
        [
            "--formal-initialization-controlled-executor-design-json",
            str(controlled_design),
            "--output-dir",
            str(cli_out),
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert rc == 0
    assert payload["overall_status"] == "ready_for_readonly_real_com_preflight_design_review"
    assert payload["execution_supported"] is False
    assert payload["read_only_real_com_execution_allowed"] is False
    assert payload["live_execution_allowed"] is False
    assert payload["opens_com_ports"] is False
    assert payload["writes_sn"] is False
    assert payload["writes_coefficients"] is False
    assert (cli_out / "v1_5_formal_initialization_readonly_com_preflight_design.json").exists()


def test_readonly_com_preflight_design_reviews_missing_controlled_design(tmp_path: Path) -> None:
    tables = build_v1_5_formal_initialization_readonly_com_preflight_design(
        formal_initialization_controlled_executor_design_json=tmp_path / "missing.json",
    )
    manifest = tables["manifest"]
    gates = {row["gate"]: row for row in tables["boundary_gates"]}

    assert manifest["overall_status"] == "review_required"
    assert manifest["review_required_count"] == 1
    assert gates["controlled_design_consumed"]["status"] == "review_required"
    assert gates["controlled_design_consumed"]["evidence"] == "controlled_executor_design_evidence_missing"
