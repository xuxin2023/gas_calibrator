import csv
import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_formal_initialization_readonly_com_preflight_controlled_executor_design import (
    main as cli_main,
)
from gas_calibrator.validation.v1_5_formal_initialization_readonly_com_preflight_controlled_executor_design import (
    build_v1_5_formal_initialization_readonly_com_preflight_controlled_executor_design,
    write_v1_5_formal_initialization_readonly_com_preflight_controlled_executor_design,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _blocked_executor_json(
    tmp_path: Path,
    *,
    side_effect_lock_clean: bool = True,
    review_required_count: int = 0,
) -> Path:
    return _write_json(
        tmp_path
        / "blocked"
        / "v1_5_formal_initialization_readonly_com_preflight_blocked_executor.json",
        {
            "schema": "v1_5_formal_initialization_readonly_com_preflight_blocked_executor_v1",
            "overall_status": (
                "blocked_pending_readonly_real_com_preflight_implementation"
                if review_required_count == 0
                else "review_required"
            ),
            "blocker_count": 0,
            "review_required_count": review_required_count,
            "blocked_executor_ready": review_required_count == 0,
            "contract_ready_for_future_readonly_com_review": review_required_count == 0,
            "execution_supported": False,
            "live_execution_allowed": False,
            "read_only_real_com_execution_allowed": False,
            "controlled_write_execution_allowed": False,
            "real_com_execution_allowed": False,
            "execute_flag_allowed": False,
            "opens_com_ports": False if side_effect_lock_clean else True,
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


def test_readonly_com_preflight_controlled_executor_design_is_offline_and_locked(
    tmp_path: Path,
) -> None:
    tables = build_v1_5_formal_initialization_readonly_com_preflight_controlled_executor_design(
        formal_initialization_readonly_com_preflight_blocked_executor_json=_blocked_executor_json(tmp_path),
    )
    manifest = tables["manifest"]
    gates = {row["gate"]: row for row in tables["boundary_gates"]}

    assert manifest["schema"] == (
        "v1_5_formal_initialization_readonly_com_preflight_controlled_executor_design_v1"
    )
    assert manifest["overall_status"] == "ready_for_readonly_com_preflight_controlled_executor_design_review"
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
    assert gates["blocked_executor_consumed"]["status"] == "pass"
    assert gates["future_readonly_com_still_locked"]["status"] == "pass"
    assert gates["no_write_no_database_no_route"]["status"] == "pass"


def test_readonly_com_preflight_controlled_executor_design_defines_physical_contracts(
    tmp_path: Path,
) -> None:
    tables = build_v1_5_formal_initialization_readonly_com_preflight_controlled_executor_design(
        formal_initialization_readonly_com_preflight_blocked_executor_json=_blocked_executor_json(tmp_path),
    )
    auth = {row["gate"]: row for row in tables["authorization_contract"]}
    ports = {row["step"]: row for row in tables["port_inventory_contract"]}
    reads = {row["read"]: row for row in tables["read_sequence_contract"]}
    evidence = {row["artifact"]: row for row in tables["evidence_contract"]}
    holds = {row["trigger"]: row for row in tables["hold_contract"]}

    assert auth["explicit_read_only_real_com_execute_flag"]["future_flag"] == "--execute-read-only-real-com"
    assert auth["controlled_write_flags_excluded"]["future_flag_excluded"] == "--execute-controlled-writes"
    assert auth["active_device_scope"]["future_scope"] == "1_to_6_active_analyzers"
    assert ports["load_reviewed_port_inventory"]["failure_policy"] == "abort_before_opening_any_com_port"
    assert ports["enforce_serial_command_spacing"]["physical_meaning"].endswith("at least 1.0s spacing.")
    assert reads["sn_code_device_code"]["command_scope"] == "SN,YGAS,FFF"
    assert reads["sn_code_device_code"]["expected"] == "8 numeric digits; device_code equals sn_code"
    assert reads["getco_epoch0"]["command_scope"] == "GETCO1 through GETCO9"
    assert reads["check_monitor"]["command_scope"] == (
        "CHECK,YGAS,FFF for CHECK-capable/new-algorithm analyzers only"
    )
    assert "old-algorithm devices without CHECK support are skipped" in reads["check_monitor"]["expected"]
    assert evidence["readonly_com_authorization.json"]["required"] is True
    assert "GETCO1-9" in evidence["readonly_com_identity_getco_snapshot.csv"]["contents"]
    assert holds["serial_timeout_or_schema_mismatch"]["release_policy"].endswith("never write recovery commands")
    assert holds["identity_mismatch_or_duplicate_sn"]["hold_action"].startswith("hold all write")


def test_readonly_com_preflight_controlled_executor_design_writer_and_cli_create_artifacts(
    tmp_path: Path,
    capsys,
) -> None:
    blocked = _blocked_executor_json(tmp_path)
    outputs = write_v1_5_formal_initialization_readonly_com_preflight_controlled_executor_design(
        tmp_path / "design",
        formal_initialization_readonly_com_preflight_blocked_executor_json=blocked,
    )

    manifest = json.loads(Path(outputs["manifest"]).read_text(encoding="utf-8-sig"))
    assert manifest["not_real_acceptance_evidence"] is True
    assert Path(outputs["authorization_contract"]).exists()
    assert Path(outputs["port_inventory_contract"]).exists()
    assert Path(outputs["read_sequence_contract"]).exists()
    assert Path(outputs["evidence_contract"]).exists()
    assert Path(outputs["hold_contract"]).exists()
    assert "does not implement real COM execution" in Path(outputs["summary"]).read_text(encoding="utf-8")
    assert _read_csv(Path(outputs["boundary_gates"]))[0]["gate"] == "design_only_no_com"

    cli_out = tmp_path / "cli"
    rc = cli_main(
        [
            "--formal-initialization-readonly-com-preflight-blocked-executor-json",
            str(blocked),
            "--output-dir",
            str(cli_out),
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert rc == 0
    assert payload["overall_status"] == "ready_for_readonly_com_preflight_controlled_executor_design_review"
    assert payload["execution_supported"] is False
    assert payload["read_only_real_com_execution_allowed"] is False
    assert payload["live_execution_allowed"] is False
    assert payload["opens_com_ports"] is False
    assert payload["writes_sn"] is False
    assert payload["writes_coefficients"] is False
    assert (
        cli_out / "v1_5_formal_initialization_readonly_com_preflight_controlled_executor_design.json"
    ).exists()


def test_readonly_com_preflight_controlled_executor_design_reviews_missing_blocked_executor(
    tmp_path: Path,
) -> None:
    tables = build_v1_5_formal_initialization_readonly_com_preflight_controlled_executor_design(
        formal_initialization_readonly_com_preflight_blocked_executor_json=tmp_path / "missing.json",
    )
    manifest = tables["manifest"]
    gates = {row["gate"]: row for row in tables["boundary_gates"]}

    assert manifest["overall_status"] == "review_required"
    assert manifest["review_required_count"] == 1
    assert gates["blocked_executor_consumed"]["status"] == "review_required"
    assert gates["blocked_executor_consumed"]["evidence"] == "readonly_com_preflight_blocked_executor_missing"


def test_readonly_com_preflight_controlled_executor_design_reviews_dirty_blocked_executor(
    tmp_path: Path,
) -> None:
    tables = build_v1_5_formal_initialization_readonly_com_preflight_controlled_executor_design(
        formal_initialization_readonly_com_preflight_blocked_executor_json=_blocked_executor_json(
            tmp_path,
            side_effect_lock_clean=False,
        ),
    )
    gates = {row["gate"]: row for row in tables["boundary_gates"]}

    assert tables["manifest"]["overall_status"] == "review_required"
    assert gates["blocked_executor_consumed"]["status"] == "review_required"
    assert "blocked_executor_boundary_opens_com_ports=True" in gates["blocked_executor_consumed"]["evidence"]
