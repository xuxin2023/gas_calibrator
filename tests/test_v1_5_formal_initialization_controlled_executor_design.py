import csv
import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_formal_initialization_controlled_executor_design import main as cli_main
from gas_calibrator.validation.v1_5_formal_initialization_controlled_executor_design import (
    build_v1_5_formal_initialization_controlled_executor_design,
    write_v1_5_formal_initialization_controlled_executor_design,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _blocked_executor_json(tmp_path: Path) -> Path:
    return _write_json(
        tmp_path / "blocked" / "v1_5_formal_initialization_blocked_executor.json",
        {
            "schema": "v1_5_formal_initialization_blocked_executor_v1",
            "overall_status": "blocked_pending_controlled_initialization_executor_implementation",
            "blocker_count": 0,
            "review_required_count": 0,
            "blocked_executor_ready": True,
            "execution_supported": False,
            "execution_requested": False,
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
        },
    )


def test_initialization_controlled_executor_design_is_offline_and_locked(tmp_path: Path) -> None:
    tables = build_v1_5_formal_initialization_controlled_executor_design(
        formal_initialization_blocked_executor_json=_blocked_executor_json(tmp_path),
    )
    manifest = tables["manifest"]
    gates = {row["gate"]: row for row in tables["boundary_gates"]}

    assert manifest["schema"] == "v1_5_formal_initialization_controlled_executor_design_v1"
    assert manifest["overall_status"] == "ready_for_controlled_initialization_executor_design_review"
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
    assert manifest["required_future_execute_flag"] == "--execute-controlled-initialization"
    assert gates["design_only_no_com"]["status"] == "pass"
    assert gates["future_live_execution_still_locked"]["status"] == "pass"
    assert gates["serial_command_gap_contract"]["evidence"] == "minimum_serial_command_gap_s=1"


def test_initialization_controlled_executor_design_defines_physical_contracts(tmp_path: Path) -> None:
    tables = build_v1_5_formal_initialization_controlled_executor_design(
        formal_initialization_blocked_executor_json=_blocked_executor_json(tmp_path),
    )
    auth = {row["gate"]: row for row in tables["authorization_contract"]}
    real_com = {row["step"]: row for row in tables["real_com_contract"]}
    writes = {row["write_scope"]: row for row in tables["controlled_write_contract"]}
    readback = {row["readback"]: row for row in tables["readback_contract"]}
    hold = {row["trigger"]: row for row in tables["hold_contract"]}

    assert auth["explicit_controlled_initialization_flag"]["future_flag"] == (
        "--execute-controlled-initialization"
    )
    assert auth["read_only_real_com_unlock"]["future_flag"] == "--execute-read-only-real-com"
    assert auth["controlled_write_unlock"]["future_flag"] == "--execute-controlled-writes"
    assert auth["active_device_scope"]["future_scope"] == "1_to_6_active_analyzers"
    assert real_com["mode2_1hz_runtime_setup"]["physical_meaning"].startswith("Set analyzers to MODE2")
    assert real_com["chamber_temperature_stable_then_check"]["physical_meaning"].endswith(
        "CHECK-capable/new-algorithm analyzers."
    )
    assert writes["sn_device_code"]["readback"].startswith("SN,YGAS,FFF")
    assert writes["senco7_senco8_temperature_neutral"]["excluded"].startswith(
        "do not perform temperature calibration"
    )
    assert writes["component_and_r0_coefficients"]["allowed_when"] == "never in initialization"
    assert readback["runtime_setup"]["expected"].startswith("MODE2, 1Hz")
    assert hold["write_readback_mismatch"]["hold_action"].startswith("stop affected device")


def test_initialization_controlled_executor_design_writer_and_cli_create_artifacts(
    tmp_path: Path,
    capsys,
) -> None:
    blocked = _blocked_executor_json(tmp_path)
    outputs = write_v1_5_formal_initialization_controlled_executor_design(
        tmp_path / "design",
        formal_initialization_blocked_executor_json=blocked,
    )

    manifest = json.loads(Path(outputs["manifest"]).read_text(encoding="utf-8-sig"))
    assert manifest["not_real_acceptance_evidence"] is True
    assert Path(outputs["authorization_contract"]).exists()
    assert Path(outputs["real_com_contract"]).exists()
    assert Path(outputs["controlled_write_contract"]).exists()
    assert Path(outputs["readback_contract"]).exists()
    assert Path(outputs["hold_contract"]).exists()
    assert "does not implement the real executor" in Path(outputs["summary"]).read_text(encoding="utf-8")
    assert _read_csv(Path(outputs["boundary_gates"]))[0]["gate"] == "design_only_no_com"

    cli_out = tmp_path / "cli"
    rc = cli_main(
        [
            "--formal-initialization-blocked-executor-json",
            str(blocked),
            "--output-dir",
            str(cli_out),
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert rc == 0
    assert payload["overall_status"] == "ready_for_controlled_initialization_executor_design_review"
    assert payload["execution_supported"] is False
    assert payload["live_execution_allowed"] is False
    assert payload["opens_com_ports"] is False
    assert payload["writes_sn"] is False
    assert payload["writes_coefficients"] is False
    assert (cli_out / "v1_5_formal_initialization_controlled_executor_design.json").exists()


def test_initialization_controlled_executor_design_reviews_missing_blocked_executor(tmp_path: Path) -> None:
    tables = build_v1_5_formal_initialization_controlled_executor_design(
        formal_initialization_blocked_executor_json=tmp_path / "missing.json",
    )
    manifest = tables["manifest"]
    gates = {row["gate"]: row for row in tables["boundary_gates"]}

    assert manifest["overall_status"] == "review_required"
    assert manifest["review_required_count"] == 1
    assert gates["blocked_executor_consumed"]["status"] == "review_required"
    assert gates["blocked_executor_consumed"]["evidence"] == "blocked_executor_evidence_missing"
