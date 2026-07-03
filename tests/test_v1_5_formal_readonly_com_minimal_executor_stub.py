import json
from pathlib import Path

from gas_calibrator.tools.run_v1_5_formal_readonly_com_minimal_executor_stub import main as cli_main
from gas_calibrator.validation.v1_5_formal_readonly_com_minimal_executor_stub import (
    SCHEMA,
    build_v1_5_formal_readonly_com_minimal_executor_stub,
    write_v1_5_formal_readonly_com_minimal_executor_stub_outputs,
)


def _minimal_review_payload(**overrides):
    payload = {
        "schema": "v1_5_formal_readonly_com_minimal_executor_review_v1",
        "overall_status": "blocked_pending_minimal_readonly_com_executor_implementation",
        "minimal_executor_review_ready": True,
        "blocker_count": 0,
        "review_required_count": 0,
        "production_state": "blocked_review_only",
        "execution_supported": False,
        "live_execution_allowed": False,
        "read_only_real_com_execution_allowed": False,
        "controlled_write_execution_allowed": False,
        "real_com_execution_allowed": False,
        "execute_flag_allowed": False,
        "opens_com_ports": False,
        "connects_postgresql": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "writes_sn": False,
        "writes_device_id": False,
        "writes_coefficients": False,
        "database_written": False,
        "formal_release_allowed": False,
        "database_import_allowed": False,
        "not_real_acceptance_evidence": True,
        "minimum_serial_command_gap_s": 1.0,
        "future_command_count": 9,
        "future_check_command_count": 1,
        "old_algorithm_check_skip_count": 1,
        "future_output_evidence_contract": [
            {"artifact": "readonly_com_attempts.csv"},
            {"artifact": "readonly_com_holds.csv"},
        ],
        "future_failure_hold_matrix": [
            {"hold": "authorization_missing"},
            {"hold": "legacy_check_requested"},
        ],
    }
    payload.update(overrides)
    return payload


def _write_review(path: Path, **overrides) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_minimal_review_payload(**overrides), indent=2), encoding="utf-8")
    return path


def test_minimal_executor_stub_builds_would_execute_artifact_without_unlocking_com(
    tmp_path: Path,
) -> None:
    review_path = _write_review(tmp_path / "review.json")

    model = build_v1_5_formal_readonly_com_minimal_executor_stub(
        formal_readonly_com_minimal_executor_review_json=review_path,
    )

    assert model["schema"] == SCHEMA
    assert model["overall_status"] == "blocked_plan_only_minimal_readonly_com_executor_stub"
    assert model["minimal_executor_stub_ready"] is True
    assert model["would_execute_artifact_ready"] is True
    assert model["execution_supported"] is False
    assert model["live_execution_allowed"] is False
    assert model["read_only_real_com_execution_allowed"] is False
    assert model["opens_com_ports"] is False
    assert model["writes_sn"] is False
    assert model["writes_device_id"] is False
    assert model["writes_coefficients"] is False
    assert model["connects_postgresql"] is False
    assert model["controls_pressure"] is False
    assert model["controls_water_or_gas_routes"] is False
    assert model["formal_release_allowed"] is False
    assert model["database_import_allowed"] is False
    assert model["authorization_context_consumed_as_unlock"] is False
    assert {row["would_execute"] for row in model["would_execute_rows"]} == {False}


def test_minimal_executor_stub_records_authorization_context_as_inert_metadata(
    tmp_path: Path,
) -> None:
    review_path = _write_review(tmp_path / "review.json")

    model = build_v1_5_formal_readonly_com_minimal_executor_stub(
        formal_readonly_com_minimal_executor_review_json=review_path,
        operator_confirmation_text="已复核端口，只读不写入",
        authorization_id="AUTH-49",
        reviewer="reviewer-a",
        approver="approver-a",
        reviewed_port_inventory_json=tmp_path / "ports.json",
        active_analyzer_list_json=tmp_path / "active.json",
    )

    assert model["authorization_context_present"] is True
    assert model["authorization_context_consumed_as_unlock"] is False
    assert model["opens_com_ports"] is False
    assert model["execution_supported"] is False
    assert model["read_only_real_com_execution_allowed"] is False


def test_minimal_executor_stub_requires_clean_minimal_executor_review(
    tmp_path: Path,
) -> None:
    review_path = _write_review(
        tmp_path / "review.json",
        opens_com_ports=True,
        minimum_serial_command_gap_s=0.2,
    )

    model = build_v1_5_formal_readonly_com_minimal_executor_stub(
        formal_readonly_com_minimal_executor_review_json=review_path,
    )
    reasons = ";".join(model["checks"][0]["reasons"])

    assert model["overall_status"] == "review_required"
    assert model["minimal_executor_stub_ready"] is False
    assert "minimum_serial_command_gap_s" in reasons
    assert "minimal_review_boundary_opens_com_ports=True" in reasons


def test_minimal_executor_stub_writer_and_cli_emit_no_com_outputs(tmp_path: Path) -> None:
    review_path = _write_review(tmp_path / "review.json")
    out_dir = tmp_path / "out"

    model = build_v1_5_formal_readonly_com_minimal_executor_stub(
        formal_readonly_com_minimal_executor_review_json=review_path,
    )
    outputs = write_v1_5_formal_readonly_com_minimal_executor_stub_outputs(model, out_dir)

    assert outputs["json"].exists()
    assert outputs["would_execute_csv"].read_text(encoding="utf-8-sig").count("False") >= 3

    cli_out = tmp_path / "cli"
    assert cli_main(
        [
            "--formal-readonly-com-minimal-executor-review-json",
            str(review_path),
            "--output-dir",
            str(cli_out),
        ]
    ) == 0
    payload = json.loads(
        (cli_out / "v1_5_formal_readonly_com_minimal_executor_stub.json").read_text(
            encoding="utf-8-sig"
        )
    )
    assert payload["opens_com_ports"] is False
    assert payload["would_execute_artifact_ready"] is True


def test_minimal_executor_stub_cli_rejects_real_com_unlock_without_outputs(
    tmp_path: Path,
) -> None:
    review_path = _write_review(tmp_path / "review.json")
    out_dir = tmp_path / "blocked"

    exit_code = cli_main(
        [
            "--formal-readonly-com-minimal-executor-review-json",
            str(review_path),
            "--output-dir",
            str(out_dir),
            "--execute-read-only-real-com",
        ]
    )

    assert exit_code == 2
    assert not (out_dir / "v1_5_formal_readonly_com_minimal_executor_stub.json").exists()
