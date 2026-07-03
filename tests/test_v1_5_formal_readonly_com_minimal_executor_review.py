import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_formal_readonly_com_minimal_executor_review import (
    main as minimal_review_main,
)
from gas_calibrator.validation.v1_5_formal_readonly_com_minimal_executor_review import (
    build_v1_5_formal_readonly_com_minimal_executor_review,
    write_v1_5_formal_readonly_com_minimal_executor_review_outputs,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _plan_preview_json(
    tmp_path: Path,
    *,
    status: str = "ready_for_readonly_com_execution_plan_preview_review",
    opens_com_ports: bool = False,
    review_required_count: int = 0,
) -> Path:
    return _write_json(
        tmp_path / "plan_preview" / "v1_5_formal_readonly_com_execution_plan_preview.json",
        {
            "schema": "v1_5_formal_readonly_com_execution_plan_preview_v1",
            "overall_status": status,
            "review_required_count": review_required_count,
            "plan_preview_ready": status == "ready_for_readonly_com_execution_plan_preview_review",
            "minimum_serial_command_gap_s": 1.0,
            "future_command_count": 13,
            "future_check_command_count": 1,
            "old_algorithm_check_skip_count": 1,
            "command_plan": [
                {
                    "order": 1,
                    "ga_label": "GA01",
                    "port": "COM36",
                    "protocol_device_id": "001",
                    "sn_code": "01260701",
                    "read_role": "sn_code_device_code",
                    "command_or_source": "SN,YGAS,FFF",
                    "serial_command": True,
                    "serial_gap_before_s": 1.0,
                    "opens_com_ports_in_this_package": False,
                    "writes_sn": False,
                    "writes_coefficients": False,
                }
            ],
            "execution_supported": False,
            "live_execution_allowed": False,
            "read_only_real_com_execution_allowed": False,
            "controlled_write_execution_allowed": False,
            "real_com_execution_allowed": False,
            "execute_flag_allowed": False,
            "opens_com_ports": opens_com_ports,
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
            "does_not_execute_commands": True,
        },
    )


def test_minimal_executor_review_consumes_plan_preview_but_keeps_com_locked(tmp_path: Path) -> None:
    model = build_v1_5_formal_readonly_com_minimal_executor_review(
        formal_readonly_com_execution_plan_preview_json=_plan_preview_json(tmp_path),
    )

    assert model["schema"] == "v1_5_formal_readonly_com_minimal_executor_review_v1"
    assert model["overall_status"] == "blocked_pending_minimal_readonly_com_executor_implementation"
    assert model["minimal_executor_review_ready"] is True
    assert model["production_state"] == "implementation_review_only_blocked_by_default"
    assert model["plan_preview_command_plan_present"] is True
    assert model["future_command_count"] == 13
    assert model["future_check_command_count"] == 1
    assert model["old_algorithm_check_skip_count"] == 1
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
    assert model["minimum_serial_command_gap_s"] == 1.0
    assert model["supports_old_algorithm_check_skip"] is True
    assert {row["status"] for row in model["checks"]} == {"ready"}
    hold_conditions = {row["hold_condition"] for row in model["future_failure_hold_matrix"]}
    assert "legacy_ratio_device_has_check_command_planned_or_required" in hold_conditions
    assert "any_serial_command_or_retry_gap_below_1s" in hold_conditions


def test_minimal_executor_review_accepts_locked_plan_preview_without_live_packet(tmp_path: Path) -> None:
    model = build_v1_5_formal_readonly_com_minimal_executor_review(
        formal_readonly_com_execution_plan_preview_json=_plan_preview_json(
            tmp_path,
            status="blocked_pending_validated_readonly_com_execution_packet",
        ),
    )

    assert model["overall_status"] == "blocked_pending_minimal_readonly_com_executor_implementation"
    assert model["minimal_executor_review_ready"] is True
    assert model["plan_preview_status"] == "blocked_pending_validated_readonly_com_execution_packet"
    assert model["opens_com_ports"] is False


def test_minimal_executor_review_reviews_dirty_plan_preview_boundary(tmp_path: Path) -> None:
    model = build_v1_5_formal_readonly_com_minimal_executor_review(
        formal_readonly_com_execution_plan_preview_json=_plan_preview_json(tmp_path, opens_com_ports=True),
    )

    assert model["overall_status"] == "review_required"
    assert model["minimal_executor_review_ready"] is False
    plan_check = next(row for row in model["checks"] if row["check"] == "plan_preview_consumed")
    assert plan_check["status"] == "review_required"
    assert "plan_preview_boundary_opens_com_ports=True" in plan_check["reasons"]
    assert model["opens_com_ports"] is False


def test_minimal_executor_review_writer_and_cli_are_no_com_no_write(tmp_path: Path) -> None:
    model = build_v1_5_formal_readonly_com_minimal_executor_review(
        formal_readonly_com_execution_plan_preview_json=_plan_preview_json(tmp_path),
    )
    outputs = write_v1_5_formal_readonly_com_minimal_executor_review_outputs(
        model,
        tmp_path / "direct_outputs",
    )
    assert outputs["json"].exists()
    assert outputs["holds_csv"].exists()

    output_dir = tmp_path / "cli_outputs"
    rc = minimal_review_main(
        [
            "--formal-readonly-com-execution-plan-preview-json",
            str(_plan_preview_json(tmp_path / "cli")),
            "--output-dir",
            str(output_dir),
        ]
    )

    assert rc == 0
    payload = json.loads(
        (output_dir / "v1_5_formal_readonly_com_minimal_executor_review.json").read_text(
            encoding="utf-8-sig"
        )
    )
    assert payload["execution_supported"] is False
    assert payload["read_only_real_com_execution_allowed"] is False
    assert payload["opens_com_ports"] is False
    assert payload["writes_sn"] is False
    assert payload["writes_coefficients"] is False
    assert payload["connects_postgresql"] is False


def test_minimal_executor_review_cli_rejects_real_com_unlock_flags(tmp_path: Path, capsys) -> None:
    output_dir = tmp_path / "forbidden_outputs"

    rc = minimal_review_main(
        [
            "--formal-readonly-com-execution-plan-preview-json",
            str(_plan_preview_json(tmp_path)),
            "--output-dir",
            str(output_dir),
            "--execute-read-only-real-com",
            "--operator-confirmation-text",
            "read-only reviewed ports",
            "--reviewed-port-inventory-json",
            str(tmp_path / "ports.json"),
        ]
    )

    assert rc == 2
    captured = capsys.readouterr()
    assert "offline only" in captured.err
    assert not (output_dir / "v1_5_formal_readonly_com_minimal_executor_review.json").exists()
