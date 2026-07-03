import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_formal_readonly_com_execution_packet_validator import (
    main as packet_validator_main,
)
from gas_calibrator.validation.v1_5_formal_readonly_com_execution_packet_validator import (
    build_v1_5_formal_readonly_com_execution_packet_validator,
    write_v1_5_formal_readonly_com_execution_packet_validator_outputs,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _blocked_executor_json(tmp_path: Path, *, opens_com_ports: bool = False) -> Path:
    return _write_json(
        tmp_path / "blocked" / "v1_5_formal_readonly_com_execution_blocked_executor.json",
        {
            "schema": "v1_5_formal_readonly_com_execution_blocked_executor_v1",
            "overall_status": "blocked_pending_readonly_com_real_executor_implementation",
            "blocked_executor_ready": True,
            "review_required_count": 0,
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
            "minimum_serial_command_gap_s": 1.0,
        },
    )


def _authorization_packet_json(tmp_path: Path) -> Path:
    return _write_json(
        tmp_path / "packet" / "authorization.json",
        {
            "authorization_id": "AUTH-READONLY-001",
            "requested_flag": "--execute-read-only-real-com",
            "operator": "operator-a",
            "reviewer": "reviewer-a",
            "approver": "approver-a",
            "operator_confirmation_text": (
                "operator confirms read-only no-write initialization COM check with reviewed ports"
            ),
            "minimum_serial_command_gap_s": 1.0,
            "retry_gap_s": 1.0,
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
        },
    )


def _reviewed_ports_json(tmp_path: Path) -> Path:
    return _write_json(
        tmp_path / "packet" / "reviewed_ports.json",
        {
            "schema": "v1_5_readonly_com_reviewed_port_inventory_v1",
            "reviewed_ports": [
                {"ga_label": "GA01", "port": "COM36"},
                {"ga_label": "GA02", "port": "COM37"},
            ],
        },
    )


def _active_analyzers_json(tmp_path: Path, *, old_check_required: bool = False) -> Path:
    return _write_json(
        tmp_path / "packet" / "active_analyzers.json",
        {
            "schema": "v1_5_readonly_com_active_analyzer_list_v1",
            "active_analyzers": [
                {
                    "ga_label": "GA01",
                    "port": "COM36",
                    "protocol_device_id": "001",
                    "sn_code": "01260701",
                    "algorithm": "new_absorption",
                    "check_capable": True,
                    "check_required": True,
                },
                {
                    "ga_label": "GA02",
                    "port": "COM37",
                    "protocol_device_id": "052",
                    "sn_code": "01260702",
                    "algorithm": "legacy_ratio",
                    "check_capable": False,
                    "check_required": old_check_required,
                },
            ],
        },
    )


def test_packet_validator_without_packet_keeps_real_com_locked(tmp_path: Path) -> None:
    blocked = _blocked_executor_json(tmp_path)

    model = build_v1_5_formal_readonly_com_execution_packet_validator(
        formal_readonly_com_execution_blocked_executor_json=blocked,
    )

    assert model["schema"] == "v1_5_formal_readonly_com_execution_packet_validator_v1"
    assert model["overall_status"] == "blocked_pending_readonly_com_execution_authorization_packet"
    assert model["packet_validator_ready"] is True
    assert model["packet_inputs_present"] is False
    assert model["packet_validated_offline"] is False
    assert model["execution_supported"] is False
    assert model["live_execution_allowed"] is False
    assert model["read_only_real_com_execution_allowed"] is False
    assert model["opens_com_ports"] is False
    assert model["writes_sn"] is False
    assert model["writes_coefficients"] is False
    assert model["connects_postgresql"] is False


def test_packet_validator_accepts_complete_packet_as_offline_review_only(tmp_path: Path) -> None:
    model = build_v1_5_formal_readonly_com_execution_packet_validator(
        formal_readonly_com_execution_blocked_executor_json=_blocked_executor_json(tmp_path),
        authorization_packet_json=_authorization_packet_json(tmp_path),
        reviewed_port_inventory_json=_reviewed_ports_json(tmp_path),
        active_analyzer_list_json=_active_analyzers_json(tmp_path),
    )

    assert model["overall_status"] == "ready_for_readonly_com_execution_packet_review"
    assert model["packet_validator_ready"] is True
    assert model["packet_inputs_present"] is True
    assert model["packet_inputs_complete"] is True
    assert model["packet_validated_offline"] is True
    assert model["active_analyzer_count"] == 2
    assert model["supports_old_algorithm_check_skip"] is True
    assert model["execution_supported"] is False
    assert model["read_only_real_com_execution_allowed"] is False
    assert model["opens_com_ports"] is False
    assert {row["status"] for row in model["checks"]} == {"ready"}


def test_packet_validator_rejects_old_algorithm_check_requirement(tmp_path: Path) -> None:
    model = build_v1_5_formal_readonly_com_execution_packet_validator(
        formal_readonly_com_execution_blocked_executor_json=_blocked_executor_json(tmp_path),
        authorization_packet_json=_authorization_packet_json(tmp_path),
        reviewed_port_inventory_json=_reviewed_ports_json(tmp_path),
        active_analyzer_list_json=_active_analyzers_json(tmp_path, old_check_required=True),
    )

    assert model["overall_status"] == "review_required"
    active_check = next(row for row in model["checks"] if row["check"] == "active_analyzer_list_shape")
    assert "active_2_old_algorithm_check_must_be_skipped" in active_check["reasons"]
    assert model["opens_com_ports"] is False


def test_packet_validator_rejects_partial_packet_inputs(tmp_path: Path) -> None:
    model = build_v1_5_formal_readonly_com_execution_packet_validator(
        formal_readonly_com_execution_blocked_executor_json=_blocked_executor_json(tmp_path),
        authorization_packet_json=_authorization_packet_json(tmp_path),
    )

    assert model["overall_status"] == "review_required"
    completeness = next(row for row in model["checks"] if row["check"] == "packet_input_completeness")
    assert (
        "authorization_port_inventory_and_active_analyzer_inputs_must_arrive_together"
        in completeness["reasons"]
    )
    assert model["read_only_real_com_execution_allowed"] is False


def test_packet_validator_reviews_dirty_blocked_executor_boundary(tmp_path: Path) -> None:
    model = build_v1_5_formal_readonly_com_execution_packet_validator(
        formal_readonly_com_execution_blocked_executor_json=_blocked_executor_json(
            tmp_path,
            opens_com_ports=True,
        ),
    )

    assert model["overall_status"] == "review_required"
    blocked_check = next(row for row in model["checks"] if row["check"] == "blocked_executor_evidence_consumed")
    assert "blocked_executor_boundary_opens_com_ports=True" in blocked_check["reasons"]
    assert model["opens_com_ports"] is False


def test_packet_validator_writer_and_cli_are_no_com_no_write(tmp_path: Path) -> None:
    model = build_v1_5_formal_readonly_com_execution_packet_validator(
        formal_readonly_com_execution_blocked_executor_json=_blocked_executor_json(tmp_path),
    )
    outputs = write_v1_5_formal_readonly_com_execution_packet_validator_outputs(
        model,
        tmp_path / "direct_outputs",
    )
    assert outputs["json"].exists()
    assert outputs["checks_csv"].exists()

    output_dir = tmp_path / "cli_outputs"
    rc = packet_validator_main(
        [
            "--formal-readonly-com-execution-blocked-executor-json",
            str(_blocked_executor_json(tmp_path)),
            "--output-dir",
            str(output_dir),
        ]
    )

    assert rc == 0
    payload = json.loads(
        (output_dir / "v1_5_formal_readonly_com_execution_packet_validator.json").read_text(
            encoding="utf-8-sig"
        )
    )
    assert payload["execution_supported"] is False
    assert payload["opens_com_ports"] is False
    assert payload["writes_sn"] is False
    assert payload["writes_coefficients"] is False
    assert payload["connects_postgresql"] is False


def test_packet_validator_cli_rejects_direct_live_unlock_flags(tmp_path: Path, capsys) -> None:
    output_dir = tmp_path / "forbidden_outputs"

    rc = packet_validator_main(
        [
            "--formal-readonly-com-execution-blocked-executor-json",
            str(_blocked_executor_json(tmp_path)),
            "--output-dir",
            str(output_dir),
            "--execute-read-only-real-com",
            "--authorization-id",
            "AUTH-READONLY-001",
        ]
    )

    assert rc == 2
    captured = capsys.readouterr()
    assert "forbidden" in captured.err
    assert not (output_dir / "v1_5_formal_readonly_com_execution_packet_validator.json").exists()
