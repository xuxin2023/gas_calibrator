import json
from pathlib import Path

from gas_calibrator.tools.run_v1_5_formal_readonly_com_execution_blocked_executor import (
    main as blocked_executor_main,
)
from gas_calibrator.validation.v1_5_formal_readonly_com_execution_blocked_executor import (
    build_v1_5_formal_readonly_com_execution_blocked_executor,
    write_v1_5_formal_readonly_com_execution_blocked_executor_outputs,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _execution_contract_json(
    tmp_path: Path,
    *,
    side_effect_lock_clean: bool = True,
    review_required_count: int = 0,
) -> Path:
    return _write_json(
        tmp_path / "contract" / "v1_5_formal_readonly_com_execution_contract.json",
        {
            "schema": "v1_5_formal_readonly_com_execution_contract_v1",
            "overall_status": (
                "ready_for_readonly_com_execution_contract_review"
                if review_required_count == 0
                else "review_required"
            ),
            "blocker_count": 0,
            "review_required_count": review_required_count,
            "contract_ready": review_required_count == 0,
            "production_state": "contract_only",
            "required_future_read_only_real_com_flag": "--execute-read-only-real-com",
            "minimum_serial_command_gap_s": 1.0,
            "supports_old_algorithm_check_skip": True,
            "supported_active_analyzer_count": "1_to_6",
            "execution_supported": False,
            "live_execution_allowed": False,
            "read_only_real_com_execution_allowed": False,
            "controlled_write_execution_allowed": False,
            "real_com_execution_allowed": False,
            "execute_flag_allowed": False,
            "opens_com_ports": False if side_effect_lock_clean else True,
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


def test_readonly_com_execution_blocked_executor_consumes_contract_but_keeps_com_locked(tmp_path: Path) -> None:
    contract = _execution_contract_json(tmp_path)

    model = build_v1_5_formal_readonly_com_execution_blocked_executor(
        formal_readonly_com_execution_contract_json=contract,
    )

    assert model["schema"] == "v1_5_formal_readonly_com_execution_blocked_executor_v1"
    assert model["overall_status"] == "blocked_pending_readonly_com_real_executor_implementation"
    assert model["blocked_executor_ready"] is True
    assert model["contract_ready_for_future_readonly_com_executor_review"] is True
    assert model["production_state"] == "blocked_executor_only"
    assert model["execution_supported"] is False
    assert model["live_execution_allowed"] is False
    assert model["read_only_real_com_execution_allowed"] is False
    assert model["controlled_write_execution_allowed"] is False
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
    assert model["supported_active_analyzer_count"] == "1_to_6"
    assert model["supports_old_algorithm_check_skip"] is True
    assert {row["status"] for row in model["checks"]} == {"ready"}


def test_readonly_com_execution_blocked_executor_reviews_missing_contract(tmp_path: Path) -> None:
    missing = tmp_path / "missing.json"

    model = build_v1_5_formal_readonly_com_execution_blocked_executor(
        formal_readonly_com_execution_contract_json=missing,
    )

    assert model["overall_status"] == "review_required"
    assert model["blocked_executor_ready"] is False
    assert model["review_required_count"] == 1
    contract_check = next(row for row in model["checks"] if row["check"] == "readonly_com_execution_contract_consumed")
    assert contract_check["status"] == "review_required"
    assert "readonly_com_execution_contract_missing" in contract_check["reasons"]
    assert model["opens_com_ports"] is False
    assert model["writes_coefficients"] is False


def test_readonly_com_execution_blocked_executor_reviews_dirty_contract(tmp_path: Path) -> None:
    contract = _execution_contract_json(tmp_path, side_effect_lock_clean=False)

    model = build_v1_5_formal_readonly_com_execution_blocked_executor(
        formal_readonly_com_execution_contract_json=contract,
    )

    assert model["overall_status"] == "review_required"
    contract_check = next(row for row in model["checks"] if row["check"] == "readonly_com_execution_contract_consumed")
    assert contract_check["status"] == "review_required"
    assert "contract_boundary_opens_com_ports=True" in contract_check["reasons"]
    assert model["opens_com_ports"] is False


def test_readonly_com_execution_blocked_executor_writer_and_cli_are_no_com_no_write(tmp_path: Path) -> None:
    contract = _execution_contract_json(tmp_path)
    model = build_v1_5_formal_readonly_com_execution_blocked_executor(
        formal_readonly_com_execution_contract_json=contract,
    )
    outputs = write_v1_5_formal_readonly_com_execution_blocked_executor_outputs(
        model,
        tmp_path / "direct_outputs",
    )
    assert outputs["json"].exists()
    assert outputs["checks_csv"].exists()

    output_dir = tmp_path / "cli_outputs"
    rc = blocked_executor_main(
        [
            "--formal-readonly-com-execution-contract-json",
            str(contract),
            "--output-dir",
            str(output_dir),
        ]
    )

    assert rc == 0
    payload = json.loads(
        (output_dir / "v1_5_formal_readonly_com_execution_blocked_executor.json").read_text(
            encoding="utf-8-sig"
        )
    )
    assert payload["execution_supported"] is False
    assert payload["read_only_real_com_execution_allowed"] is False
    assert payload["opens_com_ports"] is False
    assert payload["writes_sn"] is False
    assert payload["writes_coefficients"] is False
    assert payload["connects_postgresql"] is False


def test_readonly_com_execution_blocked_executor_rejects_real_com_unlock_flags(tmp_path: Path, capsys) -> None:
    contract = _execution_contract_json(tmp_path)
    output_dir = tmp_path / "forbidden_outputs"

    rc = blocked_executor_main(
        [
            "--formal-readonly-com-execution-contract-json",
            str(contract),
            "--output-dir",
            str(output_dir),
            "--execute-read-only-real-com",
            "--operator-confirmation-text",
            "operator confirmed read-only scope",
            "--reviewed-port-inventory-json",
            str(tmp_path / "ports.json"),
        ]
    )

    assert rc == 2
    captured = capsys.readouterr()
    assert "locked" in captured.err
    assert not (output_dir / "v1_5_formal_readonly_com_execution_blocked_executor.json").exists()
