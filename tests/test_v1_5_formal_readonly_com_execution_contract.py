import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_formal_readonly_com_execution_contract import (
    main as execution_contract_main,
)
from gas_calibrator.validation.v1_5_formal_readonly_com_execution_contract import (
    build_v1_5_formal_readonly_com_execution_contract,
    write_v1_5_formal_readonly_com_execution_contract_outputs,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _controlled_blocked_executor(path: Path, **overrides) -> Path:
    payload = {
        "schema": "v1_5_formal_initialization_readonly_com_preflight_controlled_blocked_executor_v1",
        "overall_status": "blocked_pending_controlled_readonly_com_preflight_executor_implementation",
        "blocker_count": 0,
        "review_required_count": 0,
        "blocked_executor_ready": True,
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
    }
    payload.update(overrides)
    return _write_json(path, payload)


def test_readonly_com_execution_contract_is_offline_and_defines_future_packet(tmp_path: Path) -> None:
    blocked = _controlled_blocked_executor(tmp_path / "blocked" / "blocked.json")

    model = build_v1_5_formal_readonly_com_execution_contract(
        formal_initialization_readonly_com_preflight_controlled_blocked_executor_json=blocked
    )

    assert model["schema"] == "v1_5_formal_readonly_com_execution_contract_v1"
    assert model["overall_status"] == "ready_for_readonly_com_execution_contract_review"
    assert model["contract_ready"] is True
    assert model["execution_supported"] is False
    assert model["read_only_real_com_execution_allowed"] is False
    assert model["opens_com_ports"] is False
    assert model["writes_sn"] is False
    assert model["writes_coefficients"] is False
    assert model["connects_postgresql"] is False
    assert model["required_future_read_only_real_com_flag"] == "--execute-read-only-real-com"
    fields = {row["field_or_flag"] for row in model["execution_packet_contract"]}
    assert "--execute-read-only-real-com" in fields
    assert "reviewed_port_inventory_json" in fields
    assert "active_analyzer_list_json" in fields
    denied = {row["action"] for row in model["denied_action_contract"]}
    assert "--execute-controlled-writes" in denied
    assert "--allow-real-com" in denied
    reads = [row["read"] for row in model["future_read_sequence_contract"]]
    assert reads == [
        "protocol_device_id",
        "sn_code_device_code",
        "getco1_through_getco9_epoch0",
        "runtime_state_mode2_1hz_average",
        "check_monitor_for_check_capable_only",
    ]


def test_readonly_com_execution_contract_reviews_missing_or_dirty_blocked_executor(tmp_path: Path) -> None:
    missing = build_v1_5_formal_readonly_com_execution_contract(
        formal_initialization_readonly_com_preflight_controlled_blocked_executor_json=tmp_path / "missing.json"
    )
    assert missing["overall_status"] == "review_required"
    assert "controlled_blocked_executor_missing" in missing["source_review_reasons"]

    dirty = _controlled_blocked_executor(tmp_path / "dirty" / "blocked.json", opens_com_ports=True)
    dirty_model = build_v1_5_formal_readonly_com_execution_contract(
        formal_initialization_readonly_com_preflight_controlled_blocked_executor_json=dirty
    )
    assert dirty_model["overall_status"] == "review_required"
    assert "boundary_opens_com_ports=True" in dirty_model["source_review_reasons"]


def test_readonly_com_execution_contract_writer_and_cli_are_no_com(tmp_path: Path) -> None:
    blocked = _controlled_blocked_executor(tmp_path / "blocked" / "blocked.json")
    model = build_v1_5_formal_readonly_com_execution_contract(
        formal_initialization_readonly_com_preflight_controlled_blocked_executor_json=blocked
    )
    outputs = write_v1_5_formal_readonly_com_execution_contract_outputs(
        model,
        output_dir=tmp_path / "out",
    )

    assert Path(outputs["json"]).exists()
    assert Path(outputs["markdown"]).exists()
    payload = json.loads(Path(outputs["json"]).read_text(encoding="utf-8-sig"))
    assert payload["opens_com_ports"] is False
    assert payload["read_only_real_com_execution_allowed"] is False

    cli_out = tmp_path / "cli"
    rc = execution_contract_main(
        [
            "--formal-initialization-readonly-com-preflight-controlled-blocked-executor-json",
            str(blocked),
            "--output-dir",
            str(cli_out),
        ]
    )
    assert rc == 0
    assert (cli_out / "v1_5_formal_readonly_com_execution_contract.json").exists()


def test_readonly_com_execution_contract_cli_rejects_live_unlock_inputs(tmp_path: Path, capsys) -> None:
    blocked = _controlled_blocked_executor(tmp_path / "blocked" / "blocked.json")
    out = tmp_path / "forbidden"

    rc = execution_contract_main(
        [
            "--formal-initialization-readonly-com-preflight-controlled-blocked-executor-json",
            str(blocked),
            "--output-dir",
            str(out),
            "--execute-read-only-real-com",
            "--authorization-id",
            "AUTH-1",
            "--reviewed-port-inventory-json",
            str(tmp_path / "ports.json"),
        ]
    )

    assert rc == 2
    captured = capsys.readouterr()
    assert "locked" in captured.err
    assert not (out / "v1_5_formal_readonly_com_execution_contract.json").exists()
