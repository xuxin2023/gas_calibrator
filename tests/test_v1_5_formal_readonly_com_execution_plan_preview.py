import hashlib
import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_formal_readonly_com_execution_plan_preview import (
    main as plan_preview_main,
)
from gas_calibrator.validation.v1_5_formal_readonly_com_execution_plan_preview import (
    build_v1_5_formal_readonly_com_execution_plan_preview,
    write_v1_5_formal_readonly_com_execution_plan_preview_outputs,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _packet_validator_json(
    tmp_path: Path,
    *,
    status: str = "ready_for_readonly_com_execution_packet_review",
    packet_validated: bool = True,
    opens_com_ports: bool = False,
    reviewed_port_inventory_json: Path | None = None,
    active_analyzer_list_json: Path | None = None,
) -> Path:
    def digest(path: Path | None) -> str:
        return hashlib.sha256(path.read_bytes()).hexdigest() if path else ""

    return _write_json(
        tmp_path / "packet_validator" / "v1_5_formal_readonly_com_execution_packet_validator.json",
        {
            "schema": "v1_5_formal_readonly_com_execution_packet_validator_v1",
            "overall_status": status,
            "packet_validator_ready": status in {
                "ready_for_readonly_com_execution_packet_review",
                "blocked_pending_readonly_com_execution_authorization_packet",
            },
            "packet_inputs_complete": packet_validated,
            "packet_validated_offline": packet_validated,
            "review_required_count": 0,
            "minimum_serial_command_gap_s": 1.0,
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
            "reviewed_port_inventory_json": str(reviewed_port_inventory_json.resolve())
            if reviewed_port_inventory_json
            else "",
            "reviewed_port_inventory_sha256": digest(reviewed_port_inventory_json),
            "active_analyzer_list_json": str(active_analyzer_list_json.resolve())
            if active_analyzer_list_json
            else "",
            "active_analyzer_list_sha256": digest(active_analyzer_list_json),
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


def _active_analyzers_json(
    tmp_path: Path,
    *,
    old_check_required: bool = False,
    old_check_capable: bool = False,
    first_protocol_id: str = "001",
    first_sn: str = "01260701",
    second_sn: str = "01260702",
) -> Path:
    return _write_json(
        tmp_path / "packet" / "active_analyzers.json",
        {
            "schema": "v1_5_readonly_com_active_analyzer_list_v1",
            "active_analyzers": [
                {
                    "ga_label": "GA01",
                    "port": "COM36",
                    "protocol_device_id": first_protocol_id,
                    "sn_code": first_sn,
                    "algorithm": "new_absorption",
                    "check_capable": True,
                    "check_required": True,
                },
                {
                    "ga_label": "GA02",
                    "port": "COM37",
                    "protocol_device_id": "052",
                    "sn_code": second_sn,
                    "algorithm": "legacy_ratio",
                    "check_capable": old_check_capable,
                    "check_required": old_check_required,
                },
            ],
        },
    )


def test_plan_preview_requires_validated_packet_before_generating_read_sequence(tmp_path: Path) -> None:
    model = build_v1_5_formal_readonly_com_execution_plan_preview(
        formal_readonly_com_execution_packet_validator_json=_packet_validator_json(
            tmp_path,
            status="blocked_pending_readonly_com_execution_authorization_packet",
            packet_validated=False,
        )
    )

    assert model["schema"] == "v1_5_formal_readonly_com_execution_plan_preview_v1"
    assert model["overall_status"] == "blocked_pending_validated_readonly_com_execution_packet"
    assert model["plan_preview_ready"] is False
    assert model["command_plan"] == []
    assert model["execution_supported"] is False
    assert model["read_only_real_com_execution_allowed"] is False
    assert model["opens_com_ports"] is False
    assert model["writes_sn"] is False
    assert model["writes_coefficients"] is False
    assert model["connects_postgresql"] is False


def test_plan_preview_builds_future_read_order_without_opening_com(tmp_path: Path) -> None:
    ports = _reviewed_ports_json(tmp_path)
    active = _active_analyzers_json(tmp_path)
    model = build_v1_5_formal_readonly_com_execution_plan_preview(
        formal_readonly_com_execution_packet_validator_json=_packet_validator_json(
            tmp_path,
            reviewed_port_inventory_json=ports,
            active_analyzer_list_json=active,
        ),
        reviewed_port_inventory_json=ports,
        active_analyzer_list_json=active,
    )

    assert model["overall_status"] == "ready_for_readonly_com_execution_plan_preview_review"
    assert model["plan_preview_ready"] is True
    assert model["active_analyzer_count"] == 2
    assert model["future_check_command_count"] == 1
    assert model["old_algorithm_check_skip_count"] == 1
    assert model["does_not_execute_commands"] is True
    assert model["opens_com_ports"] is False
    assert model["read_only_real_com_execution_allowed"] is False
    commands = model["command_plan"]
    new_device_commands = [row["command_or_source"] for row in commands if row["ga_label"] == "GA01"]
    old_device_commands = [row["command_or_source"] for row in commands if row["ga_label"] == "GA02"]
    assert "SN,YGAS,FFF" in new_device_commands
    assert "GETCO,YGAS,FFF,1" in new_device_commands
    assert "GETCO,YGAS,FFF,9" in new_device_commands
    assert "GETCO1,YGAS,FFF" not in new_device_commands
    assert "CHECK,YGAS,FFF" in new_device_commands
    assert "CHECK,YGAS,FFF" not in old_device_commands
    actual_serial_rows = [row for row in commands if row["serial_command"] is True]
    assert actual_serial_rows
    assert all(float(row["serial_gap_before_s"]) >= 1.0 for row in actual_serial_rows)


def test_plan_preview_rejects_old_algorithm_check_requirement(tmp_path: Path) -> None:
    ports = _reviewed_ports_json(tmp_path)
    active = _active_analyzers_json(tmp_path, old_check_required=True)
    model = build_v1_5_formal_readonly_com_execution_plan_preview(
        formal_readonly_com_execution_packet_validator_json=_packet_validator_json(
            tmp_path,
            reviewed_port_inventory_json=ports,
            active_analyzer_list_json=active,
        ),
        reviewed_port_inventory_json=ports,
        active_analyzer_list_json=active,
    )

    assert model["overall_status"] == "review_required"
    input_check = next(row for row in model["checks"] if row["check"] == "detailed_plan_inputs_present")
    assert "active_2_old_algorithm_check_must_be_skipped" in input_check["reasons"]
    assert model["command_plan"] == []
    assert model["opens_com_ports"] is False


def test_plan_preview_rejects_old_algorithm_check_capable_flag(tmp_path: Path) -> None:
    ports = _reviewed_ports_json(tmp_path)
    active = _active_analyzers_json(tmp_path, old_check_capable=True)
    model = build_v1_5_formal_readonly_com_execution_plan_preview(
        formal_readonly_com_execution_packet_validator_json=_packet_validator_json(
            tmp_path,
            reviewed_port_inventory_json=ports,
            active_analyzer_list_json=active,
        ),
        reviewed_port_inventory_json=ports,
        active_analyzer_list_json=active,
    )

    assert model["overall_status"] == "review_required"
    input_check = next(row for row in model["checks"] if row["check"] == "detailed_plan_inputs_present")
    assert "active_2_old_algorithm_check_must_be_skipped" in input_check["reasons"]
    assert model["command_plan"] == []
    assert model["opens_com_ports"] is False


def test_plan_preview_rejects_active_list_missing_protocol_device_id(tmp_path: Path) -> None:
    ports = _reviewed_ports_json(tmp_path)
    active = _active_analyzers_json(tmp_path, first_protocol_id="")

    model = build_v1_5_formal_readonly_com_execution_plan_preview(
        formal_readonly_com_execution_packet_validator_json=_packet_validator_json(
            tmp_path,
            reviewed_port_inventory_json=ports,
            active_analyzer_list_json=active,
        ),
        reviewed_port_inventory_json=ports,
        active_analyzer_list_json=active,
    )

    assert model["overall_status"] == "review_required"
    input_check = next(row for row in model["checks"] if row["check"] == "detailed_plan_inputs_present")
    assert "active_1_protocol_device_id=missing" in input_check["reasons"]
    assert model["command_plan"] == []


def test_plan_preview_rejects_non_numeric_or_short_sn_code(tmp_path: Path) -> None:
    ports = _reviewed_ports_json(tmp_path)
    active = _active_analyzers_json(tmp_path, first_sn="A1260701")

    model = build_v1_5_formal_readonly_com_execution_plan_preview(
        formal_readonly_com_execution_packet_validator_json=_packet_validator_json(
            tmp_path,
            reviewed_port_inventory_json=ports,
            active_analyzer_list_json=active,
        ),
        reviewed_port_inventory_json=ports,
        active_analyzer_list_json=active,
    )

    assert model["overall_status"] == "review_required"
    input_check = next(row for row in model["checks"] if row["check"] == "detailed_plan_inputs_present")
    assert "active_1_sn_code=A1260701" in input_check["reasons"]
    assert model["command_plan"] == []


def test_plan_preview_rejects_duplicate_sn_code(tmp_path: Path) -> None:
    ports = _reviewed_ports_json(tmp_path)
    active = _active_analyzers_json(tmp_path, second_sn="01260701")

    model = build_v1_5_formal_readonly_com_execution_plan_preview(
        formal_readonly_com_execution_packet_validator_json=_packet_validator_json(
            tmp_path,
            reviewed_port_inventory_json=ports,
            active_analyzer_list_json=active,
        ),
        reviewed_port_inventory_json=ports,
        active_analyzer_list_json=active,
    )

    assert model["overall_status"] == "review_required"
    input_check = next(row for row in model["checks"] if row["check"] == "detailed_plan_inputs_present")
    assert "duplicate_active_sn_code=01260701" in input_check["reasons"]
    assert model["command_plan"] == []


def test_plan_preview_rejects_active_list_that_differs_from_packet_validator_source(
    tmp_path: Path,
) -> None:
    ports = _reviewed_ports_json(tmp_path)
    original_active = _active_analyzers_json(tmp_path)
    replacement_active = _write_json(
        tmp_path / "replacement" / "active_analyzers.json",
        json.loads(original_active.read_text(encoding="utf-8")),
    )

    model = build_v1_5_formal_readonly_com_execution_plan_preview(
        formal_readonly_com_execution_packet_validator_json=_packet_validator_json(
            tmp_path,
            reviewed_port_inventory_json=ports,
            active_analyzer_list_json=original_active,
        ),
        reviewed_port_inventory_json=ports,
        active_analyzer_list_json=replacement_active,
    )

    assert model["overall_status"] == "review_required"
    input_check = next(row for row in model["checks"] if row["check"] == "detailed_plan_inputs_present")
    assert "active_analyzer_list_json_mismatch_with_packet_validator" in input_check["reasons"]
    assert model["command_plan"] == []


def test_plan_preview_reviews_dirty_packet_validator_boundary(tmp_path: Path) -> None:
    model = build_v1_5_formal_readonly_com_execution_plan_preview(
        formal_readonly_com_execution_packet_validator_json=_packet_validator_json(
            tmp_path,
            opens_com_ports=True,
        ),
        reviewed_port_inventory_json=_reviewed_ports_json(tmp_path),
        active_analyzer_list_json=_active_analyzers_json(tmp_path),
    )

    assert model["overall_status"] == "review_required"
    packet_check = next(row for row in model["checks"] if row["check"] == "packet_validator_ready_for_plan")
    assert "packet_boundary_opens_com_ports=True" in packet_check["reasons"]
    assert model["command_plan"] == []
    assert model["opens_com_ports"] is False


def test_plan_preview_writer_outputs_json_markdown_and_csv(tmp_path: Path) -> None:
    ports = _reviewed_ports_json(tmp_path)
    active = _active_analyzers_json(tmp_path)
    model = build_v1_5_formal_readonly_com_execution_plan_preview(
        formal_readonly_com_execution_packet_validator_json=_packet_validator_json(
            tmp_path,
            reviewed_port_inventory_json=ports,
            active_analyzer_list_json=active,
        ),
        reviewed_port_inventory_json=ports,
        active_analyzer_list_json=active,
    )

    outputs = write_v1_5_formal_readonly_com_execution_plan_preview_outputs(
        model,
        output_dir=tmp_path / "out",
    )

    assert outputs["json"].exists()
    assert outputs["markdown"].exists()
    assert outputs["commands_csv"].exists()
    assert outputs["checks_csv"].exists()
    assert outputs["summary_csv"].exists()
    saved = json.loads(outputs["json"].read_text(encoding="utf-8-sig"))
    assert saved["overall_status"] == "ready_for_readonly_com_execution_plan_preview_review"
    assert "CHECK,YGAS,FFF" in outputs["commands_csv"].read_text(encoding="utf-8-sig")
    assert "opens_com_ports: `False`" in outputs["markdown"].read_text(encoding="utf-8")


def test_plan_preview_cli_rejects_live_unlock_flags_without_artifacts(tmp_path: Path) -> None:
    out = tmp_path / "out"

    rc = plan_preview_main(
        [
            "--formal-readonly-com-execution-packet-validator-json",
            str(_packet_validator_json(tmp_path)),
            "--output-dir",
            str(out),
            "--execute-read-only-real-com",
        ]
    )

    assert rc == 2
    assert not out.exists()


def test_plan_preview_cli_exports_plan_only_artifacts(tmp_path: Path) -> None:
    out = tmp_path / "out"
    ports = _reviewed_ports_json(tmp_path)
    active = _active_analyzers_json(tmp_path)

    rc = plan_preview_main(
        [
            "--formal-readonly-com-execution-packet-validator-json",
            str(
                _packet_validator_json(
                    tmp_path,
                    reviewed_port_inventory_json=ports,
                    active_analyzer_list_json=active,
                )
            ),
            "--reviewed-port-inventory-json",
            str(ports),
            "--active-analyzer-list-json",
            str(active),
            "--output-dir",
            str(out),
            "--fail-on-review-required",
        ]
    )

    assert rc == 0
    payload = json.loads(
        (out / "v1_5_formal_readonly_com_execution_plan_preview.json").read_text(encoding="utf-8-sig")
    )
    assert payload["plan_preview_ready"] is True
    assert payload["opens_com_ports"] is False
