import json

import pytest

from gas_calibrator.tools.collect_v1_5_serial_port_inventory import main as inventory_cli_main
from gas_calibrator.tools.prepare_v1_5_runtime_serial_port_binding import main as cli_main
from gas_calibrator.v1_5.orchestration.serial_port_binding import (
    build_v1_5_serial_port_inventory,
    classify_v1_5_serial_port,
    resolve_reference_port_bank_shift,
)


pytestmark = pytest.mark.v1_5_formal_gate


def _cfg_with_reference_bank() -> dict:
    return {
        "devices": {
            "pressure_controller": {"enabled": True, "port": "COM31"},
            "pressure_gauge": {"enabled": True, "port": "COM30"},
            "dewpoint_meter": {"enabled": True, "port": "COM25"},
            "humidity_generator": {"enabled": True, "port": "COM24"},
            "gas_analyzers": [
                {"name": "ga01", "enabled": True, "port": "COM35", "device_id": "002"},
                {"name": "ga02", "enabled": True, "port": "COM42", "device_id": "003"},
            ],
        }
    }


def test_reference_bank_shift_is_disabled_by_default() -> None:
    cfg = _cfg_with_reference_bank()

    result = resolve_reference_port_bank_shift(cfg)

    assert result.status == "disabled"
    assert result.changed_count == 0
    assert result.config["devices"]["pressure_controller"]["port"] == "COM31"
    assert result.config["devices"]["gas_analyzers"][0]["port"] == "COM35"


def test_v1_5_serial_port_inventory_classifies_known_banks() -> None:
    inventory = build_v1_5_serial_port_inventory("COM16,COM23,COM24,COM31,COM35,COM42,COM99")

    assert inventory["opens_com_ports"] is False
    assert inventory["sends_device_commands"] is False
    assert inventory["reference_bank_state"] == "ambiguous_both_reference_banks_present"
    by_port = {row["port"]: row for row in inventory["ports"]}
    assert by_port["COM16"]["bank_role"] == "reference_target_bank_com16_23"
    assert by_port["COM31"]["bank_role"] == "reference_source_bank_com24_31"
    assert by_port["COM35"]["gas_analyzer_identity_must_use_mode2_id"] is True
    assert by_port["COM99"]["bank_role"] == "outside_v1_5_known_banks"
    assert classify_v1_5_serial_port("com42") == "gas_analyzer_protected_bank_com35_42"


def test_reference_bank_shift_maps_only_reference_devices_when_enabled() -> None:
    cfg = _cfg_with_reference_bank()

    result = resolve_reference_port_bank_shift(
        cfg,
        enabled=True,
        available_ports="COM16,COM17,COM21,COM22,COM23,COM35,COM42",
    )

    assert result.status == "pass"
    assert result.changed_count == 4
    devices = result.config["devices"]
    assert devices["pressure_controller"]["configured_port"] == "COM31"
    assert devices["pressure_controller"]["port"] == "COM23"
    assert devices["pressure_gauge"]["port"] == "COM22"
    assert devices["dewpoint_meter"]["port"] == "COM17"
    assert devices["humidity_generator"]["port"] == "COM16"
    assert devices["gas_analyzers"][0]["port"] == "COM35"
    assert devices["gas_analyzers"][1]["port"] == "COM42"
    assert any(row["status"] == "protected_gas_analyzer_identity_uses_mode2_id" for row in result.evidence_rows)


def test_reference_bank_shift_can_require_protocol_role_match() -> None:
    result = resolve_reference_port_bank_shift(
        _cfg_with_reference_bank(),
        enabled=True,
        available_ports="COM16,COM17,COM21,COM22,COM23,COM35,COM42",
        protocol_inventory={
            "COM23": {"role": "pace_controller"},
            "COM22": {"role": "digital_pressure_gauge"},
            "COM17": {"role": "dewpoint_meter"},
            "COM16": {"role": "humidity_generator"},
        },
        require_protocol_match=True,
    )

    assert result.status == "pass"
    assert result.changed_count == 4
    assert result.config["devices"]["pressure_controller"]["port"] == "COM23"
    reference_rows = [row for row in result.evidence_rows if row["device_key"] != "gas_analyzers"]
    assert all(row["protocol_status"] == "matched" for row in reference_rows)


def test_reference_bank_shift_blocks_protocol_role_mismatch_without_partial_config_change() -> None:
    result = resolve_reference_port_bank_shift(
        _cfg_with_reference_bank(),
        enabled=True,
        available_ports="COM16,COM17,COM21,COM22,COM23,COM35,COM42",
        protocol_inventory={
            "COM23": {"role": "dewpoint_meter"},
            "COM22": {"role": "digital_pressure_gauge"},
            "COM17": {"role": "dewpoint_meter"},
            "COM16": {"role": "humidity_generator"},
        },
        require_protocol_match=True,
    )

    assert result.status == "blocked"
    assert any(row["status"] == "blocked_protocol_role_mismatch" for row in result.evidence_rows)
    assert result.config["devices"]["pressure_controller"]["port"] == "COM31"
    assert result.config["devices"]["pressure_gauge"]["port"] == "COM30"


def test_reference_bank_shift_blocks_without_available_port_inventory() -> None:
    result = resolve_reference_port_bank_shift(_cfg_with_reference_bank(), enabled=True)

    assert result.status == "blocked"
    assert result.reason == "enabled_requires_available_port_inventory"
    assert result.blocked_count == 1


def test_reference_bank_shift_blocks_when_both_paired_ports_are_present() -> None:
    result = resolve_reference_port_bank_shift(
        _cfg_with_reference_bank(),
        enabled=True,
        available_ports="COM23,COM31,COM35,COM42",
    )

    assert result.status == "blocked"
    assert any(row["status"] == "blocked_both_bank_ports_present" for row in result.evidence_rows)


def test_runtime_serial_port_binding_cli_default_is_noop(tmp_path) -> None:
    cfg_path = tmp_path / "config.json"
    cfg_path.write_text(json.dumps(_cfg_with_reference_bank(), ensure_ascii=False), encoding="utf-8")
    out = tmp_path / "binding"

    rc = cli_main(["--config", str(cfg_path), "--output-dir", str(out)])

    assert rc == 0
    summary = json.loads((out / "runtime_serial_port_binding_summary.json").read_text(encoding="utf-8"))
    bound = json.loads((out / "runtime_serial_port_bound_config.json").read_text(encoding="utf-8"))
    assert summary["status"] == "disabled"
    assert bound["devices"]["pressure_controller"]["port"] == "COM31"


def test_runtime_serial_port_binding_cli_can_enable_bank_shift(tmp_path) -> None:
    cfg_path = tmp_path / "config.json"
    cfg_path.write_text(json.dumps(_cfg_with_reference_bank(), ensure_ascii=False), encoding="utf-8")
    out = tmp_path / "binding"

    rc = cli_main(
        [
            "--config",
            str(cfg_path),
            "--output-dir",
            str(out),
            "--enable-reference-bank-shift",
            "--available-ports",
            "COM16,COM17,COM21,COM22,COM23,COM35,COM42",
        ]
    )

    assert rc == 0
    summary = json.loads((out / "runtime_serial_port_binding_summary.json").read_text(encoding="utf-8"))
    bound = json.loads((out / "runtime_serial_port_bound_config.json").read_text(encoding="utf-8"))
    assert summary["status"] == "pass"
    assert bound["devices"]["pressure_controller"]["port"] == "COM23"
    assert bound["devices"]["gas_analyzers"][0]["port"] == "COM35"


def test_runtime_serial_port_binding_cli_can_require_protocol_match(tmp_path) -> None:
    cfg_path = tmp_path / "config.json"
    cfg_path.write_text(json.dumps(_cfg_with_reference_bank(), ensure_ascii=False), encoding="utf-8")
    inventory_path = tmp_path / "protocol_inventory.json"
    inventory_path.write_text(
        json.dumps(
            {
                "COM23": {"role": "pace"},
                "COM22": {"role": "pressure_gauge"},
                "COM17": {"role": "dewpoint_meter"},
                "COM16": {"role": "humidity_generator"},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    out = tmp_path / "binding"

    rc = cli_main(
        [
            "--config",
            str(cfg_path),
            "--output-dir",
            str(out),
            "--enable-reference-bank-shift",
            "--available-ports",
            "COM16,COM17,COM21,COM22,COM23,COM35,COM42",
            "--protocol-inventory-json",
            str(inventory_path),
            "--require-protocol-match",
        ]
    )

    assert rc == 0
    summary = json.loads((out / "runtime_serial_port_binding_summary.json").read_text(encoding="utf-8"))
    evidence = (out / "runtime_serial_port_binding_evidence.csv").read_text(encoding="utf-8-sig")
    assert summary["status"] == "pass"
    assert "matched" in evidence


def test_serial_port_inventory_cli_writes_ui_ready_artifacts(tmp_path) -> None:
    out = tmp_path / "inventory"

    rc = inventory_cli_main(
        [
            "--output-dir",
            str(out),
            "--available-ports",
            "COM16,COM17,COM22,COM23,COM35,COM42",
        ]
    )

    assert rc == 0
    summary = json.loads((out / "runtime_serial_port_inventory_summary.json").read_text(encoding="utf-8"))
    csv_text = (out / "runtime_serial_port_inventory.csv").read_text(encoding="utf-8-sig")
    assert summary["opens_com_ports"] is False
    assert summary["reference_bank_state"] == "target_bank_present_com16_23"
    assert "gas_analyzer_protected_bank_com35_42" in csv_text


def test_runtime_serial_port_binding_cli_accepts_port_inventory_json(tmp_path) -> None:
    cfg_path = tmp_path / "config.json"
    cfg_path.write_text(json.dumps(_cfg_with_reference_bank(), ensure_ascii=False), encoding="utf-8")
    inventory = build_v1_5_serial_port_inventory(
        "COM16,COM17,COM21,COM22,COM23,COM35,COM42",
        protocol_inventory={
            "COM23": "pace",
            "COM22": "pressure_gauge",
            "COM17": "dewpoint_meter",
            "COM16": "humidity_generator",
        },
    )
    inventory_path = tmp_path / "runtime_serial_port_inventory.json"
    inventory_path.write_text(json.dumps(inventory, ensure_ascii=False), encoding="utf-8")
    out = tmp_path / "binding"

    rc = cli_main(
        [
            "--config",
            str(cfg_path),
            "--output-dir",
            str(out),
            "--enable-reference-bank-shift",
            "--port-inventory-json",
            str(inventory_path),
            "--require-protocol-match",
        ]
    )

    assert rc == 0
    bound = json.loads((out / "runtime_serial_port_bound_config.json").read_text(encoding="utf-8"))
    assert bound["devices"]["pressure_controller"]["port"] == "COM23"
    assert bound["devices"]["gas_analyzers"][1]["port"] == "COM42"
