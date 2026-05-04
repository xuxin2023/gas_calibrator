from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Mapping

from gas_calibrator.v2.core.run001_rs485_alignment import (
    build_rs485_v1_v2_alignment_matrix,
    write_rs485_v1_v2_alignment_matrix,
)


def _json_dump(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(dict(payload), ensure_ascii=False, indent=2), encoding="utf-8")


def _jsonl_dump(path: Path, rows: list[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(json.dumps(dict(row), ensure_ascii=False) for row in rows) + "\n",
        encoding="utf-8",
    )


def _v1_config(
    *,
    p3_dest_id: str = "01",
    p3_response_timeout_s: float = 2.2,
    relay_channel: int = 7,
) -> dict[str, Any]:
    return {
        "devices": {
            "pressure_controller": {
                "enabled": True,
                "port": "COM31",
                "baud": 9600,
                "timeout": 1.0,
                "line_ending": "LF",
                "query_line_endings": ["LF", "CRLF", "CR"],
                "pressure_queries": [":SENS:PRES:INL?", ":SENS:PRES:CONT?", ":SENS:PRES?", ":MEAS:PRES?"],
            },
            "pressure_gauge": {
                "enabled": True,
                "port": "COM30",
                "baud": 9600,
                "timeout": 1.0,
                "response_timeout_s": p3_response_timeout_s,
                "dest_id": p3_dest_id,
            },
            "relay": {"enabled": True, "port": "COM28", "baud": 38400, "addr": 1},
            "relay_8": {"enabled": True, "port": "COM29", "baud": 38400, "addr": 1},
        },
        "valves": {
            "relay_map": {
                "1": {"device": "relay", "channel": relay_channel},
                "8": {"device": "relay_8", "channel": 8},
            }
        },
    }


def _current_config(
    *,
    p3_dest_id: str = "01",
    p3_response_timeout_s: float = 2.2,
    relay_channel: int = 7,
) -> dict[str, Any]:
    cfg = _v1_config(
        p3_dest_id=p3_dest_id,
        p3_response_timeout_s=p3_response_timeout_s,
        relay_channel=relay_channel,
    )
    cfg.update(
        {
            "scope": "query_only",
            "query_only": True,
            "no_write": True,
            "route_open_enabled": False,
            "relay_output_enabled": False,
            "pressure_setpoint_enabled": False,
            "real_primary_latest_refresh": False,
        }
    )
    return cfg


def _write_current_a2_14_dir(
    tmp_path: Path,
    *,
    pc_protocol_profile: str = "pace5000_scpi_v1_aligned_readonly",
    p3_dest_id: str = "01",
    p3_response_timeout_s: float = 2.2,
    p3_drain_attempted: bool = False,
    relay_channel: int = 7,
) -> Path:
    run_dir = tmp_path / "a2_14"
    _json_dump(
        run_dir / "summary.json",
        {
            "evidence_source": "rs485_command_diagnostic_a2_14_no_write",
            "no_write": True,
            "a3_allowed": False,
            "real_primary_latest_refresh": False,
            "pressure_controller_protocol_profile": pc_protocol_profile,
            "pressure_controller_command_terminator": "LF",
            "pressure_controller_identity_query_command": "*IDN?",
            "pressure_controller_identity_query_result": "unsupported_identity_query",
            "pressure_controller_v1_aligned_readonly_ping_result": "no_response",
            "pressure_meter_protocol_profile": "paroscientific_p3_readonly",
            "pressure_meter_dest_id": p3_dest_id,
            "pressure_meter_mode": "P3 single-read query",
            "pressure_meter_drain_attempted": p3_drain_attempted,
            "pressure_meter_read_timeout_s": p3_response_timeout_s,
            "command_profile_mismatch": True,
            "command_profile_mismatch_reason": (
                "pressure_controller_v1_aligned_readonly_ping_no_response;"
                "pressure_meter_p3_no_response_or_parse_failed"
            ),
        },
    )
    _json_dump(
        run_dir / "rs485_command_diagnostic_config.json",
        _current_config(
            p3_dest_id=p3_dest_id,
            p3_response_timeout_s=p3_response_timeout_s,
            relay_channel=relay_channel,
        ),
    )
    _jsonl_dump(
        run_dir / "pressure_controller_command_trace.jsonl",
        [
            {
                "device_name": "pressure_controller",
                "port": "COM31",
                "command": "*IDN?",
                "raw_request_hex": "2A49444E3F0A",
                "raw_response_hex": "",
                "raw_response": "",
                "result": "unsupported_identity_query",
            },
            {
                "device_name": "pressure_controller",
                "port": "COM31",
                "command": ":OUTP:STAT?",
                "raw_request_hex": "3A4F5554503A535441543F0A",
                "raw_response_hex": "",
                "raw_response": "",
                "result": "unavailable",
            },
        ],
    )
    _jsonl_dump(
        run_dir / "pressure_meter_command_trace.jsonl",
        [
            {
                "device_name": "pressure_gauge",
                "port": "COM30",
                "command": f"*{p3_dest_id}00P3\\r\\n",
                "raw_request_hex": f"*{p3_dest_id}00P3\r\n".encode("ascii").hex().upper(),
                "raw_response_hex": "",
                "raw_response": "",
                "result": "unavailable",
                "error": "NO_RESPONSE",
                "parse_ok": False,
            }
        ],
    )
    return run_dir


def _write_historical_pace(tmp_path: Path) -> tuple[Path, Path]:
    identity = tmp_path / "pace_identity.json"
    readback = tmp_path / "pace_readback.json"
    _json_dump(
        identity,
        {
            "profile": "OLD_PACE5000",
            "rows": [
                {
                    "command": "*IDN?",
                    "response": "*IDN GE Druck,Pace5000 User Interface,3213201,02.00.07",
                    "duration_ms": 18.0,
                },
                {"command": ":OUTP:STAT?", "response": ":OUTP:STAT 0", "duration_ms": 8.0},
                {"command": ":SENS:PRES:INL?", "response": ":SENS:PRES:INL 1015.8275757, 0", "duration_ms": 9.0},
            ],
        },
    )
    _json_dump(
        readback,
        {
            "profile": "OLD_PACE5000",
            "rows": [
                {"command": ":OUTP:STAT?", "response": ":OUTP:STAT 0", "duration_ms": 7.0},
                {"command": ":SENS:PRES?", "response": ":SENS:PRES -0.0306539", "duration_ms": 7.5},
            ],
        },
    )
    return identity, readback


def _write_p3_success_io(tmp_path: Path, *, dest_id: str = "01") -> Path:
    path = tmp_path / "io.csv"
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["timestamp", "port", "direction", "command", "response"])
        writer.writeheader()
        writer.writerow(
            {
                "timestamp": "2026-03-06T19:16:22",
                "port": "COM30",
                "direction": "TX",
                "command": f"*{dest_id}00P3\\r\\n",
                "response": "",
            }
        )
        writer.writerow(
            {
                "timestamp": "2026-03-06T19:16:22",
                "port": "COM30",
                "direction": "RX",
                "command": "",
                "response": f"*00{dest_id}1028.645",
            }
        )
    return path


def _build_payload(tmp_path: Path, **kwargs: Any) -> dict[str, Any]:
    current_dir = _write_current_a2_14_dir(
        tmp_path,
        pc_protocol_profile=kwargs.get("current_pc_protocol_profile", "pace5000_scpi_v1_aligned_readonly"),
        p3_dest_id=kwargs.get("current_p3_dest_id", "01"),
        p3_response_timeout_s=kwargs.get("current_p3_response_timeout_s", 2.2),
        p3_drain_attempted=kwargs.get("current_p3_drain_attempted", False),
        relay_channel=kwargs.get("current_relay_channel", 7),
    )
    v1_config = tmp_path / "v1_config.json"
    _json_dump(
        v1_config,
        _v1_config(
            p3_dest_id=kwargs.get("v1_p3_dest_id", "01"),
            p3_response_timeout_s=kwargs.get("v1_p3_response_timeout_s", 2.2),
            relay_channel=kwargs.get("v1_relay_channel", 7),
        ),
    )
    identity, readback = _write_historical_pace(tmp_path)
    p3_io = _write_p3_success_io(tmp_path, dest_id=kwargs.get("v1_p3_dest_id", "01"))
    return build_rs485_v1_v2_alignment_matrix(
        current_a2_14_dir=current_dir,
        v1_config_path=v1_config,
        historical_pace_identity_path=identity,
        historical_pace_readback_path=readback,
        historical_pressure_gauge_io_path=p3_io,
    )


def _row(payload: Mapping[str, Any], device_name: str, device_role: str) -> dict[str, Any]:
    return next(
        row
        for row in payload["rows"]
        if row["device_name"] == device_name and row["device_role"] == device_role
    )


def test_alignment_matrix_uses_historical_artifacts_and_v1_config(tmp_path: Path) -> None:
    payload = _build_payload(tmp_path)

    assert payload["evidence_source"] == "a2_15_rs485_v1_v2_alignment_offline"
    assert payload["no_write"] is True
    assert payload["a3_allowed"] is False
    assert payload["evidence_search"]["historical_pace_identity_found"] is True
    assert payload["evidence_search"]["historical_pace_readback_found"] is True
    assert payload["evidence_search"]["historical_pressure_gauge_success_found"] is True

    identity = _row(payload, "pressure_controller", "identity_query")
    assert identity["historical_success_or_v1"]["command"] == "*IDN?"
    assert identity["current_v2_a2_14"]["command"] == "*IDN?"
    assert identity["current_v2_a2_14"]["parse_rule"] == "unsupported_identity_query_does_not_alone_mark_offline"
    assert "raw_response_hex:historical_nonempty_current_empty" in identity["mismatch_reason"]

    p3 = _row(payload, "pressure_meter", "p3_single_read")
    assert p3["historical_success_or_v1"]["com_port"] == "COM30"
    assert p3["current_v2_a2_14"]["dest_id"] == "01"
    assert "raw_response_hex:historical_nonempty_current_empty" in p3["mismatch_reason"]


def test_alignment_matrix_records_field_level_profile_mismatch(tmp_path: Path) -> None:
    payload = _build_payload(tmp_path, current_pc_protocol_profile="generic_scpi_not_v1_pace")

    status_ping = _row(payload, "pressure_controller", "readonly_status_ping")
    assert status_ping["alignment_status"] == "mismatch"
    assert "protocol_profile:OLD_PACE5000!=generic_scpi_not_v1_pace" in status_ping["mismatch_reason"]
    assert "raw_response_hex:historical_nonempty_current_empty" in status_ping["mismatch_reason"]
    assert payload["command_profile_mismatch"] is True
    assert payload["command_profile_mismatch_reason"] != "no_response"


def test_alignment_matrix_records_p3_dest_id_mismatch(tmp_path: Path) -> None:
    payload = _build_payload(tmp_path, v1_p3_dest_id="02", current_p3_dest_id="01")

    p3 = _row(payload, "pressure_meter", "p3_single_read")
    assert p3["alignment_status"] == "mismatch"
    assert "dest_id:02!=01" in p3["mismatch_reason"]
    assert "command:*0200P3" in p3["mismatch_reason"]


def test_alignment_matrix_records_p3_timeout_and_drain_mismatch(tmp_path: Path) -> None:
    payload = _build_payload(
        tmp_path,
        v1_p3_response_timeout_s=2.2,
        current_p3_response_timeout_s=0.5,
        current_p3_drain_attempted=True,
    )

    p3 = _row(payload, "pressure_meter", "p3_single_read")
    assert "timeout:2.2!=0.5" in p3["mismatch_reason"]
    assert "drain_required:false!=true" in p3["mismatch_reason"]


def test_alignment_matrix_records_relay_channel_mapping_mismatch(tmp_path: Path) -> None:
    payload = _build_payload(tmp_path, v1_relay_channel=7, current_relay_channel=12)

    relay = _row(payload, "relay", "channel_mapping")
    assert relay["alignment_status"] == "mismatch"
    assert "relay_map:historical_v1!=current_v2" in relay["mismatch_reason"]


def test_alignment_matrix_records_historical_success_not_found(tmp_path: Path) -> None:
    current_dir = _write_current_a2_14_dir(tmp_path)
    v1_config = tmp_path / "v1_config.json"
    _json_dump(v1_config, _v1_config())

    payload = build_rs485_v1_v2_alignment_matrix(
        current_a2_14_dir=current_dir,
        v1_config_path=v1_config,
    )

    assert payload["historical_success_not_found"] is True
    assert "historical_success_not_found" in payload["command_profile_mismatch_reason"]


def test_alignment_matrix_writer_persists_matrix(tmp_path: Path) -> None:
    current_dir = _write_current_a2_14_dir(tmp_path)
    v1_config = tmp_path / "v1_config.json"
    _json_dump(v1_config, _v1_config())
    identity, readback = _write_historical_pace(tmp_path)
    p3_io = _write_p3_success_io(tmp_path)
    output = tmp_path / "rs485_v1_v2_alignment_matrix.json"

    payload = write_rs485_v1_v2_alignment_matrix(
        output_path=output,
        current_a2_14_dir=current_dir,
        v1_config_path=v1_config,
        historical_pace_identity_path=identity,
        historical_pace_readback_path=readback,
        historical_pressure_gauge_io_path=p3_io,
    )

    saved = json.loads(output.read_text(encoding="utf-8"))
    assert saved["schema_version"] == payload["schema_version"]
    assert saved["not_real_acceptance_evidence"] is True
    assert saved["real_primary_latest_refresh"] is False
