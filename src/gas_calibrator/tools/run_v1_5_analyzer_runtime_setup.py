"""V1.5 analyzer runtime setup runner.

This tool is intentionally narrower than ``run_headless``: it only prepares gas
analyzers for V1.5 sampling by applying runtime setup commands and checking
MODE2 frames. It must not control routes, run sampling, fit data, or write
identity/SENCO coefficients.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Optional

from ..devices import GasAnalyzer
from ._analyzer_serial_pacing import (
    MIN_ANALYZER_SERIAL_COMMAND_GAP_S,
    _coerce_serial_command_gap,
    _enforce_serial_command_gap,
)


AUTHORIZATION_PHRASE = "I_AUTHORIZE_V1_5_ANALYZER_RUNTIME_SETUP"
DEFAULT_OUTPUT_ROOT = Path("_handoff") / "v1_5_analyzer_runtime_setup"


AnalyzerFactory = Callable[[Mapping[str, Any]], Any]


class RuntimeSetupError(RuntimeError):
    """Raised when the V1.5 runtime setup contract is not safe to execute."""


def _now_stamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _load_json(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise RuntimeSetupError("runtime setup config must be a JSON object")
    return data


def _enabled_analyzers(config: Mapping[str, Any]) -> list[dict[str, Any]]:
    analyzers = config.get("analyzers")
    source_name = "analyzers"
    if not isinstance(analyzers, list):
        devices = config.get("devices") if isinstance(config.get("devices"), Mapping) else {}
        analyzers = devices.get("gas_analyzers") if isinstance(devices, Mapping) else None
        source_name = "devices.gas_analyzers"
    if not isinstance(analyzers, list):
        raise RuntimeSetupError("runtime setup config requires an analyzers list or devices.gas_analyzers")
    rows = [
        _normalize_analyzer_row(item, index=index)
        for index, item in enumerate(analyzers, start=1)
        if isinstance(item, Mapping) and bool(item.get("enabled", True))
    ]
    if not rows:
        raise RuntimeSetupError(f"runtime setup config has no enabled analyzers in {source_name}")
    return rows


def _normalize_analyzer_row(item: Mapping[str, Any], *, index: int) -> dict[str, Any]:
    row = dict(item)
    slot = str(row.get("slot") or row.get("slot_id") or row.get("name") or f"GA{index:02d}").strip()
    row.setdefault("slot", slot.upper() if slot.lower().startswith("ga") else slot)
    row.setdefault("name", str(row["slot"]).lower())
    protocol_id = _protocol_device_id(row)
    if protocol_id:
        row["protocol_device_id"] = protocol_id
        row.setdefault("device_id", protocol_id)
    sn_code = str(row.get("sn_code") or row.get("current_sn") or "").strip()
    device_code = str(row.get("device_code") or sn_code).strip()
    if not sn_code and len(device_code) == 8 and device_code.isdigit():
        sn_code = device_code
    if sn_code:
        row["sn_code"] = sn_code
        row.setdefault("device_code", sn_code)
    return row


def _protocol_device_id(row: Mapping[str, Any]) -> str:
    for key in ("protocol_device_id", "device_id", "runtime_device_id", "configured_device_id"):
        text = str(row.get(key) or "").strip()
        if not text:
            continue
        return f"{int(text):03d}" if text.isdigit() and len(text) <= 3 else text
    return ""


def _validate_config(config: Mapping[str, Any]) -> None:
    safety = config.get("safety") or {}
    if not isinstance(safety, Mapping):
        raise RuntimeSetupError("safety section is required")

    forbidden_truthy = [
        "writes_senco",
        "writes_device_id",
        "writes_sn",
        "controls_gas_route",
        "controls_water_route",
        "controls_pressure",
        "controls_temperature",
        "runs_sampling",
        "runs_fitting",
    ]
    for key in forbidden_truthy:
        if bool(safety.get(key)):
            raise RuntimeSetupError(f"unsafe runtime setup config: {key}=true")

    contract = config.get("runtime_setup_contract") or {}
    if not isinstance(contract, Mapping):
        raise RuntimeSetupError("runtime_setup_contract section is required")
    if bool(contract.get("neutral_coefficient_restore_included")):
        raise RuntimeSetupError("neutral coefficient restore is SENCO writing and is not part of runtime setup")
    if not bool(contract.get("do_not_append_set_average_1_1_after_filter", True)):
        raise RuntimeSetupError("runtime setup must not append set_average(1,1) after AVERAGE1/2 filter setup")

    for item in _enabled_analyzers(config):
        slot = str(item.get("slot") or "").strip()
        port = str(item.get("port") or "").strip()
        protocol_id = str(item.get("protocol_device_id") or "").strip()
        sn_code = str(item.get("sn_code") or "").strip()
        if not slot:
            raise RuntimeSetupError("enabled analyzer is missing slot")
        if not port:
            raise RuntimeSetupError(f"{slot}: missing port")
        if not re.fullmatch(r"\d{3}", protocol_id):
            raise RuntimeSetupError(f"{slot}: protocol_device_id must be 3 digits")
        if not re.fullmatch(r"\d{8}", sn_code):
            raise RuntimeSetupError(f"{slot}: sn_code must be 8 numeric digits")


def _runtime_contract(config: Mapping[str, Any]) -> dict[str, Any]:
    contract = dict(config.get("runtime_setup_contract") or {})
    ftd_hz = int(contract.get("ftd_hz", 1))
    rate_measure_default_s = 6.0 if ftd_hz <= 2 else 2.0
    return {
        "command_gap_s": _coerce_serial_command_gap(
            contract.get("command_gap_s", MIN_ANALYZER_SERIAL_COMMAND_GAP_S)
        ),
        "pre_drain_s": float(contract.get("pre_drain_s", 0.5)),
        "mode": int(contract.get("mode", 2)),
        "active_send": bool(contract.get("active_send", True)),
        "ftd_hz": ftd_hz,
        "average1_target": int(contract.get("average1_target", 49)),
        "average2_target": int(contract.get("average2_target", 49)),
        "post_enable_stream_wait_s": float(contract.get("post_enable_stream_wait_s", 2.0)),
        "post_enable_stream_ack_wait_s": float(contract.get("post_enable_stream_ack_wait_s", 8.0)),
        "runtime_setup_retry_count": int(contract.get("runtime_setup_retry_count", 1)),
        "runtime_setup_retry_delay_s": float(contract.get("runtime_setup_retry_delay_s", 1.2)),
        "verify_active_upload_rate": bool(contract.get("verify_active_upload_rate", True)),
        "active_upload_rate_measure_s": float(contract.get("active_upload_rate_measure_s", rate_measure_default_s)),
        "active_upload_rate_tolerance_abs_hz": float(contract.get("active_upload_rate_tolerance_abs_hz", 0.3)),
        "active_upload_rate_tolerance_fraction": float(contract.get("active_upload_rate_tolerance_fraction", 0.3)),
        "ready_consecutive_mode2_frames": int(contract.get("ready_consecutive_mode2_frames", 2)),
        "frame_attempts": int(contract.get("frame_attempts", 10)),
        "frame_retry_delay_s": float(contract.get("frame_retry_delay_s", 0.2)),
        "sn_read_timeout_s": float(contract.get("sn_read_timeout_s", 1.2)),
        "sn_read_attempts": int(contract.get("sn_read_attempts", 3)),
        "sn_retry_delay_s": float(contract.get("sn_retry_delay_s", 0.2)),
        "read_sn_before_setup": bool(contract.get("read_sn_before_setup", True)),
        "read_identity_before_setup": bool(contract.get("read_identity_before_setup", True)),
        "read_mode2_frames_after_setup": bool(contract.get("read_mode2_frames_after_setup", True)),
    }


def _planned_commands(contract: Mapping[str, Any]) -> list[dict[str, Any]]:
    commands = [
        {
            "step": 1,
            "action": "set_comm_way_inactive",
            "method": "set_comm_way_with_ack",
            "args": [False],
            "command_preview": "SETCOMWAY,YGAS,FFF,0",
            "category": "runtime_setup",
        },
        {
            "step": 2,
            "action": "set_mode2",
            "method": "set_mode_with_ack",
            "args": [int(contract["mode"])],
            "command_preview": f"MODE,YGAS,FFF,{int(contract['mode'])}",
            "category": "runtime_setup",
        },
        {
            "step": 3,
            "action": "set_active_frequency",
            "method": "set_active_freq_with_ack",
            "args": [int(contract["ftd_hz"])],
            "command_preview": f"FTD,YGAS,FFF,{max(1, int(contract['ftd_hz'])):02d}",
            "category": "runtime_setup",
        },
        {
            "step": 4,
            "action": "set_average1_filter",
            "method": "set_average_filter_channel_with_ack",
            "args": [1, int(contract["average1_target"])],
            "command_preview": f"AVERAGE1,YGAS,FFF,{int(contract['average1_target'])}",
            "category": "runtime_setup",
            "physical_channel": "H2O",
        },
        {
            "step": 5,
            "action": "set_average2_filter",
            "method": "set_average_filter_channel_with_ack",
            "args": [2, int(contract["average2_target"])],
            "command_preview": f"AVERAGE2,YGAS,FFF,{int(contract['average2_target'])}",
            "category": "runtime_setup",
            "physical_channel": "CO2",
        },
    ]
    if bool(contract["active_send"]):
        commands.append(
            {
                "step": 6,
                "action": "set_comm_way_active",
                "method": "set_comm_way_with_ack",
                "args": [True],
                "command_preview": "SETCOMWAY,YGAS,FFF,1",
                "category": "runtime_setup",
            }
        )
    return commands


def build_plan(config: Mapping[str, Any]) -> dict[str, Any]:
    _validate_config(config)
    contract = _runtime_contract(config)
    analyzers = _enabled_analyzers(config)
    commands = _planned_commands(contract)
    return {
        "schema_version": "v1_5_analyzer_runtime_setup_plan_v0",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "source_schema_version": config.get("schema_version"),
        "safety": dict(config.get("safety") or {}),
        "contract": contract,
        "analyzer_count": len(analyzers),
        "analyzers": analyzers,
        "commands": commands,
        "forbidden_actions": [
            "set_senco",
            "set_device_id",
            "set_device_id_with_ack",
            "SN write",
            "route_control",
            "pressure_control",
            "temperature_control",
            "sampling",
            "fitting",
        ],
    }


def _write_outputs(plan: Mapping[str, Any], output_dir: str | Path) -> dict[str, str]:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    plan_json = out_dir / "v1_5_analyzer_runtime_setup_plan.json"
    rows_csv = out_dir / "v1_5_analyzer_runtime_setup_plan.csv"
    summary_md = out_dir / "V1_5_ANALYZER_RUNTIME_SETUP_PLAN.md"

    plan_json.write_text(json.dumps(plan, ensure_ascii=False, indent=2), encoding="utf-8")

    rows: list[dict[str, Any]] = []
    for analyzer in plan.get("analyzers", []):
        for command in plan.get("commands", []):
            rows.append(
                {
                    "slot": analyzer.get("slot", ""),
                    "port": analyzer.get("port", ""),
                    "protocol_device_id": analyzer.get("protocol_device_id", ""),
                    "sn_code": analyzer.get("sn_code", ""),
                    "step": command.get("step", ""),
                    "action": command.get("action", ""),
                    "method": command.get("method", ""),
                    "command_preview": command.get("command_preview", ""),
                    "category": command.get("category", ""),
                    "writes_senco": False,
                    "writes_device_id": False,
                    "writes_sn": False,
                    "controls_route": False,
                }
            )
    fieldnames = [
        "slot",
        "port",
        "protocol_device_id",
        "sn_code",
        "step",
        "action",
        "method",
        "command_preview",
        "category",
        "writes_senco",
        "writes_device_id",
        "writes_sn",
        "controls_route",
    ]
    with rows_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    summary_lines = [
        "# V1.5 Analyzer Runtime Setup Plan",
        "",
        f"- analyzer_count: {plan.get('analyzer_count')}",
        "- default: dry-run plan only; real COM requires explicit execute authorization.",
        "- boundary: no SENCO, no device ID/SN writes, no route/water/pressure/temperature control, no sampling.",
        "",
        "## Commands",
    ]
    for command in plan.get("commands", []):
        summary_lines.append(f"- {command.get('step')}. {command.get('command_preview')} ({command.get('action')})")
    summary_lines.append("")
    summary_lines.append("## Analyzers")
    for analyzer in plan.get("analyzers", []):
        summary_lines.append(
            f"- {analyzer.get('slot')}: {analyzer.get('port')} "
            f"id={analyzer.get('protocol_device_id')} sn={analyzer.get('sn_code')}"
        )
    summary_md.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")

    return {"plan_json": str(plan_json), "plan_csv": str(rows_csv), "summary_md": str(summary_md)}


def _default_analyzer_factory(item: Mapping[str, Any]) -> GasAnalyzer:
    return GasAnalyzer(
        str(item["port"]),
        int(item.get("baud", 115200)),
        device_id=str(item.get("protocol_device_id") or "000"),
    )


def _drain_input(analyzer: Any, drain_s: float) -> None:
    try:
        analyzer.ser.flush_input()
    except Exception:
        pass
    if drain_s <= 0:
        return
    drain = getattr(getattr(analyzer, "ser", None), "drain_input_nonblock", None)
    if callable(drain):
        try:
            drain(drain_s=float(drain_s), read_timeout_s=0.05)
            return
        except Exception:
            pass
    time.sleep(max(0.0, float(drain_s)))


def parse_sn_readback(line: str) -> Optional[str]:
    text = str(line or "").strip().strip("<>[](){} \t\r\n")
    if not text:
        return None
    for candidate in text.replace("\r", "\n").split("\n"):
        parts = [part.strip().strip("<>[](){} \t\r\n") for part in candidate.split(",")]
        parts = [part for part in parts if part]
        if len(parts) >= 4 and parts[0].upper() == "SN" and parts[1].upper() == "YGAS":
            value = parts[-1]
            if re.fullmatch(r"\d{8}", value):
                return value
        if len(parts) == 3 and parts[0].upper() == "YGAS" and re.fullmatch(r"[0-9A-Fa-f]{3}", parts[1]):
            value = parts[2]
            if re.fullmatch(r"\d{8}", value):
                return value
    return None


def _read_sn(
    analyzer: Any,
    *,
    timeout_s: float = 0.5,
    attempts: int = 1,
    retry_delay_s: float = 0.0,
) -> tuple[Optional[str], str]:
    ser = getattr(analyzer, "ser", None)
    if ser is None or not callable(getattr(ser, "write", None)):
        return None, ""
    all_seen: list[str] = []
    max_attempts = max(1, int(attempts))
    for attempt_idx in range(max_attempts):
        try:
            ser.flush_input()
        except Exception:
            pass
        ser.write("SN,YGAS,FFF\r\n")
        deadline = time.time() + max(0.05, float(timeout_s))
        while time.time() < deadline:
            line = ""
            readline = getattr(ser, "readline", None)
            if callable(readline):
                try:
                    line = str(readline() or "").strip()
                except Exception:
                    line = ""
            if line:
                all_seen.append(line)
                parsed = parse_sn_readback(line)
                if parsed:
                    return parsed, line
            drain = getattr(ser, "drain_input_nonblock", None)
            if callable(drain):
                try:
                    for candidate in drain(drain_s=0.05, read_timeout_s=0.05):
                        text = str(candidate or "").strip()
                        if not text:
                            continue
                        all_seen.append(text)
                        parsed = parse_sn_readback(text)
                        if parsed:
                            return parsed, text
                except Exception:
                    pass
            time.sleep(0.01)
        if attempt_idx + 1 < max_attempts and float(retry_delay_s) > 0:
            try:
                time.sleep(max(0.0, float(retry_delay_s)))
            except Exception:
                pass
    return None, " | ".join(all_seen)


def _read_identity_snapshot(analyzer: Any, *, prefer_stream: bool = False) -> Optional[dict[str, Any]]:
    reader = getattr(analyzer, "read_current_mode_snapshot", None)
    if callable(reader):
        try:
            snapshot = reader(prefer_stream=prefer_stream, allow_passive_fallback=True)
        except TypeError:
            snapshot = reader()
        if isinstance(snapshot, Mapping):
            return dict(snapshot)
    return None


def _call_optional_ack(method: Any, *args: Any) -> bool:
    if not callable(method):
        return False
    try:
        return bool(method(*args, require_ack=False))
    except TypeError:
        return bool(method(*args))


def _apply_runtime_commands(analyzer: Any, contract: Mapping[str, Any]) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    gap = _coerce_serial_command_gap(contract["command_gap_s"])

    def record(action: str, ok: bool) -> None:
        events.append({"action": action, "ok": bool(ok), "category": "runtime_setup"})
        if gap > 0:
            time.sleep(gap)

    record("set_comm_way_inactive", _call_optional_ack(getattr(analyzer, "set_comm_way_with_ack", None), False))
    record("set_mode2", _call_optional_ack(getattr(analyzer, "set_mode_with_ack", None), int(contract["mode"])))
    record("set_active_frequency", _call_optional_ack(getattr(analyzer, "set_active_freq_with_ack", None), int(contract["ftd_hz"])))
    set_channel = getattr(analyzer, "set_average_filter_channel_with_ack", None)
    if callable(set_channel):
        record("set_average1_filter", _call_optional_ack(set_channel, 1, int(contract["average1_target"])))
        record("set_average2_filter", _call_optional_ack(set_channel, 2, int(contract["average2_target"])))
    else:
        set_filter = getattr(analyzer, "set_average_filter_with_ack", None)
        record("set_average_filter", _call_optional_ack(set_filter, int(contract["average1_target"])))
    if bool(contract["active_send"]):
        record("set_comm_way_active", _call_optional_ack(getattr(analyzer, "set_comm_way_with_ack", None), True))
    return events


def _verify_mode2_ready(analyzer: Any, contract: Mapping[str, Any]) -> tuple[bool, list[dict[str, Any]]]:
    if not bool(contract.get("read_mode2_frames_after_setup", True)):
        return True, []
    attempts = max(1, int(contract["frame_attempts"]))
    need = max(1, int(contract["ready_consecutive_mode2_frames"]))
    retry_delay_s = max(0.0, float(contract["frame_retry_delay_s"]))
    ftd_hz = max(1, int(contract["ftd_hz"]))
    frames: list[dict[str, Any]] = []
    ready_count = 0
    read_latest = getattr(analyzer, "read_latest_data", None)
    parser = getattr(analyzer, "parse_line_mode2", None)
    for idx in range(attempts):
        raw = ""
        if callable(read_latest):
            try:
                raw = str(
                    read_latest(
                        prefer_stream=bool(contract["active_send"]),
                        drain_s=max(0.2, 2.0 / ftd_hz),
                        read_timeout_s=0.05,
                        allow_passive_fallback=False,
                    )
                    or ""
                )
            except TypeError:
                raw = str(read_latest() or "")
        parsed = parser(raw) if callable(parser) else None
        ok = isinstance(parsed, Mapping) and parsed.get("mode") == 2 and parsed.get("id")
        frames.append({"attempt": idx + 1, "raw": raw, "parsed": dict(parsed) if isinstance(parsed, Mapping) else None, "ok": bool(ok)})
        if ok:
            ready_count += 1
            if ready_count >= need:
                return True, frames
        else:
            ready_count = 0
        if idx + 1 < attempts and retry_delay_s > 0:
            time.sleep(retry_delay_s)
    return False, frames


def _measure_active_upload_rate(analyzer: Any, contract: Mapping[str, Any]) -> dict[str, Any]:
    target_hz = max(1, int(contract["ftd_hz"]))
    if not bool(contract.get("active_send", True)):
        return {"enabled": False, "reason": "active_send_disabled", "target_hz": target_hz}
    if not bool(contract.get("verify_active_upload_rate", True)):
        return {"enabled": False, "reason": "verification_disabled", "target_hz": target_hz}

    ser = getattr(analyzer, "ser", None)
    drain_input = getattr(ser, "drain_input_nonblock", None)
    if not callable(drain_input):
        return {"enabled": False, "reason": "serial_drain_unavailable", "target_hz": target_hz}

    measure_s = max(0.5, float(contract.get("active_upload_rate_measure_s", 2.0)))
    tolerance = max(
        0.0,
        float(contract.get("active_upload_rate_tolerance_abs_hz", 0.3)),
        target_hz * float(contract.get("active_upload_rate_tolerance_fraction", 0.3)),
    )
    flush_input = getattr(ser, "flush_input", None)
    if callable(flush_input):
        try:
            flush_input()
        except Exception:
            pass

    try:
        lines = list(drain_input(drain_s=measure_s, read_timeout_s=0.05) or [])
    except TypeError:
        lines = list(drain_input() or [])
    parser = getattr(analyzer, "parse_line_mode2", None)
    valid_lines: list[str] = []
    ids: list[str] = []
    for line in lines:
        text = str(line or "").strip()
        if not text:
            continue
        parsed = parser(text) if callable(parser) else None
        if isinstance(parsed, Mapping) and parsed.get("mode") == 2 and parsed.get("id"):
            valid_lines.append(text)
            ids.append(str(parsed.get("id")))

    approx_hz = len(valid_lines) / measure_s if measure_s > 0 else 0.0
    min_hz = max(0.0, target_hz - tolerance)
    max_hz = target_hz + tolerance
    return {
        "enabled": True,
        "target_hz": target_hz,
        "measure_s": round(measure_s, 3),
        "all_lines": len(lines),
        "valid_mode2_lines": len(valid_lines),
        "approx_hz": round(approx_hz, 3),
        "tolerance_hz": round(tolerance, 3),
        "min_hz": round(min_hz, 3),
        "max_hz": round(max_hz, 3),
        "ok": bool(min_hz <= approx_hz <= max_hz),
        "ids": sorted(set(ids)),
        "sample": valid_lines[:3],
    }


def _is_retryable_runtime_setup_status(status: str) -> bool:
    return status in {
        "active_upload_rate_mismatch",
        "mode2_not_ready",
        "mode2_not_ready_after_ack_wait",
    }


def _run_runtime_setup_attempt(analyzer: Any, contract: Mapping[str, Any], *, attempt: int) -> dict[str, Any]:
    attempt_row: dict[str, Any] = {
        "attempt": int(attempt),
        "runtime_setup_events": _apply_runtime_commands(analyzer, contract),
        "mode2_frames": [],
    }
    failed_events = [event for event in attempt_row["runtime_setup_events"] if not event.get("ok")]
    if failed_events:
        failed_actions = ", ".join(str(event.get("action")) for event in failed_events)
        attempt_row["status"] = "runtime_setup_command_failed"
        attempt_row["error"] = f"runtime setup command failed: {failed_actions}"
        return attempt_row

    if float(contract["post_enable_stream_wait_s"]) > 0:
        time.sleep(float(contract["post_enable_stream_wait_s"]))
    ready, frames = _verify_mode2_ready(analyzer, contract)
    attempt_row["mode2_frames"] = frames
    if ready and float(contract.get("post_enable_stream_ack_wait_s", 0.0)) > 0:
        attempt_row["post_enable_stream_ack_wait_s"] = float(contract["post_enable_stream_ack_wait_s"])
        time.sleep(float(contract["post_enable_stream_ack_wait_s"]))
        ready_after_ack, frames_after_ack = _verify_mode2_ready(analyzer, contract)
        attempt_row["mode2_frames_after_ack_wait"] = frames_after_ack
        if ready_after_ack:
            active_rate = _measure_active_upload_rate(analyzer, contract)
            attempt_row["active_upload_rate"] = active_rate
            if active_rate.get("enabled") and not active_rate.get("ok"):
                attempt_row["status"] = "active_upload_rate_mismatch"
            else:
                attempt_row["status"] = "ready"
        else:
            attempt_row["status"] = "mode2_not_ready_after_ack_wait"
    elif ready:
        active_rate = _measure_active_upload_rate(analyzer, contract)
        attempt_row["active_upload_rate"] = active_rate
        if active_rate.get("enabled") and not active_rate.get("ok"):
            attempt_row["status"] = "active_upload_rate_mismatch"
        else:
            attempt_row["status"] = "ready"
    else:
        attempt_row["status"] = "mode2_not_ready"
    return attempt_row


def _copy_attempt_summary_to_row(row: dict[str, Any], attempt_row: Mapping[str, Any]) -> None:
    row["runtime_setup_events"] = list(attempt_row.get("runtime_setup_events") or [])
    row["mode2_frames"] = list(attempt_row.get("mode2_frames") or [])
    if "mode2_frames_after_ack_wait" in attempt_row:
        row["mode2_frames_after_ack_wait"] = list(attempt_row.get("mode2_frames_after_ack_wait") or [])
    if "post_enable_stream_ack_wait_s" in attempt_row:
        row["post_enable_stream_ack_wait_s"] = attempt_row.get("post_enable_stream_ack_wait_s")
    if "active_upload_rate" in attempt_row:
        row["active_upload_rate"] = dict(attempt_row.get("active_upload_rate") or {})
    row["status"] = str(attempt_row.get("status") or "unknown")
    if attempt_row.get("error"):
        row["error"] = str(attempt_row.get("error"))


def execute_runtime_setup(
    config: Mapping[str, Any],
    *,
    output_dir: str | Path,
    analyzer_factory: Optional[AnalyzerFactory] = None,
    run_id: str | None = None,
    evidence_paths: Optional[Mapping[str, str]] = None,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    plan = build_plan(config)
    contract = dict(plan["contract"])
    factory = analyzer_factory or _default_analyzer_factory
    results: list[dict[str, Any]] = []
    generated_at = datetime.now().isoformat(timespec="seconds")
    resolved_run_id = str(run_id or f"v1_5_analyzer_runtime_setup_{_now_stamp()}").strip()

    for item in plan["analyzers"]:
        analyzer = factory(item)
        slot = str(item.get("slot") or "")
        row: dict[str, Any] = {
            "slot": slot,
            "port": item.get("port"),
            "protocol_device_id": item.get("protocol_device_id"),
            "sn_code": item.get("sn_code"),
            "status": "started",
            "runtime_setup_events": [],
            "mode2_frames": [],
        }
        try:
            opener = getattr(analyzer, "open", None)
            if callable(opener):
                opener()
            with _enforce_serial_command_gap(
                analyzer,
                contract.get("command_gap_s", MIN_ANALYZER_SERIAL_COMMAND_GAP_S),
                sleep_fn=sleep_fn,
            ) as pacing_events:
                row["serial_command_min_gap_s"] = MIN_ANALYZER_SERIAL_COMMAND_GAP_S
                row["serial_command_pacing_events"] = pacing_events
                _drain_input(analyzer, float(contract["pre_drain_s"]))
                if bool(contract["read_sn_before_setup"]):
                    sn_readback, sn_raw = _read_sn(
                        analyzer,
                        timeout_s=float(contract["sn_read_timeout_s"]),
                        attempts=int(contract["sn_read_attempts"]),
                        retry_delay_s=float(contract["sn_retry_delay_s"]),
                    )
                    row["sn_readback"] = sn_readback
                    row["sn_raw"] = sn_raw
                    if not sn_readback:
                        raise RuntimeSetupError(f"{slot}: SN readback missing")
                    if sn_readback and sn_readback != str(item.get("sn_code")):
                        raise RuntimeSetupError(f"{slot}: SN mismatch read={sn_readback} expected={item.get('sn_code')}")
                if bool(contract["read_identity_before_setup"]):
                    identity = _read_identity_snapshot(analyzer, prefer_stream=False)
                    row["identity_before"] = identity
                    if not identity:
                        raise RuntimeSetupError(f"{slot}: identity snapshot missing")
                    if identity and identity.get("id") and str(identity.get("id")) != str(item.get("protocol_device_id")):
                        raise RuntimeSetupError(
                            f"{slot}: protocol id mismatch read={identity.get('id')} expected={item.get('protocol_device_id')}"
                        )
                attempts: list[dict[str, Any]] = []
                max_attempts = 1 + max(0, int(contract.get("runtime_setup_retry_count", 0)))
                retry_delay_s = max(0.0, float(contract.get("runtime_setup_retry_delay_s", 0.0)))
                for attempt_idx in range(1, max_attempts + 1):
                    attempt_row = _run_runtime_setup_attempt(analyzer, contract, attempt=attempt_idx)
                    attempts.append(attempt_row)
                    _copy_attempt_summary_to_row(row, attempt_row)
                    if row["status"] == "runtime_setup_command_failed":
                        raise RuntimeSetupError(f"{slot}: {attempt_row.get('error')}")
                    if row["status"] == "ready":
                        break
                    if attempt_idx >= max_attempts or not _is_retryable_runtime_setup_status(str(row["status"])):
                        break
                    if retry_delay_s > 0:
                        time.sleep(retry_delay_s)
                row["runtime_setup_attempts"] = attempts
                row["runtime_setup_attempt_count"] = len(attempts)
        except Exception as exc:
            row["status"] = "error"
            row["error"] = str(exc)
        finally:
            closer = getattr(analyzer, "close", None)
            if callable(closer):
                try:
                    closer()
                except Exception:
                    pass
        results.append(row)

    summary = {
        "schema_version": "v1_5_analyzer_runtime_setup_result_v0",
        "generated_at": generated_at,
        "run_id": resolved_run_id,
        "status": "ready" if all(row.get("status") == "ready" for row in results) else "partial",
        "results": results,
        "plan": plan,
        "evidence_paths": dict(evidence_paths or {}),
        "boundary": {
            "writes_senco": False,
            "writes_device_id": False,
            "writes_sn": False,
            "opens_com": True,
            "serial_command_min_gap_s": MIN_ANALYZER_SERIAL_COMMAND_GAP_S,
        },
    }
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    result_json = out_dir / "v1_5_analyzer_runtime_setup_result.json"
    result_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    summary["evidence_paths"]["result_json"] = str(result_json)
    result_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return summary


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare V1.5 gas analyzer runtime setup.")
    parser.add_argument("--config", required=True, help="Identity-bound runtime setup JSON.")
    parser.add_argument("--output-dir", default=None, help="Output directory. Defaults under _handoff.")
    parser.add_argument("--run-id", default=None, help="Optional stable run_id for result/database lineage.")
    parser.add_argument("--execute", action="store_true", help="Open COM and send runtime setup commands.")
    parser.add_argument(
        "--operator-confirm",
        default="",
        help=f"Required with --execute. Exact phrase: {AUTHORIZATION_PHRASE}",
    )
    parser.add_argument("--import-db", action="store_true", help="Import execute result into configured storage.")
    parser.add_argument("--storage-config", help="Optional JSON config file containing a storage section.")
    parser.add_argument("--dsn", help="SQLAlchemy DSN, e.g. sqlite:///D:/tmp/storage.sqlite")
    parser.add_argument("--backend", help="Storage backend, e.g. sqlite or postgresql")
    parser.add_argument("--database", help="Database name or SQLite file path")
    parser.add_argument("--init-schema", action="store_true", help="Create database schema before import.")
    parser.add_argument(
        "--acknowledge-db-write",
        action="store_true",
        help="Required with --import-db to confirm this intentional runtime setup DB write.",
    )
    parser.add_argument("--operator", help="Optional operator name for imported runtime setup metadata.")
    return parser.parse_args(list(argv) if argv is not None else None)


def _infer_backend_from_dsn(dsn: str) -> str | None:
    lowered = str(dsn or "").strip().lower()
    if lowered.startswith("sqlite"):
        return "sqlite"
    if lowered.startswith("postgresql") or lowered.startswith("postgres"):
        return "postgresql"
    return None


def _build_storage_settings(args: argparse.Namespace) -> Any:
    from ..storage.database import StorageSettings, load_storage_config_file

    settings = load_storage_config_file(args.storage_config) if args.storage_config else StorageSettings()
    if args.dsn:
        settings.dsn = str(args.dsn)
        if not args.backend:
            inferred = _infer_backend_from_dsn(args.dsn)
            if inferred:
                settings.backend = inferred
    if args.backend:
        settings.backend = str(args.backend)
    if args.database:
        settings.database = str(args.database)
        if not args.backend and not args.dsn and settings.normalized_backend not in {"sqlite", "postgresql"}:
            settings.backend = "sqlite"
    if not settings.is_enabled:
        raise RuntimeSetupError("storage is not configured; pass --dsn, --storage-config, or --backend sqlite --database <path>")
    return settings


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    output_dir = Path(args.output_dir) if args.output_dir else DEFAULT_OUTPUT_ROOT / f"run_{_now_stamp()}"
    try:
        config = _load_json(args.config)
        plan = build_plan(config)
        outputs = _write_outputs(plan, output_dir)
        if not args.execute:
            if args.import_db:
                raise RuntimeSetupError("--import-db requires --execute because dry-run has no runtime result")
            print(json.dumps({"status": "dry_run", "outputs": outputs}, ensure_ascii=False), flush=True)
            return 0
        if str(args.operator_confirm or "").strip() != AUTHORIZATION_PHRASE:
            raise RuntimeSetupError(f"--execute requires --operator-confirm {AUTHORIZATION_PHRASE!r}")
        result = execute_runtime_setup(
            config,
            output_dir=output_dir,
            run_id=args.run_id,
            evidence_paths=outputs,
        )
        response = {"status": result["status"], "output_dir": str(output_dir), "run_id": result.get("run_id")}
        if args.import_db:
            if not args.acknowledge_db_write:
                raise RuntimeSetupError("--import-db requires --acknowledge-db-write")
            from ..storage.database import DatabaseManager
            from ..v1_5.initialization_database import import_v1_5_runtime_setup_result

            settings = _build_storage_settings(args)
            database = DatabaseManager(settings)
            try:
                if args.init_schema:
                    database.initialize()
                db_result = import_v1_5_runtime_setup_result(
                    database,
                    Path(output_dir) / "v1_5_analyzer_runtime_setup_result.json",
                    dry_run=False,
                    allow_write=True,
                    operator=args.operator,
                )
            finally:
                database.dispose()
            response["database_written"] = True
            response["database_import"] = db_result
        print(json.dumps(response, ensure_ascii=False), flush=True)
        return 0 if result["status"] == "ready" else 1
    except Exception as exc:
        print(f"V1.5 analyzer runtime setup failed: {exc}", file=sys.stderr, flush=True)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
