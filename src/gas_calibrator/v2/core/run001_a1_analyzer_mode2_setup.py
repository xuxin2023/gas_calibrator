from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import queue
import re
import subprocess
import threading
import time
from typing import Any, Callable, Mapping, Optional

from .run001_a1_analyzer_diagnostics import (
    _close_analyzer,
    _make_default_analyzer,
    _open_analyzer,
    _selected_analyzer_configs,
    diagnose_analyzer_config,
)


MODE2_SETUP_ALLOWED_COMMANDS = (
    "MODE,YGAS,FFF,2",
    "SETCOMWAY,YGAS,FFF,1",
)
MODE2_SETUP_COMMAND_SOURCE_AUDIT = (
    {
        "command": "MODE,YGAS,FFF,2",
        "driver": "raw SerialDevice.write with bounded ACK drain; fallback GasAnalyzer.set_mode_with_ack",
        "purpose": "communication_mode_setup",
    },
    {
        "command": "SETCOMWAY,YGAS,FFF,1",
        "driver": "raw SerialDevice.write with bounded ACK drain; fallback GasAnalyzer.set_comm_way_with_ack",
        "purpose": "active_send_setup",
    },
)
READ_ONLY_QUERY_COMMANDS = ("READDATA",)
MODE2_SETUP_FORBIDDEN_COMMAND_TOKENS = (
    "SENCO",
    "COEFF",
    "ZERO",
    "SPAN",
    "CALIBRATION",
    "SAVE",
    "COMMIT",
    "WRITEBACK",
    "EEPROM",
    "FLASH",
    "NVM",
    "PARAM",
)
MODE2_SETUP_COMMAND_TARGET_ID = "FFF"
_SUCCESS_ACK_RE = re.compile(r"YGAS,[0-9A-F]{3},T", re.IGNORECASE)


class Mode2SetupSafetyError(RuntimeError):
    """Raised when a Run-001/A1 analyzer MODE2 setup command is unsafe."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_command(command: Any) -> str:
    return str(command or "").strip().upper().replace("\r", "").replace("\n", "")


def _compact_command(command: Any) -> str:
    return _normalize_command(command).replace("-", "_").replace(" ", "").replace("\t", "")


def command_contains_forbidden_mode2_setup_token(command: Any) -> bool:
    compact = _compact_command(command)
    return any(token in compact for token in MODE2_SETUP_FORBIDDEN_COMMAND_TOKENS)


def validate_mode2_setup_command_plan(commands: list[str] | tuple[str, ...]) -> None:
    allowed = {_normalize_command(command) for command in MODE2_SETUP_ALLOWED_COMMANDS}
    for command in list(commands or []):
        normalized = _normalize_command(command)
        if command_contains_forbidden_mode2_setup_token(normalized):
            raise Mode2SetupSafetyError(f"Forbidden token in MODE2 setup command: {normalized}")
        if normalized not in allowed:
            raise Mode2SetupSafetyError(f"Command is not in MODE2 setup whitelist: {normalized}")


def build_mode2_setup_command_plan(_cfg: Mapping[str, Any]) -> list[str]:
    return list(MODE2_SETUP_ALLOWED_COMMANDS)


def _run_with_timeout(timeout_s: float, fn: Callable[[], Any]) -> tuple[bool, Any, str]:
    result_queue: queue.Queue[tuple[str, Any]] = queue.Queue(maxsize=1)

    def _target() -> None:
        try:
            result_queue.put(("result", fn()))
        except BaseException as exc:  # pragma: no cover - exercised through callers
            result_queue.put(("error", exc))

    thread = threading.Thread(target=_target, daemon=True)
    thread.start()
    thread.join(max(0.01, float(timeout_s or 0.01)))
    if thread.is_alive():
        return True, None, f"timeout after {float(timeout_s or 0.0):.3f}s"
    try:
        kind, value = result_queue.get_nowait()
    except queue.Empty:
        return False, None, ""
    if kind == "error":
        return False, None, str(value)
    return False, value, ""


def _is_success_ack(line: Any) -> bool:
    text = str(line or "").strip().strip("<>").upper()
    return bool(_SUCCESS_ACK_RE.search(text))


def _extract_success_ack(line: Any) -> str:
    text = str(line or "").strip().strip("<>")
    match = _SUCCESS_ACK_RE.search(text)
    return match.group(0).upper() if match else ""


def _active_upload_noise_count(lines: list[str]) -> int:
    return sum(1 for line in lines if "YGAS" in str(line or "").upper() and not _is_success_ack(line))


def _command_name(command: str) -> str:
    normalized = _normalize_command(command)
    if normalized == "MODE,YGAS,FFF,2":
        return "set_mode2"
    if normalized == "SETCOMWAY,YGAS,FFF,1":
        return "set_active_send"
    return "unsupported"


def _new_command_event(command: str) -> dict[str, Any]:
    normalized = _normalize_command(command)
    return {
        "command_name": _command_name(normalized),
        "command_payload": normalized,
        "command_target_id": MODE2_SETUP_COMMAND_TARGET_ID,
        "ack_search_policy": "scan_any_device_ack_among_active_send_frames",
        "whitelist_check": normalized in {_normalize_command(item) for item in MODE2_SETUP_ALLOWED_COMMANDS},
        "forbidden_token_check": not command_contains_forbidden_mode2_setup_token(normalized),
        "send_attempted_at": "",
        "sent": False,
        "ack_received": False,
        "ack_payload": "",
        "observed_response_count": 0,
        "ignored_active_frame_count": 0,
        "observed_response_sample": [],
        "timeout": False,
        "error": "",
        "method": "",
        "status": "planned",
    }


def _raw_serial_send_with_ack(analyzer: Any, command: str, timeout_s: float) -> tuple[bool, str, str, list[str]]:
    transport = getattr(analyzer, "ser", None)
    write = getattr(transport, "write", None)
    if not callable(write):
        raise AttributeError("serial transport write is unavailable")
    flush_input = getattr(transport, "flush_input", None)
    if callable(flush_input):
        try:
            flush_input()
        except Exception:
            pass
    write(str(command).strip() + "\r\n")
    drain = getattr(transport, "drain_input_nonblock", None)
    if not callable(drain):
        return False, "", "raw_serial_write", []
    lines = [str(line or "") for line in list(drain(drain_s=max(0.05, float(timeout_s or 0.05)), read_timeout_s=0.05) or [])]
    for line in lines:
        if _is_success_ack(line):
            return True, _extract_success_ack(line) or str(line or ""), "raw_serial_write", lines
    return False, "\n".join(str(line or "") for line in lines if str(line or "")), "raw_serial_write", lines


def _fallback_driver_send_with_ack(analyzer: Any, command: str) -> tuple[bool, str, str, list[str]]:
    normalized = _normalize_command(command)
    if normalized == "MODE,YGAS,FFF,2":
        method_names = ("set_mode_with_ack", "set_mode")
        args = (2,)
    elif normalized == "SETCOMWAY,YGAS,FFF,1":
        method_names = ("set_comm_way_with_ack", "set_comm_way")
        args = (True,)
    else:
        raise Mode2SetupSafetyError(f"Unsupported MODE2 setup command: {normalized}")
    for method_name in method_names:
        method = getattr(analyzer, method_name, None)
        if not callable(method):
            continue
        if method_name.endswith("_with_ack"):
            result = method(*args, require_ack=True)
        else:
            result = method(*args)
        return result is not False, "", method_name, []
    raise Mode2SetupSafetyError(f"Analyzer driver lacks required method: {method_names[0]}")


def _send_command_with_audit(analyzer: Any, command: str, *, timeout_s: float) -> dict[str, Any]:
    event = _new_command_event(command)
    validate_mode2_setup_command_plan([command])
    event["send_attempted_at"] = _utc_now()
    event["status"] = "send_attempted"

    def _send() -> tuple[bool, str, str, list[str]]:
        try:
            return _raw_serial_send_with_ack(analyzer, command, timeout_s)
        except AttributeError:
            return _fallback_driver_send_with_ack(analyzer, command)

    timed_out, result, error = _run_with_timeout(timeout_s, _send)
    if timed_out:
        event.update(
            {
                "timeout": True,
                "error": error,
                "status": "timeout",
            }
        )
        return event
    if error:
        event.update({"error": error, "status": "error"})
        return event
    acked, ack_payload, method_name, observed_lines = result
    event.update(
        {
            "sent": True,
            "ack_received": bool(acked),
            "ack_payload": str(ack_payload or ""),
            "observed_response_count": len(observed_lines),
            "ignored_active_frame_count": _active_upload_noise_count(observed_lines),
            "observed_response_sample": observed_lines[:5],
            "method": str(method_name or ""),
            "status": "ok" if acked else "no_ack",
        }
    )
    return event


def send_mode2_setup_commands(
    analyzer: Any,
    command_plan: list[str],
    *,
    command_timeout_s: float = 5.0,
    on_event: Optional[Callable[[dict[str, Any]], None]] = None,
) -> tuple[list[str], list[dict[str, Any]]]:
    validate_mode2_setup_command_plan(command_plan)
    sent: list[str] = []
    results: list[dict[str, Any]] = []
    for command in command_plan:
        event = _new_command_event(command)
        event["send_attempted_at"] = _utc_now()
        event["status"] = "send_attempted"
        if callable(on_event):
            on_event(dict(event))
        try:
            event = _send_command_with_audit(analyzer, command, timeout_s=command_timeout_s)
            if event.get("sent"):
                sent.append(_normalize_command(command))
            results.append(event)
            if callable(on_event):
                on_event(dict(event))
            if event.get("timeout") or event.get("error") or not event.get("ack_received"):
                break
        except Exception as exc:
            event.update({"error": str(exc), "status": "error"})
            results.append(event)
            if callable(on_event):
                on_event(dict(event))
            break
    return sent, results


def _diagnostic_snapshot(result: Optional[Mapping[str, Any]], prefix: str) -> dict[str, Any]:
    payload = dict(result or {})
    return {
        f"{prefix}_bytes_received": payload.get("bytes_received"),
        f"{prefix}_mode_detected": payload.get("observed_mode"),
        f"{prefix}_mode2_detected": payload.get("mode2_detected", payload.get("mode2_frame_detected")),
        f"{prefix}_frame_parse": payload.get("frame_parse", payload.get("frame_parse_success")),
        f"{prefix}_active_send_detected": payload.get("active_send_detected"),
        f"{prefix}_device_id": payload.get("observed_device_id"),
    }


def _make_default_setup_analyzer(cfg: Mapping[str, Any], timeout_s: float) -> Any:
    return _make_default_analyzer(cfg, timeout_s)


def _not_run_diagnostic_payload(
    *,
    selected: list[dict[str, Any]],
    requested_analyzers: list[str],
    reason: str,
) -> dict[str, Any]:
    analyzers = []
    for cfg in selected:
        analyzers.append(
            {
                **dict(cfg),
                "port_exists": bool(str(cfg.get("configured_port") or "").strip()),
                "port_open_success": False,
                "port_open": False,
                "bytes_received": 0,
                "frame_parse_success": False,
                "frame_parse": False,
                "mode2_frame_detected": False,
                "mode2_detected": False,
                "active_send_detected": False,
                "observed_device_id": "",
                "device_id_correct": False,
                "commands_sent": [],
                "error_type": reason,
                "suggested_onsite_check": "Dry-run only; no serial port was opened.",
                "final_status": "not_evaluated",
            }
        )
    return {
        "schema_version": "run001_a1.analyzer_precheck_diagnostics.1",
        "artifact_type": "run001_a1_analyzer_precheck_diagnostics",
        "generated_at": _utc_now(),
        "run_id": "Run-001/A1",
        "read_only": True,
        "default_mode": "not_run",
        "allow_read_query": False,
        "read_only_query_commands": list(READ_ONLY_QUERY_COMMANDS),
        "forbidden_persistent_command_tokens": list(MODE2_SETUP_FORBIDDEN_COMMAND_TOKENS),
        "only_failed": [],
        "requested_analyzers": list(requested_analyzers),
        "zero_based_one_based_note": (
            "gas_analyzer_0/1/2/3 are zero-based logical ids and map to physical GA01/GA02/GA03/GA04."
        ),
        "analyzers": analyzers,
        "summary": {
            "total": len(analyzers),
            "ok": 0,
            "failed": len(analyzers),
            "failed_logical_ids": [str(item.get("logical_id") or "") for item in analyzers],
            "a1_no_write_rerun_allowed": False,
            "persistent_write_command_sent": False,
        },
    }


def _setup_allowed(*, set_mode2_active_send: bool, confirm_mode2_communication_setup: bool) -> tuple[bool, str]:
    if not set_mode2_active_send:
        return False, "dry_run"
    if not confirm_mode2_communication_setup:
        return False, "missing_confirm_mode2_communication_setup"
    return True, "confirmed_mode2_communication_setup"


def _row_ready(result: Mapping[str, Any]) -> bool:
    return (
        bool(result.get("after_frame_parse"))
        and bool(result.get("after_mode2_detected"))
        and bool(result.get("after_active_send_detected"))
        and str(result.get("after_device_id") or "") == str(result.get("expected_device_id") or "")
        and str(result.get("error_type") or "") == "ok"
    )


def _bounded_diagnostic(
    cfg: Mapping[str, Any],
    *,
    analyzer_factory: Optional[Callable[[Mapping[str, Any]], Any]],
    timeout_s: float,
) -> tuple[Optional[dict[str, Any]], bool, str]:
    timed_out, result, error = _run_with_timeout(
        timeout_s,
        lambda: diagnose_analyzer_config(
            cfg,
            analyzer_factory=analyzer_factory,
            allow_read_query=False,
            timeout_s=timeout_s,
        ),
    )
    if timed_out:
        return None, True, error
    if error:
        return None, False, error
    return dict(result or {}), False, ""


def _post_diagnostics_from_setup_results(
    *,
    requested_analyzers: list[str],
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    analyzers: list[dict[str, Any]] = []
    for row in rows:
        after_parse = bool(row.get("after_frame_parse"))
        after_mode2 = bool(row.get("after_mode2_detected"))
        after_active = bool(row.get("after_active_send_detected"))
        observed_id = str(row.get("after_device_id") or "")
        expected_id = str(row.get("expected_device_id") or "")
        error_type = str(row.get("error_type") or "")
        analyzers.append(
            {
                **{
                    key: value
                    for key, value in dict(row).items()
                    if key
                    in {
                        "logical_id",
                        "zero_based_index",
                        "one_based_position",
                        "physical_label",
                        "configured_name",
                        "port",
                        "configured_port",
                        "baudrate",
                        "expected_device_id",
                        "configured_device_id",
                        "protocol",
                        "expected_mode",
                        "active_send_expected",
                        "enabled",
                        "connected",
                    }
                },
                "port_exists": bool(str(row.get("port") or "").strip()),
                "port_open_success": error_type not in {"port_open_fail", "open_failed"},
                "port_open": error_type not in {"port_open_fail", "open_failed"},
                "bytes_received": row.get("after_bytes_received") or 0,
                "frame_parse_success": after_parse,
                "frame_parse": after_parse,
                "mode2_frame_detected": after_mode2,
                "mode2_detected": after_mode2,
                "active_send_detected": after_active,
                "observed_device_id": observed_id,
                "after_device_id": observed_id,
                "device_id_correct": bool(expected_id and observed_id == expected_id),
                "commands_sent": list(row.get("commands_sent") or []),
                "error_type": error_type or ("ok" if _row_ready(row) else "not_ready"),
                "suggested_onsite_check": str(row.get("onsite_suggestion") or ""),
                "final_status": "ready" if _row_ready(row) else "not_ready",
            }
        )
    failed = [item for item in analyzers if item.get("error_type") != "ok"]
    return {
        "schema_version": "run001_a1.analyzer_precheck_diagnostics.1",
        "artifact_type": "run001_a1_analyzer_precheck_diagnostics",
        "generated_at": _utc_now(),
        "run_id": "Run-001/A1",
        "read_only": True,
        "default_mode": "post_setup_snapshot",
        "allow_read_query": False,
        "read_only_query_commands": list(READ_ONLY_QUERY_COMMANDS),
        "forbidden_persistent_command_tokens": list(MODE2_SETUP_FORBIDDEN_COMMAND_TOKENS),
        "only_failed": [],
        "requested_analyzers": list(requested_analyzers),
        "zero_based_one_based_note": (
            "gas_analyzer_0/1/2/3 are zero-based logical ids and map to physical GA01/GA02/GA03/GA04."
        ),
        "analyzers": analyzers,
        "summary": {
            "total": len(analyzers),
            "ok": len(analyzers) - len(failed),
            "failed": len(failed),
            "failed_logical_ids": [str(item.get("logical_id") or "") for item in failed],
            "a1_no_write_rerun_allowed": len(analyzers) == 4 and not failed,
            "persistent_write_command_sent": False,
        },
    }


def _git_value(args: list[str]) -> str:
    try:
        repo_root = Path(__file__).resolve().parents[4]
        result = subprocess.run(
            ["git", *args],
            cwd=str(repo_root),
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            check=False,
            timeout=5,
        )
        return result.stdout.strip() if result.returncode == 0 else ""
    except Exception:
        return ""


def _default_total_timeout_s(device_count: int, per_device_timeout_s: float) -> float:
    return max(30.0, float(device_count) * max(1.0, float(per_device_timeout_s or 30.0)) + 10.0)


def _summarize_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    rows = [dict(item) for item in list(payload.get("analyzers") or [])]
    ready_count = sum(1 for row in rows if _row_ready(row))
    flat_commands = [command for row in rows for command in list(row.get("commands_sent") or [])]
    timeout_count = sum(1 for row in rows if str(row.get("status") or "") == "timeout")
    failed = [row for row in rows if not _row_ready(row)]
    a1_rerun_allowed = bool(
        not payload.get("dry_run")
        and len(rows) == 4
        and ready_count == 4
        and not timeout_count
        and not payload.get("forbidden_command_plan_error")
    )
    return {
        "total": len(rows),
        "ready": ready_count,
        "failed": len(failed),
        "timeout": timeout_count,
        "commands_sent_count": len(flat_commands),
        "persistent_write_command_sent": False,
        "calibration_write_command_sent": False,
        "mode_setup_command_sent": bool(flat_commands),
        "a1_no_write_rerun_allowed": a1_rerun_allowed,
        "not_real_acceptance_evidence": True,
    }


def build_analyzer_mode2_setup_payload(
    raw_cfg: Mapping[str, Any],
    *,
    analyzers: Optional[list[str]] = None,
    dry_run: bool = True,
    set_mode2_active_send: bool = False,
    confirm_mode2_communication_setup: bool = False,
    timeout_s: float = 20.0,
    command_timeout_s: float = 5.0,
    device_timeout_s: float = 30.0,
    total_timeout_s: Optional[float] = None,
    analyzer_factory: Optional[Callable[[Mapping[str, Any]], Any]] = None,
    config_path: str = "",
    git_commit: str = "",
    branch: str = "",
    artifact_update: Optional[Callable[[Mapping[str, Any]], None]] = None,
) -> dict[str, Any]:
    requested_analyzers = list(analyzers or [])
    selected = _selected_analyzer_configs(raw_cfg, None, requested_analyzers)
    send_allowed, setup_reason = _setup_allowed(
        set_mode2_active_send=set_mode2_active_send,
        confirm_mode2_communication_setup=confirm_mode2_communication_setup,
    )
    effective_dry_run = bool(dry_run or not send_allowed)
    command_plan_error = ""
    started_at = _utc_now()
    total_timeout_value = float(total_timeout_s or _default_total_timeout_s(len(selected), device_timeout_s))
    total_deadline = time.monotonic() + max(1.0, total_timeout_value)
    setup_results: list[dict[str, Any]] = []

    payload: dict[str, Any] = {
        "schema_version": "run001_a1.analyzer_mode2_setup.2",
        "artifact_type": "run001_a1_analyzer_mode2_setup",
        "generated_at": started_at,
        "started_at": started_at,
        "completed_at": "",
        "git_commit": git_commit,
        "branch": branch,
        "config_path": config_path,
        "mode": "analyzer_mode2_setup",
        "run_id": "Run-001/A1",
        "scope": "GA01-GA04 MODE2 active-send communication setup only",
        "status": "running" if not effective_dry_run else "planned",
        "dry_run": bool(effective_dry_run),
        "set_mode2_active_send": bool(set_mode2_active_send),
        "confirm_mode2_communication_setup": bool(confirm_mode2_communication_setup),
        "setup_request_status": setup_reason,
        "target_analyzers": [str(item.get("logical_id") or "") for item in selected],
        "target_ports": [str(item.get("configured_port") or "") for item in selected],
        "command_whitelist": list(MODE2_SETUP_ALLOWED_COMMANDS),
        "command_source_audit": list(MODE2_SETUP_COMMAND_SOURCE_AUDIT),
        "allowed_communication_commands": list(MODE2_SETUP_ALLOWED_COMMANDS),
        "read_only_query_commands": list(READ_ONLY_QUERY_COMMANDS),
        "forbidden_tokens": list(MODE2_SETUP_FORBIDDEN_COMMAND_TOKENS),
        "forbidden_command_tokens": list(MODE2_SETUP_FORBIDDEN_COMMAND_TOKENS),
        "forbidden_command_plan_error": "",
        "timeouts": {
            "command_timeout_s": float(command_timeout_s),
            "device_timeout_s": float(device_timeout_s),
            "total_timeout_s": total_timeout_value,
            "diagnostic_timeout_s": min(float(timeout_s or 20.0), max(0.2, float(device_timeout_s or 30.0) / 4.0), 5.0),
        },
        "persistent_write_command_sent": False,
        "calibration_write_command_sent": False,
        "mode_setup_command_sent": False,
        "commands_sent": [],
        "command_events": [],
        "partial_results": setup_results,
        "protected_paths_touched": [],
        "v1_production_flow_touched": False,
        "run_app_touched": False,
        "a1_execute_invoked": False,
        "a2_invoked": False,
        "h2o_invoked": False,
        "full_group_invoked": False,
        "analyzers": setup_results,
        "analyzer_precheck_diagnostics": _not_run_diagnostic_payload(
            selected=selected,
            requested_analyzers=requested_analyzers,
            reason="running" if not effective_dry_run else setup_reason,
        ),
        "summary": {
            "total": len(selected),
            "ready": 0,
            "failed": len(selected),
            "timeout": 0,
            "commands_sent_count": 0,
            "persistent_write_command_sent": False,
            "calibration_write_command_sent": False,
            "mode_setup_command_sent": False,
            "a1_no_write_rerun_allowed": False,
            "not_real_acceptance_evidence": True,
        },
        "not_real_acceptance_evidence": True,
    }

    def _refresh(status: Optional[str] = None) -> None:
        if status:
            payload["status"] = status
        payload["generated_at"] = _utc_now()
        flat_commands = [command for row in setup_results for command in list(row.get("commands_sent") or [])]
        payload["commands_sent"] = flat_commands
        payload["mode_setup_command_sent"] = bool(flat_commands)
        payload["summary"] = _summarize_payload(payload)
        if callable(artifact_update):
            artifact_update(payload)

    _refresh()

    try:
        for cfg in selected:
            if time.monotonic() >= total_deadline:
                payload["status"] = "timeout"
                break

            command_plan = build_mode2_setup_command_plan(cfg)
            row_started = _utc_now()
            row_start_monotonic = time.monotonic()
            row_deadline = min(total_deadline, row_start_monotonic + max(1.0, float(device_timeout_s or 30.0)))
            row = {
                **dict(cfg),
                "port": str(cfg.get("configured_port") or ""),
                "planned_port": str(cfg.get("configured_port") or ""),
                "expected_device_id": str(cfg.get("configured_device_id") or ""),
                "started_at": row_started,
                "completed_at": "",
                "status": "running" if not effective_dry_run else "planned",
                "dry_run": bool(effective_dry_run),
                "setup_request_status": setup_reason,
                "command_plan": command_plan,
                "commands_planned": list(command_plan),
                "commands_sent": [],
                "command_result": [],
                "ack_result": [],
                "mode_setup_command_sent": False,
                "persistent_write_command_sent": False,
                "calibration_write_command_sent": False,
                "final_status": "planned" if effective_dry_run else "running",
                "error_type": "dry_run" if effective_dry_run else "",
                "elapsed_s": 0.0,
                "onsite_suggestion": "Dry-run only; review command plan and run with explicit confirmation onsite.",
            }
            setup_results.append(row)
            _refresh("running" if not effective_dry_run else "planned")

            try:
                validate_mode2_setup_command_plan(command_plan)
            except Exception as exc:
                command_plan_error = str(exc)
                payload["forbidden_command_plan_error"] = command_plan_error
                row.update(
                    {
                        "error_type": "unsafe_command_plan",
                        "final_status": "failed",
                        "status": "failed",
                        "onsite_suggestion": str(exc),
                    }
                )
                _refresh("failed")
                continue

            if effective_dry_run:
                row.update({"completed_at": _utc_now(), "elapsed_s": round(time.monotonic() - row_start_monotonic, 3)})
                _refresh("planned")
                continue

            remaining = max(0.01, row_deadline - time.monotonic())
            diagnostic_timeout = min(float(payload["timeouts"]["diagnostic_timeout_s"]), remaining)
            before_result, before_timeout, before_error = _bounded_diagnostic(
                cfg,
                analyzer_factory=analyzer_factory,
                timeout_s=diagnostic_timeout,
            )
            row.update(_diagnostic_snapshot(before_result, "before"))
            if before_timeout:
                row.update(
                    {
                        "status": "timeout",
                        "final_status": "timeout",
                        "error_type": "before_diagnostic_timeout",
                        "onsite_suggestion": before_error,
                    }
                )
                row["completed_at"] = _utc_now()
                row["elapsed_s"] = round(time.monotonic() - row_start_monotonic, 3)
                _refresh("timeout")
                continue

            analyzer: Any = None
            try:
                if time.monotonic() >= row_deadline:
                    raise TimeoutError("per-device timeout before command send")
                factory = analyzer_factory or (lambda item: _make_default_setup_analyzer(item, command_timeout_s))
                analyzer = factory(cfg)
                open_timeout = max(0.01, min(3.0, row_deadline - time.monotonic()))
                open_timed_out, _open_result, open_error = _run_with_timeout(open_timeout, lambda: _open_analyzer(analyzer))
                if open_timed_out:
                    raise TimeoutError(open_error or "port open timeout")
                if open_error:
                    raise RuntimeError(open_error)

                def _record_event(event: dict[str, Any]) -> None:
                    row["command_result"] = list(row.get("command_result") or [])
                    events = list(row.get("command_result") or [])
                    if events and events[-1].get("command_payload") == event.get("command_payload") and event.get("status") != "send_attempted":
                        events[-1] = event
                    else:
                        events.append(event)
                    row["command_result"] = events
                    row["ack_result"] = [
                        {
                            "command_payload": item.get("command_payload"),
                            "ack_received": item.get("ack_received"),
                            "ack_payload": item.get("ack_payload"),
                            "timeout": item.get("timeout"),
                            "error": item.get("error"),
                        }
                        for item in events
                    ]
                    payload["command_events"] = [
                        event_item
                        for result_row in setup_results
                        for event_item in list(result_row.get("command_result") or [])
                    ]
                    _refresh("running")

                sent, command_results = send_mode2_setup_commands(
                    analyzer,
                    command_plan,
                    command_timeout_s=max(0.01, min(float(command_timeout_s or 5.0), row_deadline - time.monotonic())),
                    on_event=_record_event,
                )
                row["commands_sent"] = sent
                row["command_result"] = command_results
                row["ack_result"] = [
                    {
                        "command_payload": item.get("command_payload"),
                        "ack_received": item.get("ack_received"),
                        "ack_payload": item.get("ack_payload"),
                        "timeout": item.get("timeout"),
                        "error": item.get("error"),
                    }
                    for item in command_results
                ]
                row["mode_setup_command_sent"] = bool(sent)
                command_timeout = any(bool(item.get("timeout")) for item in command_results)
                command_error = next((str(item.get("error") or "") for item in command_results if item.get("error")), "")
                no_ack = any(item.get("sent") and not item.get("ack_received") for item in command_results)
                if command_timeout:
                    row["error_type"] = "command_timeout"
                    row["status"] = "timeout"
                    row["final_status"] = "timeout"
                    row["onsite_suggestion"] = "MODE2 setup command timed out; partial artifact preserved."
                elif command_error:
                    row["error_type"] = "mode_setup_command_failed"
                    row["status"] = "failed"
                    row["final_status"] = "failed"
                    row["onsite_suggestion"] = command_error
                elif no_ack:
                    row["error_type"] = "mode_setup_no_ack"
                    row["status"] = "failed"
                    row["final_status"] = "failed"
                    row["onsite_suggestion"] = "MODE2 setup command sent but ACK was not received within the command timeout."
            except TimeoutError as exc:
                row.update(
                    {
                        "error_type": "device_timeout",
                        "status": "timeout",
                        "final_status": "timeout",
                        "onsite_suggestion": str(exc),
                    }
                )
            except Exception as exc:
                row.update(
                    {
                        "error_type": "mode_setup_command_failed",
                        "status": "failed",
                        "final_status": "failed",
                        "onsite_suggestion": str(exc),
                    }
                )
            finally:
                if analyzer is not None:
                    close_timeout = max(0.01, min(2.0, row_deadline - time.monotonic()))
                    _run_with_timeout(close_timeout, lambda: _close_analyzer(analyzer))

            if row.get("status") not in {"timeout", "failed"}:
                remaining = max(0.01, row_deadline - time.monotonic())
                after_result, after_timeout, after_error = _bounded_diagnostic(
                    cfg,
                    analyzer_factory=analyzer_factory,
                    timeout_s=min(float(payload["timeouts"]["diagnostic_timeout_s"]), remaining),
                )
                row.update(_diagnostic_snapshot(after_result, "after"))
                if after_timeout:
                    row.update(
                        {
                            "error_type": "after_diagnostic_timeout",
                            "status": "timeout",
                            "final_status": "timeout",
                            "onsite_suggestion": after_error,
                        }
                    )
                elif after_error:
                    row.update(
                        {
                            "error_type": "after_diagnostic_error",
                            "status": "failed",
                            "final_status": "failed",
                            "onsite_suggestion": after_error,
                        }
                    )
                else:
                    error_type = str((after_result or {}).get("error_type") or "")
                    row["error_type"] = error_type
                    row["final_status"] = "ready" if error_type == "ok" else "not_ready"
                    row["status"] = "success" if error_type == "ok" else "failed"
                    row["onsite_suggestion"] = str((after_result or {}).get("suggested_onsite_check") or "")

            row["completed_at"] = _utc_now()
            row["elapsed_s"] = round(time.monotonic() - row_start_monotonic, 3)
            _refresh()
    except KeyboardInterrupt:
        payload["status"] = "interrupted"
        raise
    except BaseException as exc:
        payload["status"] = "failed"
        payload["error"] = str(exc)
    finally:
        payload["completed_at"] = _utc_now()
        payload["partial_results"] = setup_results
        payload["command_events"] = [
            event
            for row in setup_results
            for event in list(row.get("command_result") or [])
        ]
        if effective_dry_run:
            payload["status"] = "planned"
            payload["analyzer_precheck_diagnostics"] = _not_run_diagnostic_payload(
                selected=selected,
                requested_analyzers=requested_analyzers,
                reason=setup_reason,
            )
        else:
            payload["analyzer_precheck_diagnostics"] = _post_diagnostics_from_setup_results(
                requested_analyzers=requested_analyzers,
                rows=setup_results,
            )
            if payload.get("status") == "interrupted":
                pass
            elif any(str(row.get("status") or "") == "timeout" for row in setup_results):
                payload["status"] = "timeout"
            elif any(not _row_ready(row) for row in setup_results):
                payload["status"] = "partial"
            elif setup_results:
                payload["status"] = "success"
            else:
                payload["status"] = "failed"
        payload["summary"] = _summarize_payload(payload)
        _refresh(str(payload.get("status") or "failed"))

    return payload


def render_analyzer_mode2_setup_report(payload: Mapping[str, Any]) -> str:
    lines = [
        "# Run-001/A1 analyzer MODE2 communication setup",
        "",
        f"- status: {payload.get('status')}",
        f"- dry_run: {payload.get('dry_run')}",
        f"- setup_request_status: {payload.get('setup_request_status')}",
        f"- mode_setup_command_sent: {payload.get('mode_setup_command_sent')}",
        f"- persistent_write_command_sent: {payload.get('persistent_write_command_sent')}",
        f"- calibration_write_command_sent: {payload.get('calibration_write_command_sent')}",
        f"- a1_no_write_rerun_allowed: {payload.get('summary', {}).get('a1_no_write_rerun_allowed')}",
        "",
        "## Timeouts",
    ]
    for key, value in dict(payload.get("timeouts") or {}).items():
        lines.append(f"- {key}: {value}")
    lines.extend(["", "## Command source audit"])
    for item in list(payload.get("command_source_audit") or []):
        lines.append(f"- {item.get('command')}: {item.get('driver')} ({item.get('purpose')})")
    lines.extend(["", "## Analyzer results"])
    for item in list(payload.get("analyzers") or []):
        lines.extend(
            [
                "",
                f"### {item.get('logical_id')} / {item.get('physical_label')}",
                f"- status: {item.get('status')}",
                f"- port: {item.get('port')} @ {item.get('baudrate')}",
                f"- expected_device_id: {item.get('expected_device_id')}",
                f"- elapsed_s: {item.get('elapsed_s')}",
                f"- before_bytes_received: {item.get('before_bytes_received')}",
                f"- before_mode_detected: {item.get('before_mode_detected')}",
                f"- before_mode2_detected: {item.get('before_mode2_detected')}",
                f"- commands_planned: {', '.join(item.get('commands_planned') or item.get('command_plan') or [])}",
                f"- commands_sent: {', '.join(item.get('commands_sent') or []) or '-'}",
                f"- after_bytes_received: {item.get('after_bytes_received')}",
                f"- after_frame_parse: {item.get('after_frame_parse')}",
                f"- after_mode2_detected: {item.get('after_mode2_detected')}",
                f"- after_active_send_detected: {item.get('after_active_send_detected')}",
                f"- after_device_id: {item.get('after_device_id') or '-'}",
                f"- final_status: {item.get('final_status')}",
                f"- error_type: {item.get('error_type')}",
                f"- onsite_suggestion: {item.get('onsite_suggestion')}",
                "",
                "#### Command audit",
            ]
        )
        for event in list(item.get("command_result") or []):
            lines.append(
                "- "
                f"{event.get('command_name')} | attempted_at={event.get('send_attempted_at') or '-'} "
                f"| sent={event.get('sent')} | ack={event.get('ack_received')} "
                f"| timeout={event.get('timeout')} | status={event.get('status')} "
                f"| error={event.get('error') or '-'}"
            )
    lines.extend(
        [
            "",
            "This artifact records communication setup only. It does not authorize A1 execution, A2, H2O, calibration writes, V2 cutover, or real acceptance.",
            "",
        ]
    )
    return "\n".join(lines)


def write_analyzer_mode2_setup_artifacts(output_dir: str | Path, payload: Mapping[str, Any]) -> dict[str, str]:
    from .run001_a1_analyzer_diagnostics import write_analyzer_precheck_diagnostics

    directory = Path(output_dir)
    directory.mkdir(parents=True, exist_ok=True)
    setup_json = directory / "analyzer_mode2_setup.json"
    setup_report = directory / "analyzer_mode2_setup.md"
    setup_json.write_text(json.dumps(dict(payload), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    setup_report.write_text(render_analyzer_mode2_setup_report(payload), encoding="utf-8")
    diagnostics_paths = write_analyzer_precheck_diagnostics(
        directory,
        dict(payload.get("analyzer_precheck_diagnostics") or {}),
    )
    return {
        "analyzer_mode2_setup_json": str(setup_json),
        "analyzer_mode2_setup_report": str(setup_report),
        "analyzer_precheck_diagnostics_json": diagnostics_paths["json"],
        "analyzer_precheck_diagnostics_report": diagnostics_paths["report"],
    }


def run_analyzer_mode2_setup(
    raw_cfg: Mapping[str, Any],
    *,
    output_dir: str | Path,
    analyzers: Optional[list[str]] = None,
    dry_run: bool = True,
    set_mode2_active_send: bool = False,
    confirm_mode2_communication_setup: bool = False,
    timeout_s: float = 20.0,
    command_timeout_s: float = 5.0,
    device_timeout_s: float = 30.0,
    total_timeout_s: Optional[float] = None,
    analyzer_factory: Optional[Callable[[Mapping[str, Any]], Any]] = None,
    config_path: str = "",
) -> tuple[dict[str, Any], dict[str, str]]:
    latest_written: dict[str, str] = {}

    def _artifact_update(payload: Mapping[str, Any]) -> None:
        nonlocal latest_written
        latest_written = write_analyzer_mode2_setup_artifacts(output_dir, payload)

    payload = build_analyzer_mode2_setup_payload(
        raw_cfg,
        analyzers=analyzers,
        dry_run=dry_run,
        set_mode2_active_send=set_mode2_active_send,
        confirm_mode2_communication_setup=confirm_mode2_communication_setup,
        timeout_s=timeout_s,
        command_timeout_s=command_timeout_s,
        device_timeout_s=device_timeout_s,
        total_timeout_s=total_timeout_s,
        analyzer_factory=analyzer_factory,
        config_path=str(config_path or ""),
        git_commit=_git_value(["rev-parse", "HEAD"]),
        branch=_git_value(["branch", "--show-current"]),
        artifact_update=_artifact_update,
    )
    if not latest_written:
        latest_written = write_analyzer_mode2_setup_artifacts(output_dir, payload)
    return payload, latest_written
