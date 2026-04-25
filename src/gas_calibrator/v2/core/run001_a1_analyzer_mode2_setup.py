from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Callable, Mapping, Optional

from .run001_a1_analyzer_diagnostics import (
    _make_default_analyzer,
    _open_analyzer,
    _close_analyzer,
    _selected_analyzer_configs,
    build_analyzer_precheck_diagnostics,
    diagnose_analyzer_config,
)


MODE2_SETUP_ALLOWED_COMMANDS = (
    "MODE,YGAS,FFF,2",
    "SETCOMWAY,YGAS,FFF,1",
)
MODE2_SETUP_COMMAND_SOURCE_AUDIT = (
    {
        "command": "MODE,YGAS,FFF,2",
        "driver": "gas_calibrator.devices.gas_analyzer.GasAnalyzer.set_mode_with_ack",
        "purpose": "communication_mode_setup",
    },
    {
        "command": "SETCOMWAY,YGAS,FFF,1",
        "driver": "gas_calibrator.devices.gas_analyzer.GasAnalyzer.set_comm_way_with_ack",
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


class Mode2SetupSafetyError(RuntimeError):
    """Raised when a Run-001/A1 analyzer MODE2 setup command is unsafe."""


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


def _call_mode_command(analyzer: Any, method_names: tuple[str, ...], *args: Any) -> tuple[bool, str]:
    for method_name in method_names:
        method = getattr(analyzer, method_name, None)
        if not callable(method):
            continue
        if method_name.endswith("_with_ack"):
            result = method(*args, require_ack=True)
        else:
            result = method(*args)
        return result is not False, method_name
    raise Mode2SetupSafetyError(f"Analyzer driver lacks required method: {method_names[0]}")


def send_mode2_setup_commands(analyzer: Any, command_plan: list[str]) -> tuple[list[str], list[dict[str, Any]]]:
    validate_mode2_setup_command_plan(command_plan)
    sent: list[str] = []
    results: list[dict[str, Any]] = []
    for command in command_plan:
        normalized = _normalize_command(command)
        try:
            if normalized == "MODE,YGAS,FFF,2":
                acked, method_name = _call_mode_command(analyzer, ("set_mode_with_ack", "set_mode"), 2)
            elif normalized == "SETCOMWAY,YGAS,FFF,1":
                acked, method_name = _call_mode_command(
                    analyzer,
                    ("set_comm_way_with_ack", "set_comm_way"),
                    True,
                )
            else:
                raise Mode2SetupSafetyError(f"Unsupported MODE2 setup command: {normalized}")
            sent.append(normalized)
            results.append(
                {
                    "command": normalized,
                    "method": method_name,
                    "ack": bool(acked),
                    "status": "ok" if acked else "no_ack",
                    "error_message": "",
                }
            )
        except Exception as exc:
            results.append(
                {
                    "command": normalized,
                    "method": "",
                    "ack": False,
                    "status": "error",
                    "error_message": str(exc),
                }
            )
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
        "generated_at": datetime.now(timezone.utc).isoformat(),
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


def _make_default_setup_analyzer(cfg: Mapping[str, Any], timeout_s: float) -> Any:
    return _make_default_analyzer(cfg, timeout_s)


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


def build_analyzer_mode2_setup_payload(
    raw_cfg: Mapping[str, Any],
    *,
    analyzers: Optional[list[str]] = None,
    dry_run: bool = True,
    set_mode2_active_send: bool = False,
    confirm_mode2_communication_setup: bool = False,
    timeout_s: float = 20.0,
    analyzer_factory: Optional[Callable[[Mapping[str, Any]], Any]] = None,
) -> dict[str, Any]:
    requested_analyzers = list(analyzers or [])
    selected = _selected_analyzer_configs(raw_cfg, None, requested_analyzers)
    send_allowed, setup_reason = _setup_allowed(
        set_mode2_active_send=set_mode2_active_send,
        confirm_mode2_communication_setup=confirm_mode2_communication_setup,
    )
    effective_dry_run = bool(dry_run or not send_allowed)
    command_plan_error = ""
    setup_results: list[dict[str, Any]] = []
    post_diagnostics: dict[str, Any]

    for cfg in selected:
        command_plan = build_mode2_setup_command_plan(cfg)
        before_result: Optional[dict[str, Any]] = None
        after_result: Optional[dict[str, Any]] = None
        commands_sent: list[str] = []
        command_result: list[dict[str, Any]] = []
        error_type = "dry_run" if effective_dry_run else "ok"
        final_status = "planned" if effective_dry_run else "ready"
        onsite_suggestion = "Dry-run only; review command plan and run with explicit confirmation onsite."

        try:
            validate_mode2_setup_command_plan(command_plan)
        except Exception as exc:
            command_plan_error = str(exc)
            error_type = "unsafe_command_plan"
            final_status = "failed"
            onsite_suggestion = str(exc)

        if not effective_dry_run and not command_plan_error:
            before_result = diagnose_analyzer_config(
                cfg,
                analyzer_factory=analyzer_factory,
                allow_read_query=False,
                timeout_s=timeout_s,
            )
            analyzer: Any = None
            try:
                factory = analyzer_factory or (lambda item: _make_default_setup_analyzer(item, timeout_s))
                analyzer = factory(cfg)
                _open_analyzer(analyzer)
                commands_sent, command_result = send_mode2_setup_commands(analyzer, command_plan)
            except Exception as exc:
                error_type = "mode_setup_command_failed"
                final_status = "failed"
                onsite_suggestion = str(exc)
            finally:
                if analyzer is not None:
                    _close_analyzer(analyzer)

            after_result = diagnose_analyzer_config(
                cfg,
                analyzer_factory=analyzer_factory,
                allow_read_query=False,
                timeout_s=timeout_s,
            )
            if error_type == "ok":
                error_type = str(after_result.get("error_type") or "")
                final_status = "ready" if error_type == "ok" else "not_ready"
                onsite_suggestion = str(after_result.get("suggested_onsite_check") or "")

        row = {
            **dict(cfg),
            "port": str(cfg.get("configured_port") or ""),
            "expected_device_id": str(cfg.get("configured_device_id") or ""),
            "dry_run": bool(effective_dry_run),
            "setup_request_status": setup_reason,
            "command_plan": command_plan,
            "commands_sent": commands_sent,
            "command_result": command_result,
            "mode_setup_command_sent": bool(commands_sent),
            "persistent_write_command_sent": False,
            "calibration_write_command_sent": False,
            **_diagnostic_snapshot(before_result, "before"),
            **_diagnostic_snapshot(after_result, "after"),
            "final_status": final_status,
            "error_type": error_type,
            "onsite_suggestion": onsite_suggestion,
        }
        setup_results.append(row)

    if effective_dry_run:
        post_diagnostics = _not_run_diagnostic_payload(
            selected=selected,
            requested_analyzers=requested_analyzers,
            reason=setup_reason,
        )
    else:
        post_diagnostics = build_analyzer_precheck_diagnostics(
            raw_cfg,
            analyzers=requested_analyzers,
            read_only=True,
            allow_read_query=False,
            timeout_s=timeout_s,
            analyzer_factory=analyzer_factory,
        )

    flat_commands = [command for row in setup_results for command in list(row.get("commands_sent") or [])]
    unsafe_sent = any(command_contains_forbidden_mode2_setup_token(command) for command in flat_commands)
    ready_count = sum(1 for row in setup_results if _row_ready(row))
    a1_rerun_allowed = bool(
        not effective_dry_run
        and len(setup_results) == 4
        and ready_count == 4
        and not unsafe_sent
        and not command_plan_error
    )
    return {
        "schema_version": "run001_a1.analyzer_mode2_setup.1",
        "artifact_type": "run001_a1_analyzer_mode2_setup",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "run_id": "Run-001/A1",
        "scope": "GA01-GA04 MODE2 active-send communication setup only",
        "dry_run": bool(effective_dry_run),
        "set_mode2_active_send": bool(set_mode2_active_send),
        "confirm_mode2_communication_setup": bool(confirm_mode2_communication_setup),
        "setup_request_status": setup_reason,
        "command_source_audit": list(MODE2_SETUP_COMMAND_SOURCE_AUDIT),
        "allowed_communication_commands": list(MODE2_SETUP_ALLOWED_COMMANDS),
        "read_only_query_commands": list(READ_ONLY_QUERY_COMMANDS),
        "forbidden_command_tokens": list(MODE2_SETUP_FORBIDDEN_COMMAND_TOKENS),
        "forbidden_command_plan_error": command_plan_error,
        "persistent_write_command_sent": False,
        "calibration_write_command_sent": False,
        "mode_setup_command_sent": bool(flat_commands),
        "commands_sent": flat_commands,
        "protected_paths_touched": [],
        "v1_production_flow_touched": False,
        "run_app_touched": False,
        "a1_execute_invoked": False,
        "a2_invoked": False,
        "h2o_invoked": False,
        "full_group_invoked": False,
        "analyzers": setup_results,
        "analyzer_precheck_diagnostics": post_diagnostics,
        "summary": {
            "total": len(setup_results),
            "ready": ready_count,
            "failed": len(setup_results) - ready_count,
            "commands_sent_count": len(flat_commands),
            "persistent_write_command_sent": False,
            "calibration_write_command_sent": False,
            "mode_setup_command_sent": bool(flat_commands),
            "a1_no_write_rerun_allowed": a1_rerun_allowed,
            "not_real_acceptance_evidence": True,
        },
        "not_real_acceptance_evidence": True,
    }


def render_analyzer_mode2_setup_report(payload: Mapping[str, Any]) -> str:
    lines = [
        "# Run-001/A1 analyzer MODE2 communication setup",
        "",
        f"- dry_run: {payload.get('dry_run')}",
        f"- setup_request_status: {payload.get('setup_request_status')}",
        f"- mode_setup_command_sent: {payload.get('mode_setup_command_sent')}",
        f"- persistent_write_command_sent: {payload.get('persistent_write_command_sent')}",
        f"- calibration_write_command_sent: {payload.get('calibration_write_command_sent')}",
        f"- a1_no_write_rerun_allowed: {payload.get('summary', {}).get('a1_no_write_rerun_allowed')}",
        "",
        "## Command source audit",
    ]
    for item in list(payload.get("command_source_audit") or []):
        lines.append(f"- {item.get('command')}: {item.get('driver')} ({item.get('purpose')})")
    lines.extend(["", "## Analyzer results"])
    for item in list(payload.get("analyzers") or []):
        lines.extend(
            [
                "",
                f"### {item.get('logical_id')} / {item.get('physical_label')}",
                f"- port: {item.get('port')} @ {item.get('baudrate')}",
                f"- expected_device_id: {item.get('expected_device_id')}",
                f"- before_bytes_received: {item.get('before_bytes_received')}",
                f"- before_mode_detected: {item.get('before_mode_detected')}",
                f"- before_mode2_detected: {item.get('before_mode2_detected')}",
                f"- command_plan: {', '.join(item.get('command_plan') or [])}",
                f"- commands_sent: {', '.join(item.get('commands_sent') or []) or '-'}",
                f"- after_bytes_received: {item.get('after_bytes_received')}",
                f"- after_frame_parse: {item.get('after_frame_parse')}",
                f"- after_mode2_detected: {item.get('after_mode2_detected')}",
                f"- after_active_send_detected: {item.get('after_active_send_detected')}",
                f"- after_device_id: {item.get('after_device_id') or '-'}",
                f"- final_status: {item.get('final_status')}",
                f"- error_type: {item.get('error_type')}",
                f"- onsite_suggestion: {item.get('onsite_suggestion')}",
            ]
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
