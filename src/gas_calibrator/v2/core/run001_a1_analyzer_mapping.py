from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import re
from typing import Any, Mapping, Optional

from .run001_a1_analyzer_mode2_setup import (
    MODE2_SETUP_ALLOWED_COMMANDS,
    MODE2_SETUP_COMMAND_TARGET_ID,
    MODE2_SETUP_FORBIDDEN_COMMAND_TOKENS,
)


SITE_CONFIRM_LABEL = "待现场确认"
OLD_GA_ID_ASSUMPTION_NOTE = "当前不能继续使用旧的 GA01=001 / GA02=002 / GA03=003 / GA04=004 假设"
SITE_CONFIRM_NOTE = "必须由现场确认 device_id 091/003/023/012 对应的实体标签"
ENABLED_LIST_NOTE = "A1 后续应以实测 port + device_id + 现场 label 作为 enabled analyzer list"
NO_AUTO_CONFIG_NOTE = "不自动修改正式配置"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_port(value: Any) -> str:
    return str(value or "").strip().upper()


def _port_sort_key(value: Any) -> tuple[str, int, str]:
    text = _normalize_port(value)
    match = re.match(r"^([A-Z]+)(\d+)$", text)
    if match:
        return match.group(1), int(match.group(2)), text
    return text, -1, text


def _observed_mode(row: Mapping[str, Any]) -> int:
    try:
        return int(row.get("observed_mode") or 0)
    except Exception:
        return 0


def _mode_label(mode: int) -> str:
    if int(mode or 0) == 1:
        return "MODE1"
    if int(mode or 0) == 2:
        return "MODE2"
    return "UNKNOWN"


def _is_detected_analyzer(row: Mapping[str, Any]) -> bool:
    return bool(
        str(row.get("observed_device_id") or "").strip()
        and bool(row.get("active_send_detected"))
        and _observed_mode(row) in {1, 2}
    )


def _candidate_suggested_action(row: Mapping[str, Any]) -> str:
    mode = _observed_mode(row)
    if mode == 2 and bool(row.get("active_send_detected")):
        return "keep"
    if mode == 1 and bool(row.get("active_send_detected")):
        return "set MODE2 + active-send"
    return "site confirmation required"


def build_analyzer_mapping_candidate_payload(
    diagnostics_payload: Mapping[str, Any],
    *,
    source_diagnostics_path: str | Path = "",
    generated_at: Optional[str] = None,
) -> dict[str, Any]:
    rows = sorted(
        [dict(item) for item in list(diagnostics_payload.get("analyzers") or [])],
        key=lambda item: _port_sort_key(item.get("configured_port") or item.get("port")),
    )
    detected_rows = [row for row in rows if _is_detected_analyzer(row)]
    candidates: list[dict[str, Any]] = []
    for index, row in enumerate(detected_rows):
        mode = _observed_mode(row)
        port = _normalize_port(row.get("configured_port") or row.get("port"))
        candidates.append(
            {
                "candidate_id": f"analyzer_candidate_{index}",
                "port": port,
                "detected_device_id": str(row.get("observed_device_id") or "").strip(),
                "detected_mode": _mode_label(mode),
                "active_send": bool(row.get("active_send_detected")),
                "suggested_action": _candidate_suggested_action(row),
                "physical_label": SITE_CONFIRM_LABEL,
                "source_logical_id": str(row.get("logical_id") or ""),
                "source_error_type": str(row.get("error_type") or ""),
                "source_mapping_suggestion": str(row.get("mapping_suggestion") or ""),
                "do_not_force_old_configured_device_id": True,
            }
        )

    no_data_ports = [
        _normalize_port(row.get("configured_port") or row.get("port"))
        for row in rows
        if str(row.get("error_type") or "") == "no_data"
    ]
    return {
        "schema_version": "run001_a1.analyzer_mapping_candidate.1",
        "artifact_type": "run001_a1_analyzer_mapping_candidate",
        "generated_at": generated_at or _utc_now(),
        "run_id": "Run-001/A1",
        "source_diagnostics_path": str(source_diagnostics_path or ""),
        "source_diagnostics_generated_at": str(diagnostics_payload.get("generated_at") or ""),
        "source_requested_ports": list(diagnostics_payload.get("requested_ports") or []),
        "read_only_source": bool(diagnostics_payload.get("read_only", True)),
        "not_real_acceptance_evidence": True,
        "not_auto_apply_to_formal_config": True,
        "old_ga01_ga04_id_assumption_valid": False,
        "mapping_policy_notes": [
            OLD_GA_ID_ASSUMPTION_NOTE,
            SITE_CONFIRM_NOTE,
            ENABLED_LIST_NOTE,
            NO_AUTO_CONFIG_NOTE,
        ],
        "detected_analyzers": candidates,
        "no_data_ports": no_data_ports,
        "summary": {
            "detected_analyzer_count": len(candidates),
            "mode1_setup_needed_count": sum(1 for item in candidates if item.get("detected_mode") == "MODE1"),
            "mode2_ready_count": sum(1 for item in candidates if item.get("detected_mode") == "MODE2"),
            "no_data_port_count": len(no_data_ports),
            "formal_config_updated": False,
        },
    }


def build_mode2_setup_target_plan_payload(
    mapping_candidate_payload: Mapping[str, Any],
    *,
    generated_at: Optional[str] = None,
) -> dict[str, Any]:
    candidates = [dict(item) for item in list(mapping_candidate_payload.get("detected_analyzers") or [])]
    setup_targets: list[dict[str, Any]] = []
    ready_analyzers: list[dict[str, Any]] = []
    for item in candidates:
        detected_mode = str(item.get("detected_mode") or "").strip().upper()
        port = _normalize_port(item.get("port"))
        detected_device_id = str(item.get("detected_device_id") or "").strip()
        if detected_mode == "MODE1":
            setup_targets.append(
                {
                    "candidate_id": str(item.get("candidate_id") or ""),
                    "port": port,
                    "expected_current_device_id": detected_device_id,
                    "detected_device_id": detected_device_id,
                    "detected_mode": detected_mode,
                    "active_send": bool(item.get("active_send")),
                    "command_target_id": MODE2_SETUP_COMMAND_TARGET_ID,
                    "commands": list(MODE2_SETUP_ALLOWED_COMMANDS),
                    "expected_after": {
                        "detected_mode": "MODE2",
                        "active_send": True,
                        "same_device_id_as_before": detected_device_id,
                    },
                    "physical_label": str(item.get("physical_label") or SITE_CONFIRM_LABEL),
                }
            )
        elif detected_mode == "MODE2" and bool(item.get("active_send")):
            ready_analyzers.append(
                {
                    "candidate_id": str(item.get("candidate_id") or ""),
                    "port": port,
                    "detected_device_id": detected_device_id,
                    "detected_mode": detected_mode,
                    "active_send": True,
                    "recommended_action": "keep",
                    "repeat_setup_recommended": False,
                    "physical_label": str(item.get("physical_label") or SITE_CONFIRM_LABEL),
                }
            )

    return {
        "schema_version": "run001_a1.analyzer_mode2_setup_target_plan.1",
        "artifact_type": "run001_a1_analyzer_mode2_setup_target_plan",
        "generated_at": generated_at or _utc_now(),
        "run_id": "Run-001/A1",
        "source_mapping_generated_at": str(mapping_candidate_payload.get("generated_at") or ""),
        "dry_run_plan_only": True,
        "not_real_acceptance_evidence": True,
        "not_auto_apply_to_formal_config": True,
        "actual_send_performed": False,
        "commands_sent": [],
        "persistent_write_command_sent": False,
        "calibration_write_command_sent": False,
        "command_target_id": MODE2_SETUP_COMMAND_TARGET_ID,
        "command_whitelist": list(MODE2_SETUP_ALLOWED_COMMANDS),
        "forbidden_tokens": list(MODE2_SETUP_FORBIDDEN_COMMAND_TOKENS),
        "setup_targets": setup_targets,
        "already_mode2_keep": ready_analyzers,
        "no_data_ports": list(mapping_candidate_payload.get("no_data_ports") or []),
        "policy_notes": [
            "只对当前 MODE1 的实测 analyzer 做受控 MODE2 setup",
            "COM37 / ID003 已是 MODE2，默认保持，不重复设置",
            "不执行 A1 --execute，不进入 A2，不写校准参数",
        ],
        "summary": {
            "setup_target_count": len(setup_targets),
            "already_mode2_keep_count": len(ready_analyzers),
            "commands_sent_count": 0,
            "formal_config_updated": False,
        },
    }


def render_analyzer_mapping_candidate_report(payload: Mapping[str, Any]) -> str:
    lines = [
        "# Run-001/A1 analyzer mapping candidate",
        "",
        f"- generated_at: {payload.get('generated_at')}",
        f"- source_diagnostics_path: {payload.get('source_diagnostics_path') or '-'}",
        f"- not_real_acceptance_evidence: {payload.get('not_real_acceptance_evidence')}",
        f"- not_auto_apply_to_formal_config: {payload.get('not_auto_apply_to_formal_config')}",
        "",
        "## Mapping policy",
    ]
    for note in list(payload.get("mapping_policy_notes") or []):
        lines.append(f"- {note}")
    lines.extend(["", "## Detected analyzers"])
    for item in list(payload.get("detected_analyzers") or []):
        lines.extend(
            [
                "",
                f"### {item.get('candidate_id')}",
                f"- port: {item.get('port')}",
                f"- detected_device_id: {item.get('detected_device_id')}",
                f"- detected_mode: {item.get('detected_mode')}",
                f"- active_send: {item.get('active_send')}",
                f"- suggested_action: {item.get('suggested_action')}",
                f"- physical_label: {item.get('physical_label')}",
            ]
        )
    lines.extend(["", "## No data ports"])
    for port in list(payload.get("no_data_ports") or []):
        lines.append(f"- {port}")
    lines.append("")
    return "\n".join(lines)


def render_mode2_setup_target_plan_report(payload: Mapping[str, Any]) -> str:
    lines = [
        "# Run-001/A1 MODE2 setup target plan",
        "",
        f"- generated_at: {payload.get('generated_at')}",
        f"- dry_run_plan_only: {payload.get('dry_run_plan_only')}",
        f"- actual_send_performed: {payload.get('actual_send_performed')}",
        f"- command_target_id: {payload.get('command_target_id')}",
        f"- commands_sent: {payload.get('commands_sent')}",
        f"- persistent_write_command_sent: {payload.get('persistent_write_command_sent')}",
        f"- calibration_write_command_sent: {payload.get('calibration_write_command_sent')}",
        "",
        "## Setup targets",
    ]
    for item in list(payload.get("setup_targets") or []):
        lines.extend(
            [
                "",
                f"### {item.get('port')}",
                f"- expected_current_device_id: {item.get('expected_current_device_id')}",
                f"- detected_mode: {item.get('detected_mode')}",
                f"- active_send: {item.get('active_send')}",
                f"- command_target_id: {item.get('command_target_id')}",
                f"- commands: {', '.join(str(command) for command in list(item.get('commands') or []))}",
                f"- expected_after: MODE2, active-send, same device_id {item.get('detected_device_id')}",
            ]
        )
    lines.extend(["", "## Already MODE2, keep"])
    for item in list(payload.get("already_mode2_keep") or []):
        lines.append(f"- {item.get('port')} / ID {item.get('detected_device_id')} / keep")
    lines.extend(["", "## Forbidden tokens"])
    for token in list(payload.get("forbidden_tokens") or []):
        lines.append(f"- {token}")
    lines.extend(["", "## Policy notes"])
    for note in list(payload.get("policy_notes") or []):
        lines.append(f"- {note}")
    lines.append("")
    return "\n".join(lines)


def write_analyzer_mapping_artifacts(
    output_dir: str | Path,
    mapping_candidate_payload: Mapping[str, Any],
    setup_target_plan_payload: Mapping[str, Any],
) -> dict[str, str]:
    directory = Path(output_dir)
    directory.mkdir(parents=True, exist_ok=True)
    mapping_json = directory / "analyzer_mapping_candidate.json"
    mapping_md = directory / "analyzer_mapping_candidate.md"
    setup_json = directory / "setup_target_plan.json"
    setup_md = directory / "setup_target_plan.md"
    mapping_json.write_text(json.dumps(dict(mapping_candidate_payload), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    mapping_md.write_text(render_analyzer_mapping_candidate_report(mapping_candidate_payload), encoding="utf-8")
    setup_json.write_text(json.dumps(dict(setup_target_plan_payload), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    setup_md.write_text(render_mode2_setup_target_plan_report(setup_target_plan_payload), encoding="utf-8")
    return {
        "analyzer_mapping_candidate_json": str(mapping_json),
        "analyzer_mapping_candidate_md": str(mapping_md),
        "setup_target_plan_json": str(setup_json),
        "setup_target_plan_md": str(setup_md),
    }
