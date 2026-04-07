from __future__ import annotations

from pathlib import Path
from typing import Any


PHASE_TRANSITION_BRIDGE_REVIEWER_ARTIFACT_KEY = "phase_transition_bridge_reviewer_artifact"


def build_phase_transition_bridge_reviewer_artifact_entry(
    *,
    artifact_path: Any,
    manifest_section: dict[str, Any] | None = None,
    reviewer_section: dict[str, Any] | None = None,
) -> dict[str, Any]:
    path_text = str(
        artifact_path
        or dict(manifest_section or {}).get("path")
        or ""
    ).strip()
    manifest_payload = dict(manifest_section or {})
    reviewer_payload = dict(reviewer_section or {})
    reviewer_display = dict(reviewer_payload.get("display", {}) or {})
    reviewer_raw = dict(reviewer_payload.get("raw", {}) or {})

    title_text = str(reviewer_display.get("title_text") or "阶段准入桥").strip() or "阶段准入桥"
    summary_text = str(manifest_payload.get("summary_text") or reviewer_display.get("summary_text") or "").strip()
    status_line = str(manifest_payload.get("status_line") or reviewer_display.get("status_line") or "").strip()
    current_stage_text = str(
        manifest_payload.get("current_stage_text") or reviewer_display.get("current_stage_text") or ""
    ).strip()
    next_stage_text = str(
        manifest_payload.get("next_stage_text") or reviewer_display.get("next_stage_text") or ""
    ).strip()
    engineering_isolation_text = str(
        manifest_payload.get("engineering_isolation_text") or reviewer_display.get("engineering_isolation_text") or ""
    ).strip()
    real_acceptance_text = str(
        manifest_payload.get("real_acceptance_text") or reviewer_display.get("real_acceptance_text") or ""
    ).strip()
    execute_now_text = str(manifest_payload.get("execute_now_text") or reviewer_display.get("execute_now_text") or "").strip()
    defer_to_stage3_text = str(
        manifest_payload.get("defer_to_stage3_text") or reviewer_display.get("defer_to_stage3_text") or ""
    ).strip()
    blocking_text = str(manifest_payload.get("blocking_text") or reviewer_display.get("blocking_text") or "").strip()
    warning_text = str(manifest_payload.get("warning_text") or reviewer_display.get("warning_text") or "").strip()
    stage_marker_text = current_stage_text or next_stage_text
    status_badge_text = warning_text or status_line

    entry_lines = [
        summary_text,
        status_line,
        current_stage_text,
        next_stage_text,
        engineering_isolation_text,
        real_acceptance_text,
        execute_now_text,
        defer_to_stage3_text,
        blocking_text,
        warning_text,
    ]
    role_status_parts = [
        stage_marker_text,
        engineering_isolation_text,
        real_acceptance_text,
        warning_text,
    ]
    return {
        "available": bool(path_text) and bool(manifest_payload.get("available", True)),
        "artifact_key": PHASE_TRANSITION_BRIDGE_REVIEWER_ARTIFACT_KEY,
        "artifact_type": str(
            manifest_payload.get("artifact_type")
            or PHASE_TRANSITION_BRIDGE_REVIEWER_ARTIFACT_KEY
        ),
        "title_text": title_text,
        "name_text": title_text,
        "filename": Path(path_text).name if path_text else "",
        "path": path_text,
        "summary_text": summary_text,
        "status_line": status_line,
        "stage_marker_text": stage_marker_text,
        "status_badge_text": status_badge_text,
        "current_stage_text": current_stage_text,
        "next_stage_text": next_stage_text,
        "engineering_isolation_text": engineering_isolation_text,
        "real_acceptance_text": real_acceptance_text,
        "execute_now_text": execute_now_text,
        "defer_to_stage3_text": defer_to_stage3_text,
        "blocking_text": blocking_text,
        "warning_text": warning_text,
        "entry_text": "\n".join(line for line in entry_lines if str(line).strip()),
        "role_status_display": " | ".join(part for part in role_status_parts if str(part).strip()),
        "note_text": summary_text or warning_text or status_line,
        "ready_for_engineering_isolation": bool(reviewer_raw.get("ready_for_engineering_isolation", False)),
        "real_acceptance_ready": bool(reviewer_raw.get("real_acceptance_ready", False)),
        "not_real_acceptance_evidence": bool(manifest_payload.get("not_real_acceptance_evidence", True)),
    }
