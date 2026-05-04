from __future__ import annotations

from pathlib import Path
from typing import Any

from .engineering_isolation_gate_evaluator import (
    ENGINEERING_ISOLATION_GATE_ARTIFACT_KEY,
    ENGINEERING_ISOLATION_GATE_REVIEWER_ARTIFACT_KEY,
)


def build_engineering_isolation_gate_artifact_entry(
    *,
    artifact_path: Any,
    reviewer_artifact_path: Any = None,
    manifest_section: dict[str, Any] | None = None,
    reviewer_manifest_section: dict[str, Any] | None = None,
) -> dict[str, Any]:
    manifest_payload = dict(manifest_section or {})
    reviewer_manifest_payload = dict(reviewer_manifest_section or {})
    path_text = str(artifact_path or manifest_payload.get("path") or "").strip()
    reviewer_path_text = str(
        reviewer_artifact_path
        or reviewer_manifest_payload.get("path")
        or manifest_payload.get("reviewer_path")
        or ""
    ).strip()
    title_text = str(
        reviewer_manifest_payload.get("title_text")
        or manifest_payload.get("title_text")
        or "Engineering Isolation Reviewer Bridge Gate / 工程隔离准入桥接总闸"
    ).strip()
    summary_text = str(
        reviewer_manifest_payload.get("summary_text")
        or manifest_payload.get("summary_text")
        or ""
    ).strip()
    status_line = str(
        reviewer_manifest_payload.get("status_line")
        or manifest_payload.get("status_line")
        or manifest_payload.get("gate_level_display")
        or ""
    ).strip()
    gate_level = str(manifest_payload.get("gate_level") or "").strip()
    bridge_note_text = str(
        reviewer_manifest_payload.get("bridge_note_text")
        or manifest_payload.get("bridge_note_text")
        or "Reviewer/admission bridge only; not formal admission approval; not real acceptance."
    ).strip()
    blocker_lines = _text_list(
        reviewer_manifest_payload.get("blocker_lines")
        or manifest_payload.get("blocker_lines")
        or manifest_payload.get("blockers")
        or []
    )
    warning_lines = _text_list(
        reviewer_manifest_payload.get("warning_lines")
        or manifest_payload.get("warning_lines")
        or manifest_payload.get("warnings")
        or []
    )
    unresolved_gap_lines = _text_list(
        reviewer_manifest_payload.get("unresolved_gap_lines")
        or manifest_payload.get("unresolved_gap_lines")
        or manifest_payload.get("unresolved_gaps")
        or []
    )
    next_action_lines = _text_list(
        reviewer_manifest_payload.get("suggested_next_action_lines")
        or manifest_payload.get("suggested_next_action_lines")
        or manifest_payload.get("suggested_next_actions")
        or []
    )
    required_evidence_categories = _text_list(
        manifest_payload.get("required_evidence_categories")
        or dict(manifest_payload.get("stage3_real_validation_plan_draft_input") or {}).get("required_evidence_categories")
        or []
    )
    boundary_filters = _text_list(
        manifest_payload.get("boundary_statements")
        or reviewer_manifest_payload.get("boundary_statements")
        or []
    )
    role_status_display = " | ".join(
        part
        for part in (
            status_line,
            f"gate_level={gate_level}" if gate_level else "",
            bridge_note_text,
        )
        if str(part).strip()
    )
    card_lines = [
        summary_text,
        status_line,
        bridge_note_text,
        f"blockers: {len(blocker_lines)}",
        f"warnings: {len(warning_lines)}",
        f"unresolved gaps: {len(unresolved_gap_lines)}",
        f"next actions: {len(next_action_lines)}",
        "required evidence categories: " + (", ".join(required_evidence_categories) if required_evidence_categories else "--"),
        f"JSON: {path_text}" if path_text else "",
        f"Markdown: {reviewer_path_text}" if reviewer_path_text else "",
    ]
    entry_lines = [
        summary_text,
        status_line,
        bridge_note_text,
        *[f"blocker: {item}" for item in blocker_lines],
        *[f"warning: {item}" for item in warning_lines],
        *[f"gap: {item}" for item in unresolved_gap_lines],
        *[f"next: {item}" for item in next_action_lines],
    ]
    return {
        "available": bool(path_text or reviewer_path_text) and bool(manifest_payload or reviewer_manifest_payload),
        "artifact_key": ENGINEERING_ISOLATION_GATE_ARTIFACT_KEY,
        "artifact_type": str(manifest_payload.get("artifact_type") or ENGINEERING_ISOLATION_GATE_ARTIFACT_KEY),
        "reviewer_artifact_key": ENGINEERING_ISOLATION_GATE_REVIEWER_ARTIFACT_KEY,
        "reviewer_artifact_type": str(
            reviewer_manifest_payload.get("artifact_type") or ENGINEERING_ISOLATION_GATE_REVIEWER_ARTIFACT_KEY
        ),
        "title_text": title_text,
        "name_text": title_text,
        "filename": Path(path_text).name if path_text else "",
        "reviewer_filename": Path(reviewer_path_text).name if reviewer_path_text else "",
        "path": path_text,
        "reviewer_path": reviewer_path_text,
        "summary_text": summary_text,
        "status_line": status_line,
        "bridge_note_text": bridge_note_text,
        "gate_level": gate_level,
        "blocker_lines": blocker_lines,
        "warning_lines": warning_lines,
        "unresolved_gap_lines": unresolved_gap_lines,
        "suggested_next_action_lines": next_action_lines,
        "required_evidence_categories": required_evidence_categories,
        "anchor_id": "engineering-isolation-gate",
        "navigation_id": "engineering-isolation-gate",
        "anchor_label": title_text,
        "phase_filters": [
            "step2_engineering_isolation_bridge",
            "step2_tail_stage3_bridge",
            "engineering_isolation_bridge",
        ],
        "artifact_role_filters": ["execution_summary", "formal_analysis"],
        "standard_family_filters": _text_list(manifest_payload.get("standard_families") or []),
        "evidence_category_filters": list(required_evidence_categories),
        "boundary_filters": boundary_filters
        or [
            "reviewer / admission bridge only",
            "simulation / offline / headless only",
            "not formal admission approval",
            "not real acceptance",
        ],
        "entry_text": "\n".join(line for line in entry_lines if str(line).strip()),
        "card_text": "\n".join(line for line in card_lines if str(line).strip()),
        "role_status_display": role_status_display,
        "note_text": summary_text or bridge_note_text or status_line,
        "not_real_acceptance_evidence": bool(
            reviewer_manifest_payload.get(
                "not_real_acceptance_evidence",
                manifest_payload.get("not_real_acceptance_evidence", True),
            )
        ),
    }


def _text_list(values: Any) -> list[str]:
    rows: list[str] = []
    for item in list(values or []):
        if isinstance(item, dict):
            text = str(dict(item or {}).get("summary") or "").strip()
        else:
            text = str(item or "").strip()
        if text and text not in rows:
            rows.append(text)
    return rows
