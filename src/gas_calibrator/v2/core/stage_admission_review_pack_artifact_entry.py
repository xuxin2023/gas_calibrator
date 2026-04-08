from __future__ import annotations

from pathlib import Path
from typing import Any


STAGE_ADMISSION_REVIEW_PACK_ARTIFACT_KEY = "stage_admission_review_pack"
STAGE_ADMISSION_REVIEW_PACK_REVIEWER_ARTIFACT_KEY = "stage_admission_review_pack_reviewer_artifact"


def build_stage_admission_review_pack_artifact_entry(
    *,
    artifact_path: Any,
    reviewer_artifact_path: Any = None,
    manifest_section: dict[str, Any] | None = None,
    reviewer_manifest_section: dict[str, Any] | None = None,
) -> dict[str, Any]:
    manifest_payload = dict(manifest_section or {})
    reviewer_manifest_payload = dict(reviewer_manifest_section or {})
    path_text = str(
        artifact_path
        or manifest_payload.get("path")
        or ""
    ).strip()
    reviewer_path_text = str(
        reviewer_artifact_path
        or reviewer_manifest_payload.get("path")
        or manifest_payload.get("reviewer_path")
        or ""
    ).strip()

    title_text = "阶段准入评审包 / Stage Admission Review Pack"
    summary_text = str(reviewer_manifest_payload.get("summary_text") or "").strip()
    status_line = str(reviewer_manifest_payload.get("status_line") or "").strip()
    current_stage_text = str(reviewer_manifest_payload.get("current_stage_text") or "").strip()
    next_stage_text = str(reviewer_manifest_payload.get("next_stage_text") or "").strip()
    engineering_isolation_text = str(
        reviewer_manifest_payload.get("engineering_isolation_text") or ""
    ).strip()
    real_acceptance_text = str(reviewer_manifest_payload.get("real_acceptance_text") or "").strip()
    execute_now_text = str(reviewer_manifest_payload.get("execute_now_text") or "").strip()
    defer_to_stage3_text = str(reviewer_manifest_payload.get("defer_to_stage3_text") or "").strip()
    blocking_text = str(reviewer_manifest_payload.get("blocking_text") or "").strip()
    warning_text = str(reviewer_manifest_payload.get("warning_text") or "").strip()
    stage_marker_text = current_stage_text or next_stage_text
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
    role_status_display = " | ".join(
        part
        for part in (
            stage_marker_text,
            engineering_isolation_text,
            real_acceptance_text,
            warning_text,
        )
        if str(part).strip()
    )
    return {
        "available": bool(path_text or reviewer_path_text) and bool(manifest_payload or reviewer_manifest_payload),
        "artifact_key": STAGE_ADMISSION_REVIEW_PACK_ARTIFACT_KEY,
        "artifact_type": str(
            manifest_payload.get("artifact_type")
            or STAGE_ADMISSION_REVIEW_PACK_ARTIFACT_KEY
        ),
        "reviewer_artifact_key": STAGE_ADMISSION_REVIEW_PACK_REVIEWER_ARTIFACT_KEY,
        "reviewer_artifact_type": str(
            reviewer_manifest_payload.get("artifact_type")
            or STAGE_ADMISSION_REVIEW_PACK_REVIEWER_ARTIFACT_KEY
        ),
        "title_text": title_text,
        "name_text": title_text,
        "filename": Path(path_text).name if path_text else "",
        "reviewer_filename": Path(reviewer_path_text).name if reviewer_path_text else "",
        "path": path_text,
        "reviewer_path": reviewer_path_text,
        "summary_text": summary_text,
        "status_line": status_line,
        "stage_marker_text": stage_marker_text,
        "current_stage_text": current_stage_text,
        "next_stage_text": next_stage_text,
        "engineering_isolation_text": engineering_isolation_text,
        "real_acceptance_text": real_acceptance_text,
        "execute_now_text": execute_now_text,
        "defer_to_stage3_text": defer_to_stage3_text,
        "blocking_text": blocking_text,
        "warning_text": warning_text,
        "entry_text": "\n".join(line for line in entry_lines if str(line).strip()),
        "role_status_display": role_status_display,
        "note_text": summary_text or warning_text or status_line,
        "ready_for_engineering_isolation": bool(
            manifest_payload.get("ready_for_engineering_isolation", False)
        ),
        "real_acceptance_ready": bool(manifest_payload.get("real_acceptance_ready", False)),
        "not_real_acceptance_evidence": bool(
            reviewer_manifest_payload.get(
                "not_real_acceptance_evidence",
                manifest_payload.get("not_real_acceptance_evidence", True),
            )
        ),
    }
