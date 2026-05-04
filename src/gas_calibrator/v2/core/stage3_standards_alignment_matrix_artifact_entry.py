from __future__ import annotations

from pathlib import Path
from typing import Any


STAGE3_STANDARDS_ALIGNMENT_MATRIX_ARTIFACT_KEY = "stage3_standards_alignment_matrix"
STAGE3_STANDARDS_ALIGNMENT_MATRIX_REVIEWER_ARTIFACT_KEY = (
    "stage3_standards_alignment_matrix_reviewer_artifact"
)


def build_stage3_standards_alignment_matrix_artifact_entry(
    *,
    artifact_path: Any,
    reviewer_artifact_path: Any = None,
    manifest_section: dict[str, Any] | None = None,
    reviewer_manifest_section: dict[str, Any] | None = None,
    digest_section: dict[str, Any] | None = None,
    reviewer_markdown_text: str = "",
) -> dict[str, Any]:
    manifest_payload = dict(manifest_section or {})
    reviewer_manifest_payload = dict(reviewer_manifest_section or {})
    digest_payload = dict(digest_section or {})
    path_text = str(artifact_path or manifest_payload.get("path") or "").strip()
    reviewer_path_text = str(
        reviewer_artifact_path
        or reviewer_manifest_payload.get("path")
        or manifest_payload.get("reviewer_path")
        or ""
    ).strip()
    parsed_markdown = _parse_reviewer_markdown(reviewer_markdown_text)
    markdown_sections = dict(parsed_markdown.get("sections") or {})

    rows = [
        dict(item)
        for item in list(manifest_payload.get("rows") or [])
        if isinstance(item, dict)
    ]
    standard_families = _dedupe(
        list(manifest_payload.get("standard_families") or [])
        + [item.get("standard_id_or_family") for item in rows]
    )
    required_evidence_categories = _dedupe(
        list(manifest_payload.get("required_evidence_categories") or [])
        + [
            category
            for item in rows
            for category in list(item.get("required_evidence_categories") or [])
        ]
    )
    readiness_status_filters = _dedupe(item.get("readiness_status") for item in rows)
    missing_coverage_filters = _dedupe(
        item.get("missing_coverage_filter")
        or ("missing_coverage" if bool(item.get("missing_coverage")) else "coverage_present")
        for item in rows
    )
    gap_filters = _dedupe(
        item.get("gap_filter")
        or ("has_gap" if str(item.get("gap_note") or "").strip() else "no_gap")
        for item in rows
    )
    boundary_lines = _dedupe(
        list(manifest_payload.get("boundary_statements") or [])
        + _section_lines(markdown_sections, "非声明边界")
    )
    matrix_lines = _section_lines(markdown_sections, "标准家族与主题映射")
    artifact_lines = _section_lines(markdown_sections, "关联工件")

    title_text = str(
        parsed_markdown.get("title_text")
        or reviewer_manifest_payload.get("title_text")
        or "Stage 3 Standards Alignment Matrix / 第三阶段标准符合性映射与证据覆盖矩阵"
    ).strip() or "Stage 3 Standards Alignment Matrix / 第三阶段标准符合性映射与证据覆盖矩阵"
    summary_text = str(reviewer_manifest_payload.get("summary_text") or "").strip()
    reviewer_note_text = _pick_text(
        parsed_markdown.get("quote_text"),
        str(reviewer_manifest_payload.get("reviewer_note_text") or "").strip(),
    )

    current_stage_lines = _section_lines(markdown_sections, "当前阶段")
    current_stage_text = _pick_text(
        current_stage_lines[0] if len(current_stage_lines) > 0 else "",
        reviewer_manifest_payload.get("current_stage_text"),
    )
    next_stage_text = _pick_text(
        current_stage_lines[1] if len(current_stage_lines) > 1 else "",
        reviewer_manifest_payload.get("next_stage_text"),
    )
    status_line = _pick_text(
        current_stage_lines[2] if len(current_stage_lines) > 2 else "",
        reviewer_manifest_payload.get("status_line"),
    )
    engineering_isolation_text = _pick_text(
        current_stage_lines[3] if len(current_stage_lines) > 3 else "",
        reviewer_manifest_payload.get("engineering_isolation_text"),
    )
    real_acceptance_text = _pick_text(
        current_stage_lines[4] if len(current_stage_lines) > 4 else "",
        reviewer_manifest_payload.get("real_acceptance_text"),
    )
    stage_bridge_text = _pick_text(
        current_stage_lines[5] if len(current_stage_lines) > 5 else "",
        reviewer_manifest_payload.get("stage_bridge_text"),
    )
    role_text = _pick_text(
        reviewer_manifest_payload.get("artifact_role_text"),
        "execution_summary + formal_analysis",
    )
    standard_families_text = "；".join(standard_families) if standard_families else "--"
    required_evidence_categories_text = "；".join(required_evidence_categories) if required_evidence_categories else "--"
    boundary_text = "；".join(boundary_lines) if boundary_lines else "--"
    digest_text = _build_digest_text(
        digest_payload=digest_payload,
        manifest_payload=manifest_payload,
        rows=rows,
        standard_families=standard_families,
        required_evidence_categories=required_evidence_categories,
    )
    artifact_paths_text = "\n".join(
        line
        for line in (
            f"JSON：{path_text}" if path_text else "",
            f"Markdown：{reviewer_path_text}" if reviewer_path_text else "",
        )
        if str(line).strip()
    )

    anchor_id = "stage3-standards-alignment-matrix"
    anchor_rows = [
        {
            "anchor_id": str(item.get("anchor_id") or f"{anchor_id}:{index}"),
            "anchor_label": str(
                item.get("anchor_label")
                or item.get("topic_or_control_object")
                or item.get("standard_family")
                or title_text
            ).strip(),
        }
        for index, item in enumerate(rows, start=1)
    ]
    phase_filters = ["step2_tail_stage3_bridge", "stage3_standards_alignment"]
    artifact_role_filters = ["execution_summary", "formal_analysis"]
    boundary_filters = _dedupe(
        list(boundary_lines)
        + ["engineering-isolation dependency"]
    )
    card_lines = [
        summary_text,
        f"角色：{role_text}",
        f"reviewer_note：{reviewer_note_text}",
        current_stage_text,
        next_stage_text,
        status_line,
        engineering_isolation_text,
        real_acceptance_text,
        stage_bridge_text,
        f"标准家族：{standard_families_text}",
        f"required evidence categories：{required_evidence_categories_text}",
        f"边界：{boundary_text}",
        f"Digest：{digest_text}",
        artifact_paths_text,
    ]
    entry_lines = [
        summary_text,
        reviewer_note_text,
        current_stage_text,
        next_stage_text,
        status_line,
        engineering_isolation_text,
        real_acceptance_text,
        stage_bridge_text,
        f"标准家族：{standard_families_text}",
        f"required evidence categories：{required_evidence_categories_text}",
        f"边界：{boundary_text}",
        f"Digest：{digest_text}",
        artifact_paths_text,
    ]
    role_status_display = " | ".join(
        part
        for part in (
            current_stage_text or next_stage_text,
            engineering_isolation_text,
            real_acceptance_text,
            stage_bridge_text,
            "simulation / offline / headless only",
        )
        if str(part).strip()
    )
    return {
        "available": bool(path_text or reviewer_path_text) and bool(
            manifest_payload or reviewer_manifest_payload or str(reviewer_markdown_text or "").strip()
        ),
        "artifact_key": STAGE3_STANDARDS_ALIGNMENT_MATRIX_ARTIFACT_KEY,
        "artifact_type": str(
            manifest_payload.get("artifact_type")
            or STAGE3_STANDARDS_ALIGNMENT_MATRIX_ARTIFACT_KEY
        ),
        "reviewer_artifact_key": STAGE3_STANDARDS_ALIGNMENT_MATRIX_REVIEWER_ARTIFACT_KEY,
        "reviewer_artifact_type": str(
            reviewer_manifest_payload.get("artifact_type")
            or STAGE3_STANDARDS_ALIGNMENT_MATRIX_REVIEWER_ARTIFACT_KEY
        ),
        "title_text": title_text,
        "name_text": title_text,
        "role_text": role_text,
        "reviewer_note_text": reviewer_note_text,
        "filename": Path(path_text).name if path_text else "",
        "reviewer_filename": Path(reviewer_path_text).name if reviewer_path_text else "",
        "path": path_text,
        "reviewer_path": reviewer_path_text,
        "summary_text": summary_text,
        "status_line": status_line,
        "current_stage_text": current_stage_text,
        "next_stage_text": next_stage_text,
        "engineering_isolation_text": engineering_isolation_text,
        "real_acceptance_text": real_acceptance_text,
        "stage_bridge_text": stage_bridge_text,
        "standard_families": standard_families,
        "standard_families_text": standard_families_text,
        "required_evidence_categories": required_evidence_categories,
        "required_evidence_categories_text": required_evidence_categories_text,
        "boundary_lines": boundary_lines,
        "boundary_text": boundary_text,
        "matrix_lines": matrix_lines,
        "artifact_lines": artifact_lines,
        "artifact_paths_text": artifact_paths_text,
        "digest": dict(digest_payload),
        "digest_text": digest_text,
        "anchor_id": anchor_id,
        "navigation_id": anchor_id,
        "anchor_label": title_text,
        "phase_filters": phase_filters,
        "artifact_role_filters": artifact_role_filters,
        "standard_family_filters": list(standard_families),
        "evidence_category_filters": list(required_evidence_categories),
        "readiness_status_filters": list(readiness_status_filters),
        "missing_coverage_filters": list(missing_coverage_filters),
        "gap_filters": list(gap_filters),
        "boundary_filters": boundary_filters,
        "anchor_rows": anchor_rows,
        "anchor_filters": _dedupe([anchor_id] + [row.get("anchor_id") for row in anchor_rows]),
        "card_text": "\n".join(line for line in card_lines if str(line).strip()),
        "entry_text": "\n".join(line for line in entry_lines if str(line).strip()),
        "role_status_display": role_status_display,
        "note_text": summary_text or reviewer_note_text or digest_text,
        "not_real_acceptance_evidence": bool(
            reviewer_manifest_payload.get(
                "not_real_acceptance_evidence",
                manifest_payload.get("not_real_acceptance_evidence", True),
            )
        ),
    }


def _build_digest_text(
    *,
    digest_payload: dict[str, Any],
    manifest_payload: dict[str, Any],
    rows: list[dict[str, Any]],
    standard_families: list[str],
    required_evidence_categories: list[str],
) -> str:
    row_count = int(
        digest_payload.get("mapping_row_count")
        or len(rows)
    )
    family_count = int(
        digest_payload.get("standard_family_count")
        or len(standard_families)
    )
    category_count = int(
        digest_payload.get("required_evidence_category_count")
        or len(required_evidence_categories)
    )
    readiness_counts = dict(
        digest_payload.get("readiness_status_counts")
        or manifest_payload.get("readiness_status_counts")
        or {}
    )
    artifact_paths = dict(
        digest_payload.get("artifact_paths")
        or manifest_payload.get("artifact_paths")
        or {}
    )
    readiness_text = "；".join(
        f"{status} {int(count or 0)}"
        for status, count in readiness_counts.items()
        if str(status).strip()
    )
    digest_lines = [
        f"标准家族 {family_count}",
        f"主题映射 {row_count}",
        f"证据类别 {category_count}",
    ]
    if readiness_text:
        digest_lines.append(f"readiness 状态：{readiness_text}")
    if artifact_paths:
        digest_lines.append(f"关联工件 {len(artifact_paths)}")
    return " | ".join(digest_lines)


def _parse_reviewer_markdown(markdown_text: str) -> dict[str, Any]:
    title_text = ""
    quote_lines: list[str] = []
    sections: dict[str, list[str]] = {}
    current_section = ""
    for raw_line in str(markdown_text or "").splitlines():
        line = str(raw_line or "").strip()
        if not line:
            continue
        if line.startswith("# "):
            title_text = line[2:].strip()
            continue
        if line.startswith("> "):
            quote_lines.append(line[2:].strip())
            continue
        if line.startswith("## "):
            current_section = line[3:].strip()
            sections.setdefault(current_section, [])
            continue
        if line.startswith("- ") and current_section:
            sections.setdefault(current_section, []).append(line[2:].strip())
    return {
        "title_text": title_text,
        "quote_text": " ".join(line for line in quote_lines if str(line).strip()),
        "sections": sections,
    }


def _section_lines(sections: dict[str, list[str]], section_name: str) -> list[str]:
    return [str(item).strip() for item in list(sections.get(section_name, []) or []) if str(item).strip()]


def _pick_text(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _dedupe(values: Any) -> list[str]:
    rows: list[str] = []
    for value in list(values or []):
        text = str(value or "").strip()
        if text and text not in rows:
            rows.append(text)
    return rows
