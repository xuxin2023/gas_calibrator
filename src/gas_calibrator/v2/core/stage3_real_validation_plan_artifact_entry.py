from __future__ import annotations

from pathlib import Path
from typing import Any


STAGE3_REAL_VALIDATION_PLAN_ARTIFACT_KEY = "stage3_real_validation_plan"
STAGE3_REAL_VALIDATION_PLAN_REVIEWER_ARTIFACT_KEY = "stage3_real_validation_plan_reviewer_artifact"

_VALIDATION_CATEGORY_LABELS = {
    "reference_instrument_enforcement": "真实参考表 / 参考仪器强制执行",
    "traceability_review": "证书 / 检定周期 / traceability 复核",
    "uncertainty_result": "真实 run 最终不确定度",
    "device_acceptance": "真机系数写入 / 回读 / acceptance",
    "real_world_repeatability": "多点复测 / 重复性 / 漂移",
    "pass_fail_contract": "real acceptance pass/fail 判定",
    "anomaly_retest": "真实异常复核 / 复测处置",
}

_VALIDATION_STATUS_LABELS = {
    "blocked_until_stage3": "待第三阶段真实执行",
    "requires_real_evidence": "需真实证据",
    "not_executable_offline": "离线不可执行",
    "planned": "计划中",
}


def build_stage3_real_validation_plan_artifact_entry(
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
    parsed_markdown = _parse_stage3_reviewer_markdown(reviewer_markdown_text)
    markdown_sections = dict(parsed_markdown.get("sections") or {})

    title_text = str(
        parsed_markdown.get("title_text")
        or reviewer_manifest_payload.get("title_text")
        or "Stage 3 Real Validation Plan / 第三阶段真实验证计划"
    ).strip() or "Stage 3 Real Validation Plan / 第三阶段真实验证计划"
    summary_text = str(reviewer_manifest_payload.get("summary_text") or "").strip()
    current_stage_lines = _section_lines(markdown_sections, "当前阶段")
    execute_now_lines = _section_lines(markdown_sections, "当前只能做到的内容")
    required_evidence_lines = _section_lines(markdown_sections, "真实证据要求")
    pass_fail_lines = _section_lines(markdown_sections, "pass/fail 基本边界")
    artifact_lines = _section_lines(markdown_sections, "关联工件")
    review_tip_lines = _section_lines(markdown_sections, "审阅提示")

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
    execute_now_text = _pick_text(
        execute_now_lines[0] if execute_now_lines else "",
        reviewer_manifest_payload.get("execute_now_text"),
    )
    blocking_text = _pick_text(
        review_tip_lines[0] if len(review_tip_lines) > 0 else "",
        reviewer_manifest_payload.get("blocking_text"),
    )
    warning_text = _pick_text(
        review_tip_lines[1] if len(review_tip_lines) > 1 else "",
        reviewer_manifest_payload.get("warning_text"),
    )
    defer_to_stage3_text = _pick_text(
        review_tip_lines[2] if len(review_tip_lines) > 2 else "",
        reviewer_manifest_payload.get("defer_to_stage3_text"),
    )
    plan_boundary_text = _pick_text(
        review_tip_lines[3] if len(review_tip_lines) > 3 else "",
        reviewer_manifest_payload.get("plan_boundary_text"),
    )
    reviewer_note_text = _pick_text(
        parsed_markdown.get("quote_text"),
        warning_text,
        plan_boundary_text,
    )

    validation_items = [
        dict(item)
        for item in list(manifest_payload.get("validation_items") or [])
        if isinstance(item, dict)
    ]
    required_evidence_categories = _build_required_evidence_categories(validation_items)
    required_evidence_categories_text = "；".join(required_evidence_categories) if required_evidence_categories else "--"
    pass_fail_contract_text = _build_pass_fail_contract_text(
        pass_fail_lines=pass_fail_lines,
        pass_fail_contract=dict(manifest_payload.get("pass_fail_contract") or {}),
    )
    digest_text = _build_digest_text(
        digest_payload=digest_payload,
        manifest_payload=manifest_payload,
    )
    artifact_paths_text = "\n".join(
        line
        for line in (
            f"JSON：{path_text}" if path_text else "",
            f"Markdown：{reviewer_path_text}" if reviewer_path_text else "",
        )
        if str(line).strip()
    )
    stage_marker_text = current_stage_text or next_stage_text
    simulation_only_text = "simulation / offline / headless only"
    role_text = "执行摘要 + 正式分析"

    card_lines = [
        summary_text,
        f"角色：{role_text}",
        f"审阅提示：{reviewer_note_text}",
        current_stage_text,
        next_stage_text,
        engineering_isolation_text,
        real_acceptance_text,
        execute_now_text,
        defer_to_stage3_text,
        plan_boundary_text,
        f"第三阶段真实验证证据类别：{required_evidence_categories_text}",
        f"pass/fail contract 摘要：{pass_fail_contract_text}",
        f"Digest：{digest_text}",
        f"边界：{simulation_only_text}",
        artifact_paths_text,
    ]
    entry_lines = [
        summary_text,
        reviewer_note_text,
        current_stage_text,
        next_stage_text,
        engineering_isolation_text,
        real_acceptance_text,
        execute_now_text,
        defer_to_stage3_text,
        warning_text,
        plan_boundary_text,
        f"第三阶段真实验证证据类别：{required_evidence_categories_text}",
        f"pass/fail contract 摘要：{pass_fail_contract_text}",
        f"Digest：{digest_text}",
        f"边界：{simulation_only_text}",
        artifact_paths_text,
    ]
    role_status_display = " | ".join(
        part
        for part in (
            stage_marker_text,
            engineering_isolation_text,
            real_acceptance_text,
            warning_text,
            simulation_only_text,
        )
        if str(part).strip()
    )
    return {
        "available": bool(path_text or reviewer_path_text) and bool(
            manifest_payload or reviewer_manifest_payload or str(reviewer_markdown_text or "").strip()
        ),
        "artifact_key": STAGE3_REAL_VALIDATION_PLAN_ARTIFACT_KEY,
        "artifact_type": str(
            manifest_payload.get("artifact_type")
            or STAGE3_REAL_VALIDATION_PLAN_ARTIFACT_KEY
        ),
        "reviewer_artifact_key": STAGE3_REAL_VALIDATION_PLAN_REVIEWER_ARTIFACT_KEY,
        "reviewer_artifact_type": str(
            reviewer_manifest_payload.get("artifact_type")
            or STAGE3_REAL_VALIDATION_PLAN_REVIEWER_ARTIFACT_KEY
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
        "stage_marker_text": stage_marker_text,
        "current_stage_text": current_stage_text,
        "next_stage_text": next_stage_text,
        "engineering_isolation_text": engineering_isolation_text,
        "real_acceptance_text": real_acceptance_text,
        "execute_now_text": execute_now_text,
        "defer_to_stage3_text": defer_to_stage3_text,
        "blocking_text": blocking_text,
        "warning_text": warning_text,
        "plan_boundary_text": plan_boundary_text,
        "required_evidence_categories": required_evidence_categories,
        "required_evidence_categories_text": required_evidence_categories_text,
        "required_evidence_lines": required_evidence_lines,
        "pass_fail_contract_summary": pass_fail_contract_text,
        "pass_fail_lines": pass_fail_lines,
        "artifact_lines": artifact_lines,
        "artifact_paths_text": artifact_paths_text,
        "digest": dict(digest_payload),
        "digest_text": digest_text,
        "simulation_only_text": simulation_only_text,
        "entry_text": "\n".join(line for line in entry_lines if str(line).strip()),
        "card_text": "\n".join(line for line in card_lines if str(line).strip()),
        "role_status_display": role_status_display,
        "note_text": summary_text or reviewer_note_text or warning_text,
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


def _parse_stage3_reviewer_markdown(markdown_text: str) -> dict[str, Any]:
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


def _build_required_evidence_categories(validation_items: list[dict[str, Any]]) -> list[str]:
    rows: list[str] = []
    for item in validation_items:
        category = str(item.get("category") or "").strip()
        label = _VALIDATION_CATEGORY_LABELS.get(category) or str(
            item.get("title_text")
            or category
        ).strip()
        if label and label not in rows:
            rows.append(label)
    return rows


def _build_pass_fail_contract_text(
    *,
    pass_fail_lines: list[str],
    pass_fail_contract: dict[str, Any],
) -> str:
    if pass_fail_lines:
        return "；".join(str(line).strip() for line in pass_fail_lines if str(line).strip())
    pass_requires = [
        str(item).strip()
        for item in list(pass_fail_contract.get("pass_requires") or [])
        if str(item).strip()
    ]
    fail_triggers = [
        str(item).strip()
        for item in list(pass_fail_contract.get("fail_triggers") or [])
        if str(item).strip()
    ]
    if not pass_requires and not fail_triggers:
        return "--"
    return (
        f"第三阶段真实判定前必须满足 {len(pass_requires)} 项通过条件，并对 {len(fail_triggers)} 类失败触发保持零未闭环。"
    )


def _build_digest_text(
    *,
    digest_payload: dict[str, Any],
    manifest_payload: dict[str, Any],
) -> str:
    validation_status_counts = dict(
        digest_payload.get("validation_status_counts")
        or manifest_payload.get("validation_status_counts")
        or {}
    )
    required_real_world_evidence = list(
        digest_payload.get("required_real_world_evidence")
        or manifest_payload.get("required_real_world_evidence")
        or []
    )
    artifact_paths = dict(
        digest_payload.get("artifact_paths")
        or manifest_payload.get("artifact_paths")
        or {}
    )
    status_parts = [
        f"{_VALIDATION_STATUS_LABELS.get(status, status)} {int(count or 0)}"
        for status, count in validation_status_counts.items()
        if str(status).strip()
    ]
    digest_parts = []
    if status_parts:
        digest_parts.append("状态计数：" + "；".join(status_parts))
    if required_real_world_evidence:
        digest_parts.append(f"真实证据项 {len(required_real_world_evidence)}")
    if artifact_paths:
        digest_parts.append(f"关联工件 {len(artifact_paths)}")
    return " | ".join(digest_parts) if digest_parts else "--"
