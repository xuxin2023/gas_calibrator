from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .metrology_calibration_contract import METROLOGY_CALIBRATION_CONTRACT_FILENAME
from .phase_transition_bridge import PHASE_TRANSITION_BRIDGE_FILENAME
from .phase_transition_bridge_reviewer_artifact import PHASE_TRANSITION_BRIDGE_REVIEWER_FILENAME
from .stage_admission_review_pack import (
    STAGE_ADMISSION_REVIEW_PACK_FILENAME,
    STAGE_ADMISSION_REVIEW_PACK_REVIEWER_FILENAME,
    build_stage_admission_review_pack,
)
from .step2_readiness import STEP2_READINESS_SUMMARY_FILENAME
from .governance_handoff_contracts import (
    GOVERNANCE_HANDOFF_FILENAMES as _GOV_FILENAMES,
    GOVERNANCE_HANDOFF_DISPLAY_LABELS as _GOV_LABELS,
    GOVERNANCE_HANDOFF_DISPLAY_LABELS_EN as _GOV_LABELS_EN,
    GOVERNANCE_HANDOFF_I18N_KEYS as _GOV_I18N_KEYS,
    GOVERNANCE_HANDOFF_ROLES as _GOV_ROLES,
)


ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_FILENAME = _GOV_FILENAMES["engineering_isolation_admission_checklist"]
ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_REVIEWER_FILENAME = _GOV_FILENAMES["engineering_isolation_admission_checklist_reviewer_artifact"]


def build_engineering_isolation_admission_checklist(
    *,
    run_id: str,
    step2_readiness_summary: dict[str, Any] | None,
    metrology_calibration_contract: dict[str, Any] | None,
    phase_transition_bridge: dict[str, Any] | None,
    stage_admission_review_pack: dict[str, Any] | None = None,
    artifact_paths: dict[str, Any] | None = None,
) -> dict[str, Any]:
    readiness = dict(step2_readiness_summary or {})
    metrology = dict(metrology_calibration_contract or {})
    bridge = dict(phase_transition_bridge or {})
    review_pack = dict(stage_admission_review_pack or {})
    if not review_pack:
        review_pack = build_stage_admission_review_pack(
            run_id=run_id,
            step2_readiness_summary=readiness,
            metrology_calibration_contract=metrology,
            phase_transition_bridge=bridge,
            artifact_paths=artifact_paths,
        )

    pack_raw = dict(review_pack.get("raw") or {})
    pack_display = dict(review_pack.get("display") or {})
    readiness_display = dict(readiness.get("reviewer_display") or {})
    metrology_display = dict(metrology.get("reviewer_display") or {})
    bridge_display = dict(bridge.get("reviewer_display") or {})

    artifact_path_map = _normalize_artifact_paths(artifact_paths, pack_raw.get("artifact_paths"))
    artifact_refs = {
        "step2_readiness_summary": _artifact_ref(
            readiness,
            artifact_path_map["step2_readiness_summary"],
            str(readiness_display.get("summary_text") or "").strip(),
        ),
        "metrology_calibration_contract": _artifact_ref(
            metrology,
            artifact_path_map["metrology_calibration_contract"],
            str(metrology_display.get("summary_text") or "").strip(),
        ),
        "phase_transition_bridge": _artifact_ref(
            bridge,
            artifact_path_map["phase_transition_bridge"],
            str(bridge_display.get("summary_text") or "").strip(),
        ),
        "stage_admission_review_pack": _artifact_ref(
            pack_raw,
            artifact_path_map["stage_admission_review_pack"],
            str(pack_display.get("summary_text") or "").strip(),
        ),
        "stage_admission_review_pack_reviewer_artifact": {
            "artifact_type": "stage_admission_review_pack_reviewer_artifact",
            "phase": str(pack_raw.get("phase") or ""),
            "overall_status": str(pack_raw.get("overall_status") or ""),
            "path": artifact_path_map["stage_admission_review_pack_reviewer_artifact"],
            "summary_text": str(pack_display.get("summary_text") or "").strip(),
        },
    }

    ready_for_engineering_isolation = bool(
        pack_raw.get(
            "ready_for_engineering_isolation",
            bridge.get("ready_for_engineering_isolation", False),
        )
    )
    real_acceptance_ready = bool(
        pack_raw.get(
            "real_acceptance_ready",
            bridge.get("real_acceptance_ready", False),
        )
    )
    blocking_items = _text_list(bridge.get("blocking_items"))
    warning_items = _text_list(pack_raw.get("warning_items") or bridge.get("warning_items"))
    missing_real_world_evidence = _text_list(
        pack_raw.get("missing_real_world_evidence") or bridge.get("missing_real_world_evidence")
    )
    defer_to_stage3_real_validation = _text_list(
        pack_raw.get("defer_to_stage3_real_validation") or bridge.get("defer_to_stage3_real_validation")
    )

    checklist_items = _build_checklist_items(
        readiness=readiness,
        metrology=metrology,
        bridge=bridge,
        review_pack=review_pack,
        ready_for_engineering_isolation=ready_for_engineering_isolation,
        missing_real_world_evidence=missing_real_world_evidence,
        defer_to_stage3_real_validation=defer_to_stage3_real_validation,
        artifact_path_map=artifact_path_map,
    )
    checklist_status_counts = _count_statuses(checklist_items)

    display = {
        "title_text": "工程隔离准入清单 / Engineering Isolation Admission Checklist",
        "summary_text": (
            "准入清单：基于现有 readiness / metrology / bridge / review pack 收口进入 "
            "engineering-isolation 前的已满足项、待确认项与仅限 Stage 3 的项。"
        ),
        "status_line": str(pack_display.get("status_line") or "").strip(),
        "current_stage_text": str(pack_display.get("current_stage_text") or "").strip(),
        "next_stage_text": str(pack_display.get("next_stage_text") or "").strip(),
        "engineering_isolation_text": str(pack_display.get("engineering_isolation_text") or "").strip(),
        "real_acceptance_text": str(pack_display.get("real_acceptance_text") or "").strip(),
        "execute_now_text": str(pack_display.get("execute_now_text") or "").strip(),
        "defer_to_stage3_text": str(pack_display.get("defer_to_stage3_text") or "").strip(),
        "blocking_text": str(pack_display.get("blocking_text") or "").strip(),
        "warning_text": str(pack_display.get("warning_text") or "").strip(),
        "done_lines": _item_lines(checklist_items, status="done"),
        "pending_lines": _item_lines(checklist_items, statuses={"pending", "blocked"}),
        "stage3_only_lines": _item_lines(checklist_items, status="stage3_only"),
        "artifact_lines": _build_artifact_lines(artifact_refs),
    }
    markdown = _render_engineering_isolation_admission_checklist_markdown(display)

    raw = {
        "schema_version": "1.0",
        "artifact_type": "engineering_isolation_admission_checklist",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "run_id": str(run_id or pack_raw.get("run_id") or bridge.get("run_id") or ""),
        "phase": str(pack_raw.get("phase") or bridge.get("phase") or "step2_tail_stage3_bridge"),
        "mode": str(pack_raw.get("mode") or bridge.get("mode") or readiness.get("mode") or "simulation_only"),
        "overall_status": str(pack_raw.get("overall_status") or bridge.get("overall_status") or "step2_tail_in_progress"),
        "recommended_next_stage": str(
            pack_raw.get("recommended_next_stage")
            or bridge.get("recommended_next_stage")
            or "close_step2_tail_gaps"
        ),
        "ready_for_engineering_isolation": ready_for_engineering_isolation,
        "real_acceptance_ready": real_acceptance_ready,
        "not_real_acceptance_evidence": True,
        "checklist_items": checklist_items,
        "checklist_status_counts": checklist_status_counts,
        "blocking_items": blocking_items,
        "warning_items": warning_items,
        "missing_real_world_evidence": missing_real_world_evidence,
        "defer_to_stage3_real_validation": defer_to_stage3_real_validation,
        "artifact_refs": artifact_refs,
        "artifact_paths": artifact_path_map,
        "notes": [
            "engineering_isolation_admission_checklist",
            "step2_tail_stage3_bridge",
            "simulation_offline_headless_only",
            "not_real_acceptance_evidence",
            "default_path_unchanged",
        ],
    }
    return {
        "available": True,
        "artifact_type": "engineering_isolation_admission_checklist",
        "filename": ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_FILENAME,
        "reviewer_filename": ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_REVIEWER_FILENAME,
        "raw": raw,
        "display": display,
        "markdown": markdown,
    }


def _normalize_artifact_paths(
    artifact_paths: dict[str, Any] | None,
    pack_artifact_paths: dict[str, Any] | None = None,
) -> dict[str, str]:
    payload = dict(pack_artifact_paths or {})
    payload.update(dict(artifact_paths or {}))
    return {
        "step2_readiness_summary": str(
            payload.get("step2_readiness_summary") or STEP2_READINESS_SUMMARY_FILENAME
        ),
        "metrology_calibration_contract": str(
            payload.get("metrology_calibration_contract") or METROLOGY_CALIBRATION_CONTRACT_FILENAME
        ),
        "phase_transition_bridge": str(
            payload.get("phase_transition_bridge") or PHASE_TRANSITION_BRIDGE_FILENAME
        ),
        "phase_transition_bridge_reviewer_artifact": str(
            payload.get("phase_transition_bridge_reviewer_artifact") or PHASE_TRANSITION_BRIDGE_REVIEWER_FILENAME
        ),
        "stage_admission_review_pack": str(
            payload.get("stage_admission_review_pack") or STAGE_ADMISSION_REVIEW_PACK_FILENAME
        ),
        "stage_admission_review_pack_reviewer_artifact": str(
            payload.get("stage_admission_review_pack_reviewer_artifact")
            or STAGE_ADMISSION_REVIEW_PACK_REVIEWER_FILENAME
        ),
    }


def _artifact_ref(payload: dict[str, Any], path_text: str, summary_text: str) -> dict[str, Any]:
    return {
        "artifact_type": str(payload.get("artifact_type") or ""),
        "phase": str(payload.get("phase") or ""),
        "overall_status": str(payload.get("overall_status") or ""),
        "path": str(path_text or ""),
        "summary_text": summary_text,
    }


def _build_checklist_items(
    *,
    readiness: dict[str, Any],
    metrology: dict[str, Any],
    bridge: dict[str, Any],
    review_pack: dict[str, Any],
    ready_for_engineering_isolation: bool,
    missing_real_world_evidence: list[str],
    defer_to_stage3_real_validation: list[str],
    artifact_path_map: dict[str, str],
) -> list[dict[str, Any]]:
    readiness_display = dict(readiness.get("reviewer_display") or {})
    metrology_display = dict(metrology.get("reviewer_display") or {})
    bridge_display = dict(bridge.get("reviewer_display") or {})
    pack_display = dict(review_pack.get("display") or {})

    items = [
        _checklist_item(
            item_id="step2_readiness_bridge_formed",
            title_text="Step 2 readiness bridge 已形成",
            category="governance_foundation",
            status="done" if readiness else "pending",
            source_artifact="step2_readiness_summary",
            details=str(readiness_display.get("summary_text") or "readiness summary 已生成。").strip(),
            required_for_engineering_isolation=True,
            stage_assignment="step2_tail",
        ),
        _checklist_item(
            item_id="metrology_contract_institutionalized",
            title_text="metrology contract 已制度化",
            category="governance_foundation",
            status="done" if metrology else "pending",
            source_artifact="metrology_calibration_contract",
            details=str(
                metrology_display.get("summary_text") or "metrology calibration contract 已生成。"
            ).strip(),
            required_for_engineering_isolation=True,
            stage_assignment="step2_tail",
        ),
        _checklist_item(
            item_id="bridge_and_review_pack_recorded",
            title_text="bridge / reviewer artifact / review pack 已落盘并可审阅",
            category="artifact_traceability",
            status="done"
            if artifact_path_map["phase_transition_bridge"]
            and artifact_path_map["stage_admission_review_pack"]
            and artifact_path_map["stage_admission_review_pack_reviewer_artifact"]
            else "pending",
            source_artifact="stage_admission_review_pack",
            details=(
                "phase transition bridge、reviewer artifact 与 stage admission review pack 已纳入离线治理链。"
            ),
            required_for_engineering_isolation=True,
            stage_assignment="step2_tail",
        ),
        _checklist_item(
            item_id="major_offline_surfaces_visible",
            title_text="major offline surface 已可见",
            category="reviewer_surface_readiness",
            status="done" if review_pack else "pending",
            source_artifact="stage_admission_review_pack",
            details=(
                "reports / review_center / review_scope / manifest / remembered_files 已能围绕统一 handoff pack 审阅。"
            ),
            required_for_engineering_isolation=True,
            stage_assignment="step2_tail",
        ),
        _checklist_item(
            item_id="engineering_isolation_gate_confirmation",
            title_text="engineering-isolation 准入状态确认",
            category="manual_confirmation",
            status="done" if ready_for_engineering_isolation else "blocked",
            source_artifact="phase_transition_bridge",
            details=str(
                pack_display.get("engineering_isolation_text")
                or bridge_display.get("engineering_isolation_text")
                or "engineering-isolation 准入状态待确认。"
            ).strip(),
            required_for_engineering_isolation=True,
            stage_assignment="engineering_isolation_admission",
        ),
        _checklist_item(
            item_id="handoff_pack_integrity_confirmation",
            title_text="handoff 包完整性人工确认",
            category="manual_confirmation",
            status="pending",
            source_artifact="stage_admission_review_pack",
            details="确认 handoff 包已包含 readiness / metrology / bridge / reviewer pack 的完整引用与交接路径。",
            required_for_engineering_isolation=True,
            stage_assignment="engineering_isolation_admission",
        ),
        _checklist_item(
            item_id="reviewer_discoverability_confirmation",
            title_text="reviewer artifact 可发现性人工确认",
            category="manual_confirmation",
            status="pending",
            source_artifact="stage_admission_review_pack",
            details="确认 reviewer 能在 reports / review_center / review_scope / manifest 链上直接定位关键工件。",
            required_for_engineering_isolation=True,
            stage_assignment="engineering_isolation_admission",
        ),
        _checklist_item(
            item_id="evidence_bundle_path_confirmation",
            title_text="关键路径与证据工件齐全性人工确认",
            category="manual_confirmation",
            status="pending",
            source_artifact="stage_admission_review_pack",
            details=(
                "确认 step2_readiness_summary / metrology_calibration_contract / "
                "phase_transition_bridge / review pack 的路径与引用仍完整。"
            ),
            required_for_engineering_isolation=True,
            stage_assignment="engineering_isolation_admission",
        ),
    ]

    seen_stage3: set[str] = set()
    for item in list(defer_to_stage3_real_validation) + list(missing_real_world_evidence):
        text = str(item or "").strip()
        if not text or text in seen_stage3:
            continue
        seen_stage3.add(text)
        items.append(
            _checklist_item(
                item_id=f"stage3_only::{text}",
                title_text=text,
                category="stage3_real_validation",
                status="stage3_only",
                source_artifact="phase_transition_bridge",
                details=text,
                required_for_engineering_isolation=False,
                stage_assignment="stage3_real_validation",
            )
        )
    return items


def _checklist_item(
    *,
    item_id: str,
    title_text: str,
    category: str,
    status: str,
    source_artifact: str,
    details: str,
    required_for_engineering_isolation: bool,
    stage_assignment: str,
) -> dict[str, Any]:
    return {
        "item_id": str(item_id),
        "title_text": str(title_text),
        "category": str(category),
        "status": str(status),
        "source_artifact": str(source_artifact),
        "details": str(details).strip(),
        "required_for_engineering_isolation": bool(required_for_engineering_isolation),
        "stage_assignment": str(stage_assignment),
    }


def _count_statuses(items: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in list(items or []):
        status = str(item.get("status") or "").strip()
        if not status:
            continue
        counts[status] = int(counts.get(status, 0) or 0) + 1
    return counts


def _item_lines(
    items: list[dict[str, Any]],
    *,
    status: str | None = None,
    statuses: set[str] | None = None,
) -> list[str]:
    rows: list[str] = []
    for item in list(items or []):
        item_status = str(item.get("status") or "").strip()
        if status is not None and item_status != status:
            continue
        if statuses is not None and item_status not in statuses:
            continue
        title_text = str(item.get("title_text") or item.get("item_id") or "").strip()
        details = str(item.get("details") or "").strip()
        line = title_text
        if details:
            line = f"{line}：{details}"
        if line and line not in rows:
            rows.append(line)
    return rows


def _build_artifact_lines(artifact_refs: dict[str, dict[str, Any]]) -> list[str]:
    order = (
        ("step2_readiness_summary", "step2_readiness_summary.json"),
        ("metrology_calibration_contract", "metrology_calibration_contract.json"),
        ("phase_transition_bridge", "phase_transition_bridge.json"),
        ("stage_admission_review_pack", "stage_admission_review_pack.json"),
        ("stage_admission_review_pack_reviewer_artifact", "stage_admission_review_pack.md"),
    )
    rows: list[str] = []
    for key, label in order:
        payload = dict(artifact_refs.get(key) or {})
        summary_text = str(payload.get("summary_text") or "").strip()
        path_text = str(payload.get("path") or "").strip()
        line = f"`{label}`"
        if summary_text:
            line = f"{line}：{summary_text}"
        if path_text:
            line = f"{line}（{path_text}）"
        rows.append(line)
    return rows


def _render_engineering_isolation_admission_checklist_markdown(display: dict[str, Any]) -> str:
    lines = [
        f"# {display.get('title_text') or '工程隔离准入清单 / Engineering Isolation Admission Checklist'}",
        "",
        "> 仅用于 Step 2 tail / Stage 3 bridge 的离线 governance / reviewer handoff 清单，不是 real acceptance，不能替代真实计量验证。",
        "",
        "## 当前阶段",
        "",
        f"- {display.get('current_stage_text') or '--'}",
        f"- {display.get('next_stage_text') or '--'}",
        f"- {display.get('status_line') or '--'}",
        f"- {display.get('engineering_isolation_text') or '--'}",
        f"- {display.get('real_acceptance_text') or '--'}",
        "",
        "## 已满足项",
        "",
    ]
    lines.extend(f"- {line}" for line in list(display.get("done_lines") or []) or ["--"])
    lines.extend(
        [
            "",
            "## 进入 engineering-isolation 前仍需确认项",
            "",
        ]
    )
    lines.extend(f"- {line}" for line in list(display.get("pending_lines") or []) or ["--"])
    lines.extend(
        [
            "",
            "## 当前执行",
            "",
            f"- {display.get('execute_now_text') or '--'}",
            "",
            "## 只能留到 Stage 3 real validation 的项",
            "",
        ]
    )
    lines.extend(f"- {line}" for line in list(display.get("stage3_only_lines") or []) or ["--"])
    lines.extend(
        [
            "",
            "## 第三阶段执行",
            "",
            f"- {display.get('defer_to_stage3_text') or '--'}",
            "",
            "## 明示警告",
            "",
            f"- {display.get('blocking_text') or '--'}",
            f"- {display.get('warning_text') or '--'}",
            "",
            "## 关联工件",
            "",
        ]
    )
    lines.extend(f"- {line}" for line in list(display.get("artifact_lines") or []) or ["--"])
    return "\n".join(line for line in lines if str(line).strip() or line == "") + "\n"


def _text_list(values: Any) -> list[str]:
    rows: list[str] = []
    for value in list(values or []):
        text = str(value or "").strip()
        if text and text not in rows:
            rows.append(text)
    return rows
