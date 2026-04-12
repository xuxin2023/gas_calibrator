from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .metrology_calibration_contract import METROLOGY_CALIBRATION_CONTRACT_FILENAME
from .phase_transition_bridge import PHASE_TRANSITION_BRIDGE_FILENAME
from .phase_transition_bridge_reviewer_artifact import (
    PHASE_TRANSITION_BRIDGE_REVIEWER_FILENAME,
    build_phase_transition_bridge_reviewer_artifact,
)
from .step2_readiness import STEP2_READINESS_SUMMARY_FILENAME
from .governance_handoff_contracts import (
    GOVERNANCE_HANDOFF_FILENAMES as _GOV_FILENAMES,
    GOVERNANCE_HANDOFF_DISPLAY_LABELS as _GOV_LABELS,
    GOVERNANCE_HANDOFF_DISPLAY_LABELS_EN as _GOV_LABELS_EN,
    GOVERNANCE_HANDOFF_I18N_KEYS as _GOV_I18N_KEYS,
    GOVERNANCE_HANDOFF_ROLES as _GOV_ROLES,
)


STAGE_ADMISSION_REVIEW_PACK_FILENAME = _GOV_FILENAMES["stage_admission_review_pack"]
STAGE_ADMISSION_REVIEW_PACK_REVIEWER_FILENAME = _GOV_FILENAMES["stage_admission_review_pack_reviewer_artifact"]


def build_stage_admission_review_pack(
    *,
    run_id: str,
    step2_readiness_summary: dict[str, Any] | None,
    metrology_calibration_contract: dict[str, Any] | None,
    phase_transition_bridge: dict[str, Any] | None,
    phase_transition_bridge_reviewer_artifact: dict[str, Any] | None = None,
    artifact_paths: dict[str, Any] | None = None,
) -> dict[str, Any]:
    readiness = dict(step2_readiness_summary or {})
    metrology = dict(metrology_calibration_contract or {})
    bridge = dict(phase_transition_bridge or {})
    reviewer_artifact = dict(phase_transition_bridge_reviewer_artifact or {})
    if not reviewer_artifact:
        reviewer_artifact = build_phase_transition_bridge_reviewer_artifact(bridge)

    reviewer_display = dict(reviewer_artifact.get("display", {}) or {})
    artifact_path_map = _normalize_artifact_paths(artifact_paths)
    artifact_refs = {
        "step2_readiness_summary": _artifact_ref(readiness, artifact_path_map["step2_readiness_summary"]),
        "metrology_calibration_contract": _artifact_ref(
            metrology,
            artifact_path_map["metrology_calibration_contract"],
        ),
        "phase_transition_bridge": _artifact_ref(bridge, artifact_path_map["phase_transition_bridge"]),
        "phase_transition_bridge_reviewer_artifact": {
            "artifact_type": str(
                reviewer_artifact.get("artifact_type")
                or "phase_transition_bridge_reviewer_artifact"
            ),
            "phase": str(bridge.get("phase") or ""),
            "overall_status": str(bridge.get("overall_status") or ""),
            "path": artifact_path_map["phase_transition_bridge_reviewer_artifact"],
            "summary_text": str(reviewer_display.get("summary_text") or "").strip(),
        },
    }

    ready_for_engineering_isolation = bool(bridge.get("ready_for_engineering_isolation", False))
    real_acceptance_ready = bool(bridge.get("real_acceptance_ready", False))
    execute_now_in_step2_tail = _text_list(bridge.get("execute_now_in_step2_tail"))
    defer_to_stage3_real_validation = _text_list(bridge.get("defer_to_stage3_real_validation"))
    blocking_items = _text_list(bridge.get("blocking_items"))
    warning_items = _text_list(bridge.get("warning_items"))
    missing_real_world_evidence = _text_list(bridge.get("missing_real_world_evidence"))

    handoff_checklist = {
        "reviewer_focus": [
            {
                "artifact_ref": "phase_transition_bridge_reviewer_artifact",
                "focus": "先确认当前阶段、下一阶段、当前执行与第三阶段执行仍保持 reviewer-facing canonical wording。",
            },
            {
                "artifact_ref": "step2_readiness_summary",
                "focus": "核对 readiness gate、阻塞项和 simulation/offline/headless 边界是否仍成立。",
            },
        ],
        "engineer_focus": [
            {
                "artifact_ref": "metrology_calibration_contract",
                "focus": "核对当前已固化的 contract/schema/template/digest/reporting contract，确认仍未越过 real acceptance 边界。",
            },
            {
                "artifact_ref": "phase_transition_bridge",
                "focus": "核对当前仍缺哪些真实世界证据，并把 Stage 3 前置补项留在 handoff checklist 中。",
            },
        ],
        "stage3_prerequisites": list(missing_real_world_evidence),
    }
    display = {
        "title_text": "阶段准入评审包 / Stage Admission Review Pack",
        "summary_text": str(reviewer_display.get("summary_text") or "").strip(),
        "status_line": str(reviewer_display.get("status_line") or "").strip(),
        "current_stage_text": str(reviewer_display.get("current_stage_text") or "").strip(),
        "next_stage_text": str(reviewer_display.get("next_stage_text") or "").strip(),
        "engineering_isolation_text": str(reviewer_display.get("engineering_isolation_text") or "").strip(),
        "real_acceptance_text": str(reviewer_display.get("real_acceptance_text") or "").strip(),
        "execute_now_text": str(reviewer_display.get("execute_now_text") or "").strip(),
        "defer_to_stage3_text": str(reviewer_display.get("defer_to_stage3_text") or "").strip(),
        "blocking_text": str(reviewer_display.get("blocking_text") or "").strip(),
        "warning_text": str(reviewer_display.get("warning_text") or "").strip(),
        "artifact_lines": _build_artifact_lines(
            artifact_refs=artifact_refs,
            readiness=readiness,
            metrology=metrology,
            bridge=bridge,
            reviewer_display=reviewer_display,
        ),
        "reviewer_handoff_lines": [
            f"{item['artifact_ref']}：{item['focus']}"
            for item in handoff_checklist["reviewer_focus"]
        ],
        "engineer_handoff_lines": [
            f"{item['artifact_ref']}：{item['focus']}"
            for item in handoff_checklist["engineer_focus"]
        ],
        "stage3_handoff_lines": [
            f"Stage 3 前补齐：{item}"
            for item in handoff_checklist["stage3_prerequisites"]
        ]
        or ["Stage 3 前补齐：无。"],
        "artifact_path_lines": [
            f"{name}: {path_text}"
            for name, path_text in artifact_path_map.items()
        ],
    }
    markdown = _render_stage_admission_review_pack_markdown(display)
    raw = {
        "schema_version": "1.0",
        "artifact_type": "stage_admission_review_pack",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "run_id": str(run_id or bridge.get("run_id") or readiness.get("run_id") or ""),
        "phase": str(bridge.get("phase") or "step2_tail_stage3_bridge"),
        "mode": str(bridge.get("mode") or readiness.get("mode") or "simulation_only"),
        "overall_status": str(bridge.get("overall_status") or "step2_tail_in_progress"),
        "recommended_next_stage": str(bridge.get("recommended_next_stage") or "close_step2_tail_gaps"),
        "ready_for_engineering_isolation": ready_for_engineering_isolation,
        "real_acceptance_ready": real_acceptance_ready,
        "not_real_acceptance_evidence": True,
        "artifact_refs": artifact_refs,
        "artifact_paths": artifact_path_map,
        "execute_now_in_step2_tail": execute_now_in_step2_tail,
        "defer_to_stage3_real_validation": defer_to_stage3_real_validation,
        "blocking_items": blocking_items,
        "warning_items": warning_items,
        "missing_real_world_evidence": missing_real_world_evidence,
        "handoff_checklist": handoff_checklist,
        "notes": [
            "stage_admission_review_pack",
            "step2_tail_stage3_bridge",
            "simulation_offline_headless_only",
            "not_real_acceptance_evidence",
            "default_path_unchanged",
        ],
    }
    return {
        "available": True,
        "artifact_type": "stage_admission_review_pack",
        "filename": STAGE_ADMISSION_REVIEW_PACK_FILENAME,
        "reviewer_filename": STAGE_ADMISSION_REVIEW_PACK_REVIEWER_FILENAME,
        "raw": raw,
        "display": display,
        "markdown": markdown,
    }


def _normalize_artifact_paths(artifact_paths: dict[str, Any] | None) -> dict[str, str]:
    payload = dict(artifact_paths or {})
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
    }


def _artifact_ref(payload: dict[str, Any], path_text: str) -> dict[str, Any]:
    return {
        "artifact_type": str(payload.get("artifact_type") or ""),
        "phase": str(payload.get("phase") or ""),
        "overall_status": str(payload.get("overall_status") or ""),
        "path": str(path_text or ""),
    }


def _build_artifact_lines(
    *,
    artifact_refs: dict[str, dict[str, Any]],
    readiness: dict[str, Any],
    metrology: dict[str, Any],
    bridge: dict[str, Any],
    reviewer_display: dict[str, Any],
) -> list[str]:
    readiness_display = dict(readiness.get("reviewer_display") or {})
    metrology_display = dict(metrology.get("reviewer_display") or {})
    bridge_display = dict(bridge.get("reviewer_display") or {})
    return [
        _artifact_line(
            "step2_readiness_summary.json",
            artifact_refs["step2_readiness_summary"]["path"],
            str(readiness_display.get("summary_text") or "").strip(),
        ),
        _artifact_line(
            "metrology_calibration_contract.json",
            artifact_refs["metrology_calibration_contract"]["path"],
            str(metrology_display.get("summary_text") or "").strip(),
        ),
        _artifact_line(
            "phase_transition_bridge.json",
            artifact_refs["phase_transition_bridge"]["path"],
            str(bridge_display.get("summary_text") or "").strip(),
        ),
        _artifact_line(
            "phase_transition_bridge_reviewer.md",
            artifact_refs["phase_transition_bridge_reviewer_artifact"]["path"],
            str(reviewer_display.get("summary_text") or "").strip(),
        ),
    ]


def _artifact_line(name: str, path_text: str, summary_text: str) -> str:
    line = f"`{name}`：{summary_text}".strip()
    if path_text:
        return f"{line}（{path_text}）"
    return line


def _render_stage_admission_review_pack_markdown(display: dict[str, Any]) -> str:
    lines = [
        f"# {display.get('title_text') or '阶段准入评审包 / Stage Admission Review Pack'}",
        "",
        "> 离线 governance handoff pack：仅用于 Step 2 tail / Stage 3 bridge 的 reviewer 审阅、治理留痕与后续 Stage 3 准入交接，不是 real acceptance，不能替代真实计量验证。",
        "",
        "## 当前阶段",
        "",
        f"- {display.get('current_stage_text') or '--'}",
        f"- {display.get('next_stage_text') or '--'}",
        f"- {display.get('status_line') or '--'}",
        f"- {display.get('engineering_isolation_text') or '--'}",
        f"- {display.get('real_acceptance_text') or '--'}",
        "",
        "## 当前已就绪的治理工件",
        "",
    ]
    lines.extend(f"- {line}" for line in list(display.get("artifact_lines") or []))
    lines.extend(
        [
            "",
            "## 现在执行",
            "",
            f"- {display.get('execute_now_text') or '--'}",
            "",
            "## 第三阶段执行",
            "",
            f"- {display.get('defer_to_stage3_text') or '--'}",
            "",
            "## 评审提示",
            "",
            f"- {display.get('blocking_text') or '--'}",
            f"- {display.get('warning_text') or '--'}",
            "",
            "## 建议交接清单",
            "",
            "### Reviewer",
            "",
        ]
    )
    lines.extend(f"- {line}" for line in list(display.get("reviewer_handoff_lines") or []))
    lines.extend(["", "### Engineer", ""])
    lines.extend(f"- {line}" for line in list(display.get("engineer_handoff_lines") or []))
    lines.extend(["", "### Stage 3 准入前补齐", ""])
    lines.extend(f"- {line}" for line in list(display.get("stage3_handoff_lines") or []))
    lines.extend(["", "## 附件路径", ""])
    lines.extend(f"- {line}" for line in list(display.get("artifact_path_lines") or []))
    return "\n".join(line for line in lines if str(line).strip() or line == "") + "\n"


def _text_list(values: Any) -> list[str]:
    rows: list[str] = []
    for value in list(values or []):
        text = str(value or "").strip()
        if text and text not in rows:
            rows.append(text)
    return rows
