from __future__ import annotations

from typing import Any

from .governance_handoff_contracts import GOVERNANCE_HANDOFF_FILENAMES as _GOV_FILENAMES


PHASE_TRANSITION_BRIDGE_REVIEWER_FILENAME = _GOV_FILENAMES["phase_transition_bridge_reviewer_artifact"]


def build_phase_transition_bridge_reviewer_artifact(
    bridge: dict[str, Any] | None,
) -> dict[str, Any]:
    section = _build_reviewer_section(bridge)
    if not bool(section.get("available", False)):
        return {
            "available": False,
            "artifact_type": "phase_transition_bridge_reviewer_artifact",
            "filename": PHASE_TRANSITION_BRIDGE_REVIEWER_FILENAME,
            "raw": {},
            "display": {},
            "section": section,
            "markdown": "",
        }

    raw = dict(section.get("raw", {}) or {})
    display = dict(section.get("display", {}) or {})
    engineering_isolation_text = str(display.get("engineering_isolation_text") or "").strip()
    real_acceptance_text = str(display.get("real_acceptance_text") or "").strip()
    markdown = _render_phase_transition_bridge_reviewer_markdown(
        title_text=str(display.get("title_text") or "阶段准入桥 / Phase Transition Bridge"),
        summary_text=str(display.get("summary_text") or "").strip(),
        status_line=str(display.get("status_line") or "").strip(),
        current_stage_text=str(display.get("current_stage_text") or "").strip(),
        next_stage_text=str(display.get("next_stage_text") or "").strip(),
        execute_now_text=str(display.get("execute_now_text") or "").strip(),
        defer_to_stage3_text=str(display.get("defer_to_stage3_text") or "").strip(),
        blocking_text=str(display.get("blocking_text") or "").strip(),
        warning_text=str(display.get("warning_text") or "").strip(),
        engineering_isolation_text=engineering_isolation_text,
        real_acceptance_text=real_acceptance_text,
    )
    reviewer_display = dict(display)
    reviewer_display["engineering_isolation_text"] = engineering_isolation_text
    reviewer_display["real_acceptance_text"] = real_acceptance_text
    reviewer_display["markdown"] = markdown
    return {
        "available": True,
        "artifact_type": "phase_transition_bridge_reviewer_artifact",
        "filename": PHASE_TRANSITION_BRIDGE_REVIEWER_FILENAME,
        "raw": raw,
        "display": reviewer_display,
        "section": section,
        "markdown": markdown,
    }


def _build_reviewer_section(
    bridge: dict[str, Any] | None,
) -> dict[str, Any]:
    payload = dict(bridge or {})
    reviewer_display = dict(payload.get("reviewer_display") or {})
    if not payload or not reviewer_display:
        return {
            "available": False,
            "raw": {},
            "display": {},
        }

    display = {
        "title_text": "阶段准入桥",
        "summary_text": str(reviewer_display.get("summary_text") or "").strip(),
        "status_line": str(reviewer_display.get("status_line") or "").strip(),
        "current_stage_text": str(reviewer_display.get("current_stage_text") or "").strip(),
        "next_stage_text": str(reviewer_display.get("next_stage_text") or "").strip(),
        "engineering_isolation_text": str(
            reviewer_display.get("engineering_isolation_text") or ""
        ).strip(),
        "real_acceptance_text": str(
            reviewer_display.get("real_acceptance_text") or ""
        ).strip(),
        "execute_now_text": str(reviewer_display.get("execute_now_text") or "").strip(),
        "defer_to_stage3_text": str(
            reviewer_display.get("defer_to_stage3_text") or ""
        ).strip(),
        "blocking_text": str(reviewer_display.get("blocking_text") or "").strip(),
        "warning_text": str(reviewer_display.get("warning_text") or "").strip(),
    }
    card_lines = [
        display["current_stage_text"],
        display["next_stage_text"],
        display["engineering_isolation_text"],
        display["real_acceptance_text"],
        display["execute_now_text"],
        display["defer_to_stage3_text"],
        display["warning_text"],
    ]
    section_lines = [
        display["summary_text"],
        display["status_line"],
        *card_lines[:6],
        display["blocking_text"],
        display["warning_text"],
    ]
    display["card_lines"] = [line for line in card_lines if line]
    display["card_text"] = "\n".join(display["card_lines"])
    display["section_lines"] = [line for line in section_lines if line]
    display["section_text"] = "\n".join(display["section_lines"])

    return {
        "available": True,
        "raw": {
            "overall_status": str(payload.get("overall_status") or "not_ready"),
            "recommended_next_stage": str(
                payload.get("recommended_next_stage") or "close_step2_tail_gaps"
            ),
            "ready_for_engineering_isolation": bool(
                payload.get("ready_for_engineering_isolation", False)
            ),
            "real_acceptance_ready": bool(payload.get("real_acceptance_ready", False)),
        },
        "display": display,
    }


def _render_phase_transition_bridge_reviewer_markdown(
    *,
    title_text: str,
    summary_text: str,
    status_line: str,
    current_stage_text: str,
    next_stage_text: str,
    execute_now_text: str,
    defer_to_stage3_text: str,
    blocking_text: str,
    warning_text: str,
    engineering_isolation_text: str,
    real_acceptance_text: str,
) -> str:
    lines = [
        f"# {title_text}",
        "",
        "> 离线 reviewer artifact：仅用于 Step 2 tail / Stage 3 bridge 的阶段评审、导出留痕与后续准入审阅，不是 real acceptance，不能替代真实计量验证。",
        "",
        "## 审阅摘要",
        "",
        summary_text,
        "",
        "## 状态与阶段",
        "",
        f"- {status_line}",
        f"- {current_stage_text}",
        f"- {next_stage_text}",
        f"- {engineering_isolation_text}",
        f"- {real_acceptance_text}",
        "",
        "## 当前执行",
        "",
        f"- {execute_now_text}",
        "",
        "## 第三阶段执行",
        "",
        f"- {defer_to_stage3_text}",
        "",
        "## 评审提示",
        "",
        f"- {blocking_text}",
        f"- {warning_text}",
    ]
    return "\n".join(line for line in lines if str(line).strip() or line == "") + "\n"
