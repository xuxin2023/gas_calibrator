from __future__ import annotations

from typing import Any

from .phase_transition_bridge_presenter import build_phase_transition_bridge_panel_payload


PHASE_TRANSITION_BRIDGE_REVIEWER_FILENAME = "phase_transition_bridge_reviewer.md"


def build_phase_transition_bridge_reviewer_artifact(
    bridge: dict[str, Any] | None,
) -> dict[str, Any]:
    section = build_phase_transition_bridge_panel_payload(bridge)
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
    engineering_isolation_text = (
        "engineering-isolation 准备：已具备。"
        if bool(raw.get("ready_for_engineering_isolation", False))
        else "engineering-isolation 准备：尚未具备。"
    )
    real_acceptance_text = (
        "real acceptance 准备：已具备。"
        if bool(raw.get("real_acceptance_ready", False))
        else "real acceptance 准备：尚未具备。"
    )
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
