from __future__ import annotations

from typing import Any


def build_phase_transition_bridge_digest(
    bridge: dict[str, Any] | None,
) -> dict[str, Any]:
    payload = dict(bridge or {})
    reviewer_display = dict(payload.get("reviewer_display") or {})
    if not payload and not reviewer_display:
        return {}

    overall_status = str(payload.get("overall_status") or "not_ready")
    recommended_next_stage = str(payload.get("recommended_next_stage") or "close_step2_tail_gaps")
    ready_for_engineering_isolation = bool(payload.get("ready_for_engineering_isolation", False))
    real_acceptance_ready = bool(payload.get("real_acceptance_ready", False))
    execute_now_items = _text_list(payload.get("execute_now_in_step2_tail"))
    defer_items = _text_list(payload.get("defer_to_stage3_real_validation"))
    blocking_items = _text_list(payload.get("blocking_items"))
    warning_items = _text_list(payload.get("warning_items"))
    gate_lines = [str(item).strip() for item in list(reviewer_display.get("gate_lines") or []) if str(item).strip()]

    status_line = str(reviewer_display.get("status_line") or "").strip() or _default_status_line(
        overall_status=overall_status,
        ready_for_engineering_isolation=ready_for_engineering_isolation,
    )
    summary_text = str(reviewer_display.get("summary_text") or "").strip() or (
        "阶段桥工件：统一汇总 Step 2 readiness 与计量设计合同，"
        "用于说明离第三阶段真实计量验证还有多远。不是 real acceptance。"
    )
    current_stage_text = str(reviewer_display.get("current_stage_text") or "").strip() or (
        "当前阶段：Step 2 tail / Stage 3 bridge。"
    )
    next_stage_text = str(reviewer_display.get("next_stage_text") or "").strip() or (
        "下一阶段：先补齐 Step 2 tail 剩余缺口，再进入 engineering-isolation 准备。"
    )
    execute_now_text = str(reviewer_display.get("execute_now_text") or "").strip() or _default_execute_now_text(
        execute_now_items
    )
    defer_to_stage3_text = str(reviewer_display.get("defer_to_stage3_text") or "").strip() or _default_defer_text(
        defer_items
    )
    blocking_text = str(reviewer_display.get("blocking_text") or "").strip() or _default_blocking_text(
        blocking_items
    )
    warning_text = str(reviewer_display.get("warning_text") or "").strip() or _default_warning_text(
        warning_items
    )

    report_lines = [
        summary_text,
        status_line,
        current_stage_text,
        next_stage_text,
        execute_now_text,
        defer_to_stage3_text,
        blocking_text,
        warning_text,
    ]
    report_lines.extend(gate_lines[:3])

    return {
        "available": True,
        "overall_status": overall_status,
        "recommended_next_stage": recommended_next_stage,
        "ready_for_engineering_isolation": ready_for_engineering_isolation,
        "real_acceptance_ready": real_acceptance_ready,
        "summary_text": summary_text,
        "status_line": status_line,
        "current_stage_text": current_stage_text,
        "next_stage_text": next_stage_text,
        "execute_now_text": execute_now_text,
        "defer_to_stage3_text": defer_to_stage3_text,
        "blocking_text": blocking_text,
        "warning_text": warning_text,
        "gate_lines": gate_lines,
        "report_lines": [line for line in report_lines if str(line).strip()],
    }


def build_phase_transition_bridge_panel_payload(
    bridge: dict[str, Any] | None,
) -> dict[str, Any]:
    digest = build_phase_transition_bridge_digest(bridge)
    if not digest.get("available"):
        return {
            "available": False,
            "raw": {},
            "display": {},
        }

    warning_text = _panel_warning_text(str(digest.get("warning_text") or "").strip())
    blocking_text = str(digest.get("blocking_text") or "").strip()
    card_lines = [
        str(digest.get("current_stage_text") or "").strip(),
        str(digest.get("next_stage_text") or "").strip(),
        str(digest.get("execute_now_text") or "").strip(),
        str(digest.get("defer_to_stage3_text") or "").strip(),
        warning_text,
    ]
    section_lines = [
        str(digest.get("summary_text") or "").strip(),
        str(digest.get("status_line") or "").strip(),
        str(digest.get("current_stage_text") or "").strip(),
        str(digest.get("next_stage_text") or "").strip(),
        str(digest.get("execute_now_text") or "").strip(),
        str(digest.get("defer_to_stage3_text") or "").strip(),
        blocking_text,
        warning_text,
    ]

    return {
        "available": True,
        "raw": {
            "overall_status": str(digest.get("overall_status") or "not_ready"),
            "recommended_next_stage": str(digest.get("recommended_next_stage") or "close_step2_tail_gaps"),
            "ready_for_engineering_isolation": bool(digest.get("ready_for_engineering_isolation", False)),
            "real_acceptance_ready": bool(digest.get("real_acceptance_ready", False)),
        },
        "display": {
            "title_text": "阶段准入桥",
            "summary_text": str(digest.get("summary_text") or "").strip(),
            "status_line": str(digest.get("status_line") or "").strip(),
            "current_stage_text": str(digest.get("current_stage_text") or "").strip(),
            "next_stage_text": str(digest.get("next_stage_text") or "").strip(),
            "execute_now_text": str(digest.get("execute_now_text") or "").strip(),
            "defer_to_stage3_text": str(digest.get("defer_to_stage3_text") or "").strip(),
            "blocking_text": blocking_text,
            "warning_text": warning_text,
            "card_lines": [line for line in card_lines if str(line).strip()],
            "card_text": "\n".join(line for line in card_lines if str(line).strip()),
            "section_lines": [line for line in section_lines if str(line).strip()],
            "section_text": "\n".join(line for line in section_lines if str(line).strip()),
        },
    }


def _text_list(value: Any) -> list[str]:
    rows: list[str] = []
    for item in list(value or []):
        text = str(item or "").strip()
        if text and text not in rows:
            rows.append(text)
    return rows


def _default_status_line(
    *,
    overall_status: str,
    ready_for_engineering_isolation: bool,
) -> str:
    if ready_for_engineering_isolation or overall_status == "ready_for_engineering_isolation":
        return "阶段状态：当前仍处于 Step 2 tail / Stage 3 bridge，但已具备 engineering-isolation 准备。不是 real acceptance。"
    if overall_status == "step2_tail_in_progress":
        return "阶段状态：当前仍处于 Step 2 tail / Stage 3 bridge，制度与治理工件已到位，但仍有 Step 2 收尾项未闭环。不是 real acceptance。"
    if overall_status == "blocked_before_stage3":
        return "阶段状态：当前仍停留在第三阶段前的准入桥阶段，尚不能进入真实计量验证。不是 real acceptance。"
    return "阶段状态：当前处于 Step 2 tail / Stage 3 bridge。不是 real acceptance。"


def _default_execute_now_text(items: list[str]) -> str:
    if not items:
        return "现在执行：无新增 Step 2 tail 事项。"
    return "现在执行：" + " / ".join(items) + "。"


def _default_defer_text(items: list[str]) -> str:
    if not items:
        return "第三阶段执行：无。"
    return "第三阶段执行：" + " / ".join(items) + "。"


def _default_blocking_text(items: list[str]) -> str:
    if not items:
        return "阻塞项：无。"
    return "阻塞项：" + " / ".join(items) + "。"


def _default_warning_text(items: list[str]) -> str:
    if not items:
        return "提示：仍然只是 simulation/offline/headless evidence，不能替代真实计量验证。"
    return "提示：" + " / ".join(items) + "。"
