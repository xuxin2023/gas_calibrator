"""Step 2 closeout readiness contracts — single source of truth for labels, status
buckets, blocker labels, next-step labels, and simulation-only boundary markers.

All closeout readiness surface text flows through this module.
No formal acceptance / formal claim / real acceptance language.

Step 2 boundary:
  - evidence_source = "simulated"
  - not_real_acceptance_evidence = True
  - not_ready_for_formal_claim = True
  - reviewer_only = True
  - readiness_mapping_only = True
"""

from __future__ import annotations

from typing import Any

# ---------------------------------------------------------------------------
# Version
# ---------------------------------------------------------------------------

CLOSEOUT_READINESS_CONTRACTS_VERSION: str = "2.20.0"

# ---------------------------------------------------------------------------
# Status buckets
# ---------------------------------------------------------------------------

CLOSEOUT_STATUS_OK: str = "ok"
CLOSEOUT_STATUS_ATTENTION: str = "attention"
CLOSEOUT_STATUS_BLOCKER: str = "blocker"
CLOSEOUT_STATUS_REVIEWER_ONLY: str = "reviewer_only"

CLOSEOUT_STATUS_BUCKETS: tuple[str, ...] = (
    CLOSEOUT_STATUS_OK,
    CLOSEOUT_STATUS_ATTENTION,
    CLOSEOUT_STATUS_BLOCKER,
    CLOSEOUT_STATUS_REVIEWER_ONLY,
)

# ---------------------------------------------------------------------------
# Title / summary labels (Chinese default)
# ---------------------------------------------------------------------------

CLOSEOUT_TITLE_ZH: str = "Step 2 收官就绪度"
CLOSEOUT_SUMMARY_LINE_ZH: str = (
    "Step 2 仿真收官视图：汇总当前阶段就绪状态、阻塞项与下一步。"
    "不是正式放行结论，不是 real acceptance evidence。"
)
CLOSEOUT_STATUS_OK_LABEL_ZH: str = "当前阶段就绪"
CLOSEOUT_STATUS_ATTENTION_LABEL_ZH: str = "存在需关注项"
CLOSEOUT_STATUS_BLOCKER_LABEL_ZH: str = "存在阻塞项"
CLOSEOUT_STATUS_REVIEWER_ONLY_LABEL_ZH: str = "仅限审阅观察"

# ---------------------------------------------------------------------------
# Title / summary labels (English fallback)
# ---------------------------------------------------------------------------

CLOSEOUT_TITLE_EN: str = "Step 2 Closeout Readiness"
CLOSEOUT_SUMMARY_LINE_EN: str = (
    "Step 2 simulation closeout view: aggregated readiness status, blockers, and next steps. "
    "Not a formal release conclusion. Not real acceptance evidence."
)
CLOSEOUT_STATUS_OK_LABEL_EN: str = "Phase ready"
CLOSEOUT_STATUS_ATTENTION_LABEL_EN: str = "Attention items present"
CLOSEOUT_STATUS_BLOCKER_LABEL_EN: str = "Blockers present"
CLOSEOUT_STATUS_REVIEWER_ONLY_LABEL_EN: str = "Reviewer-only observation"

# ---------------------------------------------------------------------------
# Blocker labels (Chinese default / English fallback)
# ---------------------------------------------------------------------------

CLOSEOUT_BLOCKER_LABELS_ZH: dict[str, str] = {
    "simulation_boundary_broken": "仿真边界已偏离",
    "real_bench_unlocked": "real bench 已解锁",
    "experiment_flags_enabled": "实验开关已开启",
    "evidence_incomplete": "治理证据不完整",
    "headless_smoke_missing": "headless smoke 路径缺失",
    "execution_gate_blocked": "执行门禁阻塞",
}

CLOSEOUT_BLOCKER_LABELS_EN: dict[str, str] = {
    "simulation_boundary_broken": "Simulation boundary broken",
    "real_bench_unlocked": "Real bench unlocked",
    "experiment_flags_enabled": "Experiment flags enabled",
    "evidence_incomplete": "Governance evidence incomplete",
    "headless_smoke_missing": "Headless smoke path missing",
    "execution_gate_blocked": "Execution gate blocked",
}

# ---------------------------------------------------------------------------
# Next-step labels (Chinese default / English fallback)
# ---------------------------------------------------------------------------

CLOSEOUT_NEXT_STEPS_ZH: dict[str, str] = {
    "fix_simulation_boundary": "修复仿真边界偏离",
    "complete_governance_evidence": "补齐治理证据字段",
    "verify_headless_smoke": "验证 headless smoke 路径",
    "resolve_experiment_flags": "关闭实验开关",
    "proceed_to_engineering_isolation": "可进入 engineering-isolation 准备",
    "await_real_acceptance": "等待 Step 3 真实验收（当前阶段不执行）",
}

CLOSEOUT_NEXT_STEPS_EN: dict[str, str] = {
    "fix_simulation_boundary": "Fix simulation boundary deviation",
    "complete_governance_evidence": "Complete governance evidence fields",
    "verify_headless_smoke": "Verify headless smoke path",
    "resolve_experiment_flags": "Disable experiment flags",
    "proceed_to_engineering_isolation": "Ready for engineering-isolation preparation",
    "await_real_acceptance": "Await Step 3 real acceptance (not executed in current phase)",
}

# ---------------------------------------------------------------------------
# Simulation-only / reviewer-only / non-claim boundary markers
# ---------------------------------------------------------------------------

CLOSEOUT_SIMULATION_ONLY_BOUNDARY_ZH: str = (
    "本视图仅基于 simulation / offline / headless 证据，"
    "不代表 real acceptance evidence，不构成正式放行结论。"
)
CLOSEOUT_SIMULATION_ONLY_BOUNDARY_EN: str = (
    "This view is based on simulation / offline / headless evidence only. "
    "It does not represent real acceptance evidence and does not constitute a formal release conclusion."
)

CLOSEOUT_REVIEWER_ONLY_NOTICE_ZH: str = "本视图仅供 reviewer 审阅，不作为 operator 操作依据。"
CLOSEOUT_REVIEWER_ONLY_NOTICE_EN: str = "This view is for reviewer review only, not as operator action basis."

CLOSEOUT_NON_CLAIM_NOTICE_ZH: str = "不形成 formal compliance claim / accreditation claim / real acceptance evidence。"
CLOSEOUT_NON_CLAIM_NOTICE_EN: str = "Does not form formal compliance claim / accreditation claim / real acceptance evidence."

# ---------------------------------------------------------------------------
# i18n keys
# ---------------------------------------------------------------------------

CLOSEOUT_I18N_KEYS: dict[str, str] = {
    "title": "closeout_readiness.title",
    "summary_line": "closeout_readiness.summary_line",
    "status_ok": "closeout_readiness.status.ok",
    "status_attention": "closeout_readiness.status.attention",
    "status_blocker": "closeout_readiness.status.blocker",
    "status_reviewer_only": "closeout_readiness.status.reviewer_only",
    "simulation_only_boundary": "closeout_readiness.simulation_only_boundary",
    "reviewer_only_notice": "closeout_readiness.reviewer_only_notice",
    "non_claim_notice": "closeout_readiness.non_claim_notice",
    "blockers_label": "closeout_readiness.blockers_label",
    "next_steps_label": "closeout_readiness.next_steps_label",
    "contributing_sections_label": "closeout_readiness.contributing_sections_label",
    # Gate fields (Step 2.19)
    "gate_status": "closeout_readiness.gate_status",
    "gate_summary": "closeout_readiness.gate_summary",
    "closeout_gate_alignment": "closeout_readiness.closeout_gate_alignment",
}

# ---------------------------------------------------------------------------
# Step 2 boundary markers (constant for all closeout readiness payloads)
# ---------------------------------------------------------------------------

CLOSEOUT_STEP2_BOUNDARY: dict[str, bool | str] = {
    "evidence_source": "simulated",
    "not_real_acceptance_evidence": True,
    "not_ready_for_formal_claim": True,
    "reviewer_only": True,
    "readiness_mapping_only": True,
    "primary_evidence_rewritten": False,
}

# ---------------------------------------------------------------------------
# Contributing section keys — the domains aggregated by the builder
# ---------------------------------------------------------------------------

CLOSEOUT_CONTRIBUTING_SECTIONS: tuple[str, ...] = (
    "compact_summary",
    "governance_handoff",
    "parity_resilience",
    "acceptance_governance",
    "phase_evidence",
)

CLOSEOUT_CONTRIBUTING_SECTION_LABELS_ZH: dict[str, str] = {
    "compact_summary": "紧凑摘要",
    "governance_handoff": "治理交接",
    "parity_resilience": "一致性/韧性",
    "acceptance_governance": "验收治理",
    "phase_evidence": "阶段证据",
}

CLOSEOUT_CONTRIBUTING_SECTION_LABELS_EN: dict[str, str] = {
    "compact_summary": "Compact Summary",
    "governance_handoff": "Governance Handoff",
    "parity_resilience": "Parity/Resilience",
    "acceptance_governance": "Acceptance Governance",
    "phase_evidence": "Phase Evidence",
}

# ---------------------------------------------------------------------------
# Helper: resolve label
# ---------------------------------------------------------------------------


def resolve_closeout_title(*, lang: str = "zh") -> str:
    return CLOSEOUT_TITLE_EN if lang == "en" else CLOSEOUT_TITLE_ZH


def resolve_closeout_summary_line(*, lang: str = "zh") -> str:
    return CLOSEOUT_SUMMARY_LINE_EN if lang == "en" else CLOSEOUT_SUMMARY_LINE_ZH


def resolve_closeout_status_label(status: str, *, lang: str = "zh") -> str:
    if status == CLOSEOUT_STATUS_OK:
        return CLOSEOUT_STATUS_OK_LABEL_EN if lang == "en" else CLOSEOUT_STATUS_OK_LABEL_ZH
    if status == CLOSEOUT_STATUS_ATTENTION:
        return CLOSEOUT_STATUS_ATTENTION_LABEL_EN if lang == "en" else CLOSEOUT_STATUS_ATTENTION_LABEL_ZH
    if status == CLOSEOUT_STATUS_BLOCKER:
        return CLOSEOUT_STATUS_BLOCKER_LABEL_EN if lang == "en" else CLOSEOUT_STATUS_BLOCKER_LABEL_ZH
    if status == CLOSEOUT_STATUS_REVIEWER_ONLY:
        return CLOSEOUT_STATUS_REVIEWER_ONLY_LABEL_EN if lang == "en" else CLOSEOUT_STATUS_REVIEWER_ONLY_LABEL_ZH
    return status


def resolve_closeout_blocker_label(key: str, *, lang: str = "zh") -> str:
    if lang == "en":
        return CLOSEOUT_BLOCKER_LABELS_EN.get(key, key)
    return CLOSEOUT_BLOCKER_LABELS_ZH.get(key, key)


def resolve_closeout_next_step_label(key: str, *, lang: str = "zh") -> str:
    if lang == "en":
        return CLOSEOUT_NEXT_STEPS_EN.get(key, key)
    return CLOSEOUT_NEXT_STEPS_ZH.get(key, key)


def resolve_closeout_simulation_only_boundary(*, lang: str = "zh") -> str:
    return CLOSEOUT_SIMULATION_ONLY_BOUNDARY_EN if lang == "en" else CLOSEOUT_SIMULATION_ONLY_BOUNDARY_ZH


def resolve_closeout_reviewer_only_notice(*, lang: str = "zh") -> str:
    return CLOSEOUT_REVIEWER_ONLY_NOTICE_EN if lang == "en" else CLOSEOUT_REVIEWER_ONLY_NOTICE_ZH


def resolve_closeout_non_claim_notice(*, lang: str = "zh") -> str:
    return CLOSEOUT_NON_CLAIM_NOTICE_EN if lang == "en" else CLOSEOUT_NON_CLAIM_NOTICE_ZH


def resolve_closeout_contributing_section_label(key: str, *, lang: str = "zh") -> str:
    if lang == "en":
        return CLOSEOUT_CONTRIBUTING_SECTION_LABELS_EN.get(key, key)
    return CLOSEOUT_CONTRIBUTING_SECTION_LABELS_ZH.get(key, key)


# ---------------------------------------------------------------------------
# Gate status labels (Step 2.19) — Chinese default / English fallback
# ---------------------------------------------------------------------------

GATE_STATUS_LABEL_ZH: dict[str, str] = {
    "ready_for_engineering_isolation": "门禁已就绪",
    "not_ready": "门禁未就绪",
}

GATE_STATUS_LABEL_EN: dict[str, str] = {
    "ready_for_engineering_isolation": "Gates ready",
    "not_ready": "Gates not ready",
}


def resolve_gate_status_label(status: str, *, lang: str = "zh") -> str:
    """Resolve gate status label. Chinese default, English fallback."""
    if lang == "en":
        return GATE_STATUS_LABEL_EN.get(status, status)
    return GATE_STATUS_LABEL_ZH.get(status, status)


# ---------------------------------------------------------------------------
# Fallback compatibility notes (Step 2.19)
# ---------------------------------------------------------------------------

CLOSEOUT_FALLBACK_NOTE_ZH: str = (
    "本数据为兼容性 fallback 生成，非原始持久化 closeout readiness。"
)
CLOSEOUT_FALLBACK_NOTE_EN: str = (
    "This data is generated by compatibility fallback, not from persisted closeout readiness."
)


# ---------------------------------------------------------------------------
# Fallback helper (Step 2.19) — unified fallback for missing closeout readiness
# ---------------------------------------------------------------------------


def build_closeout_readiness_fallback(
    *,
    lang: str = "zh",
    include_compatibility_note: bool = True,
) -> dict[str, Any]:
    """Build closeout readiness fallback default value.

    Guarantees all Step 2 boundary markers.
    Does not modify old run files; generates in-memory only.
    """
    from datetime import datetime, timezone

    _boundary = CLOSEOUT_SIMULATION_ONLY_BOUNDARY_EN if lang == "en" else CLOSEOUT_SIMULATION_ONLY_BOUNDARY_ZH
    _status_label = resolve_closeout_status_label(CLOSEOUT_STATUS_REVIEWER_ONLY, lang=lang)
    _summary_line = (
        "Step 2 closeout: fallback — no persisted data. Not real acceptance."
        if lang == "en"
        else "Step 2 收官：fallback — 无持久化数据。不是 real acceptance。"
    )
    _summary_lines = [
        CLOSEOUT_TITLE_EN if lang == "en" else CLOSEOUT_TITLE_ZH,
        f"Status: {_status_label}" if lang == "en" else f"状态：{_status_label}",
        _summary_line,
        _boundary,
        resolve_closeout_reviewer_only_notice(lang=lang),
        resolve_closeout_non_claim_notice(lang=lang),
    ]

    result: dict[str, Any] = {
        "schema_version": "1.0",
        "artifact_type": "step2_closeout_readiness_fallback",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "run_id": "",
        "phase": "step2_closeout",
        "closeout_status": CLOSEOUT_STATUS_REVIEWER_ONLY,
        "closeout_readiness_source": "fallback",
        "closeout_status_label": _status_label,
        "reviewer_summary_line": _summary_line,
        "reviewer_summary_lines": _summary_lines,
        "blockers": [],
        "next_steps": [],
        "contributing_sections": [],
        "simulation_only_boundary": _boundary,
        "rendered_compact_sections": [],
        # Gate fields (Step 2.19)
        "gate_status": "not_ready",
        "gate_summary": {
            "pass_count": 0,
            "total_count": 0,
            "blocked_count": 0,
            "blocked_gate_ids": [],
        },
        "closeout_gate_alignment": {
            "closeout_status": CLOSEOUT_STATUS_REVIEWER_ONLY,
            "gate_status": "not_ready",
            "aligned": True,
        },
        # Step 2 boundary markers — all enforced
        "evidence_source": "simulated",
        "not_real_acceptance_evidence": True,
        "not_ready_for_formal_claim": True,
        "reviewer_only": True,
        "readiness_mapping_only": True,
        "primary_evidence_rewritten": False,
        "real_acceptance_ready": False,
        # Raw inputs — empty for fallback
        "source_readiness_status": "not_ready",
        "source_blocking_items": [],
        "source_warning_items": [],
    }

    if include_compatibility_note:
        note = CLOSEOUT_FALLBACK_NOTE_EN if lang == "en" else CLOSEOUT_FALLBACK_NOTE_ZH
        result["compatibility_note"] = note

    return result
