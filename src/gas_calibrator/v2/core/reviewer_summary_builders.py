"""Reviewer summary builders — single source of truth for reviewer-facing
compact summary generation across results, reports, review center, historical,
and reviewer artifacts.

This module centralizes the builder logic for:
- Measurement review digest compact summary
- Readiness review digest compact summary
- Phase evidence compact summary
- V1.2 alignment compact summary
- Governance handoff compact summary
- Parity / resilience compact summary

Design principles:
- Chinese default, English fallback
- No formal acceptance / formal claim language
- All evidence is simulation-only
- Single source of truth — modules import from here, not hardcode their own copies
- Does not change business data structures, only unifies summary generation
"""

from __future__ import annotations

from typing import Any

from .phase_evidence_display_contracts import (
    MEASUREMENT_DIGEST_LABELS as _MEASUREMENT_DIGEST,
    MEASUREMENT_DIGEST_LABELS_EN as _MEASUREMENT_DIGEST_EN,
    READINESS_DIGEST_LABELS as _READINESS_DIGEST,
    READINESS_DIGEST_LABELS_EN as _READINESS_DIGEST_EN,
    PHASE_EVIDENCE_STEP2_BOUNDARY,
    PHASE_EVIDENCE_ARTIFACT_KEYS,
    PHASE_EVIDENCE_SUMMARY_TEXTS,
    PHASE_EVIDENCE_SUMMARY_TEXTS_EN,
    PHASE_TERMS,
    PHASE_TERMS_EN,
    BRIDGE_REVIEWER_TEXTS,
    BRIDGE_REVIEWER_TEXTS_EN,
    RESULTS_SUMMARY_LABELS as _RESULTS_SUMMARY_LABELS,
    RESULTS_SUMMARY_LABELS_EN as _RESULTS_SUMMARY_LABELS_EN,
    resolve_measurement_digest_label,
    resolve_readiness_digest_label,
    resolve_phase_term,
    resolve_bridge_reviewer_text,
)
from ..ui_v2.i18n import (
    display_compare_status,
    display_phase,
    display_route,
    t,
)

# ---------------------------------------------------------------------------
# Version
# ---------------------------------------------------------------------------
REVIEWER_SUMMARY_BUILDERS_VERSION: str = "2.12.0"

# ---------------------------------------------------------------------------
# V1.2 compact summary contract keys
# ---------------------------------------------------------------------------
V12_COMPACT_SUMMARY_KEYS: tuple[str, ...] = (
    "point_taxonomy",
    "measurement_phase_coverage",
    "phase_transition_bridge",
    "parity_resilience",
    "governance_blockers",
    "v12_alignment",
)

# ---------------------------------------------------------------------------
# V1.2 compact summary labels — Chinese default / English fallback
# ---------------------------------------------------------------------------
V12_COMPACT_SUMMARY_LABELS: dict[str, str] = {
    "point_taxonomy": "点位语义",
    "measurement_phase_coverage": "测量阶段覆盖",
    "phase_transition_bridge": "阶段桥接",
    "parity_resilience": "一致性/韧性",
    "governance_blockers": "治理阻塞",
    "v12_alignment": "V1.2 对齐",
    "header": "V1.2 审阅摘要",
    "simulated_only_note": "仅仿真 / 仅审阅 / 非真实验收证据",
    "no_formal_claim": "不构成正式放行结论",
}

V12_COMPACT_SUMMARY_LABELS_EN: dict[str, str] = {
    "point_taxonomy": "Point Taxonomy",
    "measurement_phase_coverage": "Measurement Phase Coverage",
    "phase_transition_bridge": "Phase Bridge",
    "parity_resilience": "Parity/Resilience",
    "governance_blockers": "Governance Blockers",
    "v12_alignment": "V1.2 Alignment",
    "header": "V1.2 Reviewer Summary",
    "simulated_only_note": "Simulated only / Reviewer only / Not real acceptance evidence",
    "no_formal_claim": "Does not constitute a formal release conclusion",
}

# ---------------------------------------------------------------------------
# Governance handoff compact summary labels
# ---------------------------------------------------------------------------
GOVERNANCE_HANDOFF_LABELS: dict[str, str] = {
    "current_stage": "当前阶段",
    "blockers": "阻塞项",
    "next_steps": "下一步",
    "evidence_source": "证据来源",
    "no_blockers": "无",
}

GOVERNANCE_HANDOFF_LABELS_EN: dict[str, str] = {
    "current_stage": "Current stage",
    "blockers": "Blockers",
    "next_steps": "Next steps",
    "evidence_source": "Evidence source",
    "no_blockers": "None",
}

# ---------------------------------------------------------------------------
# Parity / resilience compact summary labels
# ---------------------------------------------------------------------------
PARITY_RESILIENCE_LABELS: dict[str, str] = {
    "parity_status": "一致性状态",
    "resilience_status": "韧性状态",
    "parity_last_run": "最近一致性",
    "resilience_last_run": "最近韧性",
    "not_available": "不可用",
}

PARITY_RESILIENCE_LABELS_EN: dict[str, str] = {
    "parity_status": "Parity status",
    "resilience_status": "Resilience status",
    "parity_last_run": "Latest parity",
    "resilience_last_run": "Latest resilience",
    "not_available": "Not available",
}


def _compare_bundle_payload(payload: dict[str, Any]) -> dict[str, Any]:
    raw = dict(payload or {})
    return dict(
        raw.get("latest_control_flow_compare")
        or raw.get("control_flow_compare")
        or raw.get("control_flow_compare_summary")
        or raw
    )


def _compare_diff_state(matches: Any) -> str:
    return "diff_present" if matches is False else "no_diff"


def _physical_route_state(has_mismatch: Any) -> str:
    return "yes" if bool(has_mismatch) else "no"


def _display_compare_diff_state(state: str, *, lang: str = "zh") -> str:
    key = str(state or "").strip().lower()
    if key == "diff_present":
        return t(
            "reviewer_summary.offline_compare.diff_present",
            default="存在差异" if lang != "en" else "Diff present",
        )
    if key == "no_diff":
        return t(
            "reviewer_summary.offline_compare.no_diff",
            default="无差异" if lang != "en" else "No diff",
        )
    return t("common.none")


def _display_physical_route_state(state: str, *, lang: str = "zh") -> str:
    key = str(state or "").strip().lower()
    if key == "yes":
        return t("common.yes", default="是" if lang != "en" else "Yes")
    if key == "no":
        return t("common.no", default="否" if lang != "en" else "No")
    return t("common.none")


def _display_key_action_mismatches(value: Any, *, lang: str = "zh") -> str:
    if isinstance(value, str):
        text = value.strip()
        if not text or text.lower() == "none":
            return t("common.none")
        return text
    rows = [str(item).strip() for item in list(value or []) if str(item).strip()]
    return " / ".join(rows) if rows else t("common.none")


def _localized_compare_next_check(value: Any, *, lang: str = "zh") -> str:
    text = str(value or "").strip()
    if not text:
        return t("common.none")
    lower = text.lower()
    mapping = {
        "inspect point presence diff": (
            "reviewer_summary.offline_compare.next_check_actions.inspect_point_presence_diff",
            "检查点位存在差异",
            "Inspect point presence diff",
        ),
        "inspect sample count diff": (
            "reviewer_summary.offline_compare.next_check_actions.inspect_sample_count_diff",
            "检查样本数差异",
            "Inspect sample count diff",
        ),
        "inspect route trace diff": (
            "reviewer_summary.offline_compare.next_check_actions.inspect_route_trace_diff",
            "检查路由轨迹差异",
            "Inspect route trace diff",
        ),
        "inspect key action mismatches": (
            "reviewer_summary.offline_compare.next_check_actions.inspect_key_action_mismatches",
            "检查关键动作不一致",
            "Inspect key action mismatches",
        ),
        "inspect route physical mismatch": (
            "reviewer_summary.offline_compare.next_check_actions.inspect_route_physical_mismatch",
            "检查物理气路不一致",
            "Inspect physical route mismatch",
        ),
        "review compare report and keep simulated-only gate": (
            "reviewer_summary.offline_compare.next_check_actions.review_compare_report_keep_gate",
            "复核对齐报告并保持仅仿真门禁",
            "Review compare report and keep simulated-only gate",
        ),
        "review compare report": (
            "reviewer_summary.offline_compare.next_check_actions.review_compare_report",
            "复核对齐报告",
            "Review compare report",
        ),
    }
    if lower in mapping:
        key, zh_default, en_default = mapping[lower]
        return t(key, default=zh_default if lang != "en" else en_default)
    if lower.startswith("review ") and lower.endswith(" failure"):
        phase = text[7:-8].strip()
        return t(
            "reviewer_summary.offline_compare.next_check_actions.review_failure_phase",
            phase=phase,
            default=(f"复核 {phase} 失败阶段" if lang != "en" else f"Review {phase} failure"),
        )
    return text


def _control_flow_compare_compact_fields(payload: dict[str, Any]) -> dict[str, Any]:
    compare = _compare_bundle_payload(payload)
    metadata = dict(compare.get("metadata") or {})
    route_execution_summary = dict(compare.get("route_execution_summary") or {})
    presence = dict(compare.get("presence") or {})
    sample_count = dict(compare.get("sample_count") or {})
    route_sequence = dict(compare.get("route_sequence") or {})
    key_actions = dict(compare.get("key_actions") or {})

    key_action_mismatches = compare.get("key_action_mismatches")
    if isinstance(key_action_mismatches, str):
        key_action_mismatch_rows = [
            fragment.strip()
            for fragment in key_action_mismatches.split(",")
            if fragment.strip() and fragment.strip().lower() != "none"
        ]
    else:
        key_action_mismatch_rows = [
            str(item).strip()
            for item in list(key_action_mismatches or [])
            if str(item).strip() and str(item).strip().lower() != "none"
        ]
    if not key_action_mismatch_rows:
        key_action_mismatch_rows = [
            name
            for name, item in key_actions.items()
            if not bool(dict(item or {}).get("matches", True))
        ]

    compare_status = str(compare.get("compare_status") or "MISMATCH").strip() or "MISMATCH"
    validation_profile = str(
        compare.get("validation_profile") or metadata.get("validation_profile") or "--"
    ).strip() or "--"
    target_route = str(
        compare.get("target_route")
        or route_execution_summary.get("target_route")
        or dict(compare.get("validation_scope") or {}).get("target_route")
        or "--"
    ).strip() or "--"
    first_failure_phase = str(
        compare.get("first_failure_phase") or route_execution_summary.get("first_failure_phase") or "--"
    ).strip() or "--"
    point_presence_diff = str(
        compare.get("point_presence_diff") or _compare_diff_state(presence.get("matches", True))
    ).strip() or "no_diff"
    sample_count_diff = str(
        compare.get("sample_count_diff") or _compare_diff_state(sample_count.get("matches", True))
    ).strip() or "no_diff"
    route_trace_diff = str(
        compare.get("route_trace_diff") or _compare_diff_state(route_sequence.get("matches", True))
    ).strip() or "no_diff"
    physical_route_mismatch = str(
        compare.get("physical_route_mismatch")
        or _physical_route_state(route_execution_summary.get("has_physical_route_mismatches", False))
    ).strip() or "no"
    next_check = str(compare.get("next_check") or "").strip()
    if not next_check:
        if point_presence_diff == "diff_present":
            next_check = "inspect point presence diff"
        elif sample_count_diff == "diff_present":
            next_check = "inspect sample count diff"
        elif route_trace_diff == "diff_present":
            next_check = "inspect route trace diff"
        elif key_action_mismatch_rows:
            next_check = "inspect key action mismatches"
        elif physical_route_mismatch == "yes":
            next_check = "inspect route physical mismatch"
        elif first_failure_phase not in {"", "--"}:
            next_check = f"review {first_failure_phase} failure"
        elif compare_status.upper() == "MATCH":
            next_check = "review compare report and keep simulated-only gate"
        else:
            next_check = "review compare report"

    severity = "attention"
    if compare_status.upper() == "MATCH" and point_presence_diff == "no_diff" and sample_count_diff == "no_diff" and route_trace_diff == "no_diff" and not key_action_mismatch_rows and physical_route_mismatch == "no":
        severity = "info"

    return {
        "compare_status": compare_status,
        "compare_status_display": display_compare_status(compare_status, default=compare_status),
        "validation_profile": validation_profile,
        "target_route": target_route,
        "target_route_display": display_route(target_route, default=target_route),
        "first_failure_phase": first_failure_phase,
        "first_failure_phase_display": display_phase(first_failure_phase, default=first_failure_phase),
        "point_presence_diff": point_presence_diff,
        "sample_count_diff": sample_count_diff,
        "route_trace_diff": route_trace_diff,
        "key_action_mismatches": key_action_mismatch_rows,
        "physical_route_mismatch": physical_route_mismatch,
        "next_check": next_check,
        "next_check_display": _localized_compare_next_check(next_check),
        "severity": severity,
    }


# ---------------------------------------------------------------------------
# Resolve helpers
# ---------------------------------------------------------------------------

def resolve_v12_compact_label(key: str, *, lang: str = "zh") -> str:
    """Resolve V1.2 compact summary label. Chinese default, English fallback."""
    if lang == "en":
        return V12_COMPACT_SUMMARY_LABELS_EN.get(key, key)
    return V12_COMPACT_SUMMARY_LABELS.get(key, key)


def resolve_governance_handoff_label(key: str, *, lang: str = "zh") -> str:
    """Resolve governance handoff label. Chinese default, English fallback."""
    if lang == "en":
        return GOVERNANCE_HANDOFF_LABELS_EN.get(key, key)
    return GOVERNANCE_HANDOFF_LABELS.get(key, key)


def resolve_parity_resilience_label(key: str, *, lang: str = "zh") -> str:
    """Resolve parity/resilience label. Chinese default, English fallback."""
    if lang == "en":
        return PARITY_RESILIENCE_LABELS_EN.get(key, key)
    return PARITY_RESILIENCE_LABELS.get(key, key)


# ---------------------------------------------------------------------------
# Shared builder: measurement review digest compact summary
# ---------------------------------------------------------------------------

def build_measurement_digest_compact_summary(
    payload: dict[str, Any],
    *,
    include_boundary: bool = True,
    include_non_claim: bool = True,
) -> dict[str, Any]:
    """Build a compact measurement review digest summary from payload.

    This is the single-source builder that all surfaces should use
    for measurement digest compact summaries.

    Returns dict with:
    - summary_lines: list[str] — compact summary lines
    - detail_lines: list[str] — detail lines
    - boundary_markers: dict — Step 2 boundary markers
    """
    raw = dict(payload.get("raw") or payload or {})
    digest = dict(raw.get("digest") or payload.get("digest") or {})

    payload_complete = str(digest.get("payload_complete_phase_summary") or t("common.none"))
    payload_partial = str(digest.get("payload_partial_phase_summary") or t("common.none"))
    trace_only = str(digest.get("trace_only_phase_summary") or t("common.none"))
    blockers = str(digest.get("blocker_summary") or t("common.none"))
    next_artifacts = str(digest.get("next_required_artifacts_summary") or t("common.none"))

    summary_lines = [
        t(
            "reviewer_summary.measurement.payload_complete",
            value=payload_complete,
            default=f"{_MEASUREMENT_DIGEST['payload_complete_phases']}: {payload_complete}",
        ),
        t(
            "reviewer_summary.measurement.payload_partial",
            value=payload_partial,
            default=f"{_MEASUREMENT_DIGEST['payload_partial_phases']}: {payload_partial}",
        ),
        t(
            "reviewer_summary.measurement.trace_only",
            value=trace_only,
            default=f"{_MEASUREMENT_DIGEST['trace_only_phases']}: {trace_only}",
        ),
        t(
            "reviewer_summary.measurement.blockers",
            value=blockers,
            default=f"{_MEASUREMENT_DIGEST['blockers']}: {blockers}",
        ),
        t(
            "reviewer_summary.measurement.next_artifacts",
            value=next_artifacts,
            default=f"{_MEASUREMENT_DIGEST['next_artifacts']}: {next_artifacts}",
        ),
    ]

    detail_lines: list[str] = []
    if include_boundary:
        boundary = str(digest.get("boundary_summary") or raw.get("boundary_summary") or t("common.none"))
        detail_lines.append(
            t(
                "reviewer_summary.measurement.boundary",
                value=boundary,
                default=f"{_MEASUREMENT_DIGEST['boundary']}: {boundary}",
            )
        )
    if include_non_claim:
        non_claim = str(digest.get("non_claim_digest") or raw.get("non_claim_digest") or t("common.none"))
        detail_lines.append(
            t(
                "reviewer_summary.measurement.non_claim",
                value=non_claim,
                default=f"{_MEASUREMENT_DIGEST['non_claim']}: {non_claim}",
            )
        )

    return {
        "summary_lines": summary_lines,
        "detail_lines": detail_lines,
        "boundary_markers": dict(PHASE_EVIDENCE_STEP2_BOUNDARY),
    }


# ---------------------------------------------------------------------------
# Shared builder: readiness review digest compact summary
# ---------------------------------------------------------------------------

def build_readiness_digest_compact_summary(
    payload: dict[str, Any],
    *,
    include_boundary: bool = True,
    include_non_claim: bool = True,
) -> dict[str, Any]:
    """Build a compact readiness review digest summary from payload.

    This is the single-source builder that all surfaces should use
    for readiness digest compact summaries.

    Returns dict with:
    - summary_lines: list[str] — compact summary lines
    - detail_lines: list[str] — detail lines
    - boundary_markers: dict — Step 2 boundary markers
    """
    raw = dict(payload.get("raw") or payload or {})
    digest = dict(raw.get("digest") or payload.get("digest") or {})

    scope_overview = str(
        digest.get("scope_overview_summary")
        or dict(raw.get("scope_overview") or {}).get("summary")
        or t("common.none")
    )
    decision_rule = str(
        digest.get("decision_rule_summary")
        or dict(raw.get("decision_rule_overview") or {}).get("summary")
        or raw.get("decision_rule_id")
        or t("common.none")
    )
    readiness_status = str(
        digest.get("readiness_status_summary") or raw.get("validation_status") or t("common.none")
    )
    top_gaps = str(
        digest.get("top_gaps_summary") or digest.get("missing_evidence_summary") or t("common.none")
    )
    current_coverage = str(
        digest.get("current_evidence_coverage_summary")
        or digest.get("current_coverage_summary")
        or t("common.none")
    )

    summary_lines = [
        t(
            "reviewer_summary.readiness.scope_overview",
            value=scope_overview,
            default=f"{_READINESS_DIGEST['scope_overview']}: {scope_overview}",
        ),
        t(
            "reviewer_summary.readiness.decision_rule",
            value=decision_rule,
            default=f"{_READINESS_DIGEST['decision_rule']}: {decision_rule}",
        ),
        t(
            "reviewer_summary.readiness.readiness_status",
            value=readiness_status,
            default=f"{_READINESS_DIGEST['readiness_status']}: {readiness_status}",
        ),
        t(
            "reviewer_summary.readiness.top_gaps",
            value=top_gaps,
            default=f"{_READINESS_DIGEST['top_gaps']}: {top_gaps}",
        ),
        t(
            "reviewer_summary.readiness.current_coverage",
            value=current_coverage,
            default=f"{_READINESS_DIGEST['current_evidence_coverage']}: {current_coverage}",
        ),
    ]

    detail_lines: list[str] = []
    if include_boundary:
        boundary = str(digest.get("boundary_summary") or raw.get("boundary_summary") or t("common.none"))
        detail_lines.append(
            t(
                "reviewer_summary.readiness.boundary",
                value=boundary,
                default=f"{_READINESS_DIGEST['boundary']}: {boundary}",
            )
        )
    if include_non_claim:
        non_claim = str(digest.get("non_claim_digest") or raw.get("non_claim_digest") or t("common.none"))
        detail_lines.append(
            t(
                "reviewer_summary.readiness.non_claim",
                value=non_claim,
                default=f"{_READINESS_DIGEST['non_claim']}: {non_claim}",
            )
        )

    return {
        "summary_lines": summary_lines,
        "detail_lines": detail_lines,
        "boundary_markers": dict(PHASE_EVIDENCE_STEP2_BOUNDARY),
    }


# ---------------------------------------------------------------------------
# Shared builder: phase evidence compact summary
# ---------------------------------------------------------------------------

def build_phase_evidence_compact_summary(
    payload: dict[str, Any],
) -> dict[str, Any]:
    """Build a compact phase evidence summary from payload.

    Aggregates point taxonomy, measurement phase coverage, and
    phase transition bridge into a compact reviewer summary.

    Returns dict with:
    - summary_lines: list[str]
    - boundary_markers: dict
    """
    raw = dict(payload or {})
    taxonomy = dict(raw.get("point_taxonomy_summary") or {})
    phase_coverage = dict(raw.get("measurement_phase_coverage_report") or {})
    bridge = dict(raw.get("phase_transition_bridge") or {})

    taxonomy_text = str(
        taxonomy.get("summary_text")
        or PHASE_EVIDENCE_SUMMARY_TEXTS.get("point_taxonomy_summary", t("common.none"))
    )
    coverage_text = str(
        phase_coverage.get("summary_text")
        or PHASE_EVIDENCE_SUMMARY_TEXTS.get("measurement_phase_coverage_report", t("common.none"))
    )
    bridge_text = str(
        bridge.get("summary_text")
        or BRIDGE_REVIEWER_TEXTS.get("summary_text", t("common.none"))
    )

    summary_lines = [
        t(
            "reviewer_summary.phase_evidence.point_taxonomy",
            value=taxonomy_text,
            default=f"{resolve_phase_term('point_taxonomy')}: {taxonomy_text}",
        ),
        t(
            "reviewer_summary.phase_evidence.measurement_phase_coverage",
            value=coverage_text,
            default=f"{resolve_phase_term('measurement_phase_coverage')}: {coverage_text}",
        ),
        t(
            "reviewer_summary.phase_evidence.phase_transition_bridge",
            value=bridge_text,
            default=f"{resolve_phase_term('phase_transition')}: {bridge_text}",
        ),
    ]

    return {
        "summary_lines": summary_lines,
        "boundary_markers": dict(PHASE_EVIDENCE_STEP2_BOUNDARY),
    }


# ---------------------------------------------------------------------------
# Shared builder: V1.2 alignment compact summary
# ---------------------------------------------------------------------------

def build_v12_alignment_compact_summary(
    payload: dict[str, Any],
) -> dict[str, Any]:
    """Build a compact V1.2 alignment summary from payload.

    Aggregates:
    - point taxonomy
    - measurement phase coverage
    - phase transition bridge
    - parity / resilience
    - governance blockers / next steps

    Output explicitly marks:
    - simulated only
    - reviewer only
    - not real acceptance evidence

    Returns dict with:
    - summary_lines: list[str]
    - boundary_markers: dict
    - v12_compact: dict — structured compact summary
    """
    raw = dict(payload or {})

    # Aggregate from sub-sections
    taxonomy = dict(raw.get("point_taxonomy_summary") or {})
    phase_coverage = dict(raw.get("measurement_phase_coverage_report") or {})
    bridge = dict(raw.get("phase_transition_bridge") or {})
    parity_resilience = dict(raw.get("parity_resilience_summary") or {})
    governance = dict(raw.get("governance_handoff_summary") or {})

    taxonomy_status = str(taxonomy.get("status") or taxonomy.get("summary_text") or t("common.none"))
    coverage_status = str(phase_coverage.get("status") or phase_coverage.get("summary_text") or t("common.none"))
    bridge_status = str(bridge.get("status") or bridge.get("summary_text") or t("common.none"))
    parity_status = str(parity_resilience.get("parity_status") or parity_resilience.get("status") or t("common.none"))
    resilience_status = str(parity_resilience.get("resilience_status") or t("common.none"))
    governance_blockers = str(governance.get("blockers") or governance.get("blocking_items") or GOVERNANCE_HANDOFF_LABELS["no_blockers"])
    governance_next = str(governance.get("next_steps") or governance.get("recommended_next_stage") or t("common.none"))

    v12_compact = {
        "point_taxonomy": taxonomy_status,
        "measurement_phase_coverage": coverage_status,
        "phase_transition_bridge": bridge_status,
        "parity": parity_status,
        "resilience": resilience_status,
        "governance_blockers": governance_blockers,
        "governance_next_steps": governance_next,
    }

    summary_lines = [
        t(
            "reviewer_summary.v12_alignment.header",
            default=V12_COMPACT_SUMMARY_LABELS["header"],
        ),
        t(
            "reviewer_summary.v12_alignment.point_taxonomy",
            value=taxonomy_status,
            default=f"{V12_COMPACT_SUMMARY_LABELS['point_taxonomy']}: {taxonomy_status}",
        ),
        t(
            "reviewer_summary.v12_alignment.measurement_phase_coverage",
            value=coverage_status,
            default=f"{V12_COMPACT_SUMMARY_LABELS['measurement_phase_coverage']}: {coverage_status}",
        ),
        t(
            "reviewer_summary.v12_alignment.phase_transition_bridge",
            value=bridge_status,
            default=f"{V12_COMPACT_SUMMARY_LABELS['phase_transition_bridge']}: {bridge_status}",
        ),
        t(
            "reviewer_summary.v12_alignment.parity_resilience",
            parity=parity_status,
            resilience=resilience_status,
            default=f"{V12_COMPACT_SUMMARY_LABELS['parity_resilience']}: {parity_status} / {resilience_status}",
        ),
        t(
            "reviewer_summary.v12_alignment.governance_blockers",
            value=governance_blockers,
            default=f"{V12_COMPACT_SUMMARY_LABELS['governance_blockers']}: {governance_blockers}",
        ),
        t(
            "reviewer_summary.v12_alignment.simulated_only_note",
            default=V12_COMPACT_SUMMARY_LABELS["simulated_only_note"],
        ),
        t(
            "reviewer_summary.v12_alignment.no_formal_claim",
            default=V12_COMPACT_SUMMARY_LABELS["no_formal_claim"],
        ),
    ]

    return {
        "summary_lines": summary_lines,
        "boundary_markers": dict(PHASE_EVIDENCE_STEP2_BOUNDARY),
        "v12_compact": v12_compact,
    }


# ---------------------------------------------------------------------------
# Shared builder: governance handoff compact summary
# ---------------------------------------------------------------------------

def build_governance_handoff_compact_summary(
    payload: dict[str, Any],
) -> dict[str, Any]:
    """Build a compact governance handoff summary from payload.

    Returns dict with:
    - summary_lines: list[str]
    - boundary_markers: dict
    """
    raw = dict(payload or {})
    current_stage = str(raw.get("current_stage") or BRIDGE_REVIEWER_TEXTS.get("current_stage_text", t("common.none")))
    blockers = raw.get("blockers") or raw.get("blocking_items") or []
    if isinstance(blockers, str):
        blockers = [blockers]
    blockers_text = " | ".join(str(b).strip() for b in blockers if str(b).strip()) or GOVERNANCE_HANDOFF_LABELS["no_blockers"]
    next_steps = str(raw.get("next_steps") or raw.get("recommended_next_stage") or t("common.none"))
    evidence_source = str(raw.get("evidence_source") or "simulated")

    summary_lines = [
        t(
            "reviewer_summary.governance_handoff.current_stage",
            value=current_stage,
            default=f"{GOVERNANCE_HANDOFF_LABELS['current_stage']}: {current_stage}",
        ),
        t(
            "reviewer_summary.governance_handoff.blockers",
            value=blockers_text,
            default=f"{GOVERNANCE_HANDOFF_LABELS['blockers']}: {blockers_text}",
        ),
        t(
            "reviewer_summary.governance_handoff.next_steps",
            value=next_steps,
            default=f"{GOVERNANCE_HANDOFF_LABELS['next_steps']}: {next_steps}",
        ),
        t(
            "reviewer_summary.governance_handoff.evidence_source",
            value=evidence_source,
            default=f"{GOVERNANCE_HANDOFF_LABELS['evidence_source']}: {evidence_source}",
        ),
    ]

    return {
        "summary_lines": summary_lines,
        "boundary_markers": dict(PHASE_EVIDENCE_STEP2_BOUNDARY),
    }


# ---------------------------------------------------------------------------
# Shared builder: parity / resilience compact summary
# ---------------------------------------------------------------------------

def build_parity_resilience_compact_summary(
    payload: dict[str, Any],
) -> dict[str, Any]:
    """Build a compact parity / resilience summary from payload.

    Returns dict with:
    - summary_lines: list[str]
    - boundary_markers: dict
    """
    raw = dict(payload or {})
    parity_status = str(raw.get("parity_status") or raw.get("parity") or PARITY_RESILIENCE_LABELS["not_available"])
    resilience_status = str(raw.get("resilience_status") or raw.get("resilience") or PARITY_RESILIENCE_LABELS["not_available"])
    parity_last = str(raw.get("parity_last_run") or t("common.none"))
    resilience_last = str(raw.get("resilience_last_run") or t("common.none"))

    summary_lines = [
        t(
            "reviewer_summary.parity_resilience.parity_status",
            value=parity_status,
            default=f"{PARITY_RESILIENCE_LABELS['parity_status']}: {parity_status}",
        ),
        t(
            "reviewer_summary.parity_resilience.resilience_status",
            value=resilience_status,
            default=f"{PARITY_RESILIENCE_LABELS['resilience_status']}: {resilience_status}",
        ),
        t(
            "reviewer_summary.parity_resilience.parity_last_run",
            value=parity_last,
            default=f"{PARITY_RESILIENCE_LABELS['parity_last_run']}: {parity_last}",
        ),
        t(
            "reviewer_summary.parity_resilience.resilience_last_run",
            value=resilience_last,
            default=f"{PARITY_RESILIENCE_LABELS['resilience_last_run']}: {resilience_last}",
        ),
    ]

    return {
        "summary_lines": summary_lines,
        "boundary_markers": dict(PHASE_EVIDENCE_STEP2_BOUNDARY),
    }


def build_control_flow_compare_compact_summary(
    payload: dict[str, Any],
) -> dict[str, Any]:
    """Build a compact summary for V1/V2 offline control-flow compare evidence."""
    compare = _control_flow_compare_compact_fields(payload)
    point_presence_display = _display_compare_diff_state(compare["point_presence_diff"])
    sample_count_display = _display_compare_diff_state(compare["sample_count_diff"])
    route_trace_display = _display_compare_diff_state(compare["route_trace_diff"])
    key_action_display = _display_key_action_mismatches(compare["key_action_mismatches"])
    physical_route_display = _display_physical_route_state(compare["physical_route_mismatch"])
    first_failure_display = compare["first_failure_phase_display"] if compare["first_failure_phase"] != "--" else t("common.none")
    reviewer_summary_line = t(
        "reviewer_summary.offline_compare.summary_line",
        status=compare["compare_status_display"],
        failure=first_failure_display,
        next_check=compare["next_check_display"],
        default=(
            f"离线对齐：{compare['compare_status_display']} | "
            f"首个失败阶段：{first_failure_display} | "
            f"下一步检查：{compare['next_check_display']}"
        ),
    )
    summary_lines = [
        t(
            "reviewer_summary.offline_compare.status",
            value=compare["compare_status_display"],
            default=f"对齐状态：{compare['compare_status_display']}",
        ),
        t(
            "reviewer_summary.offline_compare.profile_route",
            profile=compare["validation_profile"],
            route=compare["target_route_display"],
            default=f"对齐配置：{compare['validation_profile']} | 目标气路：{compare['target_route_display']}",
        ),
        t(
            "reviewer_summary.offline_compare.primary_diffs",
            point_presence=point_presence_display,
            sample_count=sample_count_display,
            default=f"点位存在差异：{point_presence_display} | 样本数差异：{sample_count_display}",
        ),
        t(
            "reviewer_summary.offline_compare.secondary_diffs",
            route_trace=route_trace_display,
            key_actions=key_action_display,
            default=f"路由轨迹差异：{route_trace_display} | 关键动作不一致：{key_action_display}",
        ),
        t(
            "reviewer_summary.offline_compare.route_gate",
            physical_route=physical_route_display,
            first_failure=first_failure_display,
            default=f"物理气路不一致：{physical_route_display} | 首个失败阶段：{first_failure_display}",
        ),
        t(
            "reviewer_summary.offline_compare.next_check",
            value=compare["next_check_display"],
            default=f"下一步检查：{compare['next_check_display']}",
        ),
    ]
    return {
        "summary_lines": summary_lines,
        "boundary_markers": dict(PHASE_EVIDENCE_STEP2_BOUNDARY),
        "compare_compact": {
            **compare,
            "reviewer_summary_line": reviewer_summary_line,
            "reviewer_summary_lines": summary_lines,
            "readiness_mapping_only": True,
            "reviewer_only": True,
            "real_acceptance_ready": False,
        },
    }
