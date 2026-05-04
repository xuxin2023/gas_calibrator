"""Step 2 freeze seal contracts — canonical field names, status buckets,
drift labels, missing-surface labels, source-mismatch labels, boundary
markers, and i18n keys for the final no-drift guardrail layer.

This module does NOT introduce new business objects.  It only serves the
no-drift seal that locks the five core Step 2 closeout objects:

  1. step2_closeout_readiness
  2. step2_closeout_package
  3. step2_freeze_audit
  4. step3_admission_dossier
  5. step2_closeout_verification

Step 2 boundary:
  - evidence_source = "simulated"
  - not_real_acceptance_evidence = True
  - not_ready_for_formal_claim = True
  - reviewer_only = True
  - readiness_mapping_only = True
  - primary_evidence_rewritten = False
  - real_acceptance_ready = False
"""

from __future__ import annotations

from .step2_closure_schema_registry import (
    CANONICAL_BOUNDARY_MARKER_FIELDS,
    CANONICAL_SOURCE_PRIORITY,
    CANONICAL_STEP2_BOUNDARY,
    STEP2_FREEZE_SEAL_AUDIT_OBJECT_KEYS,
    build_consumable_field_union,
    build_source_field_map,
    build_status_field_map,
)

# ---------------------------------------------------------------------------
# Version
# ---------------------------------------------------------------------------

FREEZE_SEAL_CONTRACTS_VERSION: str = "2.26.0"

# ---------------------------------------------------------------------------
# Audited object keys — the canonical five
# ---------------------------------------------------------------------------

AUDITED_OBJECT_KEYS: tuple[str, ...] = STEP2_FREEZE_SEAL_AUDIT_OBJECT_KEYS

# ---------------------------------------------------------------------------
# Status buckets
# ---------------------------------------------------------------------------

FREEZE_SEAL_STATUS_OK = "ok"
FREEZE_SEAL_STATUS_ATTENTION = "attention"
FREEZE_SEAL_STATUS_BLOCKER = "blocker"
FREEZE_SEAL_STATUS_REVIEWER_ONLY = "reviewer_only"

# ---------------------------------------------------------------------------
# Title / summary
# ---------------------------------------------------------------------------

FREEZE_SEAL_TITLE_ZH: str = "Step 2 封板守护"
FREEZE_SEAL_TITLE_EN: str = "Step 2 Freeze Seal"

FREEZE_SEAL_SUMMARY_ZH: str = (
    "Step 2 封板守护：审计五个核心收官对象在 results / reports / "
    "historical / review index / UI 之间的一致性，防止后续漂移。"
    "不是正式放行结论，不是 real acceptance evidence。"
)
FREEZE_SEAL_SUMMARY_EN: str = (
    "Step 2 freeze seal: audits the five core closeout objects for "
    "cross-surface consistency across results / reports / historical / "
    "review index / UI, preventing future drift. "
    "Not a formal release conclusion. Not real acceptance evidence."
)

# ---------------------------------------------------------------------------
# Drift labels
# ---------------------------------------------------------------------------

DRIFT_LABELS_ZH: dict[str, str] = {
    "field_missing": "字段缺失",
    "field_value_mismatch": "字段值不一致",
    "boundary_marker_mismatch": "边界标记不一致",
    "status_bucket_mismatch": "状态桶不一致",
    "source_priority_mismatch": "source 优先级不一致",
}

DRIFT_LABELS_EN: dict[str, str] = {
    "field_missing": "Field missing",
    "field_value_mismatch": "Field value mismatch",
    "boundary_marker_mismatch": "Boundary marker mismatch",
    "status_bucket_mismatch": "Status bucket mismatch",
    "source_priority_mismatch": "Source priority mismatch",
}

# ---------------------------------------------------------------------------
# Missing-surface labels
# ---------------------------------------------------------------------------

MISSING_SURFACE_LABELS_ZH: dict[str, str] = {
    "results": "results 层缺失",
    "reports": "reports 层缺失",
    "historical": "historical 层缺失",
    "review_index": "review index 层缺失",
    "ui": "UI 层缺失",
}

MISSING_SURFACE_LABELS_EN: dict[str, str] = {
    "results": "Missing from results",
    "reports": "Missing from reports",
    "historical": "Missing from historical",
    "review_index": "Missing from review index",
    "ui": "Missing from UI",
}

# ---------------------------------------------------------------------------
# Source-mismatch labels
# ---------------------------------------------------------------------------

SOURCE_MISMATCH_LABELS_ZH: dict[str, str] = {
    "persisted_vs_rebuilt": "persisted 与 rebuilt 不一致",
    "rebuilt_vs_fallback": "rebuilt 与 fallback 不一致",
    "unexpected_source": "非预期 source 值",
}

SOURCE_MISMATCH_LABELS_EN: dict[str, str] = {
    "persisted_vs_rebuilt": "persisted vs rebuilt mismatch",
    "rebuilt_vs_fallback": "rebuilt vs fallback mismatch",
    "unexpected_source": "Unexpected source value",
}

# ---------------------------------------------------------------------------
# Simulation-only / reviewer-only / non-claim boundary text
# ---------------------------------------------------------------------------

FREEZE_SEAL_SIMULATION_ONLY_BOUNDARY_ZH: str = (
    "本封板守护仅基于仿真/离线/headless 证据，不代表 real acceptance evidence，"
    "不构成正式放行结论。"
)
FREEZE_SEAL_SIMULATION_ONLY_BOUNDARY_EN: str = (
    "This freeze seal is based on simulation/offline/headless evidence only. "
    "Not real acceptance evidence. Not a formal release conclusion."
)

FREEZE_SEAL_REVIEWER_ONLY_NOTICE_ZH: str = (
    "本封板守护仅供 reviewer 审阅，不作为 operator 操作依据。"
)
FREEZE_SEAL_REVIEWER_ONLY_NOTICE_EN: str = (
    "This freeze seal is for reviewer review only, not as operator action basis."
)

FREEZE_SEAL_NON_CLAIM_NOTICE_ZH: str = (
    "不形成 formal compliance claim / accreditation claim / real acceptance evidence。"
)
FREEZE_SEAL_NON_CLAIM_NOTICE_EN: str = (
    "Does not form formal compliance claim / accreditation claim / real acceptance evidence."
)

# ---------------------------------------------------------------------------
# Step 2 boundary markers — canonical set
# ---------------------------------------------------------------------------

FREEZE_SEAL_STEP2_BOUNDARY: dict[str, str | bool] = dict(CANONICAL_STEP2_BOUNDARY)

# ---------------------------------------------------------------------------
# Canonical boundary marker field names — locked
# ---------------------------------------------------------------------------

BOUNDARY_MARKER_FIELDS: tuple[str, ...] = CANONICAL_BOUNDARY_MARKER_FIELDS

# ---------------------------------------------------------------------------
# Canonical status field names per object — locked
# ---------------------------------------------------------------------------

OBJECT_STATUS_FIELD: dict[str, str] = build_status_field_map(AUDITED_OBJECT_KEYS)

# ---------------------------------------------------------------------------
# Canonical source field names per object — locked
# ---------------------------------------------------------------------------

OBJECT_SOURCE_FIELD: dict[str, str] = build_source_field_map(AUDITED_OBJECT_KEYS)

# ---------------------------------------------------------------------------
# Source priority order — locked
# ---------------------------------------------------------------------------

SOURCE_PRIORITY: tuple[str, ...] = CANONICAL_SOURCE_PRIORITY

# ---------------------------------------------------------------------------
# Cross-surface consumable fields — locked
# ---------------------------------------------------------------------------

CONSUMABLE_FIELDS: tuple[str, ...] = build_consumable_field_union(AUDITED_OBJECT_KEYS)

# ---------------------------------------------------------------------------
# i18n keys
# ---------------------------------------------------------------------------

FREEZE_SEAL_I18N_KEYS: tuple[str, ...] = (
    "freeze_seal.title",
    "freeze_seal.summary",
    "freeze_seal.status.ok",
    "freeze_seal.status.attention",
    "freeze_seal.status.blocker",
    "freeze_seal.status.reviewer_only",
    "freeze_seal.simulation_only_boundary",
    "freeze_seal.reviewer_only_notice",
    "freeze_seal.non_claim_notice",
    "freeze_seal.drift.field_missing",
    "freeze_seal.drift.field_value_mismatch",
    "freeze_seal.drift.boundary_marker_mismatch",
    "freeze_seal.drift.status_bucket_mismatch",
    "freeze_seal.drift.source_priority_mismatch",
    "freeze_seal.missing_surface.results",
    "freeze_seal.missing_surface.reports",
    "freeze_seal.missing_surface.historical",
    "freeze_seal.missing_surface.review_index",
    "freeze_seal.missing_surface.ui",
    "freeze_seal.source_mismatch.persisted_vs_rebuilt",
    "freeze_seal.source_mismatch.rebuilt_vs_fallback",
    "freeze_seal.source_mismatch.unexpected_source",
    "freeze_seal.panel",
    "freeze_seal.no_content",
    "freeze_seal.audited_objects",
    "freeze_seal.freeze_seal_status",
)

# ---------------------------------------------------------------------------
# Resolve helpers
# ---------------------------------------------------------------------------


def resolve_freeze_seal_title(lang: str = "zh") -> str:
    return FREEZE_SEAL_TITLE_EN if lang == "en" else FREEZE_SEAL_TITLE_ZH


def resolve_freeze_seal_summary(lang: str = "zh") -> str:
    return FREEZE_SEAL_SUMMARY_EN if lang == "en" else FREEZE_SEAL_SUMMARY_ZH


def resolve_freeze_seal_status_label(status: str, *, lang: str = "zh") -> str:
    _ZH = {
        FREEZE_SEAL_STATUS_OK: "封板守护就绪",
        FREEZE_SEAL_STATUS_ATTENTION: "存在漂移风险",
        FREEZE_SEAL_STATUS_BLOCKER: "存在漂移阻塞",
        FREEZE_SEAL_STATUS_REVIEWER_ONLY: "仅限审阅观察",
    }
    _EN = {
        FREEZE_SEAL_STATUS_OK: "Freeze seal ready",
        FREEZE_SEAL_STATUS_ATTENTION: "Drift risk present",
        FREEZE_SEAL_STATUS_BLOCKER: "Drift blocker present",
        FREEZE_SEAL_STATUS_REVIEWER_ONLY: "Reviewer-only observation",
    }
    return (_EN if lang == "en" else _ZH).get(status, status)


def resolve_drift_label(key: str, *, lang: str = "zh") -> str:
    labels = DRIFT_LABELS_EN if lang == "en" else DRIFT_LABELS_ZH
    return labels.get(key, key)


def resolve_missing_surface_label(surface: str, *, lang: str = "zh") -> str:
    labels = MISSING_SURFACE_LABELS_EN if lang == "en" else MISSING_SURFACE_LABELS_ZH
    return labels.get(surface, surface)


def resolve_source_mismatch_label(key: str, *, lang: str = "zh") -> str:
    labels = SOURCE_MISMATCH_LABELS_EN if lang == "en" else SOURCE_MISMATCH_LABELS_ZH
    return labels.get(key, key)


def resolve_freeze_seal_simulation_only_boundary(lang: str = "zh") -> str:
    return FREEZE_SEAL_SIMULATION_ONLY_BOUNDARY_EN if lang == "en" else FREEZE_SEAL_SIMULATION_ONLY_BOUNDARY_ZH


def resolve_freeze_seal_reviewer_only_notice(lang: str = "zh") -> str:
    return FREEZE_SEAL_REVIEWER_ONLY_NOTICE_EN if lang == "en" else FREEZE_SEAL_REVIEWER_ONLY_NOTICE_ZH


def resolve_freeze_seal_non_claim_notice(lang: str = "zh") -> str:
    return FREEZE_SEAL_NON_CLAIM_NOTICE_EN if lang == "en" else FREEZE_SEAL_NON_CLAIM_NOTICE_ZH
