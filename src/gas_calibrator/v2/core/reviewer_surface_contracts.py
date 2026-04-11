"""Shared single-source-of-truth constants for WP6 + step2_closeout_digest reviewer surfaces.

All modules that reference the 7 WP6+closeout artifact keys, their display order,
labels, or role assignments must import from this module instead of maintaining
local hard-coded copies.

Step 2 boundary:
  - evidence_source = "simulated"
  - not_real_acceptance_evidence = True
  - not_ready_for_formal_claim = True
  - primary_evidence_rewritten = False
  - readiness_mapping_only = True / reviewer_only = True
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Contract version
# ---------------------------------------------------------------------------

REVIEWER_SURFACE_CONTRACTS_VERSION = "step2-reviewer-surface-v1"

# ---------------------------------------------------------------------------
# The 7 canonical WP6 + closeout artifact keys in display order
# ---------------------------------------------------------------------------

WP6_CLOSEOUT_ARTIFACT_KEYS: tuple[str, ...] = (
    "pt_ilc_registry",
    "external_comparison_importer",
    "comparison_evidence_pack",
    "scope_comparison_view",
    "comparison_digest",
    "comparison_rollup",
    "step2_closeout_digest",
)

# ---------------------------------------------------------------------------
# Chinese display labels (default)
# ---------------------------------------------------------------------------

WP6_CLOSEOUT_DISPLAY_LABELS: dict[str, str] = {
    "pt_ilc_registry": "PT/ILC 比对注册表",
    "external_comparison_importer": "外部比对导入器",
    "comparison_evidence_pack": "比对证据包",
    "scope_comparison_view": "范围比对视图",
    "comparison_digest": "比对摘要",
    "comparison_rollup": "比对汇总",
    "step2_closeout_digest": "Step 2 阶段收口摘要",
}

# ---------------------------------------------------------------------------
# English fallback display labels
# ---------------------------------------------------------------------------

WP6_CLOSEOUT_DISPLAY_LABELS_EN: dict[str, str] = {
    "pt_ilc_registry": "PT/ILC Comparison Registry",
    "external_comparison_importer": "External Comparison Importer",
    "comparison_evidence_pack": "Comparison Evidence Pack",
    "scope_comparison_view": "Scope Comparison View",
    "comparison_digest": "Comparison Digest",
    "comparison_rollup": "Comparison Rollup",
    "step2_closeout_digest": "Step 2 Closeout Digest",
}

# ---------------------------------------------------------------------------
# i18n keys for reviewer surface display
# ---------------------------------------------------------------------------

WP6_CLOSEOUT_I18N_KEYS: dict[str, str] = {
    "pt_ilc_registry": "reviewer_surface.wp6_closeout.pt_ilc_registry",
    "external_comparison_importer": "reviewer_surface.wp6_closeout.external_comparison_importer",
    "comparison_evidence_pack": "reviewer_surface.wp6_closeout.comparison_evidence_pack",
    "scope_comparison_view": "reviewer_surface.wp6_closeout.scope_comparison_view",
    "comparison_digest": "reviewer_surface.wp6_closeout.comparison_digest",
    "comparison_rollup": "reviewer_surface.wp6_closeout.comparison_rollup",
    "step2_closeout_digest": "reviewer_surface.wp6_closeout.step2_closeout_digest",
}

# ---------------------------------------------------------------------------
# Artifact role assignments
# ---------------------------------------------------------------------------

WP6_CLOSEOUT_ARTIFACT_ROLES: dict[str, str] = {
    "pt_ilc_registry": "execution_summary",
    "external_comparison_importer": "execution_summary",
    "comparison_evidence_pack": "diagnostic_analysis",
    "scope_comparison_view": "diagnostic_analysis",
    "comparison_digest": "diagnostic_analysis",
    "comparison_rollup": "diagnostic_analysis",
    "step2_closeout_digest": "diagnostic_analysis",
}

# ---------------------------------------------------------------------------
# Anchor definitions for reviewer navigation
# ---------------------------------------------------------------------------

WP6_CLOSEOUT_ANCHOR_DEFAULTS: dict[str, dict[str, str]] = {
    "pt_ilc_registry": {"anchor_id": "pt-ilc-registry", "anchor_label": "PT/ILC 注册表"},
    "external_comparison_importer": {
        "anchor_id": "external-comparison-importer",
        "anchor_label": "外部比对导入器",
    },
    "comparison_evidence_pack": {
        "anchor_id": "comparison-evidence-pack",
        "anchor_label": "比对证据包",
    },
    "scope_comparison_view": {
        "anchor_id": "scope-comparison-view",
        "anchor_label": "范围比对视图",
    },
    "comparison_digest": {"anchor_id": "comparison-digest", "anchor_label": "比对摘要"},
    "comparison_rollup": {"anchor_id": "comparison-rollup", "anchor_label": "比对汇总"},
    "step2_closeout_digest": {
        "anchor_id": "step2-closeout-digest",
        "anchor_label": "Step 2 阶段收口摘要",
    },
}

# ---------------------------------------------------------------------------
# Navigation / next-artifact defaults
# ---------------------------------------------------------------------------

WP6_CLOSEOUT_NEXT_ARTIFACT_DEFAULTS: dict[str, list[str]] = {
    "pt_ilc_registry": ["external_comparison_importer", "comparison_evidence_pack"],
    "external_comparison_importer": ["comparison_evidence_pack", "scope_comparison_view"],
    "comparison_evidence_pack": ["scope_comparison_view", "comparison_digest"],
    "scope_comparison_view": ["comparison_digest", "comparison_rollup"],
    "comparison_digest": ["comparison_rollup", "step2_closeout_digest"],
    "comparison_rollup": ["step2_closeout_digest"],
    "step2_closeout_digest": [],
}

# ---------------------------------------------------------------------------
# Blocker defaults
# ---------------------------------------------------------------------------

WP6_CLOSEOUT_BLOCKER_DEFAULTS: dict[str, list[str]] = {
    "pt_ilc_registry": [
        "PT/ILC registry is readiness-mapping-only and not a formal comparison record",
        "imported data is simulated and not from real PT/ILC participation",
    ],
    "external_comparison_importer": [
        "importer only supports local file sources, no network access",
        "all imported comparison data is marked simulated",
    ],
    "comparison_evidence_pack": [
        "evidence pack is reviewer-facing only and not a formal accreditation pack",
        "linked references are navigational, not approval chains",
    ],
    "scope_comparison_view": [
        "scope comparison view is readiness-mapping-only",
        "does not constitute formal scope equivalence",
    ],
    "comparison_digest": [
        "digest is sidecar-first reviewer evidence",
        "does not close formal comparison evidence",
    ],
    "comparison_rollup": [
        "rollup is reviewer-facing summary only",
        "does not constitute formal PT/ILC compliance claim",
    ],
    "step2_closeout_digest": [
        "closeout digest is Step 2 governance summary only",
        "does not constitute formal phase completion or acceptance evidence",
    ],
}

# ---------------------------------------------------------------------------
# Missing-evidence defaults
# ---------------------------------------------------------------------------

WP6_CLOSEOUT_MISSING_EVIDENCE_DEFAULTS: dict[str, list[str]] = {
    "pt_ilc_registry": [
        "PT/ILC registry is readiness-mapping-only and not a formal comparison record",
        "imported data is simulated and not from real PT/ILC participation",
    ],
    "external_comparison_importer": [
        "importer only supports local file sources, no network access",
        "all imported comparison data is marked simulated",
    ],
    "comparison_evidence_pack": [
        "evidence pack is reviewer-facing only and not a formal accreditation pack",
        "linked references are navigational, not approval chains",
    ],
    "scope_comparison_view": [
        "scope comparison view is readiness-mapping-only",
        "does not constitute formal scope equivalence",
    ],
    "comparison_digest": [
        "digest is sidecar-first reviewer evidence",
        "does not close formal comparison evidence",
    ],
    "comparison_rollup": [
        "rollup is reviewer-facing summary only",
        "does not constitute formal PT/ILC compliance claim",
    ],
    "step2_closeout_digest": [
        "closeout digest is Step 2 governance summary only",
        "does not constitute formal phase completion or acceptance evidence",
    ],
}

# ---------------------------------------------------------------------------
# Filename constants (single source, referenced by all consumers)
# ---------------------------------------------------------------------------

PT_ILC_REGISTRY_FILENAME = "pt_ilc_registry.json"
PT_ILC_REGISTRY_MARKDOWN_FILENAME = "pt_ilc_registry.md"
EXTERNAL_COMPARISON_IMPORTER_FILENAME = "external_comparison_importer.json"
EXTERNAL_COMPARISON_IMPORTER_MARKDOWN_FILENAME = "external_comparison_importer.md"
COMPARISON_EVIDENCE_PACK_FILENAME = "comparison_evidence_pack.json"
COMPARISON_EVIDENCE_PACK_MARKDOWN_FILENAME = "comparison_evidence_pack.md"
SCOPE_COMPARISON_VIEW_FILENAME = "scope_comparison_view.json"
SCOPE_COMPARISON_VIEW_MARKDOWN_FILENAME = "scope_comparison_view.md"
COMPARISON_DIGEST_FILENAME = "comparison_digest.json"
COMPARISON_DIGEST_MARKDOWN_FILENAME = "comparison_digest.md"
COMPARISON_ROLLUP_FILENAME = "comparison_rollup.json"
COMPARISON_ROLLUP_MARKDOWN_FILENAME = "comparison_rollup.md"
STEP2_CLOSEOUT_DIGEST_FILENAME = "step2_closeout_digest.json"
STEP2_CLOSEOUT_DIGEST_MARKDOWN_FILENAME = "step2_closeout_digest.md"

# Ordered mapping from artifact key to (json_filename, markdown_filename)
WP6_CLOSEOUT_FILENAME_MAP: dict[str, tuple[str, str]] = {
    "pt_ilc_registry": (PT_ILC_REGISTRY_FILENAME, PT_ILC_REGISTRY_MARKDOWN_FILENAME),
    "external_comparison_importer": (EXTERNAL_COMPARISON_IMPORTER_FILENAME, EXTERNAL_COMPARISON_IMPORTER_MARKDOWN_FILENAME),
    "comparison_evidence_pack": (COMPARISON_EVIDENCE_PACK_FILENAME, COMPARISON_EVIDENCE_PACK_MARKDOWN_FILENAME),
    "scope_comparison_view": (SCOPE_COMPARISON_VIEW_FILENAME, SCOPE_COMPARISON_VIEW_MARKDOWN_FILENAME),
    "comparison_digest": (COMPARISON_DIGEST_FILENAME, COMPARISON_DIGEST_MARKDOWN_FILENAME),
    "comparison_rollup": (COMPARISON_ROLLUP_FILENAME, COMPARISON_ROLLUP_MARKDOWN_FILENAME),
    "step2_closeout_digest": (STEP2_CLOSEOUT_DIGEST_FILENAME, STEP2_CLOSEOUT_DIGEST_MARKDOWN_FILENAME),
}

# ---------------------------------------------------------------------------
# Helper: resolve display label with i18n fallback
# ---------------------------------------------------------------------------


def resolve_wp6_closeout_display_label(
    key: str,
    *,
    locale: str | None = None,
) -> str:
    """Resolve display label for a WP6+closeout artifact key.

    Tries i18n lookup first, falls back to Chinese default, then English.
    """
    i18n_key = WP6_CLOSEOUT_I18N_KEYS.get(key)
    if i18n_key and locale is not None:
        try:
            from ..ui_v2.i18n import t
            label = t(i18n_key, locale=locale)
            if label and label != i18n_key:
                return label
        except Exception:
            pass
    # Chinese default
    cn = WP6_CLOSEOUT_DISPLAY_LABELS.get(key)
    if cn:
        return cn
    # English fallback
    en = WP6_CLOSEOUT_DISPLAY_LABELS_EN.get(key)
    if en:
        return en
    return key
