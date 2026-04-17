from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence


STEP2_CLOSEOUT_BUNDLE_SCHEMA_VERSION = "step2-closeout-bundle-v1"
STEP2_CLOSEOUT_BUNDLE_BUILDER_VERSION = "2.27.0"

STEP2_CLOSEOUT_BUNDLE_FILENAME = "step2_closeout_bundle.json"
STEP2_CLOSEOUT_EVIDENCE_INDEX_FILENAME = "step2_closeout_evidence_index.json"
STEP2_CLOSEOUT_SUMMARY_FILENAME = "step2_closeout_summary.md"

STEP2_CLOSEOUT_BUNDLE_ARTIFACT_TYPE = "step2_closeout_bundle"
STEP2_CLOSEOUT_EVIDENCE_INDEX_ARTIFACT_TYPE = "step2_closeout_evidence_index"
STEP2_CLOSEOUT_SUMMARY_ARTIFACT_TYPE = "step2_closeout_summary"

STEP2_CLOSEOUT_COMPACT_KEY = "step2_closeout"
STEP2_CLOSEOUT_TITLE = "Step 2 \u6536\u5c3e\u603b\u5305"
STEP2_CLOSEOUT_BOUNDARY_LINES = [
    "reviewer_only = true",
    "readiness_mapping_only = true",
    "not_real_acceptance_evidence = true",
    "not_ready_for_formal_claim = true",
    "file_artifact_first_preserved = true",
    "main_chain_dependency = false",
]
STEP2_CLOSEOUT_BOUNDARY_SUMMARY = (
    "reviewer_only=true | readiness_mapping_only=true | "
    "not_real_acceptance_evidence=true | not_ready_for_formal_claim=true | "
    "file_artifact_first_preserved=true | main_chain_dependency=false"
)

_REQUIRED_BOUNDARY_FIELDS: dict[str, Any] = {
    "reviewer_only": True,
    "readiness_mapping_only": True,
    "not_real_acceptance_evidence": True,
    "not_ready_for_formal_claim": True,
    "file_artifact_first_preserved": True,
    "main_chain_dependency": False,
}

_ALL_REQUIRED_CATEGORY_IDS = {
    "scope_definition",
    "asset_readiness",
    "uncertainty",
    "method_confirmation",
    "software_validation",
    "comparison",
}


def build_step2_closeout_bundle(
    *,
    run_id: str = "",
    run_dir: str | Path | None = None,
    scope_definition_pack: dict[str, Any] | None = None,
    decision_rule_profile: dict[str, Any] | None = None,
    conformity_statement_profile: dict[str, Any] | None = None,
    reference_asset_registry: dict[str, Any] | None = None,
    certificate_lifecycle_summary: dict[str, Any] | None = None,
    pre_run_readiness_gate: dict[str, Any] | None = None,
    uncertainty_report_pack: dict[str, Any] | None = None,
    uncertainty_rollup: dict[str, Any] | None = None,
    method_confirmation_protocol: dict[str, Any] | None = None,
    verification_rollup: dict[str, Any] | None = None,
    software_validation_traceability_matrix: dict[str, Any] | None = None,
    requirement_design_code_test_links: dict[str, Any] | None = None,
    validation_evidence_index: dict[str, Any] | None = None,
    change_impact_summary: dict[str, Any] | None = None,
    rollback_readiness_summary: dict[str, Any] | None = None,
    release_manifest: dict[str, Any] | None = None,
    release_scope_summary: dict[str, Any] | None = None,
    release_boundary_digest: dict[str, Any] | None = None,
    release_evidence_pack_index: dict[str, Any] | None = None,
    release_validation_manifest: dict[str, Any] | None = None,
    software_validation_rollup: dict[str, Any] | None = None,
    audit_readiness_digest: dict[str, Any] | None = None,
    comparison_evidence_pack: dict[str, Any] | None = None,
    scope_comparison_view: dict[str, Any] | None = None,
    comparison_digest: dict[str, Any] | None = None,
    comparison_rollup: dict[str, Any] | None = None,
    step2_closeout_digest: dict[str, Any] | None = None,
    sidecar_index_summary: dict[str, Any] | None = None,
    review_copilot_payload: dict[str, Any] | None = None,
    model_governance_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_run_id = str(run_id or "").strip()
    normalized_run_dir = str(Path(run_dir)) if run_dir is not None else ""
    normalized_conformity = dict(
        conformity_statement_profile
        or dict(decision_rule_profile or {}).get("conformity_statement_profile")
        or {}
    )
    categories = [
        _build_category(
            "scope_definition",
            "\u8303\u56f4\u4e0e\u5224\u5b9a",
            [
                ("scope_definition_pack", scope_definition_pack, True),
                ("decision_rule_profile", decision_rule_profile, True),
                ("conformity_statement_profile", normalized_conformity, True),
            ],
        ),
        _build_category(
            "asset_readiness",
            "\u8d44\u4ea7\u4e0e\u5f00\u8dd1\u5c31\u7eea",
            [
                ("reference_asset_registry", reference_asset_registry, True),
                ("certificate_lifecycle_summary", certificate_lifecycle_summary, True),
                ("pre_run_readiness_gate", pre_run_readiness_gate, True),
            ],
        ),
        _build_category(
            "uncertainty",
            "\u4e0d\u786e\u5b9a\u5ea6",
            [
                ("uncertainty_report_pack", uncertainty_report_pack, True),
                ("uncertainty_rollup", uncertainty_rollup, True),
            ],
        ),
        _build_category(
            "method_confirmation",
            "\u65b9\u6cd5\u786e\u8ba4",
            [
                ("method_confirmation_protocol", method_confirmation_protocol, True),
                ("verification_rollup", verification_rollup, True),
            ],
        ),
        _build_category(
            "software_validation",
            "\u8f6f\u4ef6\u9a8c\u8bc1\u4e0e\u53d1\u5e03\u6cbb\u7406",
            [
                ("software_validation_traceability_matrix", software_validation_traceability_matrix, True),
                ("requirement_design_code_test_links", requirement_design_code_test_links, True),
                ("validation_evidence_index", validation_evidence_index, True),
                ("change_impact_summary", change_impact_summary, True),
                ("rollback_readiness_summary", rollback_readiness_summary, True),
                ("release_manifest", release_manifest, True),
                ("release_scope_summary", release_scope_summary, True),
                ("release_boundary_digest", release_boundary_digest, True),
                ("release_evidence_pack_index", release_evidence_pack_index, True),
                ("release_validation_manifest", release_validation_manifest, True),
                ("software_validation_rollup", software_validation_rollup, False),
                ("audit_readiness_digest", audit_readiness_digest, True),
            ],
        ),
        _build_category(
            "comparison",
            "\u6bd4\u5bf9\u4e0e\u6536\u53e3",
            [
                ("comparison_evidence_pack", comparison_evidence_pack, True),
                ("scope_comparison_view", scope_comparison_view, True),
                ("comparison_digest", comparison_digest, True),
                ("comparison_rollup", comparison_rollup, True),
                ("step2_closeout_digest", step2_closeout_digest, True),
            ],
        ),
        _build_category(
            "sidecar",
            "\u65c1\u8def\u4e0e\u6a21\u578b\u6cbb\u7406",
            [
                ("sidecar_index_summary", sidecar_index_summary, False),
                ("review_copilot_payload", review_copilot_payload, False),
                ("model_governance_summary", model_governance_summary, False),
            ],
        ),
    ]

    category_summaries = [_category_summary_entry(category) for category in categories]
    all_entries = [entry for category in categories for entry in list(category.get("entries") or [])]
    missing_evidence_categories = [
        str(category.get("category_id") or "")
        for category in categories
        if bool(category.get("required")) and not bool(category.get("present"))
    ]
    unresolved_non_claim_items = _build_unresolved_non_claim_items(all_entries)
    reviewer_attention_items = _build_reviewer_attention_items(categories)
    bridge_to_stage3_candidates = _build_bridge_candidates(
        categories=categories,
        missing_evidence_categories=missing_evidence_categories,
    )
    blocker_items = [
        f"\u7f3a\u5c11 {str(category.get('display_label') or category.get('category_id') or '--')} reviewer/readiness \u5de5\u4ef6\u3002"
        for category in categories
        if bool(category.get("required")) and not bool(category.get("present"))
    ]
    warning_items = _build_warning_items(categories)
    info_items = _build_info_items(categories)
    summary_line = _build_summary_line(
        categories=categories,
        blocker_items=blocker_items,
        warning_items=warning_items,
        bridge_to_stage3_candidates=bridge_to_stage3_candidates,
    )
    present_category_count = sum(1 for category in categories if bool(category.get("present")))
    summary_lines = [
        summary_line,
        f"\u8bc1\u636e\u7c7b\u522b: {present_category_count}/{len(categories)} \u5df2\u6c47\u603b\u3002",
        f"blocker/warning/info: {len(blocker_items)}/{len(warning_items)}/{len(info_items)}\u3002",
    ]
    if missing_evidence_categories:
        summary_lines.append("\u7f3a\u5931\u7c7b\u522b: " + ", ".join(missing_evidence_categories))
    if reviewer_attention_items:
        summary_lines.append("reviewer attention: " + "; ".join(reviewer_attention_items[:3]))
    if bridge_to_stage3_candidates:
        summary_lines.append("bridge_to_stage3: " + ", ".join(bridge_to_stage3_candidates))

    compact_section = {
        "summary_key": STEP2_CLOSEOUT_COMPACT_KEY,
        "display_label": STEP2_CLOSEOUT_TITLE,
        "summary_line": summary_line,
        "summary_lines": list(summary_lines),
        "compact_summary_lines": [
            *summary_lines,
            *[f"blocker: {item}" for item in blocker_items[:2]],
            *[f"warning: {item}" for item in warning_items[:2]],
            *[f"info: {item}" for item in info_items[:2]],
        ],
        "blocker_count": len(blocker_items),
        "warning_count": len(warning_items),
        "info_count": len(info_items),
        "missing_evidence_categories": list(missing_evidence_categories),
        "unresolved_non_claim_items": list(unresolved_non_claim_items),
        "reviewer_attention_items": list(reviewer_attention_items),
        "bridge_to_stage3_candidates": list(bridge_to_stage3_candidates),
        "boundary_summary": STEP2_CLOSEOUT_BOUNDARY_SUMMARY,
        **_REQUIRED_BOUNDARY_FIELDS,
    }

    artifact_paths = {
        "step2_closeout_bundle": _artifact_output_path(
            normalized_run_dir,
            STEP2_CLOSEOUT_BUNDLE_FILENAME,
        ),
        "step2_closeout_evidence_index": _artifact_output_path(
            normalized_run_dir,
            STEP2_CLOSEOUT_EVIDENCE_INDEX_FILENAME,
        ),
        "step2_closeout_summary_markdown": _artifact_output_path(
            normalized_run_dir,
            STEP2_CLOSEOUT_SUMMARY_FILENAME,
        ),
    }
    review_surface = {
        "summary_text": summary_line,
        "summary_lines": list(summary_lines),
        "artifact_paths": dict(artifact_paths),
    }
    bundle = {
        "schema_version": STEP2_CLOSEOUT_BUNDLE_SCHEMA_VERSION,
        "builder_version": STEP2_CLOSEOUT_BUNDLE_BUILDER_VERSION,
        "artifact_type": STEP2_CLOSEOUT_BUNDLE_ARTIFACT_TYPE,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "run_id": normalized_run_id,
        "run_dir": normalized_run_dir,
        "title": STEP2_CLOSEOUT_TITLE,
        "summary_line": summary_line,
        "summary_lines": list(summary_lines),
        "compact_summary_lines": list(compact_section.get("compact_summary_lines") or []),
        "category_summaries": category_summaries,
        "evidence_categories": categories,
        "evidence_entry_count": len(all_entries),
        "present_category_count": present_category_count,
        "required_category_count": sum(1 for category in categories if bool(category.get("required"))),
        "blocker_items": blocker_items,
        "warning_items": warning_items,
        "info_items": info_items,
        "missing_evidence_categories": list(missing_evidence_categories),
        "unresolved_non_claim_items": list(unresolved_non_claim_items),
        "reviewer_attention_items": list(reviewer_attention_items),
        "bridge_to_stage3_candidates": list(bridge_to_stage3_candidates),
        "compact_section": dict(compact_section),
        "artifact_paths": dict(artifact_paths),
        "source_artifact_refs": _build_source_artifact_refs(all_entries),
        "review_surface": review_surface,
        "sidecar_injected": any(
            bool(category.get("present"))
            for category in categories
            if str(category.get("category_id") or "") == "sidecar"
        ),
        "evidence_source": "simulated",
        "not_in_default_chain": False,
        "primary_evidence_rewritten": False,
        "boundary_summary": STEP2_CLOSEOUT_BOUNDARY_SUMMARY,
        **_REQUIRED_BOUNDARY_FIELDS,
    }

    evidence_index = {
        "schema_version": STEP2_CLOSEOUT_BUNDLE_SCHEMA_VERSION,
        "artifact_type": STEP2_CLOSEOUT_EVIDENCE_INDEX_ARTIFACT_TYPE,
        "generated_at": str(bundle.get("generated_at") or ""),
        "run_id": normalized_run_id,
        "run_dir": normalized_run_dir,
        "title": STEP2_CLOSEOUT_TITLE,
        "summary_line": summary_line,
        "entries": all_entries,
        "category_index": category_summaries,
        "missing_evidence_categories": list(missing_evidence_categories),
        "unresolved_non_claim_items": list(unresolved_non_claim_items),
        "reviewer_attention_items": list(reviewer_attention_items),
        "bridge_to_stage3_candidates": list(bridge_to_stage3_candidates),
        "boundary_summary": STEP2_CLOSEOUT_BOUNDARY_SUMMARY,
        **_REQUIRED_BOUNDARY_FIELDS,
    }

    summary_markdown = _render_summary_markdown(
        bundle=bundle,
        categories=categories,
        blocker_items=blocker_items,
        warning_items=warning_items,
        info_items=info_items,
    )

    return {
        "step2_closeout_bundle": bundle,
        "step2_closeout_evidence_index": evidence_index,
        "step2_closeout_summary_markdown": summary_markdown,
        "step2_closeout_compact_section": compact_section,
    }


def _artifact_output_path(run_dir: str, filename: str) -> str:
    if not run_dir:
        return filename
    return str(Path(run_dir) / filename)


def _build_category(
    category_id: str,
    display_label: str,
    items: Sequence[tuple[str, dict[str, Any] | None, bool]],
) -> dict[str, Any]:
    entries = [
        _build_evidence_entry(category_id=category_id, key=key, payload=payload, required=required)
        for key, payload, required in items
    ]
    required_entries = [entry for entry in entries if bool(entry.get("required"))]
    required_present = [entry for entry in required_entries if bool(entry.get("available"))]
    available_entries = [entry for entry in entries if bool(entry.get("available"))]
    missing_entries = [
        str(entry.get("key") or "")
        for entry in required_entries
        if not bool(entry.get("available"))
    ]
    attention_count = sum(
        1
        for entry in entries
        if str(entry.get("attention_level") or "") in {"warning", "blocker"}
    )
    return {
        "category_id": category_id,
        "display_label": display_label,
        "required": any(bool(entry.get("required")) for entry in entries),
        "present": (
            len(required_present) == len(required_entries)
            if required_entries
            else bool(available_entries)
        ),
        "entry_count": len(entries),
        "required_entry_count": len(required_entries),
        "present_entry_count": len(required_present),
        "available_entry_count": len(available_entries),
        "missing_entries": missing_entries,
        "attention_count": attention_count,
        "entries": entries,
    }


def _build_evidence_entry(
    *,
    category_id: str,
    key: str,
    payload: dict[str, Any] | None,
    required: bool,
) -> dict[str, Any]:
    normalized = dict(payload or {})
    digest = dict(normalized.get("digest") or {})
    review_surface = dict(normalized.get("review_surface") or {})
    artifact_paths = dict(normalized.get("artifact_paths") or review_surface.get("artifact_paths") or {})
    summary_text = str(
        normalized.get("summary_line")
        or normalized.get("reviewer_summary_line")
        or digest.get("summary")
        or review_surface.get("summary_text")
        or normalized.get("summary")
        or ""
    ).strip()
    readiness_only = bool(normalized.get("readiness_mapping_only", True))
    reviewer_only = bool(normalized.get("reviewer_only", True))
    not_real_acceptance = bool(normalized.get("not_real_acceptance_evidence", True))
    not_formal_claim = bool(normalized.get("not_ready_for_formal_claim", True))
    file_artifact_first_preserved = bool(normalized.get("file_artifact_first_preserved", True))
    main_chain_dependency = bool(normalized.get("main_chain_dependency", False))
    available = bool(normalized)
    attention_level = "info"
    if required and not available:
        attention_level = "blocker"
    elif available and (
        not reviewer_only
        or not readiness_only
        or not not_real_acceptance
        or not not_formal_claim
        or not file_artifact_first_preserved
        or main_chain_dependency
    ):
        attention_level = "warning"
    return {
        "category_id": category_id,
        "key": key,
        "required": required,
        "available": available,
        "artifact_type": str(normalized.get("artifact_type") or key),
        "summary": summary_text,
        "artifact_path": _pick_primary_artifact_path(key=key, artifact_paths=artifact_paths),
        "artifact_paths": artifact_paths,
        "evidence_source": str(normalized.get("evidence_source") or ("simulated" if available else "")),
        "reviewer_only": reviewer_only,
        "readiness_mapping_only": readiness_only,
        "not_real_acceptance_evidence": not_real_acceptance,
        "not_ready_for_formal_claim": not_formal_claim,
        "file_artifact_first_preserved": file_artifact_first_preserved,
        "main_chain_dependency": main_chain_dependency,
        "attention_level": attention_level,
    }


def _pick_primary_artifact_path(*, key: str, artifact_paths: dict[str, Any]) -> str:
    direct = str(artifact_paths.get(key) or "").strip()
    if direct:
        return direct
    for candidate_key, candidate_value in artifact_paths.items():
        if str(candidate_key or "").strip() == key:
            return str(candidate_value or "").strip()
    return ""


def _category_summary_entry(category: dict[str, Any]) -> dict[str, Any]:
    return {
        "category_id": str(category.get("category_id") or ""),
        "display_label": str(category.get("display_label") or ""),
        "present": bool(category.get("present")),
        "required": bool(category.get("required")),
        "missing_entries": list(category.get("missing_entries") or []),
        "attention_count": int(category.get("attention_count", 0) or 0),
    }


def _build_unresolved_non_claim_items(entries: Sequence[dict[str, Any]]) -> list[str]:
    items: list[str] = []
    for entry in entries:
        if not bool(entry.get("available")):
            continue
        if not bool(entry.get("reviewer_only")):
            items.append(f"{entry.get('key')}: reviewer_only \u9700\u8981\u4fdd\u6301 true\u3002")
        if not bool(entry.get("readiness_mapping_only")):
            items.append(f"{entry.get('key')}: readiness_mapping_only \u9700\u8981\u4fdd\u6301 true\u3002")
        if not bool(entry.get("not_real_acceptance_evidence")):
            items.append(f"{entry.get('key')}: not_real_acceptance_evidence \u9700\u8981\u4fdd\u6301 true\u3002")
        if not bool(entry.get("not_ready_for_formal_claim")):
            items.append(f"{entry.get('key')}: not_ready_for_formal_claim \u9700\u8981\u4fdd\u6301 true\u3002")
        if not bool(entry.get("file_artifact_first_preserved")):
            items.append(f"{entry.get('key')}: file_artifact_first_preserved \u9700\u8981\u4fdd\u6301 true\u3002")
        if bool(entry.get("main_chain_dependency")):
            items.append(f"{entry.get('key')}: main_chain_dependency \u5fc5\u987b\u4fdd\u6301 false\u3002")
    return items


def _build_reviewer_attention_items(categories: Sequence[dict[str, Any]]) -> list[str]:
    rows: list[str] = []
    for category in categories:
        display_label = str(category.get("display_label") or category.get("category_id") or "--")
        missing_entries = list(category.get("missing_entries") or [])
        if missing_entries:
            rows.append(f"{display_label}: \u7f3a\u5c11 {', '.join(missing_entries)}")
            continue
        warning_entries = [
            str(entry.get("key") or "")
            for entry in list(category.get("entries") or [])
            if str(entry.get("attention_level") or "") == "warning"
        ]
        if warning_entries:
            rows.append(f"{display_label}: \u8fb9\u754c\u5b57\u6bb5\u9700\u590d\u6838 {', '.join(warning_entries)}")
    return rows


def _build_warning_items(categories: Sequence[dict[str, Any]]) -> list[str]:
    rows: list[str] = []
    for category in categories:
        warning_entries = [
            str(entry.get("key") or "")
            for entry in list(category.get("entries") or [])
            if str(entry.get("attention_level") or "") == "warning"
        ]
        if not warning_entries:
            continue
        display_label = str(category.get("display_label") or category.get("category_id") or "--")
        rows.append(f"{display_label} \u4ecd\u6709 reviewer attention \u9879: {', '.join(warning_entries)}\u3002")
    return rows


def _build_bridge_candidates(
    *,
    categories: Sequence[dict[str, Any]],
    missing_evidence_categories: Sequence[str],
) -> list[str]:
    if list(missing_evidence_categories):
        return []
    present_ids = {
        str(category.get("category_id") or "")
        for category in categories
        if bool(category.get("present"))
    }
    if not _ALL_REQUIRED_CATEGORY_IDS.issubset(present_ids):
        return []
    return [
        "internal_step2_review",
        "phase_freeze_signoff",
        "engineering_isolation_admission_bridge",
    ]


def _build_info_items(categories: Sequence[dict[str, Any]]) -> list[str]:
    rows: list[str] = []
    for category in categories:
        category_id = str(category.get("category_id") or "")
        display_label = str(category.get("display_label") or category_id or "--")
        required_entry_count = int(category.get("required_entry_count", 0) or 0)
        present_entry_count = int(category.get("present_entry_count", 0) or 0)
        available_entry_count = int(category.get("available_entry_count", 0) or 0)
        if category_id == "sidecar" and not available_entry_count:
            rows.append(
                "sidecar \u672a\u6ce8\u5165\uff0c\u4fdd\u6301\u7a7a\u6458\u8981\uff0c\u4e0d\u5f71\u54cd\u9ed8\u8ba4\u4e3b\u94fe\u3002"
            )
            continue
        if required_entry_count:
            rows.append(
                f"{display_label}: {present_entry_count}/{required_entry_count} \u4e2a required entries \u5df2\u5c31\u7eea\u3002"
            )
        else:
            rows.append(
                f"{display_label}: {available_entry_count} \u4e2a optional entries \u5df2\u6c47\u603b\u3002"
            )
    return rows


def _build_summary_line(
    *,
    categories: Sequence[dict[str, Any]],
    blocker_items: Sequence[str],
    warning_items: Sequence[str],
    bridge_to_stage3_candidates: Sequence[str],
) -> str:
    present_categories = sum(1 for category in categories if bool(category.get("present")))
    total_categories = len(categories)
    blocker_count = len(blocker_items)
    warning_count = len(warning_items)
    bridge_count = len(bridge_to_stage3_candidates)
    if blocker_count:
        return (
            f"{STEP2_CLOSEOUT_TITLE}\u5df2\u805a\u5408 {present_categories}/{total_categories} \u7c7b reviewer/readiness "
            f"\u5de5\u4ef6\uff0c\u5b58\u5728 {blocker_count} \u4e2a blocker\uff0c\u4ecd\u4fdd\u6301 "
            "reviewer/readiness/non-claim \u8fb9\u754c\u3002"
        )
    if warning_count:
        return (
            f"{STEP2_CLOSEOUT_TITLE}\u5df2\u805a\u5408 {present_categories}/{total_categories} \u7c7b reviewer/readiness "
            f"\u5de5\u4ef6\uff0c\u5b58\u5728 {warning_count} \u4e2a warning\uff0c\u53ef\u7528\u4e8e\u5185\u90e8"
            "\u8bc4\u5ba1\uff0c\u4f46\u5c1a\u4e0d\u80fd\u5f62\u6210\u6b63\u5f0f\u7ed3\u8bba\u94fe\u3002"
        )
    if bridge_count:
        return (
            f"{STEP2_CLOSEOUT_TITLE}\u5df2\u805a\u5408 {present_categories}/{total_categories} \u7c7b reviewer/readiness "
            "\u5de5\u4ef6\uff0c\u4fdd\u6301 reviewer/readiness/non-claim \u8fb9\u754c\uff0c"
            "\u53ef\u7528\u4e8e\u5185\u90e8\u8bc4\u5ba1\u3001\u9636\u6bb5\u5c01\u7248\u4e0e engineering-isolation bridge "
            "\u51c6\u5907\u3002"
        )
    return (
        f"{STEP2_CLOSEOUT_TITLE}\u5df2\u805a\u5408 {present_categories}/{total_categories} \u7c7b reviewer/readiness "
        "\u5de5\u4ef6\uff0c\u4fdd\u6301 reviewer/readiness/non-claim \u8fb9\u754c\uff0c\u4e0d\u8fdb\u5165\u4e3b\u94fe"
        "\u7ed3\u8bba\u4f9d\u8d56\u3002"
    )


def _build_source_artifact_refs(entries: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for entry in entries:
        rows.append(
            {
                "category_id": str(entry.get("category_id") or ""),
                "key": str(entry.get("key") or ""),
                "required": bool(entry.get("required")),
                "available": bool(entry.get("available")),
                "artifact_type": str(entry.get("artifact_type") or ""),
                "artifact_path": str(entry.get("artifact_path") or ""),
                "summary": str(entry.get("summary") or ""),
            }
        )
    return rows


def _render_summary_markdown(
    *,
    bundle: dict[str, Any],
    categories: Sequence[dict[str, Any]],
    blocker_items: Sequence[str],
    warning_items: Sequence[str],
    info_items: Sequence[str],
) -> str:
    lines = [
        f"# {STEP2_CLOSEOUT_TITLE}",
        "",
        str(bundle.get("summary_line") or ""),
        "",
        "## \u8fb9\u754c",
        "",
        *[f"- {line}" for line in STEP2_CLOSEOUT_BOUNDARY_LINES],
        "",
        "## \u5206\u7c7b\u6458\u8981",
        "",
    ]
    for category in categories:
        lines.append(
            "- "
            + f"{str(category.get('display_label') or category.get('category_id') or '--')}: "
            + ("present" if bool(category.get("present")) else "missing")
        )
    lines.extend(["", "## blocker", ""])
    lines.extend([f"- {item}" for item in blocker_items] or ["- \u65e0"])
    lines.extend(["", "## warning", ""])
    lines.extend([f"- {item}" for item in warning_items] or ["- \u65e0"])
    lines.extend(["", "## info", ""])
    lines.extend([f"- {item}" for item in info_items] or ["- \u65e0"])
    lines.extend(["", "## reviewer attention", ""])
    lines.extend([f"- {item}" for item in list(bundle.get("reviewer_attention_items") or [])] or ["- \u65e0"])
    lines.extend(["", "## bridge to stage3", ""])
    lines.extend([f"- {item}" for item in list(bundle.get("bridge_to_stage3_candidates") or [])] or ["- \u65e0"])
    return "\n".join(line for line in lines if line is not None).strip() + "\n"
