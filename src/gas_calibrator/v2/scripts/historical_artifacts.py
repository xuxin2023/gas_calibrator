from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any

from ..core.artifact_compatibility import (
    ARTIFACT_COMPATIBILITY_INDEX_SCHEMA_VERSION,
    HISTORICAL_ARTIFACT_ROLLUP_TOOL,
    PRIMARY_READER_FILENAMES,
    build_artifact_compatibility_rollup,
    load_or_build_artifact_compatibility_payloads,
    regenerate_artifact_compatibility_sidecars,
)
from ..core.phase_evidence_display_contracts import (
    HISTORICAL_ROLLUP_LABELS as _ROLLUP_LABELS,
)
from ..adapters.method_confirmation_gateway import MethodConfirmationGateway
from ..adapters.recognition_scope_gateway import RecognitionScopeGateway
from ..adapters.software_validation_gateway import SoftwareValidationGateway
from ..adapters.uncertainty_gateway import UncertaintyGateway
from ..adapters.wp6_gateway import Wp6Gateway
from ..core.reviewer_surface_contracts import (
    WP6_CLOSEOUT_ARTIFACT_KEYS as _SHARED_WP6_CLOSEOUT_KEYS,
)
from ..core.reviewer_surface_payloads import (
    extract_wp6_closeout_payloads as _extract_wp6_closeout_payloads,
)
from ..core.step2_closeout_verification import (
    build_step2_closeout_verification_surface_payload,
)
from ..core.step2_final_closure_matrix import (
    build_step2_final_closure_matrix_surface_payload,
)
from ._cli_safety import build_step2_historical_cli_lines

try:
    from ..core.reviewer_summary_packs import build_compact_summary_render_context
except ImportError:
    build_compact_summary_render_context = None

try:
    from ..core.compact_summary_rendering import (
        build_visible_sections as _shared_build_visible_sections,
        build_compact_summary_pack_fields as _shared_build_pack_fields,
        build_old_run_fallback as _shared_build_old_run_fallback,
    )
except ImportError:
    _shared_build_visible_sections = None
    _shared_build_pack_fields = None
    _shared_build_old_run_fallback = None


def create_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Scan or regenerate Step 2 historical artifact compatibility sidecars without rewriting primary evidence.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("scan", "export-summary", "regenerate", "reindex"):
        subparser = subparsers.add_parser(command)
        _add_target_arguments(subparser)
        subparser.add_argument("--dry-run", action="store_true", help="Preview only; do not write sidecars.")
        subparser.add_argument(
            "--output",
            type=str,
            default=None,
            help="Optional JSON output path. Required for export-summary.",
        )
    return parser


def _add_target_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--run-dir",
        action="append",
        default=[],
        help="Explicit run directory to scan/regenerate.",
    )
    parser.add_argument(
        "--root-dir",
        action="append",
        default=[],
        help="Root directory to recursively discover historical runs.",
    )


def _discover_run_dirs(root_dir: Path) -> list[Path]:
    root_dir = Path(root_dir).resolve()
    if not root_dir.exists():
        return []
    discovered: dict[str, Path] = {}
    for filename in PRIMARY_READER_FILENAMES:
        for candidate in root_dir.rglob(filename):
            if candidate.is_file():
                resolved = candidate.parent.resolve()
                discovered[str(resolved)] = resolved
    return [discovered[key] for key in sorted(discovered)]


def _collect_run_dirs(*, run_dirs: list[str], root_dirs: list[str]) -> list[Path]:
    collected: dict[str, Path] = {}
    for raw_path in list(run_dirs or []):
        candidate = Path(raw_path).resolve()
        if candidate.exists() and candidate.is_dir():
            collected[str(candidate)] = candidate
    for raw_root in list(root_dirs or []):
        for candidate in _discover_run_dirs(Path(raw_root)):
            collected[str(candidate)] = candidate
    return [collected[key] for key in sorted(collected)]


def _summarize_counts(rows: list[dict[str, Any]], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        value = str(dict(row or {}).get(key) or "").strip() or "missing"
        counts[value] = int(counts.get(value, 0) or 0) + 1
    return counts


def _resolve_rollup_scope(*, run_dirs: list[str], root_dirs: list[str], run_count: int) -> str:
    if root_dirs and not run_dirs:
        return "root-dir"
    if root_dirs or run_count > 1:
        return "batch"
    return "run-dir"


def _build_run_report(
    run_dir: Path,
    *,
    operation: str,
    dry_run: bool,
) -> dict[str, Any]:
    run_dir = Path(run_dir).resolve()
    written_paths: dict[str, Any] = {}
    compatibility_payloads: dict[str, dict[str, Any]] = {}
    if operation in {"regenerate", "reindex"} and not dry_run:
        regenerate_payload = regenerate_artifact_compatibility_sidecars(run_dir)
        compatibility_payloads = {
            "run_artifact_index": dict(regenerate_payload.get("run_artifact_index") or {}),
            "artifact_contract_catalog": dict(regenerate_payload.get("artifact_contract_catalog") or {}),
            "compatibility_scan_summary": dict(regenerate_payload.get("compatibility_scan_summary") or {}),
            "reindex_manifest": dict(regenerate_payload.get("reindex_manifest") or {}),
        }
        written_paths = dict(regenerate_payload.get("written_paths") or {})
    else:
        compatibility_payloads = load_or_build_artifact_compatibility_payloads(run_dir)
    run_artifact_index = dict(compatibility_payloads.get("run_artifact_index") or {})
    artifact_contract_catalog = dict(compatibility_payloads.get("artifact_contract_catalog") or {})
    compatibility_scan_summary = dict(compatibility_payloads.get("compatibility_scan_summary") or {})
    reindex_manifest = dict(compatibility_payloads.get("reindex_manifest") or {})
    compatibility_overview = dict(compatibility_scan_summary.get("compatibility_overview") or {})
    compatibility_rollup = dict(
        compatibility_scan_summary.get("compatibility_rollup")
        or compatibility_overview.get("compatibility_rollup")
        or {}
    )
    summary_payload = {}
    summary_path = run_dir / "summary.json"
    if summary_path.exists():
        try:
            summary_payload = dict(json.loads(summary_path.read_text(encoding="utf-8")))
        except Exception:
            summary_payload = {}
    recognition_scope_payload = RecognitionScopeGateway(
        run_dir,
        summary=summary_payload,
        scope_readiness_summary=(
            dict(summary_payload.get("scope_readiness_summary") or {})
            or dict(dict(summary_payload.get("stats") or {}).get("scope_readiness_summary") or {})
        ),
        compatibility_scan_summary=compatibility_scan_summary,
    ).read_payload()
    recognition_scope_rollup = dict(recognition_scope_payload.get("recognition_scope_rollup") or {})
    scope_definition_pack = dict(recognition_scope_payload.get("scope_definition_pack") or {})
    decision_rule_profile = dict(recognition_scope_payload.get("decision_rule_profile") or {})
    reference_asset_registry = dict(recognition_scope_payload.get("reference_asset_registry") or {})
    certificate_lifecycle_summary = dict(recognition_scope_payload.get("certificate_lifecycle_summary") or {})
    pre_run_readiness_gate = dict(recognition_scope_payload.get("pre_run_readiness_gate") or {})
    uncertainty_payload = UncertaintyGateway(
        run_dir,
        summary=summary_payload,
        scope_readiness_summary=(
            dict(summary_payload.get("scope_readiness_summary") or {})
            or dict(dict(summary_payload.get("stats") or {}).get("scope_readiness_summary") or {})
        ),
        compatibility_scan_summary=compatibility_scan_summary,
    ).read_payload()
    uncertainty_report_pack = dict(uncertainty_payload.get("uncertainty_report_pack") or {})
    uncertainty_digest_payload = dict(uncertainty_payload.get("uncertainty_digest") or {})
    uncertainty_rollup = dict(uncertainty_payload.get("uncertainty_rollup") or {})
    method_confirmation_payload = MethodConfirmationGateway(
        run_dir,
        summary=summary_payload,
        scope_readiness_summary=(
            dict(summary_payload.get("scope_readiness_summary") or {})
            or dict(dict(summary_payload.get("stats") or {}).get("scope_readiness_summary") or {})
        ),
        compatibility_scan_summary=compatibility_scan_summary,
    ).read_payload()
    method_confirmation_protocol = dict(method_confirmation_payload.get("method_confirmation_protocol") or {})
    route_specific_validation_matrix = dict(method_confirmation_payload.get("route_specific_validation_matrix") or {})
    verification_digest_payload = dict(method_confirmation_payload.get("verification_digest") or {})
    verification_rollup = dict(method_confirmation_payload.get("verification_rollup") or {})
    software_validation_payload = SoftwareValidationGateway(
        run_dir,
        summary=summary_payload,
        scope_readiness_summary=(
            dict(summary_payload.get("scope_readiness_summary") or {})
            or dict(dict(summary_payload.get("stats") or {}).get("scope_readiness_summary") or {})
        ),
        compatibility_scan_summary=compatibility_scan_summary,
    ).read_payload()
    software_validation_traceability_matrix = dict(
        software_validation_payload.get("software_validation_traceability_matrix") or {}
    )
    artifact_hash_registry = dict(software_validation_payload.get("artifact_hash_registry") or {})
    environment_fingerprint = dict(software_validation_payload.get("environment_fingerprint") or {})
    release_manifest = dict(software_validation_payload.get("release_manifest") or {})
    release_scope_summary = dict(software_validation_payload.get("release_scope_summary") or {})
    release_boundary_digest = dict(software_validation_payload.get("release_boundary_digest") or {})
    release_evidence_pack_index = dict(
        software_validation_payload.get("release_evidence_pack_index") or {}
    )
    software_validation_rollup = dict(software_validation_payload.get("software_validation_rollup") or {})
    wp6_payload = Wp6Gateway(
        run_dir,
        summary=summary_payload,
        scope_readiness_summary=(
            dict(summary_payload.get("scope_readiness_summary") or {})
            or dict(dict(summary_payload.get("stats") or {}).get("scope_readiness_summary") or {})
        ),
        compatibility_scan_summary=compatibility_scan_summary,
    ).read_payload()
    _wp6_closeout = _extract_wp6_closeout_payloads(wp6_payload)
    reference_asset_digest = dict(reference_asset_registry.get("digest") or {})
    certificate_lifecycle_digest = dict(certificate_lifecycle_summary.get("digest") or {})
    pre_run_gate_digest = dict(pre_run_readiness_gate.get("digest") or {})
    uncertainty_digest = dict(
        uncertainty_rollup.get("digest")
        or uncertainty_digest_payload.get("digest")
        or uncertainty_report_pack.get("digest")
        or {}
    )
    verification_digest = dict(
        verification_rollup.get("digest")
        or verification_digest_payload.get("digest")
        or route_specific_validation_matrix.get("digest")
        or {}
    )
    software_validation_digest = dict(software_validation_traceability_matrix.get("digest") or {})
    hash_registry_digest = dict(artifact_hash_registry.get("digest") or {})
    release_manifest_digest = dict(release_manifest.get("digest") or {})
    asset_readiness_overview = str(
        recognition_scope_rollup.get("asset_readiness_overview")
        or reference_asset_digest.get("asset_readiness_overview")
        or reference_asset_digest.get("summary")
        or "--"
    )
    certificate_lifecycle_overview = str(
        recognition_scope_rollup.get("certificate_lifecycle_overview")
        or certificate_lifecycle_digest.get("certificate_lifecycle_overview")
        or certificate_lifecycle_digest.get("summary")
        or "--"
    )
    pre_run_gate_status = str(
        recognition_scope_rollup.get("pre_run_gate_status")
        or pre_run_readiness_gate.get("gate_status")
        or pre_run_gate_digest.get("pre_run_gate_status")
        or "--"
    )
    blocking_digest = str(
        recognition_scope_rollup.get("blocking_digest")
        or pre_run_gate_digest.get("blocker_summary")
        or "--"
    )
    warning_digest = str(
        recognition_scope_rollup.get("warning_digest")
        or pre_run_gate_digest.get("warning_summary")
        or "--"
    )
    pre_run_gate_summary = str(pre_run_gate_digest.get("summary") or "--")
    readiness_mapping_boundary = str(
        pre_run_readiness_gate.get("non_claim_note")
        or recognition_scope_rollup.get("non_claim_note")
        or decision_rule_profile.get("non_claim_note")
        or scope_definition_pack.get("non_claim_note")
        or "--"
    )
    current_reader_mode = str(
        compatibility_overview.get("current_reader_mode")
        or compatibility_scan_summary.get("current_reader_mode")
        or ""
    ).strip()
    generated_at = str(
        compatibility_rollup.get("generated_at")
        or compatibility_overview.get("generated_at")
        or compatibility_scan_summary.get("generated_at")
        or run_artifact_index.get("generated_at")
        or ""
    ).strip()
    uncertainty_readiness_status = (
        "ready_for_readiness_mapping"
        if bool(uncertainty_rollup.get("ready_for_readiness_mapping", True))
        else "placeholder_only"
    )
    verification_readiness_status = str(
        verification_rollup.get("readiness_status_summary")
        or verification_digest.get("readiness_status_summary")
        or "ready_for_readiness_mapping"
    )
    # Compact summary pack — explicit consumption for historical surface
    _raw_packs = list(summary_payload.get("compact_summary_packs") or [])
    if _shared_build_pack_fields is not None and _shared_build_visible_sections is not None:
        _pack_fields = _shared_build_pack_fields(_raw_packs, surface="historical")
        _compact_summary_packs = list(_pack_fields.get("compact_summary_packs") or [])
        _compact_summary_sections = list(_pack_fields.get("compact_summary_sections") or [])
        _compact_summary_order = list(_pack_fields.get("compact_summary_order") or [])
        _compact_summary_budget = dict(_pack_fields.get("compact_summary_budget") or {})
        _visible = _shared_build_visible_sections(
            _compact_summary_packs, budget=_compact_summary_budget,
        )
        _rendered_summary_sections = list(_visible.get("rendered_summary_sections") or [])
        _omitted_summary_sections = list(_visible.get("omitted_summary_sections") or [])
        _compact_summary_budget_display = dict(_visible.get("compact_summary_budget_display") or {})
    elif _raw_packs and build_compact_summary_render_context is not None:
        try:
            _render_ctx = build_compact_summary_render_context(_raw_packs, surface="historical")
            _compact_summary_packs = list(_render_ctx.get("compact_summary_packs") or [])
            _compact_summary_sections = list(_render_ctx.get("compact_summary_sections") or [])
            _compact_summary_order = list(_render_ctx.get("compact_summary_order") or [])
            _compact_summary_budget = dict(_render_ctx.get("compact_summary_budget") or {})
            if _shared_build_visible_sections is not None:
                _visible = _shared_build_visible_sections(
                    _compact_summary_packs, budget=_compact_summary_budget,
                )
                _rendered_summary_sections = list(_visible.get("rendered_summary_sections") or [])
                _omitted_summary_sections = list(_visible.get("omitted_summary_sections") or [])
                _compact_summary_budget_display = dict(_visible.get("compact_summary_budget_display") or {})
            else:
                _rendered_summary_sections = []
                _omitted_summary_sections = []
                _compact_summary_budget_display = {}
        except Exception:
            _compact_summary_packs = []
            _compact_summary_sections = []
            _compact_summary_order = []
            _compact_summary_budget = {}
            _rendered_summary_sections = []
            _omitted_summary_sections = []
            _compact_summary_budget_display = {}
    else:
        # Old-run fallback: use shared fallback when no packs available
        if _shared_build_old_run_fallback is not None:
            _fallback = _shared_build_old_run_fallback()
            _compact_summary_packs = _fallback["compact_summary_packs"]
            _compact_summary_sections = _fallback["compact_summary_sections"]
            _compact_summary_order = _fallback["compact_summary_order"]
            _compact_summary_budget = _fallback["compact_summary_budget"]
            _rendered_summary_sections = _fallback["rendered_summary_sections"]
            _omitted_summary_sections = _fallback["omitted_summary_sections"]
            _compact_summary_budget_display = _fallback["compact_summary_budget_display"]
        else:
            _compact_summary_packs = []
            _compact_summary_sections = []
            _compact_summary_order = []
            _compact_summary_budget = {}
            _rendered_summary_sections = []
            _omitted_summary_sections = []
            _compact_summary_budget_display = {}

    return {
        "run_id": str(
            compatibility_scan_summary.get("run_id")
            or run_artifact_index.get("run_id")
            or artifact_contract_catalog.get("run_id")
            or reindex_manifest.get("run_id")
            or run_dir.name
        ).strip(),
        "run_dir": str(run_dir),
        "operation": operation,
        "dry_run": bool(dry_run),
        "index_schema_version": str(
            compatibility_rollup.get("index_schema_version")
            or compatibility_overview.get("index_schema_version")
            or ARTIFACT_COMPATIBILITY_INDEX_SCHEMA_VERSION
        ),
        "generated_at": generated_at,
        "generated_by_tool": str(
            compatibility_rollup.get("generated_by_tool")
            or compatibility_overview.get("generated_by_tool")
            or compatibility_scan_summary.get("generated_by_tool")
            or HISTORICAL_ARTIFACT_ROLLUP_TOOL
        ).strip()
        or HISTORICAL_ARTIFACT_ROLLUP_TOOL,
        "summary": str(compatibility_scan_summary.get("summary") or compatibility_overview.get("summary") or ""),
        "current_reader_mode": current_reader_mode,
        "current_reader_mode_display": str(
            compatibility_overview.get("current_reader_mode_display")
            or compatibility_scan_summary.get("current_reader_mode_display")
            or current_reader_mode
        ),
        "compatibility_status": str(
            compatibility_overview.get("compatibility_status")
            or compatibility_scan_summary.get("compatibility_status")
            or ""
        ),
        "compatibility_status_display": str(
            compatibility_overview.get("compatibility_status_display")
            or compatibility_scan_summary.get("compatibility_status_display")
            or ""
        ),
        "canonical_direct": current_reader_mode == "canonical_direct",
        "compatibility_adapter": current_reader_mode == "compatibility_adapter",
        "observed_contract_versions": list(compatibility_overview.get("observed_contract_versions") or []),
        "observed_contract_version_summary": str(
            compatibility_overview.get("observed_contract_version_summary")
            or compatibility_scan_summary.get("schema_or_contract_version_summary")
            or "--"
        ),
        "schema_contract_summary": str(compatibility_overview.get("schema_contract_summary_display") or ""),
        "artifact_count": int(
            compatibility_rollup.get("artifact_count")
            or compatibility_overview.get("observed_artifact_count")
            or len(list(run_artifact_index.get("entries") or []))
            or 0
        ),
        "contract_row_count": int(
            compatibility_rollup.get("contract_row_count")
            or compatibility_overview.get("contract_row_count")
            or len(list(artifact_contract_catalog.get("contract_rows") or []))
            or 0
        ),
        "linked_surface_visibility": list(
            compatibility_rollup.get("linked_surface_visibility")
            or compatibility_overview.get("linked_surface_visibility")
            or []
        ),
        "regenerate_recommended": bool(
            compatibility_overview.get(
                "regenerate_recommended",
                compatibility_scan_summary.get("regenerate_recommended", False),
            )
        ),
        "regenerate_scope": str(
            compatibility_overview.get("regenerate_scope")
            or compatibility_scan_summary.get("regenerate_scope")
            or "reviewer_index_sidecar_only"
        ),
        "primary_evidence_rewritten": False,
        "non_primary_boundary": str(compatibility_overview.get("non_primary_boundary_display") or ""),
        "non_primary_chain": str(compatibility_overview.get("non_primary_chain_display") or ""),
        "boundary_digest": str(compatibility_overview.get("boundary_digest") or ""),
        "non_claim_digest": str(compatibility_overview.get("non_claim_digest") or ""),
        "scope_overview": str(
            recognition_scope_rollup.get("scope_overview_display")
            or dict(scope_definition_pack.get("digest") or {}).get("scope_overview_summary")
            or "--"
        ),
        "decision_rule_overview": str(
            recognition_scope_rollup.get("decision_rule_display")
            or dict(decision_rule_profile.get("digest") or {}).get("decision_rule_summary")
            or "--"
        ),
        "conformity_boundary": str(
            recognition_scope_rollup.get("conformity_boundary_display")
            or dict(decision_rule_profile.get("digest") or {}).get("conformity_boundary_summary")
            or "--"
        ),
        "readiness_status": str(
            recognition_scope_rollup.get("readiness_status")
            or scope_definition_pack.get("readiness_status")
            or decision_rule_profile.get("readiness_status")
            or "ready_for_readiness_mapping"
        ),
        "scope_non_claim_note": str(
            recognition_scope_rollup.get("non_claim_note")
            or decision_rule_profile.get("non_claim_note")
            or scope_definition_pack.get("non_claim_note")
            or "--"
        ),
        "method_confirmation_overview": str(
            verification_digest.get("protocol_overview_summary")
            or dict(method_confirmation_protocol.get("digest") or {}).get("summary")
            or method_confirmation_protocol.get("protocol_id")
            or "--"
        ),
        "validation_matrix_completeness": str(
            verification_rollup.get("rollup_summary_display")
            or verification_digest.get("matrix_completeness_summary")
            or "--"
        ),
        "software_validation_overview": str(
            software_validation_rollup.get("rollup_summary_display")
            or release_manifest_digest.get("summary")
            or software_validation_digest.get("summary")
            or "--"
        ),
        "traceability_completeness": str(
            software_validation_rollup.get("traceability_completeness_summary")
            or software_validation_digest.get("current_coverage_summary")
            or "--"
        ),
        "audit_hash_summary": str(
            software_validation_rollup.get("hash_registry_summary")
            or hash_registry_digest.get("summary")
            or "--"
        ),
        "environment_fingerprint_summary": str(
            software_validation_rollup.get("environment_summary")
            or environment_fingerprint.get("environment_summary")
            or dict(environment_fingerprint.get("digest") or {}).get("summary")
            or "--"
        ),
        "release_manifest_overview": str(
            software_validation_rollup.get("release_manifest_summary")
            or release_manifest_digest.get("summary")
            or "--"
        ),
        "release_scope_overview": str(
            dict(release_scope_summary.get("digest") or {}).get("summary")
            or release_scope_summary.get("scope_id")
            or "--"
        ),
        "release_boundary_overview": str(
            dict(release_boundary_digest.get("digest") or {}).get("summary")
            or release_boundary_digest.get("non_claim_note")
            or "--"
        ),
        "release_evidence_pack_overview": str(
            dict(release_evidence_pack_index.get("digest") or {}).get("summary")
            or "--"
        ),
        "linked_scope_ids": list(release_manifest.get("linked_scope_ids") or [scope_definition_pack.get("scope_id")] or []),
        "linked_decision_rules": list(release_manifest.get("linked_decision_rules") or [decision_rule_profile.get("decision_rule_id")] or []),
        "linked_uncertainty_cases": list(release_manifest.get("linked_uncertainty_cases") or []),
        "linked_method_confirmation_protocols": list(
            release_manifest.get("linked_method_confirmation_protocols") or []
        ),
        "linked_traceability_matrix": dict(release_manifest.get("linked_traceability_matrix") or {}),
        "linked_hash_registry": dict(release_manifest.get("linked_hash_registry") or {}),
        "linked_test_suites": list(release_manifest.get("linked_test_suites") or []),
        "software_validation_rollup": software_validation_rollup,
        "pt_ilc_registry": _wp6_closeout["pt_ilc_registry"],
        "external_comparison_importer": _wp6_closeout["external_comparison_importer"],
        "comparison_evidence_pack": _wp6_closeout["comparison_evidence_pack"],
        "scope_comparison_view": _wp6_closeout["scope_comparison_view"],
        "comparison_digest": _wp6_closeout["comparison_digest"],
        "comparison_rollup": _wp6_closeout["comparison_rollup"],
        "step2_closeout_digest": _wp6_closeout["step2_closeout_digest"],
        "current_evidence_coverage": str(
            verification_digest.get("current_evidence_coverage_summary")
            or verification_digest.get("current_coverage_summary")
            or "--"
        ),
        "top_gaps": str(
            verification_digest.get("top_gaps_summary")
            or verification_digest.get("missing_evidence_summary")
            or "--"
        ),
        "reviewer_actions": str(verification_digest.get("reviewer_action_summary") or "--"),
        "verification_non_claim_note": str(
            verification_rollup.get("non_claim_note")
            or verification_digest_payload.get("non_claim_note")
            or verification_digest.get("non_claim_digest")
            or "--"
        ),
        "verification_readiness_status": verification_readiness_status,
        "verification_rollup": verification_rollup,
        "method_confirmation_placeholder_used": bool(
            verification_rollup.get("legacy_placeholder_used")
            or verification_digest_payload.get("reviewer_placeholder")
            or route_specific_validation_matrix.get("reviewer_placeholder")
        ),
        "method_confirmation_ready_for_readiness_mapping": bool(
            verification_rollup.get("ready_for_readiness_mapping", True)
        ),
        "method_confirmation_not_ready_for_formal_claim": bool(
            verification_rollup.get("not_ready_for_formal_claim", True)
        ),
        "method_confirmation_not_real_acceptance_evidence": bool(
            verification_rollup.get("not_real_acceptance_evidence", True)
        ),
        "method_confirmation_primary_evidence_rewritten": False,
        "asset_readiness_overview": asset_readiness_overview,
        "certificate_lifecycle_overview": certificate_lifecycle_overview,
        "pre_run_gate_status": pre_run_gate_status,
        "pre_run_gate_summary": pre_run_gate_summary,
        "uncertainty_overview": str(
            uncertainty_rollup.get("overview_display")
            or uncertainty_rollup.get("rollup_summary_display")
            or uncertainty_digest.get("uncertainty_overview_summary")
            or "--"
        ),
        "uncertainty_budget_completeness": str(
            uncertainty_rollup.get("budget_completeness_summary")
            or uncertainty_digest.get("budget_component_summary")
            or "--"
        ),
        "uncertainty_top_contributors": str(
            uncertainty_rollup.get("top_contributors_summary")
            or uncertainty_digest.get("top_contributors_summary")
            or "--"
        ),
        "uncertainty_data_completeness": str(
            uncertainty_rollup.get("data_completeness_summary")
            or uncertainty_digest.get("data_completeness_summary")
            or "--"
        ),
        "uncertainty_placeholder_completeness": str(
            uncertainty_rollup.get("placeholder_completeness_summary")
            or uncertainty_digest.get("placeholder_completeness_summary")
            or "--"
        ),
        "uncertainty_readiness_status": uncertainty_readiness_status,
        "uncertainty_non_claim_note": str(
            uncertainty_rollup.get("non_claim_note")
            or uncertainty_report_pack.get("non_claim_note")
            or uncertainty_digest.get("non_claim_digest")
            or "--"
        ),
        "uncertainty_rollup": uncertainty_rollup,
        "uncertainty_placeholder_used": bool(uncertainty_rollup.get("legacy_placeholder_used", False)),
        "blocking_digest": blocking_digest,
        "warning_digest": warning_digest,
        "ready_for_readiness_mapping": bool(
            pre_run_readiness_gate.get("ready_for_readiness_mapping")
            or recognition_scope_rollup.get("readiness_status") == "ready_for_readiness_mapping"
        ),
        "not_ready_for_formal_claim": bool(
            pre_run_readiness_gate.get("not_ready_for_formal_claim", True)
        ),
        "uncertainty_ready_for_readiness_mapping": bool(
            uncertainty_rollup.get("ready_for_readiness_mapping", True)
        ),
        "uncertainty_not_ready_for_formal_claim": bool(
            uncertainty_rollup.get("not_ready_for_formal_claim", True)
        ),
        "reviewer_only_boundary": readiness_mapping_boundary,
        "evidence_source": str(
            pre_run_readiness_gate.get("evidence_source")
            or certificate_lifecycle_summary.get("evidence_source")
            or reference_asset_registry.get("evidence_source")
            or "simulated"
        ),
        "uncertainty_evidence_source": str(
            uncertainty_rollup.get("evidence_source")
            or uncertainty_report_pack.get("evidence_source")
            or "simulated"
        ),
        "not_real_acceptance_evidence": bool(
            pre_run_readiness_gate.get("not_real_acceptance_evidence", True)
            and certificate_lifecycle_summary.get("not_real_acceptance_evidence", True)
            and reference_asset_registry.get("not_real_acceptance_evidence", True)
        ),
        "uncertainty_not_real_acceptance_evidence": bool(
            uncertainty_rollup.get("not_real_acceptance_evidence", True)
        ),
        "software_validation_not_real_acceptance_evidence": bool(
            software_validation_rollup.get("not_real_acceptance_evidence", True)
        ),
        "software_validation_not_ready_for_formal_claim": bool(
            software_validation_rollup.get("not_ready_for_formal_claim", True)
        ),
        "software_validation_primary_evidence_rewritten": bool(
            software_validation_rollup.get("primary_evidence_rewritten", False)
        ),
        "recognition_scope_rollup": recognition_scope_rollup,
        "compatibility_rollup": compatibility_rollup,
        "rollup_summary": str(
            compatibility_rollup.get("rollup_summary_display")
            or compatibility_overview.get("rollup_summary_display")
            or ""
        ).strip(),
        "uncertainty_primary_evidence_rewritten": bool(
            uncertainty_rollup.get("primary_evidence_rewritten", False)
        ),
        "compact_summary_packs": _compact_summary_packs,
        "compact_summary_sections": _compact_summary_sections,
        "compact_summary_order": _compact_summary_order,
        "compact_summary_budget": _compact_summary_budget,
        "rendered_summary_sections": _rendered_summary_sections,
        "omitted_summary_sections": _omitted_summary_sections,
        "compact_summary_budget_display": _compact_summary_budget_display,
        "compact_summary_legacy_mode": not bool(_raw_packs),
        "step2_closeout_readiness": (
            # Main path: prefer persisted closeout readiness (Step 2.19)
            dict(summary_payload.get("step2_closeout_readiness") or {})
            or _build_closeout_readiness_from_summary(summary_payload, _compact_summary_packs)
        ),
        "step2_closeout_package": (
            # Main path: prefer persisted closeout package (Step 2.22)
            dict(summary_payload.get("step2_closeout_package") or {})
            or _build_closeout_package_from_summary(summary_payload, _compact_summary_packs)
        ),
        "step2_freeze_audit": (
            # Main path: prefer persisted freeze audit (Step 2.23)
            dict(summary_payload.get("step2_freeze_audit") or {})
            or _build_freeze_audit_from_summary(summary_payload, _compact_summary_packs)
        ),
        "step3_admission_dossier": (
            # Main path: prefer persisted admission dossier (Step 2.24)
            dict(summary_payload.get("step3_admission_dossier") or {})
            or _build_admission_dossier_from_summary(summary_payload, _compact_summary_packs)
        ),
        "step2_closeout_verification": (
            # Main path: prefer persisted closeout verification (Step 2.25)
            dict(summary_payload.get("step2_closeout_verification") or {})
            or _build_closeout_verification_from_summary(summary_payload, _compact_summary_packs)
        ),
        "step2_freeze_seal": (
            # Main path: prefer persisted freeze seal (Step 2.25)
            dict(summary_payload.get("step2_freeze_seal") or {})
            or _build_freeze_seal_from_summary(summary_payload, _compact_summary_packs)
        ),
        "step2_final_closure_matrix": (
            dict(summary_payload.get("step2_final_closure_matrix") or {})
            or _build_final_closure_matrix_from_summary(summary_payload, _compact_summary_packs)
        ),
        "written_paths": written_paths,
    }


def _build_closeout_readiness_from_summary(
    summary_payload: dict[str, Any],
    compact_summary_packs: list[dict[str, Any]],
) -> dict[str, Any]:
    """Build Step 2 closeout readiness from a historical run summary payload."""
    try:
        from gas_calibrator.v2.core.step2_closeout_readiness_builder import build_step2_closeout_readiness
        result = build_step2_closeout_readiness(
            run_id=str(summary_payload.get("run_id") or ""),
            step2_readiness_summary=dict(summary_payload.get("step2_readiness_summary") or {}),
            compact_summary_packs=compact_summary_packs,
            governance_handoff=dict(summary_payload.get("config_governance_handoff") or {}),
            parity_resilience=dict(summary_payload.get("parity_resilience") or {}),
            acceptance_governance=dict(summary_payload.get("acceptance_governance") or {}),
            phase_evidence=dict(summary_payload.get("phase_evidence") or {}),
        )
        result["closeout_readiness_source"] = "rebuilt"
        return result
    except Exception:
        from gas_calibrator.v2.core.step2_closeout_readiness_contracts import build_closeout_readiness_fallback
        result = build_closeout_readiness_fallback()
        result["closeout_readiness_source"] = "fallback"
        return result


def _build_closeout_package_from_summary(
    summary_payload: dict[str, Any],
    compact_summary_packs: list[dict[str, Any]],
) -> dict[str, Any]:
    """Build Step 2 closeout package from a historical run summary payload."""
    try:
        from gas_calibrator.v2.core.step2_closeout_package_builder import build_step2_closeout_package
        _closeout_readiness = (
            dict(summary_payload.get("step2_closeout_readiness") or {})
            or _build_closeout_readiness_from_summary(summary_payload, compact_summary_packs)
        )
        return build_step2_closeout_package(
            run_id=str(summary_payload.get("run_id") or ""),
            step2_closeout_readiness=_closeout_readiness,
            step2_closeout_digest=dict(summary_payload.get("step2_closeout_digest") or {}),
            stage_admission_review_pack=dict(summary_payload.get("stage_admission_review_pack") or {}),
            engineering_isolation_admission_checklist=dict(summary_payload.get("engineering_isolation_admission_checklist") or {}),
            compact_summary_packs=compact_summary_packs,
            governance_handoff=dict(summary_payload.get("config_governance_handoff") or {}),
            parity_resilience=dict(summary_payload.get("parity_resilience") or {}),
            phase_evidence=dict(summary_payload.get("phase_evidence") or {}),
        )
    except Exception:
        from gas_calibrator.v2.core.step2_closeout_package_builder import build_closeout_package_fallback
        return build_closeout_package_fallback()


def _build_freeze_audit_from_summary(
    summary_payload: dict[str, Any],
    compact_summary_packs: list[dict[str, Any]],
) -> dict[str, Any]:
    """Build Step 2 freeze audit from a historical run summary payload."""
    try:
        from gas_calibrator.v2.core.step2_freeze_audit_builder import build_step2_freeze_audit
        _closeout_pkg = (
            dict(summary_payload.get("step2_closeout_package") or {})
            or _build_closeout_package_from_summary(summary_payload, compact_summary_packs)
        )
        _closeout_readiness = (
            dict(summary_payload.get("step2_closeout_readiness") or {})
            or _build_closeout_readiness_from_summary(summary_payload, compact_summary_packs)
        )
        return build_step2_freeze_audit(
            run_id=str(summary_payload.get("run_id") or ""),
            step2_closeout_package=_closeout_pkg if _closeout_pkg else None,
            step2_closeout_readiness=_closeout_readiness if _closeout_readiness else None,
            parity_resilience_summary=dict(summary_payload.get("parity_resilience") or {}),
            governance_handoff=dict(summary_payload.get("config_governance_handoff") or {}),
            acceptance_governance=dict(summary_payload.get("acceptance_governance") or {}),
            phase_evidence=dict(summary_payload.get("phase_evidence") or {}),
        )
    except Exception:
        from gas_calibrator.v2.core.step2_freeze_audit_builder import build_freeze_audit_fallback
        return build_freeze_audit_fallback()


def _build_admission_dossier_from_summary(
    summary_payload: dict[str, Any],
    compact_summary_packs: list[dict[str, Any]],
) -> dict[str, Any]:
    """Build Step 3 admission dossier from a historical run summary payload."""
    try:
        from gas_calibrator.v2.core.step3_admission_dossier_builder import build_step3_admission_dossier
        _freeze_audit = (
            dict(summary_payload.get("step2_freeze_audit") or {})
            or _build_freeze_audit_from_summary(summary_payload, compact_summary_packs)
        )
        _closeout_pkg = (
            dict(summary_payload.get("step2_closeout_package") or {})
            or _build_closeout_package_from_summary(summary_payload, compact_summary_packs)
        )
        _closeout_readiness = (
            dict(summary_payload.get("step2_closeout_readiness") or {})
            or _build_closeout_readiness_from_summary(summary_payload, compact_summary_packs)
        )
        dossier = build_step3_admission_dossier(
            run_id=str(summary_payload.get("run_id") or ""),
            step2_freeze_audit=_freeze_audit if _freeze_audit else None,
            step2_closeout_package=_closeout_pkg if _closeout_pkg else None,
            step2_closeout_readiness=_closeout_readiness if _closeout_readiness else None,
            governance_handoff=dict(summary_payload.get("config_governance_handoff") or {}),
            parity_resilience_summary=dict(summary_payload.get("parity_resilience") or {}),
            phase_evidence=dict(summary_payload.get("phase_evidence") or {}),
            acceptance_governance=dict(summary_payload.get("acceptance_governance") or {}),
        )
        dossier["admission_dossier_source"] = "rebuilt"
        return dossier
    except Exception:
        from gas_calibrator.v2.core.step3_admission_dossier_builder import build_admission_dossier_fallback
        fb = build_admission_dossier_fallback()
        fb["admission_dossier_source"] = "fallback"
        return fb


def _build_closeout_verification_from_summary(
    summary_payload: dict[str, Any],
    compact_summary_packs: list[dict[str, Any]],
) -> dict[str, Any]:
    """Build Step 2 closeout verification from a historical run summary payload."""
    _readiness = (
        dict(summary_payload.get("step2_closeout_readiness") or {})
        or _build_closeout_readiness_from_summary(summary_payload, compact_summary_packs)
    )
    _pkg = (
        dict(summary_payload.get("step2_closeout_package") or {})
        or _build_closeout_package_from_summary(summary_payload, compact_summary_packs)
    )
    _audit = (
        dict(summary_payload.get("step2_freeze_audit") or {})
        or _build_freeze_audit_from_summary(summary_payload, compact_summary_packs)
    )
    _dossier = (
        dict(summary_payload.get("step3_admission_dossier") or {})
        or _build_admission_dossier_from_summary(summary_payload, compact_summary_packs)
    )
    return build_step2_closeout_verification_surface_payload(
        run_id=str(summary_payload.get("run_id") or ""),
        step2_closeout_readiness=_readiness if _readiness else None,
        step2_closeout_package=_pkg if _pkg else None,
        step2_freeze_audit=_audit if _audit else None,
        step3_admission_dossier=_dossier if _dossier else None,
        governance_handoff=dict(summary_payload.get("config_governance_handoff") or {}),
        parity_resilience_summary=dict(summary_payload.get("parity_resilience") or {}),
        phase_evidence=dict(summary_payload.get("phase_evidence") or {}),
    )


def _build_freeze_seal_from_summary(
    summary_payload: dict[str, Any],
    compact_summary_packs: list[dict[str, Any]],
) -> dict[str, Any]:
    """Build Step 2 freeze seal from a historical run summary payload."""
    try:
        from gas_calibrator.v2.core.step2_freeze_seal_builder import build_step2_freeze_seal
        _readiness = (
            dict(summary_payload.get("step2_closeout_readiness") or {})
            or _build_closeout_readiness_from_summary(summary_payload, compact_summary_packs)
        )
        _pkg = (
            dict(summary_payload.get("step2_closeout_package") or {})
            or _build_closeout_package_from_summary(summary_payload, compact_summary_packs)
        )
        _audit = (
            dict(summary_payload.get("step2_freeze_audit") or {})
            or _build_freeze_audit_from_summary(summary_payload, compact_summary_packs)
        )
        _dossier = (
            dict(summary_payload.get("step3_admission_dossier") or {})
            or _build_admission_dossier_from_summary(summary_payload, compact_summary_packs)
        )
        _verification = (
            dict(summary_payload.get("step2_closeout_verification") or {})
            or _build_closeout_verification_from_summary(summary_payload, compact_summary_packs)
        )
        seal = build_step2_freeze_seal(
            run_id=str(summary_payload.get("run_id") or ""),
            step2_closeout_readiness=_readiness if _readiness else None,
            step2_closeout_package=_pkg if _pkg else None,
            step2_freeze_audit=_audit if _audit else None,
            step3_admission_dossier=_dossier if _dossier else None,
            step2_closeout_verification=_verification if _verification else None,
        )
        seal["freeze_seal_source"] = "rebuilt"
        return seal
    except Exception:
        from gas_calibrator.v2.core.step2_freeze_seal_builder import build_freeze_seal_fallback
        fb = build_freeze_seal_fallback()
        fb["freeze_seal_source"] = "fallback"
        return fb


def _build_final_closure_matrix_from_summary(
    summary_payload: dict[str, Any],
    compact_summary_packs: list[dict[str, Any]],
) -> dict[str, Any]:
    """Build Step 2 final closure matrix from a historical run summary payload."""
    _readiness = (
        dict(summary_payload.get("step2_closeout_readiness") or {})
        or _build_closeout_readiness_from_summary(summary_payload, compact_summary_packs)
    )
    _pkg = (
        dict(summary_payload.get("step2_closeout_package") or {})
        or _build_closeout_package_from_summary(summary_payload, compact_summary_packs)
    )
    _audit = (
        dict(summary_payload.get("step2_freeze_audit") or {})
        or _build_freeze_audit_from_summary(summary_payload, compact_summary_packs)
    )
    _dossier = (
        dict(summary_payload.get("step3_admission_dossier") or {})
        or _build_admission_dossier_from_summary(summary_payload, compact_summary_packs)
    )
    _seal = (
        dict(summary_payload.get("step2_freeze_seal") or {})
        or _build_freeze_seal_from_summary(summary_payload, compact_summary_packs)
    )
    return build_step2_final_closure_matrix_surface_payload(
        run_id=str(summary_payload.get("run_id") or ""),
        step2_closeout_readiness=_readiness if _readiness else None,
        step2_closeout_package=_pkg if _pkg else None,
        step2_freeze_audit=_audit if _audit else None,
        step3_admission_dossier=_dossier if _dossier else None,
        step2_freeze_seal=_seal if _seal else None,
        surface_results=True,
        surface_reports=True,
        surface_historical=True,
        surface_review_index=False,
    )


def _build_operation_report(
    *,
    command: str,
    run_dirs: list[Path],
    dry_run: bool,
    rollup_scope: str,
) -> dict[str, Any]:
    runs = [
        _build_run_report(run_dir, operation=command, dry_run=dry_run)
        for run_dir in run_dirs
    ]
    compatibility_rollup = build_artifact_compatibility_rollup(
        run_reports=runs,
        rollup_scope=rollup_scope,
        generated_by_tool=HISTORICAL_ARTIFACT_ROLLUP_TOOL,
    )
    readiness_status_counts = _summarize_counts(runs, "readiness_status")
    pre_run_gate_status_counts = _summarize_counts(runs, "pre_run_gate_status")
    recognition_scope_rollup = {
        "schema_version": "step2-recognition-scope-batch-rollup-v1",
        "generated_by_tool": HISTORICAL_ARTIFACT_ROLLUP_TOOL,
        "generated_at": str(compatibility_rollup.get("generated_at") or ""),
        "rollup_scope": rollup_scope,
        "parent_run_count": len(runs),
        "canonical_direct_count": int(
            sum(1 for row in runs if bool(row.get("canonical_direct", False)))
        ),
        "compatibility_adapter_count": int(
            sum(1 for row in runs if bool(row.get("compatibility_adapter", False)))
        ),
        "readiness_status_counts": readiness_status_counts,
        "pre_run_gate_status_counts": pre_run_gate_status_counts,
        "summary_lines": [
            "pre-run gate: " + " | ".join(f"{key} {value}" for key, value in pre_run_gate_status_counts.items()),
            f"{_ROLLUP_LABELS['scope_run_count']}：{len(runs)}",
            f"{_ROLLUP_LABELS['canonical_direct']}：{int(sum(1 for row in runs if bool(row.get('canonical_direct', False))))}",
            f"{_ROLLUP_LABELS['compatibility_adapter']}：{int(sum(1 for row in runs if bool(row.get('compatibility_adapter', False))))}",
            f"{_ROLLUP_LABELS['readiness_status']}：" + " | ".join(f"{key} {value}" for key, value in readiness_status_counts.items()),
        ],
    }
    return {
        "operation": command,
        "run_count": len(runs),
        "target_mode": "batch" if len(runs) != 1 else "single",
        "dry_run": bool(dry_run),
        "index_schema_version": str(
            compatibility_rollup.get("index_schema_version") or ARTIFACT_COMPATIBILITY_INDEX_SCHEMA_VERSION
        ),
        "generated_at": str(compatibility_rollup.get("generated_at") or ""),
        "generated_by_tool": str(
            compatibility_rollup.get("generated_by_tool") or HISTORICAL_ARTIFACT_ROLLUP_TOOL
        ),
        "rollup_scope": str(compatibility_rollup.get("rollup_scope") or rollup_scope),
        "parent_run_count": int(compatibility_rollup.get("parent_run_count") or len(runs)),
        "artifact_count": int(compatibility_rollup.get("artifact_count") or 0),
        "compatible_run_count": int(compatibility_rollup.get("compatible_run_count") or 0),
        "legacy_run_count": int(compatibility_rollup.get("legacy_run_count") or 0),
        "primary_evidence_rewritten": False,
        "reader_mode_counts": _summarize_counts(runs, "current_reader_mode"),
        "compatibility_status_counts": _summarize_counts(runs, "compatibility_status"),
        "pre_run_gate_status_counts": pre_run_gate_status_counts,
        "ready_for_readiness_mapping_count": sum(
            1 for row in runs if bool(row.get("ready_for_readiness_mapping", False))
        ),
        "regenerate_recommended_count": sum(
            1 for row in runs if bool(row.get("regenerate_recommended", False))
        ),
        "linked_surface_visibility": list(compatibility_rollup.get("linked_surface_visibility") or []),
        "summary_lines": list(compatibility_rollup.get("summary_lines") or []),
        "detail_lines": list(compatibility_rollup.get("detail_lines") or []),
        "recognition_scope_rollup": recognition_scope_rollup,
        "compatibility_rollup": compatibility_rollup,
        "runs": runs,
    }


def _emit_json_report(report: dict[str, Any], *, output_path: str | None) -> None:
    if output_path:
        destination = Path(output_path).resolve()
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(f"[historical-artifacts] summary_path={destination}", flush=True)
        return
    print(json.dumps(report, ensure_ascii=False), flush=True)


def main(argv: list[str] | None = None) -> int:
    args = create_argument_parser().parse_args(argv)
    run_dir_args = list(args.run_dir or [])
    root_dir_args = list(args.root_dir or [])
    run_dirs = _collect_run_dirs(
        run_dirs=run_dir_args,
        root_dirs=root_dir_args,
    )
    if not run_dirs:
        raise SystemExit("No historical run directories found. Provide --run-dir or --root-dir.")
    if args.command == "export-summary" and not args.output:
        raise SystemExit("--output is required for export-summary")
    for line in build_step2_historical_cli_lines(
        operation=str(args.command),
        run_count=len(run_dirs),
        dry_run=bool(args.dry_run),
    ):
        print(line, flush=True)
    report = _build_operation_report(
        command=str(args.command),
        run_dirs=run_dirs,
        dry_run=bool(args.dry_run),
        rollup_scope=_resolve_rollup_scope(
            run_dirs=run_dir_args,
            root_dirs=root_dir_args,
            run_count=len(run_dirs),
        ),
    )
    _emit_json_report(report, output_path=str(args.output) if args.output else None)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main(sys.argv[1:]))
