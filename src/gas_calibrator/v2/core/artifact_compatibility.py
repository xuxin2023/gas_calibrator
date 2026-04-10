from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
from typing import Any, Iterable

from .artifact_catalog import KNOWN_REPORT_ARTIFACTS, infer_artifact_identity, merge_role_catalog
from .controlled_state_machine_profile import (
    STATE_TRANSITION_EVIDENCE_FILENAME,
    STATE_TRANSITION_EVIDENCE_MARKDOWN_FILENAME,
)
from .measurement_phase_coverage import (
    MEASUREMENT_PHASE_COVERAGE_REPORT_FILENAME,
    MEASUREMENT_PHASE_COVERAGE_REPORT_MARKDOWN_FILENAME,
)
from .multi_source_stability import (
    MULTI_SOURCE_STABILITY_EVIDENCE_FILENAME,
    MULTI_SOURCE_STABILITY_EVIDENCE_MARKDOWN_FILENAME,
    SIMULATION_EVIDENCE_SIDECAR_BUNDLE_FILENAME,
)
from . import recognition_readiness_artifacts as recognition_readiness
from .reviewer_fragments_contract import (
    REVIEWER_FRAGMENTS_CONTRACT_VERSION,
    BOUNDARY_FRAGMENT_FAMILY,
    NON_CLAIM_FRAGMENT_FAMILY,
    fragment_filter_rows_to_ids,
    normalize_fragment_filter_rows,
    normalize_fragment_rows,
)


ARTIFACT_COMPATIBILITY_SCHEMA_VERSION = "step2-artifact-compatibility-v1"
ARTIFACT_COMPATIBILITY_INDEX_SCHEMA_VERSION = "step2-artifact-compatibility-index-v1"
ARTIFACT_COMPATIBILITY_BUNDLE_TOOL = (
    "gas_calibrator.v2.core.artifact_compatibility.build_artifact_compatibility_bundle"
)
HISTORICAL_ARTIFACT_ROLLUP_TOOL = "gas_calibrator.v2.scripts.historical_artifacts"

RUN_ARTIFACT_INDEX_FILENAME = "run_artifact_index.json"
RUN_ARTIFACT_INDEX_MARKDOWN_FILENAME = "run_artifact_index.md"
ARTIFACT_CONTRACT_CATALOG_FILENAME = "artifact_contract_catalog.json"
ARTIFACT_CONTRACT_CATALOG_MARKDOWN_FILENAME = "artifact_contract_catalog.md"
COMPATIBILITY_SCAN_SUMMARY_FILENAME = "compatibility_scan_summary.json"
COMPATIBILITY_SCAN_SUMMARY_MARKDOWN_FILENAME = "compatibility_scan_summary.md"
REINDEX_MANIFEST_FILENAME = "reindex_manifest.json"
REINDEX_MANIFEST_MARKDOWN_FILENAME = "reindex_manifest.md"

COMPATIBILITY_BUNDLE_DEFINITIONS: dict[str, dict[str, str]] = {
    "run_artifact_index": {
        "filename": RUN_ARTIFACT_INDEX_FILENAME,
        "markdown_filename": RUN_ARTIFACT_INDEX_MARKDOWN_FILENAME,
        "json_role": "execution_summary",
        "markdown_role": "execution_summary",
    },
    "artifact_contract_catalog": {
        "filename": ARTIFACT_CONTRACT_CATALOG_FILENAME,
        "markdown_filename": ARTIFACT_CONTRACT_CATALOG_MARKDOWN_FILENAME,
        "json_role": "diagnostic_analysis",
        "markdown_role": "formal_analysis",
    },
    "compatibility_scan_summary": {
        "filename": COMPATIBILITY_SCAN_SUMMARY_FILENAME,
        "markdown_filename": COMPATIBILITY_SCAN_SUMMARY_MARKDOWN_FILENAME,
        "json_role": "diagnostic_analysis",
        "markdown_role": "formal_analysis",
    },
    "reindex_manifest": {
        "filename": REINDEX_MANIFEST_FILENAME,
        "markdown_filename": REINDEX_MANIFEST_MARKDOWN_FILENAME,
        "json_role": "execution_summary",
        "markdown_role": "execution_summary",
    },
}

REGENERABLE_SIDECAR_FILENAMES = frozenset(
    {
        definition["filename"]
        for definition in COMPATIBILITY_BUNDLE_DEFINITIONS.values()
    }
    | {
        definition["markdown_filename"]
        for definition in COMPATIBILITY_BUNDLE_DEFINITIONS.values()
    }
)

CANONICAL_SURFACE_FILENAMES = frozenset(
    {
        MULTI_SOURCE_STABILITY_EVIDENCE_FILENAME,
        MULTI_SOURCE_STABILITY_EVIDENCE_MARKDOWN_FILENAME,
        STATE_TRANSITION_EVIDENCE_FILENAME,
        STATE_TRANSITION_EVIDENCE_MARKDOWN_FILENAME,
        SIMULATION_EVIDENCE_SIDECAR_BUNDLE_FILENAME,
        MEASUREMENT_PHASE_COVERAGE_REPORT_FILENAME,
        MEASUREMENT_PHASE_COVERAGE_REPORT_MARKDOWN_FILENAME,
        recognition_readiness.SCOPE_READINESS_SUMMARY_FILENAME,
        recognition_readiness.SCOPE_READINESS_SUMMARY_MARKDOWN_FILENAME,
        recognition_readiness.CERTIFICATE_READINESS_SUMMARY_FILENAME,
        recognition_readiness.CERTIFICATE_READINESS_SUMMARY_MARKDOWN_FILENAME,
        recognition_readiness.UNCERTAINTY_METHOD_READINESS_SUMMARY_FILENAME,
        recognition_readiness.UNCERTAINTY_METHOD_READINESS_SUMMARY_MARKDOWN_FILENAME,
        recognition_readiness.AUDIT_READINESS_DIGEST_FILENAME,
        recognition_readiness.AUDIT_READINESS_DIGEST_MARKDOWN_FILENAME,
    }
)

PRIMARY_READER_FILENAMES = frozenset({"summary.json", "manifest.json", "results.json"})

COMPATIBILITY_STATUS_LABELS = {
    "canonical_current": "当前 canonical 工件",
    "compatibility_read": "兼容读取",
    "missing_regenerable": "缺少可再生成 sidecar",
    "missing_primary": "缺少主链工件",
    "unclassified_observed": "已发现未分类工件",
}

READER_MODE_LABELS = {
    "canonical_direct": "canonical contract 直读",
    "compatibility_adapter": "compatibility adapter 兼容读取",
    "scan_only": "仅扫描发现",
    "canonical_index": "canonical compatibility sidecar",
    "regenerate_sidecar": "待再生成 sidecar",
    "observed_only": "仅发现文件",
}

_SCAN_ANCHOR_ID = "artifact-compatibility-scan"
_SCAN_ANCHOR_LABEL = "Artifact compatibility / reindex"


def load_or_build_artifact_compatibility_payloads(
    run_dir: Path,
    *,
    summary: dict[str, Any] | None = None,
    manifest: dict[str, Any] | None = None,
    results: dict[str, Any] | None = None,
    output_files: Iterable[Any] | None = None,
    role_catalog: dict[str, Any] | None = None,
) -> dict[str, dict[str, Any]]:
    run_dir = Path(run_dir)
    loaded: dict[str, dict[str, Any]] = {}
    for artifact_key, definition in COMPATIBILITY_BUNDLE_DEFINITIONS.items():
        payload = _load_json_dict(run_dir / definition["filename"])
        if payload:
            loaded[artifact_key] = payload
    if len(loaded) == len(COMPATIBILITY_BUNDLE_DEFINITIONS):
        return _normalize_artifact_compatibility_payloads(loaded)
    bundle = build_artifact_compatibility_bundle(
        run_dir,
        summary=summary,
        manifest=manifest,
        results=results,
        output_files=output_files,
        role_catalog=role_catalog,
    )
    return _normalize_artifact_compatibility_payloads(
        {
            artifact_key: dict(bundle_item.get("raw") or {})
            for artifact_key, bundle_item in bundle.items()
        }
    )


def build_artifact_compatibility_overview(
    *,
    run_artifact_index: dict[str, Any] | None,
    artifact_contract_catalog: dict[str, Any] | None,
    compatibility_scan_summary: dict[str, Any] | None,
    reindex_manifest: dict[str, Any] | None,
) -> dict[str, Any]:
    run_artifact_index = dict(run_artifact_index or {})
    artifact_contract_catalog = dict(artifact_contract_catalog or {})
    compatibility_scan_summary = dict(compatibility_scan_summary or {})
    reindex_manifest = dict(reindex_manifest or {})
    entries = [
        dict(entry)
        for entry in list(run_artifact_index.get("entries") or [])
        if isinstance(entry, dict)
    ]
    contract_rows = [
        dict(row)
        for row in list(artifact_contract_catalog.get("contract_rows") or [])
        if isinstance(row, dict)
    ]
    schema_or_contract_version_counts = _count_by_key(entries, "schema_or_contract_version")
    current_reader_mode = str(
        compatibility_scan_summary.get("current_reader_mode")
        or run_artifact_index.get("current_reader_mode")
        or reindex_manifest.get("current_reader_mode")
        or ""
    ).strip()
    current_reader_mode_display = str(
        compatibility_scan_summary.get("current_reader_mode_display")
        or run_artifact_index.get("current_reader_mode_display")
        or reindex_manifest.get("current_reader_mode_display")
        or _display_reader_mode(current_reader_mode)
    ).strip() or "--"
    compatibility_status = str(
        compatibility_scan_summary.get("compatibility_status")
        or run_artifact_index.get("compatibility_status")
        or reindex_manifest.get("compatibility_status")
        or ""
    ).strip()
    compatibility_status_display = str(
        compatibility_scan_summary.get("compatibility_status_display")
        or run_artifact_index.get("compatibility_status_display")
        or reindex_manifest.get("compatibility_status_display")
        or _display_compatibility_status(compatibility_status)
    ).strip() or "--"
    regenerate_recommended = bool(
        compatibility_scan_summary.get(
            "regenerate_recommended",
            run_artifact_index.get(
                "regenerate_recommended",
                reindex_manifest.get("regenerate_recommended", False),
            ),
        )
    )
    regenerate_scope = str(
        compatibility_scan_summary.get("regenerate_scope")
        or run_artifact_index.get("regenerate_scope")
        or reindex_manifest.get("regenerate_scope")
        or "reviewer_index_sidecar_only"
    ).strip()
    primary_evidence_rewritten = bool(
        compatibility_scan_summary.get(
            "primary_evidence_rewritten",
            run_artifact_index.get(
                "primary_evidence_rewritten",
                reindex_manifest.get("primary_evidence_rewritten", False),
            ),
        )
    )
    boundary_digest = str(
        compatibility_scan_summary.get("boundary_digest")
        or run_artifact_index.get("boundary_digest")
        or artifact_contract_catalog.get("boundary_digest")
        or reindex_manifest.get("boundary_digest")
        or ""
    ).strip()
    non_claim_digest = str(
        compatibility_scan_summary.get("non_claim_digest")
        or run_artifact_index.get("non_claim_digest")
        or artifact_contract_catalog.get("non_claim_digest")
        or reindex_manifest.get("non_claim_digest")
        or ""
    ).strip()
    observed_contract_versions = sorted(
        version
        for version in schema_or_contract_version_counts
        if str(version or "").strip()
    )
    observed_contract_version_summary = _count_summary(schema_or_contract_version_counts)
    linked_surface_visibility = _collect_linked_surface_visibility(
        entries,
        compatibility_scan_summary.get("linked_surface_visibility"),
        run_artifact_index.get("linked_surface_visibility"),
        artifact_contract_catalog.get("linked_surface_visibility"),
        reindex_manifest.get("linked_surface_visibility"),
    )
    generated_at = _resolve_generated_at(
        compatibility_scan_summary.get("generated_at"),
        run_artifact_index.get("generated_at"),
        artifact_contract_catalog.get("generated_at"),
        reindex_manifest.get("generated_at"),
    )
    compatibility_rollup = build_artifact_compatibility_rollup(
        run_reports=[
            {
                "run_id": str(
                    compatibility_scan_summary.get("run_id")
                    or run_artifact_index.get("run_id")
                    or artifact_contract_catalog.get("run_id")
                    or reindex_manifest.get("run_id")
                    or ""
                ).strip(),
                "run_dir": str(
                    compatibility_scan_summary.get("run_dir")
                    or run_artifact_index.get("run_dir")
                    or artifact_contract_catalog.get("run_dir")
                    or reindex_manifest.get("run_dir")
                    or ""
                ).strip(),
                "current_reader_mode": current_reader_mode,
                "compatibility_status": compatibility_status,
                "regenerate_recommended": regenerate_recommended,
                "canonical_direct": current_reader_mode == "canonical_direct",
                "compatibility_adapter": current_reader_mode == "compatibility_adapter",
                "artifact_count": len(entries),
                "contract_row_count": len(contract_rows),
                "linked_surface_visibility": linked_surface_visibility,
                "boundary_digest": boundary_digest,
                "non_claim_digest": non_claim_digest,
                "primary_evidence_rewritten": primary_evidence_rewritten,
                "generated_at": generated_at,
            }
        ],
        rollup_scope="run-dir",
        generated_by_tool=str(
            compatibility_scan_summary.get("generated_by_tool")
            or run_artifact_index.get("generated_by_tool")
            or artifact_contract_catalog.get("generated_by_tool")
            or reindex_manifest.get("generated_by_tool")
            or ARTIFACT_COMPATIBILITY_BUNDLE_TOOL
        ).strip()
        or ARTIFACT_COMPATIBILITY_BUNDLE_TOOL,
        generated_at=generated_at,
    )
    regenerate_recommendation_display = (
        "仅重建 reviewer/index sidecar"
        if regenerate_recommended
        else "当前 compatibility sidecar 可直接复用"
    )
    non_primary_boundary_display = "仅重建 reviewer/index sidecar，不改写 summary.json / manifest.json / results.json"
    non_primary_chain_display = "compatibility / regenerate sidecar 仍属于 non-primary evidence chain"
    summary_line = str(
        compatibility_scan_summary.get("summary")
        or run_artifact_index.get("summary")
        or compatibility_scan_summary.get("summary_line")
        or ""
    ).strip()
    return {
        "compatibility_schema_version": ARTIFACT_COMPATIBILITY_SCHEMA_VERSION,
        "index_schema_version": ARTIFACT_COMPATIBILITY_INDEX_SCHEMA_VERSION,
        "generated_at": generated_at,
        "generated_by_tool": ARTIFACT_COMPATIBILITY_BUNDLE_TOOL,
        "observed_artifact_count": len(entries),
        "contract_row_count": len(contract_rows),
        "schema_or_contract_version_counts": schema_or_contract_version_counts,
        "observed_contract_versions": observed_contract_versions,
        "observed_contract_version_summary": observed_contract_version_summary,
        "schema_contract_summary_display": (
            f"compatibility bundle {ARTIFACT_COMPATIBILITY_SCHEMA_VERSION} | observed {observed_contract_version_summary}"
        ),
        "current_reader_mode": current_reader_mode,
        "current_reader_mode_display": current_reader_mode_display,
        "compatibility_status": compatibility_status,
        "compatibility_status_display": compatibility_status_display,
        "regenerate_recommended": regenerate_recommended,
        "regenerate_scope": regenerate_scope,
        "regenerate_recommendation_display": regenerate_recommendation_display,
        "primary_evidence_rewritten": primary_evidence_rewritten,
        "linked_surface_visibility": linked_surface_visibility,
        "non_primary_boundary_display": non_primary_boundary_display,
        "non_primary_chain_display": non_primary_chain_display,
        "boundary_digest": boundary_digest,
        "non_claim_digest": non_claim_digest,
        "compatibility_rollup": compatibility_rollup,
        "rollup_scope": str(compatibility_rollup.get("rollup_scope") or "run-dir"),
        "rollup_summary_display": str(
            compatibility_rollup.get("rollup_summary_display")
            or compatibility_rollup.get("summary")
            or ""
        ).strip(),
        "summary": summary_line,
        "summary_lines": [
            f"合同/Schema: compatibility bundle {ARTIFACT_COMPATIBILITY_SCHEMA_VERSION} | observed {observed_contract_version_summary}",
            f"读取方式: {current_reader_mode_display}",
            f"兼容状态: {compatibility_status_display}",
            f"建议动作: {regenerate_recommendation_display}",
            f"主证据改写: {str(primary_evidence_rewritten).lower()}",
        ],
        "detail_lines": [
            f"边界摘要: {boundary_digest or '--'}",
            f"非主张摘要: {non_claim_digest or '--'}",
            f"non-primary 边界: {non_primary_boundary_display}",
            f"证据链声明: {non_primary_chain_display}",
        ],
    }


def _normalize_artifact_compatibility_payloads(
    payloads: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    normalized = {
        str(key): dict(value or {})
        for key, value in dict(payloads or {}).items()
    }
    overview = build_artifact_compatibility_overview(
        run_artifact_index=normalized.get("run_artifact_index"),
        artifact_contract_catalog=normalized.get("artifact_contract_catalog"),
        compatibility_scan_summary=normalized.get("compatibility_scan_summary"),
        reindex_manifest=normalized.get("reindex_manifest"),
    )
    for payload in normalized.values():
        payload["compatibility_overview"] = dict(overview)
        payload["schema_or_contract_version_summary"] = str(
            overview.get("observed_contract_version_summary") or "--"
        )
        payload["schema_or_contract_version_counts"] = dict(
            overview.get("schema_or_contract_version_counts") or {}
        )
    compatibility_scan_summary = dict(normalized.get("compatibility_scan_summary") or {})
    if compatibility_scan_summary:
        digest = dict(compatibility_scan_summary.get("digest") or {})
        digest.update(
            {
                "schema_contract_summary": str(overview.get("schema_contract_summary_display") or ""),
                "regenerate_summary": str(overview.get("regenerate_recommendation_display") or ""),
                "boundary_summary": str(overview.get("boundary_digest") or ""),
                "non_claim_summary": str(overview.get("non_claim_digest") or ""),
                "non_primary_chain_summary": str(overview.get("non_primary_chain_display") or ""),
            }
        )
        review_surface = dict(compatibility_scan_summary.get("review_surface") or {})
        review_surface["summary_lines"] = _merge_unique_lines(
            list(review_surface.get("summary_lines") or []),
            list(overview.get("summary_lines") or []),
        )
        review_surface["detail_lines"] = _merge_unique_lines(
            list(review_surface.get("detail_lines") or []),
            list(overview.get("detail_lines") or []),
        )
        reviewer_note = str(review_surface.get("reviewer_note") or "").strip()
        extra_note = str(overview.get("non_primary_boundary_display") or "").strip()
        if extra_note and extra_note not in reviewer_note:
            review_surface["reviewer_note"] = " | ".join(
                part for part in (reviewer_note, extra_note) if str(part).strip()
            )
        compatibility_scan_summary["digest"] = digest
        compatibility_scan_summary["review_surface"] = review_surface
        compatibility_scan_summary["compatibility_overview"] = dict(overview)
        normalized["compatibility_scan_summary"] = compatibility_scan_summary
    return {artifact_key: dict(bundle_item or {}) for artifact_key, bundle_item in normalized.items()}


def build_artifact_compatibility_bundle(
    run_dir: Path,
    *,
    summary: dict[str, Any] | None = None,
    manifest: dict[str, Any] | None = None,
    results: dict[str, Any] | None = None,
    output_files: Iterable[Any] | None = None,
    role_catalog: dict[str, Any] | None = None,
) -> dict[str, dict[str, Any]]:
    run_dir = Path(run_dir)
    merged_role_catalog = merge_role_catalog(role_catalog)
    run_id = _resolve_run_id(run_dir, summary=summary, manifest=manifest, results=results)
    candidate_paths = _collect_candidate_paths(
        run_dir,
        summary=summary,
        manifest=manifest,
        results=results,
        output_files=output_files,
    )
    base_entries = [
        _build_base_entry(path, run_dir=run_dir, run_id=run_id, role_catalog=merged_role_catalog)
        for path in candidate_paths
    ]
    base_entries_by_key = {
        str(entry.get("artifact_key") or ""): dict(entry)
        for entry in base_entries
        if str(entry.get("artifact_key") or "").strip()
    }
    canonical_surface_present = any(
        bool(entry.get("present_on_disk", False)) and str(entry.get("artifact_name") or "") in CANONICAL_SURFACE_FILENAMES
        for entry in base_entries
    )
    primary_reader_present = any(
        bool(entry.get("present_on_disk", False)) and str(entry.get("artifact_name") or "") in PRIMARY_READER_FILENAMES
        for entry in base_entries
    )
    compatibility_sidecars_present = all(
        any(
            bool(entry.get("present_on_disk", False))
            and str(entry.get("artifact_name") or "") == definition["filename"]
            for entry in base_entries
        )
        for definition in COMPATIBILITY_BUNDLE_DEFINITIONS.values()
    )
    if canonical_surface_present:
        current_reader_mode = "canonical_direct"
    elif primary_reader_present:
        current_reader_mode = "compatibility_adapter"
    else:
        current_reader_mode = "scan_only"
    entries = [
        _finalize_entry(
            entry,
            run_mode=current_reader_mode,
            pair_lookup=base_entries_by_key,
        )
        for entry in base_entries
    ]
    status_counts = _count_by_key(entries, "compatibility_status")
    version_source_counts = _count_by_key(entries, "schema_version_source")
    canonical_reader_count = sum(1 for entry in entries if bool(entry.get("canonical_reader_available", False)))
    compatibility_read_count = int(status_counts.get("compatibility_read", 0) or 0)
    missing_regenerable_count = int(status_counts.get("missing_regenerable", 0) or 0)
    compatibility_status = (
        "compatibility_read"
        if current_reader_mode != "canonical_direct" or compatibility_read_count > 0 or missing_regenerable_count > 0
        else "canonical_current"
    )
    regenerate_recommended = bool(
        current_reader_mode != "canonical_direct" or not compatibility_sidecars_present or missing_regenerable_count > 0
    )
    linked_surface_visibility = ["results", "review_center", "workbench"]
    boundary_payload = _compatibility_boundary_payload()
    non_claim_payload = _compatibility_non_claim_payload()
    summary_line = _scan_summary_line(
        current_reader_mode=current_reader_mode,
        compatibility_status=compatibility_status,
        regenerate_recommended=regenerate_recommended,
    )
    detail_lines = _scan_detail_lines(
        current_reader_mode=current_reader_mode,
        compatibility_status=compatibility_status,
        status_counts=status_counts,
        version_source_counts=version_source_counts,
        canonical_reader_count=canonical_reader_count,
        total_entries=len(entries),
        regenerate_recommended=regenerate_recommended,
    )

    run_artifact_index = _build_run_artifact_index_payload(
        run_dir=run_dir,
        run_id=run_id,
        current_reader_mode=current_reader_mode,
        compatibility_status=compatibility_status,
        status_counts=status_counts,
        version_source_counts=version_source_counts,
        canonical_reader_count=canonical_reader_count,
        regenerate_recommended=regenerate_recommended,
        linked_surface_visibility=linked_surface_visibility,
        entries=entries,
        summary_line=summary_line,
        detail_lines=detail_lines,
        boundary_payload=boundary_payload,
        non_claim_payload=non_claim_payload,
    )
    contract_catalog = _build_contract_catalog_payload(
        run_dir=run_dir,
        run_id=run_id,
        current_reader_mode=current_reader_mode,
        compatibility_status=compatibility_status,
        compatibility_read_count=compatibility_read_count,
        missing_regenerable_count=missing_regenerable_count,
        regenerate_recommended=regenerate_recommended,
        boundary_payload=boundary_payload,
        non_claim_payload=non_claim_payload,
        contract_rows=_build_contract_rows(entries),
    )
    compatibility_scan_summary = _build_scan_summary_payload(
        run_dir=run_dir,
        run_id=run_id,
        current_reader_mode=current_reader_mode,
        compatibility_status=compatibility_status,
        status_counts=status_counts,
        version_source_counts=version_source_counts,
        canonical_reader_count=canonical_reader_count,
        regenerate_recommended=regenerate_recommended,
        linked_surface_visibility=linked_surface_visibility,
        summary_line=summary_line,
        detail_lines=detail_lines,
        boundary_payload=boundary_payload,
        non_claim_payload=non_claim_payload,
    )
    reindex_manifest = _build_reindex_manifest_payload(
        run_dir=run_dir,
        run_id=run_id,
        current_reader_mode=current_reader_mode,
        compatibility_status=compatibility_status,
        regenerate_recommended=regenerate_recommended,
        linked_surface_visibility=linked_surface_visibility,
        boundary_payload=boundary_payload,
        non_claim_payload=non_claim_payload,
    )
    normalized_raw_payloads = _normalize_artifact_compatibility_payloads(
        {
            "run_artifact_index": run_artifact_index,
            "artifact_contract_catalog": contract_catalog,
            "compatibility_scan_summary": compatibility_scan_summary,
            "reindex_manifest": reindex_manifest,
        }
    )
    return {
        "run_artifact_index": {
            "raw": dict(normalized_raw_payloads.get("run_artifact_index") or {}),
            "markdown": _build_run_artifact_index_markdown(
                dict(normalized_raw_payloads.get("run_artifact_index") or {})
            ),
            **COMPATIBILITY_BUNDLE_DEFINITIONS["run_artifact_index"],
        },
        "artifact_contract_catalog": {
            "raw": dict(normalized_raw_payloads.get("artifact_contract_catalog") or {}),
            "markdown": _build_contract_catalog_markdown(
                dict(normalized_raw_payloads.get("artifact_contract_catalog") or {})
            ),
            **COMPATIBILITY_BUNDLE_DEFINITIONS["artifact_contract_catalog"],
        },
        "compatibility_scan_summary": {
            "raw": dict(normalized_raw_payloads.get("compatibility_scan_summary") or {}),
            "markdown": _build_scan_summary_markdown(
                dict(normalized_raw_payloads.get("compatibility_scan_summary") or {})
            ),
            **COMPATIBILITY_BUNDLE_DEFINITIONS["compatibility_scan_summary"],
        },
        "reindex_manifest": {
            "raw": dict(normalized_raw_payloads.get("reindex_manifest") or {}),
            "markdown": _build_reindex_manifest_markdown(
                dict(normalized_raw_payloads.get("reindex_manifest") or {})
            ),
            **COMPATIBILITY_BUNDLE_DEFINITIONS["reindex_manifest"],
        },
    }


def write_artifact_compatibility_sidecars(
    run_dir: Path,
    bundle: dict[str, dict[str, Any]],
) -> dict[str, tuple[Path, Path]]:
    run_dir = Path(run_dir)
    written_paths: dict[str, tuple[Path, Path]] = {}
    for artifact_key, bundle_item in bundle.items():
        definition = COMPATIBILITY_BUNDLE_DEFINITIONS.get(str(artifact_key), {})
        json_path = run_dir / str(definition.get("filename") or f"{artifact_key}.json")
        markdown_path = run_dir / str(definition.get("markdown_filename") or f"{artifact_key}.md")
        json_path.write_text(
            json.dumps(dict(bundle_item.get("raw") or {}), ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        markdown_path.write_text(str(bundle_item.get("markdown") or ""), encoding="utf-8")
        written_paths[str(artifact_key)] = (json_path, markdown_path)
    return written_paths


def regenerate_artifact_compatibility_sidecars(run_dir: Path) -> dict[str, Any]:
    run_dir = Path(run_dir)
    summary = _load_json_dict(run_dir / "summary.json")
    manifest = _load_json_dict(run_dir / "manifest.json")
    results = _load_json_dict(run_dir / "results.json")
    output_files = list(dict(summary or {}).get("stats", {}).get("output_files", []) or [])
    role_catalog = dict(dict(manifest or {}).get("artifacts", {}) or {}).get("role_catalog", {})
    bundle = build_artifact_compatibility_bundle(
        run_dir,
        summary=summary,
        manifest=manifest,
        results=results,
        output_files=output_files,
        role_catalog=role_catalog if isinstance(role_catalog, dict) else None,
    )
    written_paths = write_artifact_compatibility_sidecars(run_dir, bundle)
    return {
        "run_dir": str(run_dir.resolve()),
        "written_paths": {
            key: {"json_path": str(paths[0]), "markdown_path": str(paths[1])}
            for key, paths in written_paths.items()
        },
        "primary_evidence_rewritten": False,
        "regenerate_scope": "reviewer_index_sidecar_only",
        "compatibility_scan_summary": dict(bundle.get("compatibility_scan_summary", {}).get("raw") or {}),
    }


def _build_run_artifact_index_payload(
    *,
    run_dir: Path,
    run_id: str,
    current_reader_mode: str,
    compatibility_status: str,
    status_counts: dict[str, int],
    version_source_counts: dict[str, int],
    canonical_reader_count: int,
    regenerate_recommended: bool,
    linked_surface_visibility: list[str],
    entries: list[dict[str, Any]],
    summary_line: str,
    detail_lines: list[str],
    boundary_payload: dict[str, Any],
    non_claim_payload: dict[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": ARTIFACT_COMPATIBILITY_SCHEMA_VERSION,
        "artifact_type": "run_artifact_index",
        "run_id": run_id,
        "run_dir": str(run_dir.resolve()),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "current_reader_mode": current_reader_mode,
        "current_reader_mode_display": _display_reader_mode(current_reader_mode),
        "compatibility_status": compatibility_status,
        "compatibility_status_display": _display_compatibility_status(compatibility_status),
        "compatibility_status_counts": status_counts,
        "schema_version_source_counts": version_source_counts,
        "canonical_reader_available_count": canonical_reader_count,
        "regenerate_recommended": regenerate_recommended,
        "linked_surface_visibility": linked_surface_visibility,
        "summary": summary_line,
        "detail_lines": detail_lines,
        "entries": entries,
        "regenerate_scope": "reviewer_index_sidecar_only",
        "primary_evidence_rewritten": False,
        "primary_evidence_preserved": True,
        "canonical_reader_available": canonical_reader_count > 0,
        "evidence_source": "simulated_protocol",
        "evidence_state": "shadow_only",
        "not_real_acceptance_evidence": True,
        "artifact_paths": _self_artifact_paths(run_dir, "run_artifact_index"),
        **boundary_payload,
        **non_claim_payload,
    }


def _build_contract_catalog_payload(
    *,
    run_dir: Path,
    run_id: str,
    current_reader_mode: str,
    compatibility_status: str,
    compatibility_read_count: int,
    missing_regenerable_count: int,
    regenerate_recommended: bool,
    boundary_payload: dict[str, Any],
    non_claim_payload: dict[str, Any],
    contract_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": ARTIFACT_COMPATIBILITY_SCHEMA_VERSION,
        "artifact_type": "artifact_contract_catalog",
        "run_id": run_id,
        "run_dir": str(run_dir.resolve()),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "current_reader_mode": current_reader_mode,
        "current_reader_mode_display": _display_reader_mode(current_reader_mode),
        "compatibility_status": compatibility_status,
        "compatibility_status_display": _display_compatibility_status(compatibility_status),
        "regenerate_recommended": regenerate_recommended,
        "summary": (
            f"contracts {len(contract_rows)} | compatibility-read {compatibility_read_count} | "
            f"missing-sidecar {missing_regenerable_count}"
        ),
        "detail_lines": [
            f"contracts: {len(contract_rows)}",
            f"compatibility-read entries: {compatibility_read_count}",
            f"missing regenerable sidecars: {missing_regenerable_count}",
        ],
        "contract_rows": contract_rows,
        "linked_surface_visibility": ["results", "review_center"],
        "regenerate_scope": "reviewer_index_sidecar_only",
        "primary_evidence_rewritten": False,
        "primary_evidence_preserved": True,
        "evidence_source": "simulated_protocol",
        "evidence_state": "shadow_only",
        "not_real_acceptance_evidence": True,
        "artifact_paths": _self_artifact_paths(run_dir, "artifact_contract_catalog"),
        **boundary_payload,
        **non_claim_payload,
    }


def _build_scan_summary_payload(
    *,
    run_dir: Path,
    run_id: str,
    current_reader_mode: str,
    compatibility_status: str,
    status_counts: dict[str, int],
    version_source_counts: dict[str, int],
    canonical_reader_count: int,
    regenerate_recommended: bool,
    linked_surface_visibility: list[str],
    summary_line: str,
    detail_lines: list[str],
    boundary_payload: dict[str, Any],
    non_claim_payload: dict[str, Any],
) -> dict[str, Any]:
    review_surface = {
        "title_text": "历史工件兼容 / 再索引",
        "summary_text": summary_line,
        "summary_lines": [
            f"读取方式: {_display_reader_mode(current_reader_mode)}",
            f"兼容状态: {_display_compatibility_status(compatibility_status)}",
            (
                "建议动作: 仅重建 reviewer/index sidecar"
                if regenerate_recommended
                else "建议动作: 当前 compatibility sidecar 可直接复用"
            ),
        ],
        "detail_lines": detail_lines,
        "reviewer_note": "再生成对象仅限 reviewer/index sidecar，不改写原始主证据。",
        "phase_filters": ["step2_tail_stage3_bridge"],
        "artifact_role_filters": ["diagnostic_analysis", "execution_summary"],
        "evidence_category_filters": ["artifact_compatibility", "reviewer_sidecar"],
        "boundary_filter_rows": [dict(item) for item in list(boundary_payload.get("boundary_filter_rows") or [])],
        "boundary_filters": list(boundary_payload.get("boundary_filters") or []),
        "non_claim_filter_rows": [dict(item) for item in list(non_claim_payload.get("non_claim_filter_rows") or [])],
        "non_claim_filters": list(non_claim_payload.get("non_claim_filters") or []),
        "evidence_source_filters": ["simulated_protocol"],
        "anchor_id": _SCAN_ANCHOR_ID,
        "anchor_label": _SCAN_ANCHOR_LABEL,
    }
    return {
        "schema_version": ARTIFACT_COMPATIBILITY_SCHEMA_VERSION,
        "artifact_type": "compatibility_scan_summary",
        "run_id": run_id,
        "run_dir": str(run_dir.resolve()),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "summary": summary_line,
        "detail_lines": detail_lines,
        "status_counts": status_counts,
        "version_source_counts": version_source_counts,
        "current_reader_mode": current_reader_mode,
        "current_reader_mode_display": _display_reader_mode(current_reader_mode),
        "compatibility_status": compatibility_status,
        "compatibility_status_display": _display_compatibility_status(compatibility_status),
        "canonical_reader_available": canonical_reader_count > 0,
        "canonical_reader_available_count": canonical_reader_count,
        "regenerate_recommended": regenerate_recommended,
        "linked_surface_visibility": linked_surface_visibility,
        "compatibility_non_claim": "compatibility / regenerate sidecar 不是 real acceptance evidence。",
        "review_surface": review_surface,
        "digest": {
            "summary": summary_line,
            "reader_mode_summary": _display_reader_mode(current_reader_mode),
            "compatibility_status_summary": _display_compatibility_status(compatibility_status),
            "status_count_summary": _count_summary(status_counts, display=_display_compatibility_status),
            "version_source_summary": _count_summary(version_source_counts),
            "regenerate_summary": (
                "建议执行 sidecar 再生成 / 再索引"
                if regenerate_recommended
                else "当前 sidecar 已就绪"
            ),
            "boundary_summary": str(boundary_payload.get("boundary_digest") or ""),
            "non_claim_summary": str(non_claim_payload.get("non_claim_digest") or ""),
        },
        "regenerate_scope": "reviewer_index_sidecar_only",
        "primary_evidence_rewritten": False,
        "primary_evidence_preserved": True,
        "evidence_source": "simulated_protocol",
        "evidence_state": "shadow_only",
        "not_real_acceptance_evidence": True,
        "artifact_paths": _self_artifact_paths(run_dir, "compatibility_scan_summary"),
        **boundary_payload,
        **non_claim_payload,
    }


def _build_reindex_manifest_payload(
    *,
    run_dir: Path,
    run_id: str,
    current_reader_mode: str,
    compatibility_status: str,
    regenerate_recommended: bool,
    linked_surface_visibility: list[str],
    boundary_payload: dict[str, Any],
    non_claim_payload: dict[str, Any],
) -> dict[str, Any]:
    detail_lines = [
        f"读取方式: {_display_reader_mode(current_reader_mode)}",
        f"兼容状态: {_display_compatibility_status(compatibility_status)}",
        "作用范围: 仅重建 reviewer/index sidecar",
        "保护边界: 不改写 summary / manifest / results 等原始主证据",
        (
            "建议: 当前旧 run 建议执行轻量 reindex/regenerate"
            if regenerate_recommended
            else "建议: 当前 sidecar 已齐备，仅在索引变化时再生成"
        ),
    ]
    return {
        "schema_version": ARTIFACT_COMPATIBILITY_SCHEMA_VERSION,
        "artifact_type": "reindex_manifest",
        "run_id": run_id,
        "run_dir": str(run_dir.resolve()),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "current_reader_mode": current_reader_mode,
        "current_reader_mode_display": _display_reader_mode(current_reader_mode),
        "compatibility_status": compatibility_status,
        "compatibility_status_display": _display_compatibility_status(compatibility_status),
        "regenerate_recommended": regenerate_recommended,
        "summary": "reindex / regenerate 仅作用于 reviewer/index sidecar",
        "detail_lines": detail_lines,
        "linked_surface_visibility": linked_surface_visibility,
        "reindex_allowed": True,
        "reindex_scope": "reviewer_index_sidecar_only",
        "regenerate_scope": "reviewer_index_sidecar_only",
        "primary_evidence_preserved": True,
        "primary_evidence_rewritten": False,
        "canonical_reader_available": True,
        "evidence_source": "simulated_protocol",
        "evidence_state": "shadow_only",
        "not_real_acceptance_evidence": True,
        "artifact_paths": _self_artifact_paths(run_dir, "reindex_manifest"),
        **boundary_payload,
        **non_claim_payload,
    }


def _build_base_entry(
    path: Path,
    *,
    run_dir: Path,
    run_id: str,
    role_catalog: dict[str, Any],
) -> dict[str, Any]:
    candidate = Path(path)
    if not candidate.is_absolute():
        candidate = run_dir / candidate
    identity = infer_artifact_identity(candidate, role_catalog=role_catalog)
    artifact_name = candidate.name
    payload = _load_json_dict(candidate) if candidate.suffix.lower() == ".json" and candidate.exists() else {}
    schema_or_contract_version, schema_version_source = _schema_version_from_payload(
        artifact_name=artifact_name,
        artifact_key=str(identity.get("artifact_key") or ""),
        payload=payload,
        present_on_disk=candidate.exists(),
    )
    return {
        "run_id": run_id,
        "run_dir": str(run_dir.resolve()),
        "artifact_name": artifact_name,
        "artifact_path": str(candidate.resolve()),
        "artifact_key": str(identity.get("artifact_key") or ""),
        "artifact_role": str(identity.get("artifact_role") or "unclassified"),
        "schema_or_contract_version": schema_or_contract_version,
        "schema_version_source": schema_version_source,
        "present_on_disk": bool(candidate.exists()),
        "payload": payload,
    }


def _finalize_entry(
    entry: dict[str, Any],
    *,
    run_mode: str,
    pair_lookup: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    artifact_name = str(entry.get("artifact_name") or "")
    artifact_key = str(entry.get("artifact_key") or "")
    base_key = artifact_key[:-9] if artifact_key.endswith("_markdown") else artifact_key
    paired = dict(pair_lookup.get(base_key) or {})
    present_on_disk = bool(entry.get("present_on_disk", False))
    schema_or_contract_version = str(entry.get("schema_or_contract_version") or "")
    schema_version_source = str(entry.get("schema_version_source") or "")
    if not schema_or_contract_version and paired:
        schema_or_contract_version = str(paired.get("schema_or_contract_version") or "")
        if schema_or_contract_version:
            schema_version_source = str(paired.get("schema_version_source") or "paired")

    regenerable_sidecar = artifact_name in REGENERABLE_SIDECAR_FILENAMES
    canonical_surface = artifact_name in CANONICAL_SURFACE_FILENAMES
    primary_reader = artifact_name in PRIMARY_READER_FILENAMES
    canonical_reader_available = bool(
        artifact_key
        or regenerable_sidecar
        or artifact_name in KNOWN_REPORT_ARTIFACTS
    )
    if not present_on_disk and regenerable_sidecar:
        compatibility_status = "missing_regenerable"
        reader_mode = "regenerate_sidecar"
    elif not present_on_disk and canonical_reader_available:
        compatibility_status = "missing_primary"
        reader_mode = "compatibility_adapter" if run_mode != "scan_only" else "observed_only"
    elif regenerable_sidecar:
        compatibility_status = "canonical_current"
        reader_mode = "canonical_index"
    elif canonical_surface and schema_or_contract_version:
        compatibility_status = "canonical_current"
        reader_mode = "canonical_direct"
    elif primary_reader or canonical_reader_available:
        compatibility_status = "compatibility_read"
        reader_mode = "compatibility_adapter" if run_mode != "scan_only" else "observed_only"
    else:
        compatibility_status = "unclassified_observed"
        reader_mode = "observed_only"

    regenerate_recommended = bool(
        compatibility_status in {"compatibility_read", "missing_regenerable"}
        or (run_mode == "compatibility_adapter" and canonical_reader_available and not regenerable_sidecar)
    )
    return {
        **{key: value for key, value in entry.items() if key != "payload"},
        "schema_or_contract_version": schema_or_contract_version,
        "schema_version_source": schema_version_source or ("missing" if not schema_or_contract_version else "explicit"),
        "compatibility_status": compatibility_status,
        "compatibility_status_display": _display_compatibility_status(compatibility_status),
        "reader_mode": reader_mode,
        "reader_mode_display": _display_reader_mode(reader_mode),
        "canonical_reader_available": canonical_reader_available,
        "regenerate_recommended": regenerate_recommended,
        "linked_surface_visibility": _surface_visibility(
            artifact_key=artifact_key,
            artifact_role=str(entry.get("artifact_role") or "unclassified"),
        ),
        "primary_evidence": not regenerable_sidecar,
        "regenerable_sidecar": regenerable_sidecar,
        "boundary_digest": _compatibility_boundary_payload().get("boundary_digest"),
        "non_claim_digest": _compatibility_non_claim_payload().get("non_claim_digest"),
        "compatibility_non_claim": "不构成 real acceptance evidence",
    }


def _build_contract_rows(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for entry in entries:
        artifact_key = str(entry.get("artifact_key") or entry.get("artifact_name") or "").strip()
        artifact_role = str(entry.get("artifact_role") or "unclassified")
        if not artifact_key:
            continue
        group = grouped.setdefault(
            (artifact_key, artifact_role),
            {
                "artifact_key": artifact_key,
                "artifact_role": artifact_role,
                "artifact_names": [],
                "artifact_paths": [],
                "schema_versions": [],
                "compatibility_statuses": [],
                "canonical_reader_available": False,
                "regenerate_recommended": False,
                "linked_surface_visibility": [],
            },
        )
        artifact_name = str(entry.get("artifact_name") or "")
        artifact_path = str(entry.get("artifact_path") or "")
        version = str(entry.get("schema_or_contract_version") or "")
        status = str(entry.get("compatibility_status") or "")
        if artifact_name and artifact_name not in group["artifact_names"]:
            group["artifact_names"].append(artifact_name)
        if artifact_path and artifact_path not in group["artifact_paths"]:
            group["artifact_paths"].append(artifact_path)
        if version and version not in group["schema_versions"]:
            group["schema_versions"].append(version)
        if status and status not in group["compatibility_statuses"]:
            group["compatibility_statuses"].append(status)
        group["canonical_reader_available"] = bool(
            group["canonical_reader_available"] or entry.get("canonical_reader_available", False)
        )
        group["regenerate_recommended"] = bool(
            group["regenerate_recommended"] or entry.get("regenerate_recommended", False)
        )
        for surface in list(entry.get("linked_surface_visibility") or []):
            text = str(surface or "").strip()
            if text and text not in group["linked_surface_visibility"]:
                group["linked_surface_visibility"].append(text)
    return sorted(
        grouped.values(),
        key=lambda item: (str(item.get("artifact_role") or ""), str(item.get("artifact_key") or "")),
    )


def _collect_candidate_paths(
    run_dir: Path,
    *,
    summary: dict[str, Any] | None,
    manifest: dict[str, Any] | None,
    results: dict[str, Any] | None,
    output_files: Iterable[Any] | None,
) -> list[Path]:
    candidates: list[Path] = []
    seen: set[str] = set()

    def _remember(value: Any) -> None:
        candidate = _coerce_candidate_path(run_dir, value)
        if candidate is None:
            return
        key = str(candidate).lower()
        if key in seen:
            return
        seen.add(key)
        candidates.append(candidate)

    for filename in REGENERABLE_SIDECAR_FILENAMES:
        _remember(run_dir / filename)
    for filename in PRIMARY_READER_FILENAMES:
        _remember(run_dir / filename)
    for filename in CANONICAL_SURFACE_FILENAMES:
        _remember(run_dir / filename)
    if run_dir.exists():
        for path in run_dir.iterdir():
            if path.is_file():
                _remember(path)
    for payload in (summary, manifest, results):
        for path in _collect_explicit_paths(payload):
            _remember(path)
    for path in list(output_files or []):
        _remember(path)
    return candidates


def _collect_explicit_paths(payload: Any) -> list[Any]:
    rows: list[Any] = []
    if isinstance(payload, dict):
        for key, value in payload.items():
            key_text = str(key or "").strip().lower()
            if key_text in {"path", "markdown_path", "report_path"}:
                rows.append(value)
                continue
            if key_text == "artifact_paths" and isinstance(value, dict):
                rows.extend(value.values())
                continue
            if key_text in {"output_files", "primary_artifact_paths", "supporting_artifact_paths", "remembered_files"}:
                rows.extend(list(value or []))
                continue
            rows.extend(_collect_explicit_paths(value))
    elif isinstance(payload, list):
        for item in payload:
            rows.extend(_collect_explicit_paths(item))
    return rows


def _coerce_candidate_path(run_dir: Path, value: Any) -> Path | None:
    text = str(value or "").strip()
    if not text:
        return None
    candidate = Path(text)
    if not candidate.is_absolute():
        candidate = run_dir / candidate
    if candidate.exists() and candidate.is_dir():
        return None
    if not candidate.exists() and not candidate.suffix:
        return None
    return candidate


def _schema_version_from_payload(
    *,
    artifact_name: str,
    artifact_key: str,
    payload: dict[str, Any],
    present_on_disk: bool,
) -> tuple[str, str]:
    for key in (
        "schema_version",
        "contract_version",
        "taxonomy_contract_version",
        "reviewer_fragments_contract_version",
    ):
        value = str(payload.get(key) or "").strip()
        if value:
            return value, "explicit"
    if artifact_name in REGENERABLE_SIDECAR_FILENAMES:
        return ARTIFACT_COMPATIBILITY_SCHEMA_VERSION, "planned" if not present_on_disk else "explicit"
    if present_on_disk and (artifact_key or artifact_name in KNOWN_REPORT_ARTIFACTS):
        return "inferred-unversioned", "inferred"
    return "", "missing"


def _resolve_run_id(
    run_dir: Path,
    *,
    summary: dict[str, Any] | None,
    manifest: dict[str, Any] | None,
    results: dict[str, Any] | None,
) -> str:
    for payload in (summary, manifest, results):
        value = str(dict(payload or {}).get("run_id") or "").strip()
        if value:
            return value
    return run_dir.name


def _count_by_key(rows: Iterable[dict[str, Any]], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        value = str(dict(row or {}).get(key) or "").strip() or "missing"
        counts[value] = int(counts.get(value, 0) or 0) + 1
    return counts


def _merge_unique_lines(base: Iterable[Any], extra: Iterable[Any]) -> list[str]:
    rows: list[str] = []
    seen: set[str] = set()
    for value in list(base or []) + list(extra or []):
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        rows.append(text)
    return rows


def _surface_visibility(*, artifact_key: str, artifact_role: str) -> list[str]:
    surfaces = ["results"]
    if artifact_role in {"execution_summary", "diagnostic_analysis", "formal_analysis"}:
        surfaces.append("review_center")
    if artifact_key in {
        "run_artifact_index",
        "artifact_contract_catalog",
        "compatibility_scan_summary",
        "reindex_manifest",
        "multi_source_stability_evidence",
        "state_transition_evidence",
        "simulation_evidence_sidecar_bundle",
        "measurement_phase_coverage_report",
        "scope_readiness_summary",
        "certificate_readiness_summary",
        "uncertainty_method_readiness_summary",
        "audit_readiness_digest",
    }:
        if "review_center" not in surfaces:
            surfaces.append("review_center")
        surfaces.append("workbench")
    return surfaces


def _self_artifact_paths(run_dir: Path, artifact_key: str) -> dict[str, str]:
    definition = COMPATIBILITY_BUNDLE_DEFINITIONS.get(str(artifact_key), {})
    if not definition:
        return {}
    return {
        str(artifact_key): str((run_dir / str(definition.get("filename") or "")).resolve()),
        f"{artifact_key}_markdown": str((run_dir / str(definition.get("markdown_filename") or "")).resolve()),
    }


def _compatibility_boundary_payload() -> dict[str, Any]:
    fragment_keys = [
        "step2_tail_stage3_bridge",
        "step2_reviewer_readiness_only",
        "simulation_offline_headless_only",
        "file_artifact_first_reviewer_evidence",
        "not_real_acceptance_boundary",
        "does_not_modify_live_sampling_gate",
    ]
    boundary_fragments = normalize_fragment_rows(BOUNDARY_FRAGMENT_FAMILY, fragment_keys, display_locale="zh_CN")
    boundary_filter_rows = normalize_fragment_filter_rows(BOUNDARY_FRAGMENT_FAMILY, fragment_keys, display_locale="zh_CN")
    boundary_digest = " | ".join(
        [
            "Step 2 收尾 / Step 3 桥接边界",
            "仅用于 reviewer readiness / compatibility",
            "仅限 simulation / offline / headless",
            "兼容读取与再生成只服务 reviewer/index sidecar",
            "不改写原始主证据",
        ]
    )
    return {
        "boundary_fragments": boundary_fragments,
        "boundary_fragment_keys": fragment_keys,
        "boundary_statements": [
            "compatibility / reindex / regenerate 仅重建 reviewer/index sidecar",
            "不改写原始主证据",
        ],
        "boundary_filter_rows": boundary_filter_rows,
        "boundary_filters": fragment_filter_rows_to_ids(boundary_filter_rows),
        "boundary_digest": boundary_digest,
        "reviewer_fragments_contract_version": REVIEWER_FRAGMENTS_CONTRACT_VERSION,
    }


def _compatibility_non_claim_payload() -> dict[str, Any]:
    fragment_keys = [
        "simulation_synthetic_reviewer_evidence_only",
        "not_real_acceptance",
        "not_live_gate",
        "not_release_gate",
        "not_live_acceptance",
        "not_compliance_claim",
        "not_accreditation_claim",
    ]
    non_claim_fragments = normalize_fragment_rows(NON_CLAIM_FRAGMENT_FAMILY, fragment_keys, display_locale="zh_CN")
    non_claim_filter_rows = normalize_fragment_filter_rows(NON_CLAIM_FRAGMENT_FAMILY, fragment_keys, display_locale="zh_CN")
    non_claim_digest = " | ".join(
        [
            "仅为 simulation / synthetic reviewer evidence",
            "不是 real acceptance",
            "不是 live gate / release gate",
            "不是 compliance / accreditation claim",
        ]
    )
    return {
        "non_claim_fragments": non_claim_fragments,
        "non_claim_fragment_keys": fragment_keys,
        "non_claim_filter_rows": non_claim_filter_rows,
        "non_claim_filters": fragment_filter_rows_to_ids(non_claim_filter_rows),
        "non_claim_digest": non_claim_digest,
        "reviewer_fragments_contract_version": REVIEWER_FRAGMENTS_CONTRACT_VERSION,
    }


def _scan_summary_line(
    *,
    current_reader_mode: str,
    compatibility_status: str,
    regenerate_recommended: bool,
) -> str:
    if current_reader_mode == "canonical_direct" and not regenerate_recommended:
        return "当前 run 已具备 canonical contract 直读，compatibility sidecar 已就绪。"
    if current_reader_mode == "compatibility_adapter":
        return "当前 run 通过 compatibility adapter 兼容读取，建议补齐 reviewer/index sidecar。"
    return (
        "当前 run 已完成 compatibility 扫描"
        + ("，建议补齐 reviewer/index sidecar。" if regenerate_recommended else "。")
        + f" 当前状态: {_display_compatibility_status(compatibility_status)}"
    )


def _scan_detail_lines(
    *,
    current_reader_mode: str,
    compatibility_status: str,
    status_counts: dict[str, int],
    version_source_counts: dict[str, int],
    canonical_reader_count: int,
    total_entries: int,
    regenerate_recommended: bool,
) -> list[str]:
    return [
        f"读取方式: {_display_reader_mode(current_reader_mode)}",
        f"兼容状态: {_display_compatibility_status(compatibility_status)}",
        f"状态计数: {_count_summary(status_counts, display=_display_compatibility_status)}",
        f"版本识别: {_count_summary(version_source_counts)}",
        f"canonical reader 可用: {canonical_reader_count}/{total_entries}",
        (
            "建议动作: 运行轻量 reindex/regenerate，仅重建 reviewer/index sidecar"
            if regenerate_recommended
            else "建议动作: 当前 compatibility sidecar 已齐备"
        ),
        "边界提醒: regenerate 目标仅限 reviewer/index sidecar，不改写原始主证据",
    ]


def _count_summary(counts: dict[str, int], *, display: Any | None = None) -> str:
    if not counts:
        return "--"
    parts: list[str] = []
    for key, value in sorted(counts.items()):
        label = display(key) if callable(display) else str(key)
        parts.append(f"{label} {int(value or 0)}")
    return " | ".join(parts)


def _display_compatibility_status(value: Any) -> str:
    status = str(value or "").strip()
    return COMPATIBILITY_STATUS_LABELS.get(status, status or "--")


def _display_reader_mode(value: Any) -> str:
    mode = str(value or "").strip()
    return READER_MODE_LABELS.get(mode, mode or "--")


def _build_run_artifact_index_markdown(payload: dict[str, Any]) -> str:
    overview = dict(payload.get("compatibility_overview") or {})
    lines = [
        "# Run Artifact Index",
        "",
        f"- run_id: {payload.get('run_id')}",
        f"- reader_mode: {payload.get('current_reader_mode')}",
        f"- compatibility_status: {payload.get('compatibility_status')}",
        f"- regenerate_recommended: {payload.get('regenerate_recommended')}",
        f"- schema_contract_summary: {overview.get('schema_contract_summary_display') or payload.get('schema_or_contract_version_summary')}",
        f"- summary: {payload.get('summary')}",
        "",
        "| artifact | version | compatibility | regenerate | surfaces |",
        "| --- | --- | --- | --- | --- |",
    ]
    for entry in list(payload.get("entries") or []):
        lines.append(
            "| {artifact} | {version} | {status} | {regenerate} | {surfaces} |".format(
                artifact=str(entry.get("artifact_name") or "--"),
                version=str(entry.get("schema_or_contract_version") or "--"),
                status=str(entry.get("compatibility_status") or "--"),
                regenerate="yes" if bool(entry.get("regenerate_recommended", False)) else "no",
                surfaces=", ".join(list(entry.get("linked_surface_visibility") or [])) or "--",
            )
        )
    lines.extend(
        [
            "",
            f"boundary: {payload.get('boundary_digest')}",
            f"non_claim: {payload.get('non_claim_digest')}",
        ]
    )
    return "\n".join(lines) + "\n"


def _build_contract_catalog_markdown(payload: dict[str, Any]) -> str:
    overview = dict(payload.get("compatibility_overview") or {})
    lines = [
        "# Artifact Contract Catalog",
        "",
        f"- run_id: {payload.get('run_id')}",
        f"- schema_contract_summary: {overview.get('schema_contract_summary_display') or payload.get('schema_or_contract_version_summary')}",
        f"- summary: {payload.get('summary')}",
        "",
        "| artifact_key | role | versions | compatibility |",
        "| --- | --- | --- | --- |",
    ]
    for row in list(payload.get("contract_rows") or []):
        lines.append(
            "| {artifact_key} | {role} | {versions} | {statuses} |".format(
                artifact_key=str(row.get("artifact_key") or "--"),
                role=str(row.get("artifact_role") or "--"),
                versions=", ".join(list(row.get("schema_versions") or [])) or "--",
                statuses=", ".join(list(row.get("compatibility_statuses") or [])) or "--",
            )
        )
    return "\n".join(lines) + "\n"


def _build_scan_summary_markdown(payload: dict[str, Any]) -> str:
    overview = dict(payload.get("compatibility_overview") or {})
    lines = [
        "# Compatibility Scan Summary",
        "",
        f"- run_id: {payload.get('run_id')}",
        f"- summary: {payload.get('summary')}",
        f"- reader_mode: {payload.get('current_reader_mode')}",
        f"- compatibility_status: {payload.get('compatibility_status')}",
        f"- regenerate_recommended: {payload.get('regenerate_recommended')}",
        f"- schema_contract_summary: {overview.get('schema_contract_summary_display') or payload.get('schema_or_contract_version_summary')}",
        f"- primary_evidence_rewritten: {payload.get('primary_evidence_rewritten')}",
        "",
    ]
    for line in list(payload.get("detail_lines") or []):
        lines.append(f"- {line}")
    lines.extend(
        [
            "",
            f"- boundary: {payload.get('boundary_digest')}",
            f"- non_claim: {payload.get('non_claim_digest')}",
        ]
    )
    return "\n".join(lines) + "\n"


def _build_reindex_manifest_markdown(payload: dict[str, Any]) -> str:
    overview = dict(payload.get("compatibility_overview") or {})
    lines = [
        "# Reindex Manifest",
        "",
        f"- run_id: {payload.get('run_id')}",
        f"- schema_contract_summary: {overview.get('schema_contract_summary_display') or payload.get('schema_or_contract_version_summary')}",
        f"- regenerate_scope: {payload.get('regenerate_scope')}",
        f"- primary_evidence_preserved: {payload.get('primary_evidence_preserved')}",
        f"- primary_evidence_rewritten: {payload.get('primary_evidence_rewritten')}",
        "",
    ]
    for line in list(payload.get("detail_lines") or []):
        lines.append(f"- {line}")
    lines.extend(
        [
            "",
            f"- boundary: {payload.get('boundary_digest')}",
            f"- non_claim: {payload.get('non_claim_digest')}",
        ]
    )
    return "\n".join(lines) + "\n"


def _load_json_dict(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception:
        return {}
    return dict(payload) if isinstance(payload, dict) else {}
