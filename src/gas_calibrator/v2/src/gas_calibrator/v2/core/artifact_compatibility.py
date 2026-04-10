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


ARTIFACT_COMPATIBILITY_SCHEMA_VERSION = "1.0"

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
        "markdown_role": "diagnostic_analysis",
    },
    "compatibility_scan_summary": {
        "filename": COMPATIBILITY_SCAN_SUMMARY_FILENAME,
        "markdown_filename": COMPATIBILITY_SCAN_SUMMARY_MARKDOWN_FILENAME,
        "json_role": "diagnostic_analysis",
        "markdown_role": "diagnostic_analysis",
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
    "canonical_current": "当前规范直读",
    "compatibility_read": "兼容读取",
    "missing_regenerable": "缺失，可再生成 sidecar",
    "missing_primary": "缺失原始工件",
    "unclassified_observed": "已发现，未分类",
}

READER_MODE_LABELS = {
    "canonical_direct": "canonical contract 直读",
    "compatibility_adapter": "compatibility adapter 兼容读取",
    "scan_only": "仅兼容扫描",
    "canonical_index": "canonical index sidecar",
    "regenerate_sidecar": "待生成 compatibility sidecar",
    "observed_only": "仅观察到文件",
}


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
        return loaded
    bundle = build_artifact_compatibility_bundle(
        run_dir,
        summary=summary,
        manifest=manifest,
        results=results,
        output_files=output_files,
        role_catalog=role_catalog,
    )
    return {
        artifact_key: dict(bundle_item.get("raw") or {})
        for artifact_key, bundle_item in bundle.items()
    }


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
    compatibility_sidecars_present = all(
        bool(
            next(
                (
                    entry
                    for entry in base_entries
                    if str(entry.get("artifact_name") or "") == definition["filename"]
                    and bool(entry.get("present_on_disk", False))
                ),
                {},
            )
        )
        for definition in COMPATIBILITY_BUNDLE_DEFINITIONS.values()
    )
    primary_reader_present = any(
        bool(entry.get("present_on_disk", False)) and str(entry.get("artifact_name") or "") in PRIMARY_READER_FILENAMES
        for entry in base_entries
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
    boundary_digest = str(boundary_payload.get("digest") or "")
    non_claim_digest = str(non_claim_payload.get("digest") or "")
    summary_line = _scan_summary_line(
        current_reader_mode=current_reader_mode,
        compatibility_status=compatibility_status,
        regenerate_recommended=regenerate_recommended,
    )
    detail_lines = [
        f"读取模式: {_display_reader_mode(current_reader_mode)}",
        f"兼容状态: {_display_compatibility_status(compatibility_status)}",
        (
            "状态计数: "
            + " | ".join(
                f"{_display_compatibility_status(key)} {int(value or 0)}"
                for key, value in sorted(status_counts.items())
            )
        )
        if status_counts
        else "状态计数: 无",
        (
            "版本识别: "
            + " | ".join(
                f"{key} {int(value or 0)}"
                for key, value in sorted(version_source_counts.items())
            )
        )
        if version_source_counts
        else "版本识别: 无",
        f"canonical reader 可用: {canonical_reader_count}/{len(entries)}",
        (
            "建议动作: 再生成 reviewer/index sidecar"
            if regenerate_recommended
            else "建议动作: 当前 compatibility sidecar 已就位"
        ),
        "再生成范围: 仅 reviewer/index sidecar，不改写 summary / manifest / results 等原始主证据",
        "可见面: results / review_center / workbench",
    ]

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
        boundary_digest=boundary_digest,
        non_claim_digest=non_claim_digest,
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
        boundary_digest=boundary_digest,
        non_claim_digest=non_claim_digest,
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
        boundary_digest=boundary_digest,
        non_claim_digest=non_claim_digest,
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
        boundary_digest=boundary_digest,
        non_claim_digest=non_claim_digest,
    )
    return {
        "run_artifact_index": {
            "raw": run_artifact_index,
            "markdown": _build_run_artifact_index_markdown(run_artifact_index),
            **COMPATIBILITY_BUNDLE_DEFINITIONS["run_artifact_index"],
        },
        "artifact_contract_catalog": {
            "raw": contract_catalog,
            "markdown": _build_contract_catalog_markdown(contract_catalog),
            **COMPATIBILITY_BUNDLE_DEFINITIONS["artifact_contract_catalog"],
        },
        "compatibility_scan_summary": {
            "raw": compatibility_scan_summary,
            "markdown": _build_scan_summary_markdown(compatibility_scan_summary),
            **COMPATIBILITY_BUNDLE_DEFINITIONS["compatibility_scan_summary"],
        },
        "reindex_manifest": {
            "raw": reindex_manifest,
            "markdown": _build_reindex_manifest_markdown(reindex_manifest),
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
    scan_summary = dict(bundle.get("compatibility_scan_summary", {}).get("raw") or {})
    return {
        "run_dir": str(run_dir.resolve()),
        "written_paths": {
            key: {"json_path": str(paths[0]), "markdown_path": str(paths[1])}
            for key, paths in written_paths.items()
        },
        "primary_evidence_rewritten": False,
        "regenerate_scope": "reviewer_index_sidecar_only",
        "compatibility_scan_summary": scan_summary,
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
    boundary_digest: str,
    non_claim_digest: str,
) -> dict[str, Any]:
    return {
        "schema_version": ARTIFACT_COMPATIBILITY_SCHEMA_VERSION,
        "artifact_type": "run_artifact_index",
        "run_id": run_id,
        "run_dir": str(run_dir.resolve()),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "current_reader_mode": current_reader_mode,
        "compatibility_status": compatibility_status,
        "compatibility_status_counts": status_counts,
        "schema_version_source_counts": version_source_counts,
        "canonical_reader_available_count": canonical_reader_count,
        "regenerate_recommended": regenerate_recommended,
        "entries": entries,
        "summary": summary_line,
        "detail_lines": detail_lines,
        "linked_surface_visibility": linked_surface_visibility,
        "boundary_digest": boundary_digest,
        "non_claim_digest": non_claim_digest,
        **boundary_payload,
        **non_claim_payload,
        "reviewer_fragments_contract_version": REVIEWER_FRAGMENTS_CONTRACT_VERSION,
        "artifact_paths": _self_artifact_paths(run_dir, "run_artifact_index"),
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
    boundary_digest: str,
    non_claim_digest: str,
    contract_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": ARTIFACT_COMPATIBILITY_SCHEMA_VERSION,
        "artifact_type": "artifact_contract_catalog",
        "run_id": run_id,
        "run_dir": str(run_dir.resolve()),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "current_reader_mode": current_reader_mode,
        "compatibility_status": compatibility_status,
        "regenerate_recommended": regenerate_recommended,
        "contract_rows": contract_rows,
        "summary": (
            f"contract catalog | contracts {len(contract_rows)} | "
            f"compatibility-read {compatibility_read_count} | missing-sidecar {missing_regenerable_count}"
        ),
        "detail_lines": [
            f"contracts: {len(contract_rows)}",
            f"compatibility-read entries: {compatibility_read_count}",
            f"missing regenerable sidecars: {missing_regenerable_count}",
        ],
        "linked_surface_visibility": ["results", "review_center"],
        "boundary_digest": boundary_digest,
        "non_claim_digest": non_claim_digest,
        **boundary_payload,
        **non_claim_payload,
        "reviewer_fragments_contract_version": REVIEWER_FRAGMENTS_CONTRACT_VERSION,
        "artifact_paths": _self_artifact_paths(run_dir, "artifact_contract_catalog"),
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
    boundary_digest: str,
    non_claim_digest: str,
) -> dict[str, Any]:
    return {
        "schema_version": ARTIFACT_COMPATIBILITY_SCHEMA_VERSION,
        "artifact_type": "compatibility_scan_summary",
        "run_id": run_id,
        "run_dir": str(run_dir.resolve()),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "current_reader_mode": current_reader_mode,
        "compatibility_status": compatibility_status,
        "compatibility_adapter_used": bool(current_reader_mode == "compatibility_adapter"),
        "compatibility_status_counts": status_counts,
        "schema_version_source_counts": version_source_counts,
        "canonical_reader_available_count": canonical_reader_count,
        "regenerate_recommended": regenerate_recommended,
        "surface_visibility_summary": "results / review_center / workbench",
        "regenerate_target_summary": "reviewer/index sidecar only",
        "summary": summary_line,
        "detail_lines": detail_lines,
        "recommended_actions": [
            "运行轻量 reindex / regenerate，仅重建 reviewer/index sidecar",
            "保持 summary / manifest / results 等原始主证据不改写",
        ]
        if regenerate_recommended
        else [
            "保留当前 canonical contract 直读链路",
            "后续仅在索引变化时刷新 compatibility index sidecar",
        ],
        "linked_surface_visibility": linked_surface_visibility,
        "boundary_digest": boundary_digest,
        "non_claim_digest": non_claim_digest,
        **boundary_payload,
        **non_claim_payload,
        "reviewer_fragments_contract_version": REVIEWER_FRAGMENTS_CONTRACT_VERSION,
        "review_surface": {
            "title_text": "历史工件兼容 / 再索引摘要",
            "summary_text": summary_line,
            "summary_lines": [summary_line, *detail_lines[:3]],
            "detail_lines": [
                *detail_lines,
                f"边界: {boundary_digest}",
                f"非声明: {non_claim_digest}",
            ],
            "reviewer_note": (
                "当前 regenerate / reindex 仅重建 reviewer/index sidecar，"
                "不改写原始主证据，也不构成 real acceptance evidence。"
            ),
            "anchor_id": "artifact-compatibility-scan",
            "anchor_label": "Artifact compatibility scan",
            "boundary_filter_rows": list(boundary_payload.get("filter_rows") or []),
            "boundary_filters": list(boundary_payload.get("filter_ids") or []),
            "non_claim_filter_rows": list(non_claim_payload.get("filter_rows") or []),
            "non_claim_filters": list(non_claim_payload.get("filter_ids") or []),
        },
        "digest": {
            "summary": summary_line,
            "current_reader_mode_summary": _display_reader_mode(current_reader_mode),
            "compatibility_status_summary": _display_compatibility_status(compatibility_status),
            "regenerate_target_summary": "reviewer/index sidecar only",
            "surface_visibility_summary": "results / review_center / workbench",
            "boundary_digest": boundary_digest,
            "non_claim_digest": non_claim_digest,
        },
        "artifact_paths": _self_artifact_paths(run_dir, "compatibility_scan_summary"),
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
    boundary_digest: str,
    non_claim_digest: str,
) -> dict[str, Any]:
    return {
        "schema_version": ARTIFACT_COMPATIBILITY_SCHEMA_VERSION,
        "artifact_type": "reindex_manifest",
        "run_id": run_id,
        "run_dir": str(run_dir.resolve()),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "current_reader_mode": current_reader_mode,
        "compatibility_status": compatibility_status,
        "regenerate_recommended": regenerate_recommended,
        "primary_evidence_preserved": True,
        "primary_evidence_rewritten": False,
        "regenerate_scope": "reviewer_index_sidecar_only",
        "sidecar_targets": [
            definition["filename"]
            for definition in COMPATIBILITY_BUNDLE_DEFINITIONS.values()
        ],
        "summary": (
            "reindex manifest | "
            + (
                "建议生成 compatibility sidecar"
                if regenerate_recommended
                else "compatibility sidecar 已齐备"
            )
        ),
        "detail_lines": [
            "目标: run_artifact_index / artifact_contract_catalog / compatibility_scan_summary / reindex_manifest",
            "范围: 仅 reviewer/index sidecar",
            "约束: 不改写 summary / manifest / results / primary evidence",
            (
                "建议: 对历史旧 run 执行轻量 regenerate"
                if regenerate_recommended
                else "建议: 仅在索引变化时再生成"
            ),
        ],
        "linked_surface_visibility": linked_surface_visibility,
        "boundary_digest": boundary_digest,
        "non_claim_digest": non_claim_digest,
        **boundary_payload,
        **non_claim_payload,
        "reviewer_fragments_contract_version": REVIEWER_FRAGMENTS_CONTRACT_VERSION,
        "artifact_paths": _self_artifact_paths(run_dir, "reindex_manifest"),
    }
