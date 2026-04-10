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

    canonical_reader_available = bool(
        artifact_key
        or artifact_name in REGENERABLE_SIDECAR_FILENAMES
        or artifact_name in KNOWN_REPORT_ARTIFACTS
    )
    regenerable_sidecar = artifact_name in REGENERABLE_SIDECAR_FILENAMES
    primary_evidence = not regenerable_sidecar
    if not present_on_disk and regenerable_sidecar:
        compatibility_status = "missing_regenerable"
        reader_mode = "regenerate_sidecar"
    elif not present_on_disk and canonical_reader_available:
        compatibility_status = "missing_primary"
        reader_mode = run_mode if run_mode in READER_MODE_LABELS else "compatibility_adapter"
    elif artifact_name in REGENERABLE_SIDECAR_FILENAMES:
        compatibility_status = "canonical_current"
        reader_mode = "canonical_index"
    elif artifact_name in CANONICAL_SURFACE_FILENAMES and schema_or_contract_version:
        compatibility_status = "canonical_current"
        reader_mode = "canonical_direct"
    elif canonical_reader_available:
        compatibility_status = "compatibility_read"
        reader_mode = "compatibility_adapter" if run_mode != "scan_only" else "observed_only"
    else:
        compatibility_status = "unclassified_observed"
        reader_mode = "observed_only"
    regenerate_recommended = bool(
        compatibility_status in {"compatibility_read", "missing_regenerable"}
        or (run_mode == "compatibility_adapter" and canonical_reader_available)
    )
    boundary_payload = _compatibility_boundary_payload()
    non_claim_payload = _compatibility_non_claim_payload()
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
        "primary_evidence": primary_evidence,
        "regenerable_sidecar": regenerable_sidecar,
        "boundary_digest": boundary_payload.get("digest"),
        "non_claim_digest": non_claim_payload.get("digest"),
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

    for filename in list(KNOWN_REPORT_ARTIFACTS) + list(REGENERABLE_SIDECAR_FILENAMES):
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
            if key_text in {"output_files", "primary_artifact_paths", "supporting_artifact_paths"}:
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
    if present_on_disk and (artifact_key or artifact_name in KNOWN_REPORT_ARTIFACTS or artifact_name in REGENERABLE_SIDECAR_FILENAMES):
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
    fragment_rows = normalize_fragment_rows(BOUNDARY_FRAGMENT_FAMILY, fragment_keys, display_locale="zh_CN")
    filter_rows = normalize_fragment_filter_rows(BOUNDARY_FRAGMENT_FAMILY, fragment_keys, display_locale="zh_CN")
    digest = " | ".join(
        [
            "Step 2 收尾 / Step 3 桥接边界",
            "仅用于 Step 2 审阅就绪度",
            "仅限 simulation / offline / headless",
            "以文件工件为先的审阅证据链",
            "compatibility / reindex / regenerate 仅重建 reviewer/index sidecar",
            "不改写原始主证据",
        ]
    )
    return {
        "boundary_fragments": fragment_rows,
        "boundary_fragment_keys": fragment_keys,
        "boundary_statements": [
            "compatibility / reindex / regenerate 仅重建 reviewer/index sidecar",
            "不改写原始主证据",
        ],
        "filter_rows": filter_rows,
        "filter_ids": fragment_filter_rows_to_ids(filter_rows),
        "digest": digest,
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
    fragment_rows = normalize_fragment_rows(NON_CLAIM_FRAGMENT_FAMILY, fragment_keys, display_locale="zh_CN")
    filter_rows = normalize_fragment_filter_rows(NON_CLAIM_FRAGMENT_FAMILY, fragment_keys, display_locale="zh_CN")
    digest = " | ".join(
        [
            "仅为 simulation / synthetic reviewer evidence",
            "不是 real acceptance",
            "不是 live gate",
            "不是 live acceptance",
            "不是 compliance claim",
            "不是 accreditation claim",
        ]
    )
    return {
        "non_claim_fragments": fragment_rows,
        "non_claim_fragment_keys": fragment_keys,
        "non_claim": [],
        "filter_rows": filter_rows,
        "filter_ids": fragment_filter_rows_to_ids(filter_rows),
        "digest": digest,
    }


def _scan_summary_line(
    *,
    current_reader_mode: str,
    compatibility_status: str,
    regenerate_recommended: bool,
) -> str:
    if current_reader_mode == "canonical_direct" and not regenerate_recommended:
        return "当前 run 已具备 canonical contract 直读，compatibility index sidecar 已就位"
    if current_reader_mode == "compatibility_adapter":
        return "当前 run 以旧格式 compatibility adapter 兼容读取，建议再生成 reviewer/index sidecar"
    return (
        "当前 run 已完成 compatibility 扫描"
        + ("，建议再生成 reviewer/index sidecar" if regenerate_recommended else "")
        + f"；状态：{_display_compatibility_status(compatibility_status)}"
    )


def _display_compatibility_status(value: Any) -> str:
    status = str(value or "").strip()
    return COMPATIBILITY_STATUS_LABELS.get(status, status or "--")


def _display_reader_mode(value: Any) -> str:
    mode = str(value or "").strip()
    return READER_MODE_LABELS.get(mode, mode or "--")


def _build_run_artifact_index_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Run Artifact Index",
        "",
        f"- run_id: {payload.get('run_id')}",
        f"- reader_mode: {payload.get('current_reader_mode')}",
        f"- compatibility_status: {payload.get('compatibility_status')}",
        f"- regenerate_recommended: {payload.get('regenerate_recommended')}",
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
    lines.extend(["", f"boundary: {payload.get('boundary_digest')}", f"non_claim: {payload.get('non_claim_digest')}"])
    return "\n".join(lines) + "\n"


def _build_contract_catalog_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Artifact Contract Catalog",
        "",
        f"- run_id: {payload.get('run_id')}",
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
    lines = [
        "# Compatibility Scan Summary",
        "",
        f"- run_id: {payload.get('run_id')}",
        f"- summary: {payload.get('summary')}",
        f"- reader_mode: {payload.get('current_reader_mode')}",
        f"- compatibility_status: {payload.get('compatibility_status')}",
        f"- regenerate_recommended: {payload.get('regenerate_recommended')}",
        "",
    ]
    for line in list(payload.get("detail_lines") or []):
        lines.append(f"- {line}")
    lines.extend(["", f"- boundary: {payload.get('boundary_digest')}", f"- non_claim: {payload.get('non_claim_digest')}"])
    return "\n".join(lines) + "\n"


def _build_reindex_manifest_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Reindex Manifest",
        "",
        f"- run_id: {payload.get('run_id')}",
        f"- regenerate_scope: {payload.get('regenerate_scope')}",
        f"- primary_evidence_preserved: {payload.get('primary_evidence_preserved')}",
        f"- primary_evidence_rewritten: {payload.get('primary_evidence_rewritten')}",
        "",
    ]
    for line in list(payload.get("detail_lines") or []):
        lines.append(f"- {line}")
    lines.extend(["", f"- boundary: {payload.get('boundary_digest')}", f"- non_claim: {payload.get('non_claim_digest')}"])
    return "\n".join(lines) + "\n"


def _load_json_dict(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception:
        return {}
    return dict(payload) if isinstance(payload, dict) else {}
