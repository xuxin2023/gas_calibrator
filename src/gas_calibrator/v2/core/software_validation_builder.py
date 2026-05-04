from __future__ import annotations

import hashlib
import json
import platform
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SCHEMA_VERSION = "step2-software-validation-wp5-v1"
WORKSPACE_MODE = "step2_simulation_only_file_artifact_first"
GENERATED_BY_TOOL = "gas_calibrator.v2.core.software_validation_builder"
HASH_ALGORITHM = "sha256"
LINKED_REVIEW_SURFACES = [
    "results_payload",
    "reports",
    "review_center",
    "workbench_recognition_readiness",
]


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _dedupe(values: list[Any]) -> list[str]:
    seen: set[str] = set()
    rows: list[str] = []
    for item in values:
        text = str(item or "").strip()
        if text and text not in seen:
            seen.add(text)
            rows.append(text)
    return rows


def _render_markdown(title: str, lines: list[str]) -> str:
    body = "\n".join(f"- {line}" for line in lines if str(line).strip())
    return f"# {title}\n\n{body}\n"


def _canonical_json_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _hash_text(value: str) -> str:
    return hashlib.sha256(str(value or "").encode("utf-8")).hexdigest()


def _hash_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _load_json_dict(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return dict(payload) if isinstance(payload, dict) else {}


def _git_text(*args: str) -> str:
    repo_root = Path(__file__).resolve().parents[4]
    try:
        completed = subprocess.run(
            ["git", "-C", str(repo_root), *args],
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
    except Exception:
        return ""
    return str(completed.stdout or "").strip()


def _git_context() -> tuple[str, str]:
    repo_ref = _git_text("rev-parse", "--short", "HEAD")
    branch_or_head = _git_text("branch", "--show-current") or "detached-head"
    return repo_ref or "git-unresolved", branch_or_head


def _read_validation_status(run_dir: Path, filename: str, *, field: str = "status") -> str:
    payload = _load_json_dict(run_dir / filename)
    return str(payload.get(field) or "").strip()


def _as_raw(payload: dict[str, Any]) -> dict[str, Any]:
    return dict(payload.get("raw") or payload or {})


def _as_digest(payload: dict[str, Any]) -> dict[str, Any]:
    return dict(_as_raw(payload).get("digest") or payload.get("digest") or {})


def _bundle_path_map(path_map: dict[str, str], *keys: str) -> dict[str, str]:
    return {
        str(key): str(path_map.get(key) or "").strip()
        for key in keys
        if str(path_map.get(key) or "").strip()
    }


def _artifact_ref(artifact_type: str, path: str) -> dict[str, str]:
    return {
        "artifact_type": str(artifact_type or "").strip(),
        "path": str(path or "").strip(),
    }


def _hash_payload_or_path(path: str, payload: dict[str, Any]) -> str:
    path_text = str(path or "").strip()
    resolved = Path(path_text) if path_text else None
    if resolved is not None and resolved.exists() and resolved.is_file():
        try:
            return _hash_bytes(resolved.read_bytes())
        except Exception:
            pass
    if payload:
        return _hash_bytes(_canonical_json_bytes(payload))
    return _hash_text(path_text or "missing-payload")


def _bundle(
    *,
    run_id: str,
    artifact_type: str,
    filename: str,
    markdown_filename: str,
    artifact_role: str,
    title_text: str,
    reviewer_note: str,
    summary_text: str,
    summary_lines: list[str],
    detail_lines: list[str],
    artifact_paths: dict[str, str],
    body: dict[str, Any],
    digest: dict[str, Any],
    boundary_statements: list[str],
    evidence_categories: list[str],
    evidence_source: str = "simulated",
) -> dict[str, Any]:
    anchor_id = artifact_type.replace("_", "-")
    raw = {
        "schema_version": SCHEMA_VERSION,
        "artifact_type": artifact_type,
        "generated_at": _now_iso(),
        "run_id": run_id,
        "artifact_role": artifact_role,
        "evidence_source": evidence_source,
        "evidence_state": "reviewer_readiness_only",
        "reviewer_only": True,
        "readiness_mapping_only": True,
        "simulation_only": True,
        "ready_for_readiness_mapping": True,
        "not_real_acceptance_evidence": True,
        "not_ready_for_formal_claim": True,
        "primary_evidence_rewritten": False,
        "anchor_id": anchor_id,
        "anchor_label": title_text,
        "linked_surface_visibility": list(LINKED_REVIEW_SURFACES),
        "surface_visibility_summary": " | ".join(LINKED_REVIEW_SURFACES),
        "boundary_statements": list(boundary_statements),
        "digest": dict(digest),
        "review_surface": {
            "title_text": title_text,
            "role_text": artifact_role,
            "reviewer_note": reviewer_note,
            "summary_text": summary_text,
            "summary_lines": [line for line in summary_lines if str(line).strip()],
            "detail_lines": [line for line in detail_lines if str(line).strip()],
            "anchor_id": anchor_id,
            "anchor_label": title_text,
            "phase_filters": ["step2_tail_recognition_ready"],
            "route_filters": [],
            "signal_family_filters": [],
            "decision_result_filters": [],
            "policy_version_filters": [],
            "boundary_filter_rows": [],
            "boundary_filters": [],
            "non_claim_filter_rows": [],
            "non_claim_filters": [],
            "evidence_source_filters": [evidence_source, "reviewer_readiness_only"],
            "artifact_paths": dict(artifact_paths),
            "linked_surface_visibility": list(LINKED_REVIEW_SURFACES),
            "surface_visibility_summary": " | ".join(LINKED_REVIEW_SURFACES),
        },
        "artifact_paths": dict(artifact_paths),
        "evidence_categories": list(evidence_categories),
        **body,
    }
    markdown = _render_markdown(
        title_text,
        [
            f"summary: {summary_text}",
            *[str(line) for line in summary_lines if str(line).strip()],
            *[str(line) for line in detail_lines if str(line).strip()],
        ],
    )
    return {
        "available": True,
        "artifact_type": artifact_type,
        "filename": filename,
        "markdown_filename": markdown_filename,
        "raw": raw,
        "markdown": markdown,
        "digest": dict(digest),
    }


def _compatibility_alias(
    *,
    alias_type: str,
    filename: str,
    markdown_filename: str,
    source_bundle: dict[str, Any],
    title_text: str,
) -> dict[str, Any]:
    source_raw = dict(source_bundle.get("raw") or {})
    source_digest = dict(source_bundle.get("digest") or {})
    source_paths = dict(source_raw.get("artifact_paths") or {})
    alias_raw = dict(source_raw)
    alias_raw["artifact_type"] = alias_type
    alias_raw["anchor_id"] = alias_type.replace("_", "-")
    alias_raw["anchor_label"] = title_text
    alias_raw["compatibility_alias_of"] = str(source_raw.get("artifact_type") or "")
    alias_raw["artifact_paths"] = {
        **source_paths,
        alias_type: source_paths.get(alias_type) or "",
        f"{alias_type}_markdown": source_paths.get(f"{alias_type}_markdown") or "",
    }
    alias_raw["review_surface"] = {
        **dict(alias_raw.get("review_surface") or {}),
        "title_text": title_text,
        "anchor_id": alias_raw["anchor_id"],
        "anchor_label": title_text,
    }
    return {
        "available": True,
        "artifact_type": alias_type,
        "filename": filename,
        "markdown_filename": markdown_filename,
        "raw": alias_raw,
        "markdown": str(source_bundle.get("markdown") or ""),
        "digest": {
            **source_digest,
            "compatibility_alias_of": str(source_raw.get("artifact_type") or ""),
        },
    }


def _digest(
    *,
    summary: str,
    scope_overview_summary: str,
    decision_rule_summary: str,
    conformity_boundary_summary: str,
    current_coverage_summary: str,
    missing_evidence_summary: str,
    reviewer_next_step_digest: str,
    non_claim_digest: str,
    asset_readiness_overview: str = "",
    certificate_lifecycle_overview: str = "",
    pre_run_gate_status: str = "",
    scope_reference_assets_summary: str = "",
    decision_rule_dependency_summary: str = "",
) -> dict[str, Any]:
    payload = {
        "summary": summary,
        "scope_overview_summary": scope_overview_summary,
        "decision_rule_summary": decision_rule_summary,
        "conformity_boundary_summary": conformity_boundary_summary,
        "current_coverage_summary": current_coverage_summary,
        "missing_evidence_summary": missing_evidence_summary,
        "reviewer_next_step_digest": reviewer_next_step_digest,
        "non_claim_digest": non_claim_digest,
    }
    if asset_readiness_overview:
        payload["asset_readiness_overview"] = asset_readiness_overview
    if certificate_lifecycle_overview:
        payload["certificate_lifecycle_overview"] = certificate_lifecycle_overview
    if pre_run_gate_status:
        payload["pre_run_gate_status"] = pre_run_gate_status
    if scope_reference_assets_summary:
        payload["scope_reference_assets_summary"] = scope_reference_assets_summary
    if decision_rule_dependency_summary:
        payload["decision_rule_dependency_summary"] = decision_rule_dependency_summary
    return payload


def build_software_validation_wp5_artifacts(
    *,
    run_id: str,
    run_dir: Path,
    path_map: dict[str, str],
    filenames: dict[str, str],
    boundary_statements: list[str],
    scope_definition_pack: dict[str, Any],
    decision_rule_profile: dict[str, Any],
    reference_asset_registry: dict[str, Any],
    certificate_lifecycle_summary: dict[str, Any],
    pre_run_readiness_gate: dict[str, Any],
    uncertainty_report_pack: dict[str, Any],
    uncertainty_rollup: dict[str, Any],
    method_confirmation_protocol: dict[str, Any],
    route_specific_validation_matrix: dict[str, Any],
    validation_run_set: dict[str, Any],
    verification_digest: dict[str, Any],
    verification_rollup: dict[str, Any],
    version_payload: dict[str, Any],
    lineage_payload: dict[str, Any],
    analytics_payload: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    run_dir = Path(run_dir)
    repo_ref, branch_or_head = _git_context()
    scope_raw = _as_raw(scope_definition_pack)
    decision_raw = _as_raw(decision_rule_profile)
    reference_raw = _as_raw(reference_asset_registry)
    certificate_raw = _as_raw(certificate_lifecycle_summary)
    gate_raw = _as_raw(pre_run_readiness_gate)
    uncertainty_report_raw = _as_raw(uncertainty_report_pack)
    uncertainty_rollup_raw = _as_raw(uncertainty_rollup)
    method_protocol_raw = _as_raw(method_confirmation_protocol)
    validation_run_set_raw = _as_raw(validation_run_set)
    verification_digest_raw = _as_raw(verification_digest)
    verification_rollup_raw = _as_raw(verification_rollup)
    scope_digest = _as_digest(scope_definition_pack)
    decision_digest = _as_digest(decision_rule_profile)
    uncertainty_digest = _as_digest(uncertainty_rollup) or _as_digest(uncertainty_report_pack)
    method_digest = _as_digest(verification_rollup) or _as_digest(verification_digest)

    scope_id = str(scope_raw.get("scope_id") or f"{run_id}-scope-package")
    decision_rule_id = str(
        decision_raw.get("decision_rule_id")
        or scope_raw.get("decision_rule_id")
        or "step2_readiness_reviewer_rule_v1"
    )
    uncertainty_case_id = str(
        uncertainty_rollup_raw.get("uncertainty_case_id")
        or uncertainty_report_raw.get("uncertainty_case_id")
        or f"{run_id}-uncertainty-case"
    )
    method_confirmation_protocol_id = str(
        method_protocol_raw.get("protocol_id")
        or method_protocol_raw.get("method_confirmation_protocol_id")
        or f"{run_id}-method-confirmation-protocol"
    )
    traceability_id = f"{run_id}-software-validation-traceability"
    traceability_version = "v1.2-step2-reviewer"
    release_id = f"{run_id}-release-manifest"
    release_version = "v1.2-step2-reviewer"
    hash_registry_id = f"{run_id}-artifact-hash-registry"

    limitation_note = (
        "Step 2 builds reviewer-facing software validation, audit hash, and release sidecars only; "
        "real release approval, formal compliance claims, and accreditation claims remain out of scope."
    )
    non_claim_note = (
        "Current artifacts support readiness mapping and reviewer review only; they are not real acceptance evidence, "
        "formal release approval, or formal compliance claims."
    )
    reviewer_note = (
        "This chain expresses requirement -> design -> code -> test -> artifact linkage in file-backed sidecars. "
        "No real signature, approval, anti-tamper, database default path, or primary-evidence rewrite is introduced."
    )
    release_manifest_reviewer_note = (
        "Release manifest stays reviewer-facing and Step 2 only. It summarizes release scope, linked validation, "
        "and current boundaries, but it is not formal release approval, not real acceptance evidence, and not a formal compliance claim."
    )
    audit_readiness_reviewer_note = (
        "Traceability skeleton, artifact hash registry, and release boundary summaries stay reviewer-facing in Step 2. "
        "No real audit ledger, anti-tamper guarantee, or formal release approval is introduced."
    )
    reviewer_actions = [
        "Confirm the linked scope, decision rule, uncertainty case, and method confirmation protocol are the intended Step 2 inputs.",
        "Check parity, resilience, and smoke linkage before using the pack for reviewer mapping.",
        "Keep the pack reviewer-only; do not treat it as real acceptance evidence or a formal release approval object.",
    ]

    reference_assets = [dict(item) for item in list(reference_raw.get("assets") or []) if isinstance(item, dict)]
    certificate_rows = [dict(item) for item in list(certificate_raw.get("certificate_rows") or []) if isinstance(item, dict)]
    validation_runs = [
        dict(item)
        for item in list(validation_run_set_raw.get("validation_run_set") or validation_run_set_raw.get("runs") or [])
        if isinstance(item, dict)
    ]
    parity_status = _read_validation_status(run_dir, "summary_parity_report.json") or "not_linked"
    resilience_status = _read_validation_status(run_dir, "export_resilience_report.json") or str(
        dict(analytics_payload.get("export_resilience_status") or {}).get("overall_status") or ""
    ).strip() or "not_linked"
    smoke_status = (
        str(analytics_payload.get("smoke_status") or "").strip()
        or ("simulation_run_present" if (run_dir / "summary.json").exists() else "not_linked")
    )
    linked_assets_certificates_summary = {
        "scope_asset_count": len(reference_assets),
        "certificate_count": len(certificate_rows),
        "asset_ids": _dedupe([item.get("asset_id") for item in reference_assets])[:8],
        "certificate_ids": _dedupe([item.get("certificate_id") for item in certificate_rows])[:8],
        "summary": " | ".join([f"assets {len(reference_assets)}", f"certificates {len(certificate_rows)}"]),
    }

    requirement_refs = [
        {
            "requirement_id": "wp5-traceability-chain",
            "title": "Software validation traceability chain",
            "summary": "Expose requirement -> design -> code -> test -> artifact linkage in a reviewer-facing skeleton.",
        },
        {
            "requirement_id": "wp5-audit-hash-registry",
            "title": "Audit hash and fingerprint summary",
            "summary": "Provide file-backed artifact hash, environment fingerprint, and release input digest sidecars without formal anti-tamper claims.",
        },
        {
            "requirement_id": "wp5-release-manifest",
            "title": "Step 2 release manifest",
            "summary": "Summarize what changed, what was linked, what was verified, and why formal claims remain blocked.",
        },
        {
            "requirement_id": "wp5-impact-and-rollback",
            "title": "Change impact and rollback readiness",
            "summary": "Document Step 2 impact scope and sidecar-first rollback expectations without touching primary evidence.",
        },
    ]
    design_refs = [
        "recognition_scope_repository/file-backed reviewer skeleton",
        "uncertainty_repository/file-backed reviewer skeleton",
        "method_confirmation_repository/file-backed reviewer skeleton",
        "software_validation_repository/file-backed reviewer skeleton",
        "results_gateway/review_center/workbench/historical sidecar integration",
    ]
    code_refs = [
        "src/gas_calibrator/v2/core/software_validation_builder.py",
        "src/gas_calibrator/v2/core/software_validation_repository.py",
        "src/gas_calibrator/v2/adapters/software_validation_gateway.py",
        "src/gas_calibrator/v2/adapters/results_gateway.py",
        "src/gas_calibrator/v2/ui_v2/controllers/app_facade.py",
        "src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py",
        "src/gas_calibrator/v2/scripts/historical_artifacts.py",
    ]
    test_refs = [
        "tests/v2/test_results_gateway.py",
        "tests/v2/test_build_offline_governance_artifacts.py",
        "tests/v2/test_ui_v2_review_center.py",
        "tests/v2/test_ui_v2_workbench_evidence.py",
        "tests/v2/test_historical_artifacts_cli.py",
        "tests/v2/test_export_resilience.py",
        "tests/v2/test_summary_parity.py",
        "tests/v2/test_software_validation_wp5_contracts.py",
    ]
    change_set_refs = [f"git:{branch_or_head}@{repo_ref}"]
    impact_scope = [
        "results_summary",
        "review_center",
        "device_workbench",
        "historical_artifacts",
        "offline_sidecars",
        "artifact_catalog_compatibility",
    ]
    changed_modules = [
        {
            "module_name": "software_validation_builder",
            "module_path": "src/gas_calibrator/v2/core/software_validation_builder.py",
            "change_scope": "Build Step 2 software validation, audit trace, fingerprint, and release sidecars.",
            "impacted_surfaces": list(LINKED_REVIEW_SURFACES),
        },
        {
            "module_name": "software_validation_repository",
            "module_path": "src/gas_calibrator/v2/core/software_validation_repository.py",
            "change_scope": "Load file-backed reviewer artifacts and summarize rollup visibility without enabling DB by default.",
            "impacted_surfaces": ["results_payload", "review_center", "workbench_recognition_readiness"],
        },
        {
            "module_name": "results_gateway",
            "module_path": "src/gas_calibrator/v2/adapters/results_gateway.py",
            "change_scope": "Expose software validation sidecars to results payloads and reports rows.",
            "impacted_surfaces": ["results_payload", "reports"],
        },
        {
            "module_name": "app_facade",
            "module_path": "src/gas_calibrator/v2/ui_v2/controllers/app_facade.py",
            "change_scope": "Keep review_center aware of software validation / audit readiness artifacts.",
            "impacted_surfaces": ["review_center"],
        },
        {
            "module_name": "device_workbench",
            "module_path": "src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py",
            "change_scope": "Reference software validation / audit readiness artifacts from workbench recognition readiness.",
            "impacted_surfaces": ["workbench_recognition_readiness"],
        },
    ]
    changed_module_paths = [str(item.get("module_path") or "").strip() for item in changed_modules]
    changed_modules_summary = " | ".join(
        _dedupe(str(item.get("module_name") or "").strip() for item in changed_modules)
    )
    linked_surface_summary = " | ".join(LINKED_REVIEW_SURFACES)
    main_execution_chain_impact_summary = (
        "No; the main execution chain stays unchanged and Step 2 remains simulation-only."
    )
    artifact_schema_impact_summary = (
        "Yes; reviewer-sidecar schema expands, but primary evidence schema remains unchanged."
    )
    results_surface_impact_summary = "Yes; results payloads and reports rows now expose the reviewer-sidecar chain."
    review_center_impact_summary = "Yes; review_center can scan and show the full software validation / audit sidechain."
    workbench_surface_impact_summary = "Yes; the workbench recognition readiness section can reference the full sidechain."
    file_artifact_first_summary = "file-artifact-first rollback over reviewer sidecars and derived indexes only"
    rollback_steps = [
        "Delete or replace the new reviewer-sidecar JSON/Markdown artifacts first.",
        "Rebuild results/reports/review_center/workbench indexes from surviving file artifacts only.",
        "Keep primary evidence, summary exports, and default DB path untouched.",
    ]
    traceability_rows = [
        {
            "requirement_id": "wp5-traceability-chain",
            "design_refs": design_refs[:2],
            "code_refs": code_refs[:3],
            "test_refs": [
                "tests/v2/test_software_validation_wp5_contracts.py",
                "tests/v2/test_results_gateway.py",
            ],
            "artifact_refs": [
                _artifact_ref("scope_definition_pack", str(path_map.get("scope_definition_pack") or "")),
                _artifact_ref("decision_rule_profile", str(path_map.get("decision_rule_profile") or "")),
                _artifact_ref(
                    "software_validation_traceability_matrix",
                    str(path_map.get("software_validation_traceability_matrix") or ""),
                ),
            ],
        },
        {
            "requirement_id": "wp5-audit-hash-registry",
            "design_refs": [design_refs[3], design_refs[4]],
            "code_refs": [
                "src/gas_calibrator/v2/core/software_validation_builder.py",
                "src/gas_calibrator/v2/core/software_validation_repository.py",
                "src/gas_calibrator/v2/adapters/results_gateway.py",
            ],
            "test_refs": [
                "tests/v2/test_results_gateway.py",
                "tests/v2/test_software_validation_wp5_contracts.py",
            ],
            "artifact_refs": [
                _artifact_ref("artifact_hash_registry", str(path_map.get("artifact_hash_registry") or "")),
                _artifact_ref("environment_fingerprint", str(path_map.get("environment_fingerprint") or "")),
                _artifact_ref("config_fingerprint", str(path_map.get("config_fingerprint") or "")),
            ],
        },
        {
            "requirement_id": "wp5-release-manifest",
            "design_refs": [design_refs[4]],
            "code_refs": [
                "src/gas_calibrator/v2/core/software_validation_builder.py",
                "src/gas_calibrator/v2/adapters/results_gateway.py",
                "src/gas_calibrator/v2/ui_v2/controllers/app_facade.py",
                "src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py",
            ],
            "test_refs": [
                "tests/v2/test_ui_v2_review_center.py",
                "tests/v2/test_ui_v2_workbench_evidence.py",
                "tests/v2/test_software_validation_wp5_contracts.py",
            ],
            "artifact_refs": [
                _artifact_ref("release_manifest", str(path_map.get("release_manifest") or "")),
                _artifact_ref("release_scope_summary", str(path_map.get("release_scope_summary") or "")),
                _artifact_ref(
                    "release_evidence_pack_index",
                    str(path_map.get("release_evidence_pack_index") or ""),
                ),
            ],
        },
        {
            "requirement_id": "wp5-impact-and-rollback",
            "design_refs": [design_refs[3], design_refs[4]],
            "code_refs": [
                "src/gas_calibrator/v2/core/software_validation_builder.py",
                "src/gas_calibrator/v2/core/software_validation_repository.py",
                "src/gas_calibrator/v2/adapters/results_gateway.py",
            ],
            "test_refs": [
                "tests/v2/test_software_validation_wp5_contracts.py",
                "tests/v2/test_ui_v2_workbench_evidence.py",
            ],
            "artifact_refs": [
                _artifact_ref("change_impact_summary", str(path_map.get("change_impact_summary") or "")),
                _artifact_ref(
                    "rollback_readiness_summary",
                    str(path_map.get("rollback_readiness_summary") or ""),
                ),
            ],
        },
    ]
    traceability_artifact_refs = [
        _artifact_ref("scope_definition_pack", str(path_map.get("scope_definition_pack") or "")),
        _artifact_ref("decision_rule_profile", str(path_map.get("decision_rule_profile") or "")),
        _artifact_ref("reference_asset_registry", str(path_map.get("reference_asset_registry") or "")),
        _artifact_ref("certificate_lifecycle_summary", str(path_map.get("certificate_lifecycle_summary") or "")),
        _artifact_ref("pre_run_readiness_gate", str(path_map.get("pre_run_readiness_gate") or "")),
        _artifact_ref("uncertainty_rollup", str(path_map.get("uncertainty_rollup") or "")),
        _artifact_ref("method_confirmation_protocol", str(path_map.get("method_confirmation_protocol") or "")),
        _artifact_ref("verification_rollup", str(path_map.get("verification_rollup") or "")),
    ]
    traceability_artifact_refs = [row for row in traceability_artifact_refs if row.get("path")]
    reviewer_sidecar_artifact_keys = [
        "software_validation_traceability_matrix",
        "requirement_design_code_test_links",
        "validation_evidence_index",
        "change_impact_summary",
        "rollback_readiness_summary",
        "artifact_hash_registry",
        "audit_event_store",
        "environment_fingerprint",
        "config_fingerprint",
        "release_input_digest",
        "release_manifest",
        "release_scope_summary",
        "release_boundary_digest",
        "release_evidence_pack_index",
        "release_validation_manifest",
        "audit_readiness_digest",
    ]
    reviewer_sidecar_artifact_refs = [
        _artifact_ref(artifact_key, str(path_map.get(artifact_key) or ""))
        for artifact_key in reviewer_sidecar_artifact_keys
        if str(path_map.get(artifact_key) or "").strip()
    ]
    traceability_completeness = "4/4 linked"

    traceability_bundle = _bundle(
        run_id=run_id,
        artifact_type="software_validation_traceability_matrix",
        filename=filenames["software_validation_traceability_matrix"],
        markdown_filename=filenames["software_validation_traceability_matrix_markdown"],
        artifact_role="execution_summary",
        title_text="Software Validation Traceability Matrix",
        reviewer_note=reviewer_note,
        summary_text=f"Software validation traceability linked {traceability_completeness}.",
        summary_lines=[
            f"traceability_id: {traceability_id}",
            f"traceability_version: {traceability_version}",
            f"scope / decision rule: {scope_id} | {decision_rule_id}",
            f"uncertainty / method: {uncertainty_case_id} | {method_confirmation_protocol_id}",
            f"traceability completeness: {traceability_completeness}",
        ],
        detail_lines=[
            f"linked assets / certificates: {linked_assets_certificates_summary['summary']}",
            f"impact scope: {' | '.join(impact_scope)}",
            f"change set refs: {' | '.join(change_set_refs)}",
            f"limitation: {limitation_note}",
            f"non-claim: {non_claim_note}",
        ],
        artifact_paths=_bundle_path_map(path_map, "software_validation_traceability_matrix", "software_validation_traceability_matrix_markdown"),
        body={
            "traceability_id": traceability_id,
            "traceability_version": traceability_version,
            "scope_id": scope_id,
            "decision_rule_id": decision_rule_id,
            "uncertainty_case_id": uncertainty_case_id,
            "method_confirmation_protocol_id": method_confirmation_protocol_id,
            "traceability_completeness": traceability_completeness,
            "requirement_refs": list(requirement_refs),
            "traceability_rows": list(traceability_rows),
            "design_refs": list(design_refs),
            "code_refs": list(code_refs),
            "test_refs": list(test_refs),
            "artifact_refs": list(traceability_artifact_refs),
            "change_set_refs": list(change_set_refs),
            "impact_scope": list(impact_scope),
            "changed_modules": list(changed_modules),
            "changed_module_paths": list(changed_module_paths),
            "linked_surface_visibility": list(LINKED_REVIEW_SURFACES),
            "linked_surface_summary": linked_surface_summary,
            "reviewer_note": reviewer_note,
            "limitation_note": limitation_note,
            "non_claim_note": non_claim_note,
        },
        digest=_digest(
            summary=f"Software validation traceability linked {traceability_completeness}.",
            scope_overview_summary=str(scope_digest.get("scope_overview_summary") or scope_raw.get("scope_name") or scope_id),
            decision_rule_summary=str(decision_digest.get("decision_rule_summary") or decision_rule_id),
            conformity_boundary_summary=non_claim_note,
            current_coverage_summary=traceability_completeness,
            missing_evidence_summary=limitation_note,
            reviewer_next_step_digest="Review the chain, keep it reviewer-only, and use release_manifest for pack-level linkage.",
            non_claim_digest=non_claim_note,
            scope_reference_assets_summary=linked_assets_certificates_summary["summary"],
            decision_rule_dependency_summary=f"uncertainty {uncertainty_case_id} | method {method_confirmation_protocol_id}",
        ),
        boundary_statements=boundary_statements,
        evidence_categories=["recognition_readiness", "software_validation", "traceability"],
    )

    links_bundle = _bundle(
        run_id=run_id,
        artifact_type="requirement_design_code_test_links",
        filename=filenames["requirement_design_code_test_links"],
        markdown_filename=filenames["requirement_design_code_test_links_markdown"],
        artifact_role="execution_summary",
        title_text="Requirement Design Code Test Links",
        reviewer_note=reviewer_note,
        summary_text="Requirement/design/code/test links stay reviewer-facing only.",
        summary_lines=[f"scope_id: {scope_id}", f"decision_rule_id: {decision_rule_id}", f"test refs: {len(test_refs)}"],
        detail_lines=[
            f"code refs: {' | '.join(code_refs)}",
            f"changed modules: {changed_modules_summary}",
            f"visible surfaces: {linked_surface_summary}",
            f"non-claim: {non_claim_note}",
        ],
        artifact_paths=_bundle_path_map(path_map, "requirement_design_code_test_links", "requirement_design_code_test_links_markdown"),
        body={
            "traceability_id": traceability_id,
            "traceability_version": traceability_version,
            "scope_id": scope_id,
            "decision_rule_id": decision_rule_id,
            "uncertainty_case_id": uncertainty_case_id,
            "method_confirmation_protocol_id": method_confirmation_protocol_id,
            "requirement_refs": list(requirement_refs),
            "traceability_rows": list(traceability_rows),
            "design_refs": list(design_refs),
            "code_refs": list(code_refs),
            "test_refs": list(test_refs),
            "artifact_refs": list(traceability_artifact_refs),
            "change_set_refs": list(change_set_refs),
            "impact_scope": list(impact_scope),
            "changed_modules": list(changed_modules),
            "changed_module_paths": list(changed_module_paths),
            "changed_modules_summary": changed_modules_summary,
            "linked_surface_visibility": list(LINKED_REVIEW_SURFACES),
            "linked_surface_summary": linked_surface_summary,
            "reviewer_note": reviewer_note,
            "limitation_note": limitation_note,
            "non_claim_note": non_claim_note,
        },
        digest=_digest(
            summary="Requirement/design/code/test links stay reviewer-facing only.",
            scope_overview_summary=scope_id,
            decision_rule_summary=decision_rule_id,
            conformity_boundary_summary=non_claim_note,
            current_coverage_summary=f"tests {len(test_refs)} | code refs {len(code_refs)}",
            missing_evidence_summary=limitation_note,
            reviewer_next_step_digest="Use this table to inspect direct linkage before reviewer sign-off discussions.",
            non_claim_digest=non_claim_note,
        ),
        boundary_statements=boundary_statements,
        evidence_categories=["recognition_readiness", "software_validation", "traceability"],
    )

    evidence_bundle = _bundle(
        run_id=run_id,
        artifact_type="validation_evidence_index",
        filename=filenames["validation_evidence_index"],
        markdown_filename=filenames["validation_evidence_index_markdown"],
        artifact_role="diagnostic_analysis",
        title_text="Validation Evidence Index",
        reviewer_note=reviewer_note,
        summary_text="Validation evidence index links reviewer-facing artifacts, tests, and suite statuses.",
        summary_lines=[
            f"artifact refs: {len(traceability_artifact_refs)}",
            f"test refs: {len(test_refs)}",
            f"parity / resilience / smoke: {parity_status} | {resilience_status} | {smoke_status}",
        ],
        detail_lines=[
            f"uncertainty digest: {str(uncertainty_digest.get('summary') or '--')}",
            f"verification digest: {str(method_digest.get('summary') or '--')}",
            f"release sidecars: {len(reviewer_sidecar_artifact_refs)}",
            f"visible surfaces: {linked_surface_summary}",
        ],
        artifact_paths=_bundle_path_map(path_map, "validation_evidence_index", "validation_evidence_index_markdown"),
        body={
            "traceability_id": traceability_id,
            "traceability_version": traceability_version,
            "scope_id": scope_id,
            "decision_rule_id": decision_rule_id,
            "uncertainty_case_id": uncertainty_case_id,
            "method_confirmation_protocol_id": method_confirmation_protocol_id,
            "artifact_refs": list(traceability_artifact_refs),
            "test_refs": list(test_refs),
            "linked_test_suites": [
                {"suite_id": "parity", "status": parity_status},
                {"suite_id": "resilience", "status": resilience_status},
                {"suite_id": "smoke", "status": smoke_status},
            ],
            "change_set_refs": list(change_set_refs),
            "impact_scope": list(impact_scope),
            "parity_status": parity_status,
            "resilience_status": resilience_status,
            "smoke_status": smoke_status,
            "linked_surface_visibility": list(LINKED_REVIEW_SURFACES),
            "linked_surface_summary": linked_surface_summary,
            "input_artifact_refs": list(traceability_artifact_refs),
            "reviewer_sidecar_artifact_refs": list(reviewer_sidecar_artifact_refs),
            "evidence_rows": [
                {
                    "row_type": "upstream_input_artifact",
                    "artifact_type": str(item.get("artifact_type") or ""),
                    "path": str(item.get("path") or ""),
                }
                for item in traceability_artifact_refs
            ]
            + [
                {
                    "row_type": "linked_test_suite",
                    "suite_id": suite_id,
                    "status": suite_status,
                }
                for suite_id, suite_status in (
                    ("parity", parity_status),
                    ("resilience", resilience_status),
                    ("smoke", smoke_status),
                )
            ],
            "reviewer_note": reviewer_note,
            "limitation_note": limitation_note,
            "non_claim_note": non_claim_note,
        },
        digest=_digest(
            summary="Validation evidence index links artifacts, tests, and suite linkage.",
            scope_overview_summary=scope_id,
            decision_rule_summary=decision_rule_id,
            conformity_boundary_summary=non_claim_note,
            current_coverage_summary=f"parity {parity_status} | resilience {resilience_status} | smoke {smoke_status}",
            missing_evidence_summary=limitation_note,
            reviewer_next_step_digest="Review parity/resilience/smoke linkage before using the release pack for reviewer mapping.",
            non_claim_digest=non_claim_note,
        ),
        boundary_statements=boundary_statements,
        evidence_categories=["recognition_readiness", "software_validation", "validation_evidence"],
    )
    change_impact_bundle = _bundle(
        run_id=run_id,
        artifact_type="change_impact_summary",
        filename=filenames["change_impact_summary"],
        markdown_filename=filenames["change_impact_summary_markdown"],
        artifact_role="diagnostic_analysis",
        title_text="Change Impact Summary",
        reviewer_note=reviewer_note,
        summary_text="Change impact stays within reviewer sidecars and linked read-only surfaces.",
        summary_lines=[
            f"changed modules: {changed_modules_summary}",
            "main execution chain impacted: no",
            "artifact schema impacted: reviewer-sidecar only",
            "results / review_center / workbench: yes | yes | yes",
        ],
        detail_lines=[
            f"impact scope: {' | '.join(impact_scope)}",
            f"change refs: {' | '.join(change_set_refs)}",
            f"visible surfaces: {linked_surface_summary}",
            "primary evidence rewritten: false",
            "default DB path: disabled",
            f"limitation: {limitation_note}",
        ],
        artifact_paths=_bundle_path_map(path_map, "change_impact_summary", "change_impact_summary_markdown"),
        body={
            "traceability_id": traceability_id,
            "traceability_version": traceability_version,
            "scope_id": scope_id,
            "decision_rule_id": decision_rule_id,
            "uncertainty_case_id": uncertainty_case_id,
            "method_confirmation_protocol_id": method_confirmation_protocol_id,
            "change_set_refs": list(change_set_refs),
            "impact_scope": list(impact_scope),
            "changed_modules": list(changed_modules),
            "changed_module_paths": list(changed_module_paths),
            "changed_modules_summary": changed_modules_summary,
            "impacts_main_execution_chain": False,
            "main_execution_chain_impact_summary": main_execution_chain_impact_summary,
            "impacts_artifact_schema": True,
            "artifact_schema_impact_summary": artifact_schema_impact_summary,
            "impacts_results_surface": True,
            "results_surface_impact_summary": results_surface_impact_summary,
            "impacts_review_center_surface": True,
            "review_center_surface_impact_summary": review_center_impact_summary,
            "impacts_workbench_surface": True,
            "workbench_surface_impact_summary": workbench_surface_impact_summary,
            "impacts_reports_surface": True,
            "reports_surface_impact_summary": "Yes; report rows expose each sidecar with reviewer-facing notes.",
            "linked_surface_visibility": list(LINKED_REVIEW_SURFACES),
            "linked_surface_summary": linked_surface_summary,
            "db_ready_stub_only": True,
            "primary_evidence_rewritten": False,
            "reviewer_note": reviewer_note,
            "limitation_note": limitation_note,
            "non_claim_note": non_claim_note,
        },
        digest=_digest(
            summary="Change impact stays within reviewer sidecars and linked read-only surfaces.",
            scope_overview_summary=scope_id,
            decision_rule_summary=decision_rule_id,
            conformity_boundary_summary=non_claim_note,
            current_coverage_summary=changed_modules_summary,
            missing_evidence_summary="No primary evidence rewrite or default DB path is introduced.",
            reviewer_next_step_digest="Keep impact limited to reviewer-facing sidecars and linked read-only surfaces.",
            non_claim_digest=non_claim_note,
        ),
        boundary_statements=boundary_statements,
        evidence_categories=["recognition_readiness", "software_validation", "change_impact"],
    )

    rollback_bundle = _bundle(
        run_id=run_id,
        artifact_type="rollback_readiness_summary",
        filename=filenames["rollback_readiness_summary"],
        markdown_filename=filenames["rollback_readiness_summary_markdown"],
        artifact_role="diagnostic_analysis",
        title_text="Rollback Readiness Summary",
        reviewer_note=reviewer_note,
        summary_text="Rollback readiness remains sidecar-first and non-destructive.",
        summary_lines=[
            "rollback mode: file-artifact-first",
            "sidecar revocable: true",
            "touch primary evidence: false",
            "default DB path: disabled",
        ],
        detail_lines=[
            *rollback_steps,
            f"rollback scope: {len(reviewer_sidecar_artifact_refs)} reviewer-sidecar artifacts",
            f"visible surfaces after rebuild: {linked_surface_summary}",
        ],
        artifact_paths=_bundle_path_map(path_map, "rollback_readiness_summary", "rollback_readiness_summary_markdown"),
        body={
            "traceability_id": traceability_id,
            "traceability_version": traceability_version,
            "scope_id": scope_id,
            "decision_rule_id": decision_rule_id,
            "uncertainty_case_id": uncertainty_case_id,
            "method_confirmation_protocol_id": method_confirmation_protocol_id,
            "change_set_refs": list(change_set_refs),
            "impact_scope": list(impact_scope),
            "rollback_mode": "file_artifact_first",
            "rollback_scope_summary": file_artifact_first_summary,
            "file_artifact_first": True,
            "sidecar_revocable": True,
            "primary_evidence_preserved": True,
            "touches_primary_evidence": False,
            "rollback_steps": list(rollback_steps),
            "rollback_scope_artifacts": list(reviewer_sidecar_artifact_refs),
            "linked_surface_visibility": list(LINKED_REVIEW_SURFACES),
            "linked_surface_summary": linked_surface_summary,
            "db_ready_stub_only": True,
            "rollback_ready": True,
            "primary_evidence_rewritten": False,
            "reviewer_note": reviewer_note,
            "limitation_note": limitation_note,
            "non_claim_note": non_claim_note,
        },
        digest=_digest(
            summary="Rollback readiness remains sidecar-first and non-destructive.",
            scope_overview_summary=scope_id,
            decision_rule_summary=decision_rule_id,
            conformity_boundary_summary=non_claim_note,
            current_coverage_summary=file_artifact_first_summary,
            missing_evidence_summary="Rollback does not provide real release approval or formal compliance closure.",
            reviewer_next_step_digest="If rollback is needed, remove new sidecars and regenerate reviewer indexes only.",
            non_claim_digest=non_claim_note,
        ),
        boundary_statements=boundary_statements,
        evidence_categories=["recognition_readiness", "software_validation", "rollback"],
    )

    environment_summary = f"python {sys.version.split()[0]} | platform {platform.platform()} | repo {repo_ref} | mode {WORKSPACE_MODE}"
    environment_bundle = _bundle(
        run_id=run_id,
        artifact_type="environment_fingerprint",
        filename=filenames["environment_fingerprint"],
        markdown_filename=filenames["environment_fingerprint_markdown"],
        artifact_role="diagnostic_analysis",
        title_text="Environment Fingerprint",
        reviewer_note=reviewer_note,
        summary_text="Environment fingerprint recorded for reviewer linkage.",
        summary_lines=[environment_summary, f"repo / branch: {repo_ref} | {branch_or_head}"],
        detail_lines=[
            f"generated_by_tool: {GENERATED_BY_TOOL}",
            "fingerprint scope: file-backed reviewer trace",
            "formal anti-tamper claim: false",
            "primary_evidence_rewritten: false",
            "reviewer_only: true",
        ],
        artifact_paths=_bundle_path_map(path_map, "environment_fingerprint", "environment_fingerprint_markdown"),
        body={
            "environment_summary": environment_summary,
            "python_version": sys.version.split()[0],
            "platform": platform.platform(),
            "repo_ref": repo_ref,
            "branch_or_head": branch_or_head,
            "workspace_mode": WORKSPACE_MODE,
            "generated_by_tool": GENERATED_BY_TOOL,
            "linked_run_id": run_id,
            "linked_scope_id": scope_id,
            "linked_release_manifest_id": release_id,
            "fingerprint_kind": "environment_fingerprint",
            "fingerprint_scope": "file_backed_reviewer_trace",
            "reviewer_trace_only": True,
            "formal_anti_tamper_claim": False,
            "tamper_evidence_claimed": False,
            "fingerprint_inputs": {
                "python_version": sys.version.split()[0],
                "platform": platform.platform(),
                "repo_ref": repo_ref,
                "branch_or_head": branch_or_head,
                "workspace_mode": WORKSPACE_MODE,
            },
            "reviewer_note": reviewer_note,
            "limitation_note": limitation_note,
            "non_claim_note": non_claim_note,
        },
        digest=_digest(
            summary="Environment fingerprint recorded for reviewer linkage.",
            scope_overview_summary=scope_id,
            decision_rule_summary=decision_rule_id,
            conformity_boundary_summary=non_claim_note,
            current_coverage_summary=environment_summary,
            missing_evidence_summary="No real audit or anti-tamper guarantee is implied by this fingerprint.",
            reviewer_next_step_digest="Use the fingerprint as reviewer context only.",
            non_claim_digest=non_claim_note,
        ),
        boundary_statements=boundary_statements,
        evidence_categories=["recognition_readiness", "audit_hash", "environment"],
    )

    config_fingerprint = _hash_bytes(
        _canonical_json_bytes(
            {
                "config_version": str(lineage_payload.get("config_version") or version_payload.get("config_version") or ""),
                "profile_version": str(lineage_payload.get("profile_version") or version_payload.get("profile_version") or ""),
                "points_version": str(lineage_payload.get("points_version") or version_payload.get("points_version") or ""),
                "algorithm_version": str(version_payload.get("algorithm_version") or ""),
            }
        )
    )
    config_bundle = _bundle(
        run_id=run_id,
        artifact_type="config_fingerprint",
        filename=filenames["config_fingerprint"],
        markdown_filename=filenames["config_fingerprint_markdown"],
        artifact_role="diagnostic_analysis",
        title_text="Config Fingerprint",
        reviewer_note=reviewer_note,
        summary_text="Config fingerprint linked to the Step 2 release inputs for reviewer trace only.",
        summary_lines=[
            f"config_version: {str(lineage_payload.get('config_version') or version_payload.get('config_version') or '--')}",
            f"profile_version: {str(lineage_payload.get('profile_version') or version_payload.get('profile_version') or '--')}",
            f"points_version: {str(lineage_payload.get('points_version') or version_payload.get('points_version') or '--')}",
        ],
        detail_lines=[
            f"algorithm_version: {str(version_payload.get('algorithm_version') or '--')}",
            f"config_fingerprint: {config_fingerprint}",
            "fingerprint scope: file-backed reviewer trace",
            "formal anti-tamper claim: false",
        ],
        artifact_paths=_bundle_path_map(path_map, "config_fingerprint", "config_fingerprint_markdown"),
        body={
            "linked_run_id": run_id,
            "linked_scope_id": scope_id,
            "linked_release_manifest_id": release_id,
            "config_version": str(lineage_payload.get("config_version") or version_payload.get("config_version") or ""),
            "profile_version": str(lineage_payload.get("profile_version") or version_payload.get("profile_version") or ""),
            "points_version": str(lineage_payload.get("points_version") or version_payload.get("points_version") or ""),
            "algorithm_version": str(version_payload.get("algorithm_version") or ""),
            "config_fingerprint": config_fingerprint,
            "generated_by_tool": GENERATED_BY_TOOL,
            "workspace_mode": WORKSPACE_MODE,
            "repo_ref": repo_ref,
            "fingerprint_kind": "config_fingerprint",
            "fingerprint_scope": "file_backed_reviewer_trace",
            "reviewer_trace_only": True,
            "formal_anti_tamper_claim": False,
            "tamper_evidence_claimed": False,
            "fingerprint_inputs": {
                "config_version": str(lineage_payload.get("config_version") or version_payload.get("config_version") or ""),
                "profile_version": str(lineage_payload.get("profile_version") or version_payload.get("profile_version") or ""),
                "points_version": str(lineage_payload.get("points_version") or version_payload.get("points_version") or ""),
                "algorithm_version": str(version_payload.get("algorithm_version") or ""),
            },
            "reviewer_note": reviewer_note,
            "limitation_note": limitation_note,
            "non_claim_note": non_claim_note,
        },
        digest=_digest(
            summary="Config fingerprint linked to the Step 2 release inputs for reviewer trace only.",
            scope_overview_summary=scope_id,
            decision_rule_summary=decision_rule_id,
            conformity_boundary_summary=non_claim_note,
            current_coverage_summary=f"config {str(lineage_payload.get('config_version') or version_payload.get('config_version') or '--')} | profile {str(lineage_payload.get('profile_version') or version_payload.get('profile_version') or '--')} | points {str(lineage_payload.get('points_version') or version_payload.get('points_version') or '--')}",
            missing_evidence_summary="Config fingerprint is reviewer-facing and does not replace released configuration governance.",
            reviewer_next_step_digest="Verify the linked config/profile/points versions before using the manifest.",
            non_claim_digest=non_claim_note,
        ),
        boundary_statements=boundary_statements,
        evidence_categories=["recognition_readiness", "audit_hash", "configuration"],
    )

    release_input_digest_value = _hash_bytes(
        _canonical_json_bytes(
            {
                "scope_id": scope_id,
                "decision_rule_id": decision_rule_id,
                "uncertainty_case_id": uncertainty_case_id,
                "method_confirmation_protocol_id": method_confirmation_protocol_id,
                "parity_status": parity_status,
                "resilience_status": resilience_status,
                "smoke_status": smoke_status,
                "config_fingerprint": config_fingerprint,
                "environment_summary": environment_summary,
            }
        )
    )
    release_input_bundle = _bundle(
        run_id=run_id,
        artifact_type="release_input_digest",
        filename=filenames["release_input_digest"],
        markdown_filename=filenames["release_input_digest_markdown"],
        artifact_role="diagnostic_analysis",
        title_text="Release Input Digest",
        reviewer_note=reviewer_note,
        summary_text="Release input digest linked all Step 2 reviewer inputs as file-backed reviewer trace.",
        summary_lines=[f"release_input_digest: {release_input_digest_value}", f"parity / resilience / smoke: {parity_status} | {resilience_status} | {smoke_status}"],
        detail_lines=[
            f"scope / decision rule: {scope_id} | {decision_rule_id}",
            f"uncertainty / method: {uncertainty_case_id} | {method_confirmation_protocol_id}",
            f"config fingerprint: {config_fingerprint}",
            "formal anti-tamper claim: false",
        ],
        artifact_paths=_bundle_path_map(path_map, "release_input_digest", "release_input_digest_markdown"),
        body={
            "linked_run_id": run_id,
            "linked_scope_id": scope_id,
            "linked_release_manifest_id": release_id,
            "repo_ref": repo_ref,
            "branch_or_head": branch_or_head,
            "workspace_mode": WORKSPACE_MODE,
            "input_ids": {
                "scope_id": scope_id,
                "decision_rule_id": decision_rule_id,
                "uncertainty_case_id": uncertainty_case_id,
                "method_confirmation_protocol_id": method_confirmation_protocol_id,
            },
            "status_inputs": {
                "parity_status": parity_status,
                "resilience_status": resilience_status,
                "smoke_status": smoke_status,
            },
            "release_input_digest": release_input_digest_value,
            "generated_by_tool": GENERATED_BY_TOOL,
            "digest_kind": "release_input_digest",
            "digest_scope": "file_backed_reviewer_trace",
            "reviewer_trace_only": True,
            "formal_anti_tamper_claim": False,
            "tamper_evidence_claimed": False,
            "linked_config_fingerprint": _artifact_ref("config_fingerprint", str(path_map.get("config_fingerprint") or "")),
            "linked_environment_fingerprint": _artifact_ref("environment_fingerprint", str(path_map.get("environment_fingerprint") or "")),
            "reviewer_note": reviewer_note,
            "limitation_note": limitation_note,
            "non_claim_note": non_claim_note,
        },
        digest=_digest(
            summary="Release input digest linked all Step 2 reviewer inputs as file-backed reviewer trace.",
            scope_overview_summary=scope_id,
            decision_rule_summary=decision_rule_id,
            conformity_boundary_summary=non_claim_note,
            current_coverage_summary=f"uncertainty {uncertainty_case_id} | method {method_confirmation_protocol_id} | parity {parity_status} | resilience {resilience_status} | smoke {smoke_status}",
            missing_evidence_summary="Release input digest is not a formal approval or release authorization record.",
            reviewer_next_step_digest="Use the digest to verify reviewer-facing input consistency only.",
            non_claim_digest=non_claim_note,
        ),
        boundary_statements=boundary_statements,
        evidence_categories=["recognition_readiness", "audit_hash", "release_inputs"],
    )
    hash_registry_entries: list[dict[str, Any]] = []
    for artifact_key, payload in [
        ("scope_definition_pack", scope_raw),
        ("decision_rule_profile", decision_raw),
        ("reference_asset_registry", reference_raw),
        ("certificate_lifecycle_summary", certificate_raw),
        ("pre_run_readiness_gate", gate_raw),
        ("uncertainty_report_pack", uncertainty_report_raw),
        ("uncertainty_rollup", uncertainty_rollup_raw),
        ("method_confirmation_protocol", method_protocol_raw),
        ("verification_digest", verification_digest_raw),
        ("verification_rollup", verification_rollup_raw),
        ("software_validation_traceability_matrix", dict(traceability_bundle.get("raw") or {})),
        ("requirement_design_code_test_links", dict(links_bundle.get("raw") or {})),
        ("validation_evidence_index", dict(evidence_bundle.get("raw") or {})),
        ("change_impact_summary", dict(change_impact_bundle.get("raw") or {})),
        ("rollback_readiness_summary", dict(rollback_bundle.get("raw") or {})),
        ("environment_fingerprint", dict(environment_bundle.get("raw") or {})),
        ("config_fingerprint", dict(config_bundle.get("raw") or {})),
        ("release_input_digest", dict(release_input_bundle.get("raw") or {})),
    ]:
        artifact_path = str(path_map.get(artifact_key) or "")
        hash_registry_entries.append(
            {
                "hash_registry_id": hash_registry_id,
                "artifact_type": artifact_key,
                "artifact_path": artifact_path,
                "content_hash": _hash_payload_or_path(artifact_path, payload),
                "hash_algorithm": HASH_ALGORITHM,
                "linked_run_id": run_id,
                "linked_scope_id": scope_id,
                "linked_release_manifest_id": release_id,
                "generated_at": _now_iso(),
                "generated_by_tool": GENERATED_BY_TOOL,
                "environment_summary": environment_summary,
                "python_version": sys.version.split()[0],
                "platform": platform.platform(),
                "repo_ref": repo_ref,
                "workspace_mode": WORKSPACE_MODE,
                "primary_evidence_rewritten": False,
                "reviewer_only": True,
                "not_real_acceptance_evidence": True,
                "reviewer_trace_only": True,
                "file_backed_only": True,
                "formal_anti_tamper_claim": False,
                "tamper_evidence_claimed": False,
                "trace_purpose": "file_backed_reviewer_trace",
            }
        )

    hash_registry_bundle = _bundle(
        run_id=run_id,
        artifact_type="artifact_hash_registry",
        filename=filenames["artifact_hash_registry"],
        markdown_filename=filenames["artifact_hash_registry_markdown"],
        artifact_role="diagnostic_analysis",
        title_text="Artifact Hash Registry",
        reviewer_note=reviewer_note,
        summary_text=f"Artifact hash registry captured {len(hash_registry_entries)} reviewer-facing hashes.",
        summary_lines=[f"hash_registry_id: {hash_registry_id}", f"entries: {len(hash_registry_entries)}", f"hash_algorithm: {HASH_ALGORITHM}"],
        detail_lines=[
            environment_summary,
            "trace purpose: file-backed reviewer trace",
            "formal anti-tamper claim: false",
            "primary_evidence_rewritten: false",
            "reviewer_only: true",
        ],
        artifact_paths=_bundle_path_map(path_map, "artifact_hash_registry", "artifact_hash_registry_markdown"),
        body={
            "hash_registry_id": hash_registry_id,
            "scope_id": scope_id,
            "decision_rule_id": decision_rule_id,
            "linked_scope_id": scope_id,
            "linked_release_manifest_id": release_id,
            "generated_by_tool": GENERATED_BY_TOOL,
            "environment_summary": environment_summary,
            "python_version": sys.version.split()[0],
            "platform": platform.platform(),
            "repo_ref": repo_ref,
            "workspace_mode": WORKSPACE_MODE,
            "hash_algorithm": HASH_ALGORITHM,
            "entries": hash_registry_entries,
            "trace_purpose": "file_backed_reviewer_trace",
            "reviewer_trace_only": True,
            "file_backed_only": True,
            "formal_anti_tamper_claim": False,
            "tamper_evidence_claimed": False,
            "reviewer_note": reviewer_note,
            "limitation_note": limitation_note,
            "non_claim_note": non_claim_note,
        },
        digest=_digest(
            summary=f"Artifact hash registry captured {len(hash_registry_entries)} reviewer-facing hashes.",
            scope_overview_summary=scope_id,
            decision_rule_summary=decision_rule_id,
            conformity_boundary_summary=non_claim_note,
            current_coverage_summary=f"hash algorithm {HASH_ALGORITHM} | entries {len(hash_registry_entries)}",
            missing_evidence_summary="Hash registry is file-backed only and does not claim formal anti-tamper protection.",
            reviewer_next_step_digest="Use hash entries as reviewer traceability context in results, workbench, review center, and historical views.",
            non_claim_digest=non_claim_note,
        ),
        boundary_statements=boundary_statements,
        evidence_categories=["recognition_readiness", "audit_hash", "hash_registry"],
    )

    audit_event_bundle = _bundle(
        run_id=run_id,
        artifact_type="audit_event_store",
        filename=filenames["audit_event_store"],
        markdown_filename=filenames["audit_event_store_markdown"],
        artifact_role="diagnostic_analysis",
        title_text="Audit Event Store",
        reviewer_note=reviewer_note,
        summary_text="Audit event store captured reviewer-facing assembly events.",
        summary_lines=["events: 3", f"linked release: {release_id}", f"hash registry: {hash_registry_id}"],
        detail_lines=[
            f"Built traceability matrix {traceability_id}.",
            f"Collected {len(hash_registry_entries)} hash rows for reviewer-facing artifacts.",
            f"Linked parity={parity_status}, resilience={resilience_status}, smoke={smoke_status}.",
        ],
        artifact_paths=_bundle_path_map(path_map, "audit_event_store", "audit_event_store_markdown"),
        body={
            "hash_registry_id": hash_registry_id,
            "linked_run_id": run_id,
            "linked_scope_id": scope_id,
            "linked_release_manifest_id": release_id,
            "events": [
                {"event_id": f"{run_id}-traceability-built", "event_type": "software_validation_traceability_built", "event_time": _now_iso()},
                {"event_id": f"{run_id}-hash-registry-built", "event_type": "artifact_hash_registry_built", "event_time": _now_iso()},
                {"event_id": f"{run_id}-release-pack-linked", "event_type": "release_pack_linked", "event_time": _now_iso()},
            ],
            "environment_summary": environment_summary,
            "python_version": sys.version.split()[0],
            "platform": platform.platform(),
            "repo_ref": repo_ref,
            "workspace_mode": WORKSPACE_MODE,
            "generated_by_tool": GENERATED_BY_TOOL,
            "event_store_mode": "file_backed_reviewer_trace",
            "reviewer_trace_only": True,
            "reviewer_note": reviewer_note,
            "limitation_note": limitation_note,
            "non_claim_note": non_claim_note,
        },
        digest=_digest(
            summary="Audit event store captured reviewer-facing assembly events.",
            scope_overview_summary=scope_id,
            decision_rule_summary=decision_rule_id,
            conformity_boundary_summary=non_claim_note,
            current_coverage_summary=f"release {release_id} | hash registry {hash_registry_id}",
            missing_evidence_summary="Event rows are reviewer artifacts only and not a formal audit ledger.",
            reviewer_next_step_digest="Use event rows to explain how the Step 2 pack was assembled.",
            non_claim_digest=non_claim_note,
        ),
        boundary_statements=boundary_statements,
        evidence_categories=["recognition_readiness", "audit_hash", "audit_events"],
    )

    release_scope_bundle = _bundle(
        run_id=run_id,
        artifact_type="release_scope_summary",
        filename=filenames["release_scope_summary"],
        markdown_filename=filenames["release_scope_summary_markdown"],
        artifact_role="execution_summary",
        title_text="Release Scope Summary",
        reviewer_note=reviewer_note,
        summary_text=f"Release scope summary linked scope {scope_id}.",
        summary_lines=[f"scope_id: {scope_id}", f"decision_rule_id: {decision_rule_id}", f"assets / certificates: {linked_assets_certificates_summary['summary']}"],
        detail_lines=[
            f"uncertainty_case_id: {uncertainty_case_id}",
            f"method_confirmation_protocol_id: {method_confirmation_protocol_id}",
            f"visible surfaces: {linked_surface_summary}",
        ],
        artifact_paths=_bundle_path_map(path_map, "release_scope_summary", "release_scope_summary_markdown"),
        body={
            "release_id": release_id,
            "release_version": release_version,
            "scope_id": scope_id,
            "decision_rule_id": decision_rule_id,
            "linked_scope_ids": [scope_id],
            "linked_decision_rules": [decision_rule_id],
            "linked_assets_certificates_summary": dict(linked_assets_certificates_summary),
            "linked_uncertainty_cases": [uncertainty_case_id],
            "linked_method_confirmation_protocols": [method_confirmation_protocol_id],
            "linked_surface_visibility": list(LINKED_REVIEW_SURFACES),
            "reviewer_note": reviewer_note,
            "limitation_note": limitation_note,
            "non_claim_note": non_claim_note,
        },
        digest=_digest(
            summary=f"Release scope summary linked scope {scope_id}.",
            scope_overview_summary=scope_id,
            decision_rule_summary=decision_rule_id,
            conformity_boundary_summary=non_claim_note,
            current_coverage_summary=linked_assets_certificates_summary["summary"],
            missing_evidence_summary=limitation_note,
            reviewer_next_step_digest="Check the linked scope, assets, and certificates before treating the manifest as reviewer context.",
            non_claim_digest=non_claim_note,
        ),
        boundary_statements=boundary_statements,
        evidence_categories=["recognition_readiness", "release_manifest", "release_scope"],
    )

    release_boundary_bundle = _bundle(
        run_id=run_id,
        artifact_type="release_boundary_digest",
        filename=filenames["release_boundary_digest"],
        markdown_filename=filenames["release_boundary_digest_markdown"],
        artifact_role="diagnostic_analysis",
        title_text="Release Boundary Digest",
        reviewer_note=reviewer_note,
        summary_text="Release boundary digest keeps the pack reviewer-only and simulation-only.",
        summary_lines=["simulation_only: true", "reviewer_only: true", "not_real_acceptance_evidence: true", "not_ready_for_formal_claim: true"],
        detail_lines=[f"limitation: {limitation_note}", f"non-claim: {non_claim_note}", *reviewer_actions],
        artifact_paths=_bundle_path_map(path_map, "release_boundary_digest", "release_boundary_digest_markdown"),
        body={
            "release_id": release_id,
            "release_version": release_version,
            "simulation_only": True,
            "reviewer_only": True,
            "not_real_acceptance_evidence": True,
            "not_ready_for_formal_claim": True,
            "non_claim_note": non_claim_note,
            "limitation_note": limitation_note,
            "reviewer_note": reviewer_note,
            "reviewer_actions": list(reviewer_actions),
        },
        digest=_digest(
            summary="Release boundary digest keeps the pack reviewer-only and simulation-only.",
            scope_overview_summary=scope_id,
            decision_rule_summary=decision_rule_id,
            conformity_boundary_summary=non_claim_note,
            current_coverage_summary="simulation_only | reviewer_only | readiness_mapping_only",
            missing_evidence_summary=limitation_note,
            reviewer_next_step_digest="Keep the pack outside real release approval, formal compliance claims, and accreditation claims.",
            non_claim_digest=non_claim_note,
        ),
        boundary_statements=boundary_statements,
        evidence_categories=["recognition_readiness", "release_manifest", "release_boundary"],
    )

    release_evidence_bundle = _bundle(
        run_id=run_id,
        artifact_type="release_evidence_pack_index",
        filename=filenames["release_evidence_pack_index"],
        markdown_filename=filenames["release_evidence_pack_index_markdown"],
        artifact_role="execution_summary",
        title_text="Release Evidence Pack Index",
        reviewer_note=reviewer_note,
        summary_text="Release evidence pack index linked reviewer-facing artifacts.",
        summary_lines=[
            f"artifact pack rows: {len(reviewer_sidecar_artifact_refs)} sidecars | {len(traceability_artifact_refs)} upstream inputs",
            f"suite linkage: parity {parity_status} | resilience {resilience_status} | smoke {smoke_status}",
        ],
        detail_lines=[
            f"scope / decision rule: {scope_id} | {decision_rule_id}",
            f"uncertainty / method: {uncertainty_case_id} | {method_confirmation_protocol_id}",
            f"visible surfaces: {linked_surface_summary}",
        ],
        artifact_paths=_bundle_path_map(path_map, "release_evidence_pack_index", "release_evidence_pack_index_markdown"),
        body={
            "release_id": release_id,
            "release_version": release_version,
            "scope_id": scope_id,
            "decision_rule_id": decision_rule_id,
            "uncertainty_case_id": uncertainty_case_id,
            "method_confirmation_protocol_id": method_confirmation_protocol_id,
            "artifact_refs": list(reviewer_sidecar_artifact_refs),
            "upstream_artifact_refs": list(traceability_artifact_refs),
            "linked_test_suites": [
                {"suite_id": "parity", "status": parity_status},
                {"suite_id": "resilience", "status": resilience_status},
                {"suite_id": "smoke", "status": smoke_status},
            ],
            "parity_status": parity_status,
            "resilience_status": resilience_status,
            "smoke_status": smoke_status,
            "linked_change_impact_summary": _artifact_ref("change_impact_summary", str(path_map.get("change_impact_summary") or "")),
            "linked_rollback_readiness_summary": _artifact_ref(
                "rollback_readiness_summary",
                str(path_map.get("rollback_readiness_summary") or ""),
            ),
            "linked_release_validation_manifest": _artifact_ref(
                "release_validation_manifest",
                str(path_map.get("release_validation_manifest") or ""),
            ),
            "linked_surface_visibility": list(LINKED_REVIEW_SURFACES),
            "reviewer_note": reviewer_note,
            "limitation_note": limitation_note,
            "non_claim_note": non_claim_note,
        },
        digest=_digest(
            summary="Release evidence pack index linked reviewer-facing artifacts.",
            scope_overview_summary=scope_id,
            decision_rule_summary=decision_rule_id,
            conformity_boundary_summary=non_claim_note,
            current_coverage_summary=f"parity {parity_status} | resilience {resilience_status} | smoke {smoke_status}",
            missing_evidence_summary=limitation_note,
            reviewer_next_step_digest="Use the index to navigate linked evidence packs without treating them as real acceptance evidence.",
            non_claim_digest=non_claim_note,
        ),
        boundary_statements=boundary_statements,
        evidence_categories=["recognition_readiness", "release_manifest", "release_evidence_pack"],
    )

    release_manifest_bundle = _bundle(
        run_id=run_id,
        artifact_type="release_manifest",
        filename=filenames["release_manifest"],
        markdown_filename=filenames["release_manifest_markdown"],
        artifact_role="execution_summary",
        title_text="Release Manifest",
        reviewer_note=release_manifest_reviewer_note,
        summary_text="Release manifest prepared for Step 2 reviewer mapping only.",
        summary_lines=[
            f"release_id: {release_id}",
            f"release_version: {release_version}",
            f"repo / branch: {repo_ref} | {branch_or_head}",
            f"parity / resilience / smoke: {parity_status} | {resilience_status} | {smoke_status}",
        ],
        detail_lines=[
            f"scope / decision rule: {scope_id} | {decision_rule_id}",
            f"uncertainty / method: {uncertainty_case_id} | {method_confirmation_protocol_id}",
            f"hash registry: {hash_registry_id}",
            f"change impact modules: {changed_modules_summary}",
            "rollback mode: file-artifact-first / primary evidence untouched",
            f"assets / certificates: {linked_assets_certificates_summary['summary']}",
            f"limitation: {limitation_note}",
        ],
        artifact_paths=_bundle_path_map(path_map, "release_manifest", "release_manifest_markdown"),
        body={
            "release_id": release_id,
            "release_version": release_version,
            "created_at": _now_iso(),
            "repo_ref": repo_ref,
            "branch_or_head": branch_or_head,
            "workspace_mode": WORKSPACE_MODE,
            "linked_scope_ids": [scope_id],
            "linked_decision_rules": [decision_rule_id],
            "linked_assets_certificates_summary": dict(linked_assets_certificates_summary),
            "linked_uncertainty_cases": [uncertainty_case_id],
            "linked_method_confirmation_protocols": [method_confirmation_protocol_id],
            "linked_traceability_matrix": _artifact_ref("software_validation_traceability_matrix", str(path_map.get("software_validation_traceability_matrix") or "")),
            "linked_hash_registry": _artifact_ref("artifact_hash_registry", str(path_map.get("artifact_hash_registry") or "")),
            "linked_change_impact_summary": _artifact_ref("change_impact_summary", str(path_map.get("change_impact_summary") or "")),
            "linked_rollback_readiness_summary": _artifact_ref(
                "rollback_readiness_summary",
                str(path_map.get("rollback_readiness_summary") or ""),
            ),
            "linked_audit_event_store": _artifact_ref("audit_event_store", str(path_map.get("audit_event_store") or "")),
            "linked_environment_fingerprint": _artifact_ref(
                "environment_fingerprint",
                str(path_map.get("environment_fingerprint") or ""),
            ),
            "linked_config_fingerprint": _artifact_ref("config_fingerprint", str(path_map.get("config_fingerprint") or "")),
            "linked_release_input_digest": _artifact_ref(
                "release_input_digest",
                str(path_map.get("release_input_digest") or ""),
            ),
            "linked_release_scope_summary": _artifact_ref(
                "release_scope_summary",
                str(path_map.get("release_scope_summary") or ""),
            ),
            "linked_release_boundary_digest": _artifact_ref(
                "release_boundary_digest",
                str(path_map.get("release_boundary_digest") or ""),
            ),
            "linked_release_evidence_pack_index": _artifact_ref(
                "release_evidence_pack_index",
                str(path_map.get("release_evidence_pack_index") or ""),
            ),
            "linked_release_validation_manifest": _artifact_ref(
                "release_validation_manifest",
                str(path_map.get("release_validation_manifest") or ""),
            ),
            "linked_test_suites": [
                {"suite_id": "parity", "status": parity_status},
                {"suite_id": "resilience", "status": resilience_status},
                {"suite_id": "smoke", "status": smoke_status},
            ],
            "parity_status": parity_status,
            "resilience_status": resilience_status,
            "smoke_status": smoke_status,
            "changed_modules_summary": changed_modules_summary,
            "linked_surface_visibility": list(LINKED_REVIEW_SURFACES),
            "simulation_only": True,
            "reviewer_only": True,
            "not_real_acceptance_evidence": True,
            "not_ready_for_formal_claim": True,
            "reviewer_actions": list(reviewer_actions),
            "reviewer_note": release_manifest_reviewer_note,
            "limitation_note": limitation_note,
            "non_claim_note": non_claim_note,
        },
        digest=_digest(
            summary="Release manifest prepared for Step 2 reviewer mapping only.",
            scope_overview_summary=scope_id,
            decision_rule_summary=decision_rule_id,
            conformity_boundary_summary=non_claim_note,
            current_coverage_summary=f"traceability {traceability_id} | hash registry {hash_registry_id} | parity {parity_status} | resilience {resilience_status} | smoke {smoke_status}",
            missing_evidence_summary=limitation_note,
            reviewer_next_step_digest="Use the manifest to explain release boundaries and linked evidence; do not treat it as formal release approval.",
            non_claim_digest=non_claim_note,
        ),
        boundary_statements=boundary_statements,
        evidence_categories=["recognition_readiness", "release_manifest", "release"],
    )

    release_validation_bundle = _compatibility_alias(
        alias_type="release_validation_manifest",
        filename=filenames["release_validation_manifest"],
        markdown_filename=filenames["release_validation_manifest_markdown"],
        source_bundle=release_manifest_bundle,
        title_text="Release Validation Manifest",
    )
    release_validation_raw = dict(release_validation_bundle.get("raw") or {})
    release_validation_raw["artifact_paths"] = {
        **dict(release_validation_raw.get("artifact_paths") or {}),
        **_bundle_path_map(path_map, "release_validation_manifest", "release_validation_manifest_markdown"),
    }
    release_validation_bundle["raw"] = release_validation_raw
    release_validation_bundle["digest"] = dict(
        release_validation_raw.get("digest") or release_validation_bundle.get("digest") or {}
    )

    audit_readiness_bundle = _bundle(
        run_id=run_id,
        artifact_type="audit_readiness_digest",
        filename=filenames["audit_readiness_digest"],
        markdown_filename=filenames["audit_readiness_digest_markdown"],
        artifact_role="diagnostic_analysis",
        title_text="Audit Readiness Digest",
        reviewer_note=audit_readiness_reviewer_note,
        summary_text="software validation / audit readiness remain reviewer-only in Step 2.",
        summary_lines=[
            f"traceability completeness: {traceability_completeness}",
            f"artifact hash registry: {len(hash_registry_entries)} entries",
            f"environment fingerprint: {environment_summary}",
            f"release manifest: {release_id}",
        ],
        detail_lines=[
            f"parity / resilience / smoke: {parity_status} | {resilience_status} | {smoke_status}",
            f"change impact modules: {changed_modules_summary}",
            "rollback mode: file-artifact-first / sidecar revocable / primary evidence untouched",
            f"config fingerprint: {config_fingerprint}",
            f"non-claim: {non_claim_note}",
            f"limitation: {limitation_note}",
        ],
        artifact_paths=_bundle_path_map(path_map, "audit_readiness_digest", "audit_readiness_digest_markdown"),
        body={
            "traceability_id": traceability_id,
            "traceability_version": traceability_version,
            "hash_registry_id": hash_registry_id,
            "release_id": release_id,
            "release_version": release_version,
            "scope_id": scope_id,
            "decision_rule_id": decision_rule_id,
            "uncertainty_case_id": uncertainty_case_id,
            "method_confirmation_protocol_id": method_confirmation_protocol_id,
            "environment_summary": environment_summary,
            "parity_status": parity_status,
            "resilience_status": resilience_status,
            "smoke_status": smoke_status,
            "changed_modules_summary": changed_modules_summary,
            "linked_change_impact_summary": _artifact_ref("change_impact_summary", str(path_map.get("change_impact_summary") or "")),
            "linked_rollback_readiness_summary": _artifact_ref(
                "rollback_readiness_summary",
                str(path_map.get("rollback_readiness_summary") or ""),
            ),
            "linked_audit_event_store": _artifact_ref("audit_event_store", str(path_map.get("audit_event_store") or "")),
            "linked_environment_fingerprint": _artifact_ref(
                "environment_fingerprint",
                str(path_map.get("environment_fingerprint") or ""),
            ),
            "linked_config_fingerprint": _artifact_ref("config_fingerprint", str(path_map.get("config_fingerprint") or "")),
            "linked_release_input_digest": _artifact_ref(
                "release_input_digest",
                str(path_map.get("release_input_digest") or ""),
            ),
            "linked_release_manifest": _artifact_ref("release_manifest", str(path_map.get("release_manifest") or "")),
            "linked_release_validation_manifest": _artifact_ref(
                "release_validation_manifest",
                str(path_map.get("release_validation_manifest") or ""),
            ),
            "linked_surface_visibility": list(LINKED_REVIEW_SURFACES),
            "reviewer_note": audit_readiness_reviewer_note,
            "limitation_note": limitation_note,
            "non_claim_note": non_claim_note,
        },
        digest=_digest(
            summary="software validation / audit readiness remain reviewer-only in Step 2.",
            scope_overview_summary=scope_id,
            decision_rule_summary=decision_rule_id,
            conformity_boundary_summary=non_claim_note,
            current_coverage_summary=f"traceability {traceability_completeness} | hash entries {len(hash_registry_entries)} | parity {parity_status} | resilience {resilience_status} | smoke {smoke_status}",
            missing_evidence_summary=limitation_note,
            reviewer_next_step_digest="Use this digest as reviewer context only; formal audit conclusions remain out of scope.",
            non_claim_digest=non_claim_note,
        ),
        boundary_statements=boundary_statements,
        evidence_categories=["recognition_readiness", "software_validation", "audit_readiness"],
    )

    return {
        "software_validation_traceability_matrix": traceability_bundle,
        "requirement_design_code_test_links": links_bundle,
        "validation_evidence_index": evidence_bundle,
        "change_impact_summary": change_impact_bundle,
        "rollback_readiness_summary": rollback_bundle,
        "artifact_hash_registry": hash_registry_bundle,
        "audit_event_store": audit_event_bundle,
        "environment_fingerprint": environment_bundle,
        "config_fingerprint": config_bundle,
        "release_input_digest": release_input_bundle,
        "release_manifest": release_manifest_bundle,
        "release_scope_summary": release_scope_bundle,
        "release_boundary_digest": release_boundary_bundle,
        "release_evidence_pack_index": release_evidence_bundle,
        "release_validation_manifest": release_validation_bundle,
        "audit_readiness_digest": audit_readiness_bundle,
    }
