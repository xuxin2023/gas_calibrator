from __future__ import annotations

import hashlib
import json
import platform
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


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
) -> dict[str, Any]:
    raw = {
        "schema_version": "step2-software-validation-wp5-v1",
        "artifact_type": artifact_type,
        "generated_at": _now_iso(),
        "run_id": run_id,
        "artifact_role": artifact_role,
        "evidence_source": "simulated",
        "evidence_state": "reviewer_readiness_only",
        "reviewer_only": True,
        "readiness_mapping_only": True,
        "simulation_only": True,
        "ready_for_readiness_mapping": True,
        "not_real_acceptance_evidence": True,
        "not_ready_for_formal_claim": True,
        "primary_evidence_rewritten": False,
        "boundary_statements": list(boundary_statements),
        "digest": dict(digest),
        "review_surface": {
            "title_text": title_text,
            "role_text": artifact_role,
            "reviewer_note": reviewer_note,
            "summary_text": summary_text,
            "summary_lines": [line for line in summary_lines if str(line).strip()],
            "detail_lines": [line for line in detail_lines if str(line).strip()],
            "anchor_id": artifact_type.replace("_", "-"),
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
            "evidence_source_filters": ["simulated", "reviewer_readiness_only"],
            "artifact_paths": dict(artifact_paths),
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


def _artifact_ref(artifact_type: str, path: str) -> dict[str, str]:
    return {
        "artifact_type": str(artifact_type or "").strip(),
        "path": str(path or "").strip(),
    }


def _bundle_path_map(path_map: dict[str, str], *keys: str) -> dict[str, str]:
    return {
        str(key): str(path_map.get(key) or "").strip()
        for key in keys
        if str(path_map.get(key) or "").strip()
    }


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
    workspace_mode = "step2_simulation_only_file_artifact_first"

    scope_raw = dict(scope_definition_pack.get("raw") or {})
    decision_raw = dict(decision_rule_profile.get("raw") or {})
    reference_raw = dict(reference_asset_registry.get("raw") or {})
    certificate_raw = dict(certificate_lifecycle_summary.get("raw") or {})
    gate_raw = dict(pre_run_readiness_gate.get("raw") or {})
    uncertainty_report_raw = dict(uncertainty_report_pack.get("raw") or {})
    uncertainty_rollup_raw = dict(uncertainty_rollup.get("raw") or {})
    method_protocol_raw = dict(method_confirmation_protocol.get("raw") or {})
    route_matrix_raw = dict(route_specific_validation_matrix.get("raw") or {})
    validation_run_set_raw = dict(validation_run_set.get("raw") or {})
    verification_digest_raw = dict(verification_digest.get("raw") or {})
    verification_rollup_raw = dict(verification_rollup.get("raw") or {})

    scope_id = str(scope_raw.get("scope_id") or f"{run_id}-step2-scope-package")
    decision_rule_id = str(
        decision_raw.get("decision_rule_id")
        or scope_raw.get("decision_rule_id")
        or "step2_readiness_reviewer_rule_v1"
    )
    uncertainty_case_id = str(
        uncertainty_rollup_raw.get("uncertainty_case_id")
        or uncertainty_report_raw.get("uncertainty_case_id")
        or "step2-uncertainty-reviewer-case"
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
        "当前仅生成 Step 2 / simulation-only / reviewer-facing / file-artifact-first 的软件验证、审计留痕与 release skeleton。"
    )
    non_claim_note = (
        "当前对象仅用于 readiness mapping / reviewer 审阅，不构成 real acceptance evidence、formal compliance claim、formal release approval 或 accreditation claim。"
    )
    reviewer_note = (
        "当前链路表达需求→设计→代码→测试→artifact 的 reviewer-facing 追踪关系；真实签名、真实审批、正式防篡改承诺均明确延后。"
    )

    reference_assets = [dict(item) for item in list(reference_raw.get("assets") or []) if isinstance(item, dict)]
    certificate_rows = [dict(item) for item in list(certificate_raw.get("certificate_rows") or []) if isinstance(item, dict)]
    route_rollups = [
        dict(item)
        for item in list(
            route_matrix_raw.get("route_rollups")
            or route_matrix_raw.get("route_specific_validation_matrix")
            or []
        )
        if isinstance(item, dict)
    ]
    validation_runs = [dict(item) for item in list(validation_run_set_raw.get("validation_run_set") or []) if isinstance(item, dict)]

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
            "title": "软件验证追踪链路",
            "summary": "需求→设计→代码→测试→artifact 的 reviewer-facing skeleton。",
        },
        {
            "requirement_id": "wp5-audit-hash-registry",
            "title": "审计 hash / fingerprint 摘要",
            "summary": "仅 file-backed / sidecar-backed / reviewer-facing hash registry，不宣称正式防篡改。",
        },
        {
            "requirement_id": "wp5-release-manifest",
            "title": "Step 2 release manifest",
            "summary": "表达本轮改动、验证、边界与 non-claim 说明，不提前替代正式 release approval。",
        },
    ]
    design_refs = [
        "recognition_scope_repository/file-backed reviewer skeleton",
        "uncertainty_repository/file-backed reviewer skeleton",
        "method_confirmation_repository/file-backed reviewer skeleton",
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

    requirement_links_rows = [
        {
            "requirement_ref": requirement_refs[0]["requirement_id"],
            "design_refs": [
                "software_validation_traceability_matrix",
                "requirement_design_code_test_links",
                "validation_evidence_index",
            ],
            "code_refs": [
                "src/gas_calibrator/v2/core/software_validation_builder.py",
                "src/gas_calibrator/v2/core/software_validation_repository.py",
                "src/gas_calibrator/v2/adapters/software_validation_gateway.py",
            ],
            "test_refs": [
                "tests/v2/test_software_validation_wp5_contracts.py",
                "tests/v2/test_results_gateway.py",
            ],
            "artifact_refs": [
                path_map["software_validation_traceability_matrix"],
                path_map["requirement_design_code_test_links"],
                path_map["validation_evidence_index"],
            ],
            "change_set_refs": list(change_set_refs),
            "completeness_status": "linked",
            "reviewer_note": "Step 2 先补 reviewer-facing skeleton，确保链路可机器读取。",
        },
        {
            "requirement_ref": requirement_refs[1]["requirement_id"],
            "design_refs": [
                "artifact_hash_registry",
                "environment_fingerprint",
                "config_fingerprint",
                "release_input_digest",
            ],
            "code_refs": [
                "src/gas_calibrator/v2/core/software_validation_builder.py",
                "src/gas_calibrator/v2/adapters/results_gateway.py",
            ],
            "test_refs": [
                "tests/v2/test_software_validation_wp5_contracts.py",
                "tests/v2/test_historical_artifacts_cli.py",
            ],
            "artifact_refs": [
                path_map["artifact_hash_registry"],
                path_map["environment_fingerprint"],
                path_map["config_fingerprint"],
                path_map["release_input_digest"],
            ],
            "change_set_refs": list(change_set_refs),
            "completeness_status": "linked",
            "reviewer_note": "当前只做 file-backed 摘要与 reviewer digest，不承诺正式审计不可篡改能力。",
        },
        {
            "requirement_ref": requirement_refs[2]["requirement_id"],
            "design_refs": [
                "release_manifest",
                "release_scope_summary",
                "release_boundary_digest",
                "release_evidence_pack_index",
            ],
            "code_refs": [
                "src/gas_calibrator/v2/core/software_validation_builder.py",
                "src/gas_calibrator/v2/ui_v2/controllers/app_facade.py",
                "src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py",
            ],
            "test_refs": [
                "tests/v2/test_software_validation_wp5_contracts.py",
                "tests/v2/test_ui_v2_review_center.py",
                "tests/v2/test_ui_v2_workbench_evidence.py",
            ],
            "artifact_refs": [
                path_map["release_manifest"],
                path_map["release_scope_summary"],
                path_map["release_boundary_digest"],
                path_map["release_evidence_pack_index"],
            ],
            "change_set_refs": list(change_set_refs),
            "completeness_status": "linked",
            "reviewer_note": "manifest 仅表达 Step 2 边界、验证联动和 non-claim；不进入真实审批流。",
        },
        {
            "requirement_ref": "wp5-impact-and-rollback",
            "design_refs": ["change_impact_summary", "rollback_readiness_summary"],
            "code_refs": [
                "src/gas_calibrator/v2/core/software_validation_builder.py",
                "src/gas_calibrator/v2/scripts/historical_artifacts.py",
            ],
            "test_refs": [
                "tests/v2/test_software_validation_wp5_contracts.py",
                "tests/v2/test_historical_artifacts_cli.py",
            ],
            "artifact_refs": [
                path_map["change_impact_summary"],
                path_map["rollback_readiness_summary"],
            ],
            "change_set_refs": list(change_set_refs),
            "completeness_status": "linked",
            "reviewer_note": "强调只新增 sidecar/reviewer 工件，主证据不改写，回退以删除增量 sidecar 为主。",
        },
    ]

    traceability_artifact_refs = [
        _artifact_ref("scope_definition_pack", path_map["scope_definition_pack"]),
        _artifact_ref("decision_rule_profile", path_map["decision_rule_profile"]),
        _artifact_ref("uncertainty_rollup", path_map["uncertainty_rollup"]),
        _artifact_ref("method_confirmation_protocol", path_map["method_confirmation_protocol"]),
        _artifact_ref("verification_rollup", path_map["verification_rollup"]),
        _artifact_ref("software_validation_traceability_matrix", path_map["software_validation_traceability_matrix"]),
        _artifact_ref("artifact_hash_registry", path_map["artifact_hash_registry"]),
        _artifact_ref("release_manifest", path_map["release_manifest"]),
    ]
    completeness_summary = f"{len(requirement_links_rows)}/{len(requirement_links_rows)} requirement chains linked"
