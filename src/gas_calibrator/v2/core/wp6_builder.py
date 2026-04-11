"""WP6 builder: PT/ILC importer + comparison evidence pack + reviewer navigation.

Step 2 only — reviewer-facing / readiness-mapping-only.
No real external comparison, no formal compliance claim, no accreditation claim.
"""
from __future__ import annotations

import csv
import io
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import recognition_readiness_artifacts as recognition_readiness


# ---------------------------------------------------------------------------
# constants
# ---------------------------------------------------------------------------

WP6_BUILDER_SCHEMA_VERSION = "step2-wp6-builder-v1"
WP6_COMPARISON_VERSION = "v1.2-step2-reviewer"

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

COMPARISON_TYPES = ("PT", "ILC", "external_comparison", "readiness_demo")
IMPORT_MODES = ("local_json", "local_csv", "local_markdown", "artifact_sidecar", "manual_fixture")

_LIMITATION_NOTE = (
    "Step 2 builds reviewer-facing PT/ILC / comparison readiness sidecars only; "
    "real external comparison, formal compliance claims, and accreditation claims remain out of scope."
)
_NON_CLAIM_NOTE = (
    "Current artifacts support readiness mapping and reviewer review only; "
    "they are not real acceptance evidence, formal external comparison results, or accreditation claims."
)
_REVIEWER_NOTE = (
    "PT/ILC importer is offline / local-file only in Step 2. "
    "No network access, no real third-party system connection, no formal result certification. "
    "All imported data defaults to evidence_source=simulated."
)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _dedupe(values: list[str]) -> list[str]:
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


def _safe_str(value: Any) -> str:
    return str(value) if value is not None else ""


def _safe_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(v) for v in value if v is not None]
    if isinstance(value, (tuple, set)):
        return [str(v) for v in value if v is not None]
    return []


def _safe_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


# ---------------------------------------------------------------------------
# bundle helper (same pattern as uncertainty_builder / software_validation_builder)
# ---------------------------------------------------------------------------

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
        "schema_version": WP6_BUILDER_SCHEMA_VERSION,
        "artifact_type": artifact_type,
        "generated_at": _now_iso(),
        "run_id": run_id,
        "artifact_role": artifact_role,
        "evidence_source": "simulated",
        "evidence_state": "reviewer_readiness_only",
        "not_real_acceptance_evidence": True,
        "ready_for_readiness_mapping": True,
        "not_ready_for_formal_claim": True,
        "primary_evidence_rewritten": False,
        "reviewer_stub_only": True,
        "readiness_mapping_only": True,
        "not_released_for_formal_claim": True,
        "boundary_statements": list(boundary_statements),
        "overall_status": "ready_for_readiness_mapping",
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


# ---------------------------------------------------------------------------
# common context from upstream WP payloads
# ---------------------------------------------------------------------------

def _common_context(
    *,
    run_id: str,
    scope_definition_pack: dict[str, Any],
    decision_rule_profile: dict[str, Any],
    reference_asset_registry: dict[str, Any],
    certificate_lifecycle_summary: dict[str, Any],
    pre_run_readiness_gate: dict[str, Any],
    uncertainty_report_pack: dict[str, Any],
    uncertainty_rollup: dict[str, Any],
    method_confirmation_protocol: dict[str, Any],
    verification_digest: dict[str, Any],
    software_validation_rollup: dict[str, Any],
    path_map: dict[str, str],
) -> dict[str, Any]:
    scope_raw = dict(scope_definition_pack.get("raw") or scope_definition_pack)
    dr_raw = dict(decision_rule_profile.get("raw") or decision_rule_profile)
    ref_raw = dict(reference_asset_registry.get("raw") or reference_asset_registry)
    cert_raw = dict(certificate_lifecycle_summary.get("raw") or certificate_lifecycle_summary)
    gate_raw = dict(pre_run_readiness_gate.get("raw") or pre_run_readiness_gate)
    unc_raw = dict(uncertainty_report_pack.get("raw") or uncertainty_report_pack)
    unc_roll = dict(uncertainty_rollup.get("raw") or uncertainty_rollup)
    mc_raw = dict(method_confirmation_protocol.get("raw") or method_confirmation_protocol)
    vd_raw = dict(verification_digest.get("raw") or verification_digest)
    sv_raw = dict(software_validation_rollup.get("raw") or software_validation_rollup)

    scope_id = _safe_str(scope_raw.get("scope_id") or f"{run_id}-scope")
    decision_rule_id = _safe_str(dr_raw.get("decision_rule_id") or f"{run_id}-decision-rule")

    return {
        "run_id": run_id,
        "scope_id": scope_id,
        "decision_rule_id": decision_rule_id,
        "scope_name": _safe_str(scope_raw.get("scope_name") or "unnamed-scope"),
        "scope_raw": scope_raw,
        "dr_raw": dr_raw,
        "ref_raw": ref_raw,
        "cert_raw": cert_raw,
        "gate_raw": gate_raw,
        "unc_raw": unc_raw,
        "unc_roll": unc_roll,
        "mc_raw": mc_raw,
        "vd_raw": vd_raw,
        "sv_raw": sv_raw,
        "path_map": path_map,
        "limitation_note": _LIMITATION_NOTE,
        "non_claim_note": _NON_CLAIM_NOTE,
        "reviewer_note": _REVIEWER_NOTE,
    }


def _base_artifact_paths(path_map: dict[str, str], *keys: str) -> dict[str, str]:
    return {k: str(v) for k, v in path_map.items() if k in keys and v}


# ---------------------------------------------------------------------------
# 1) pt_ilc_registry
# ---------------------------------------------------------------------------

def _pt_ilc_registry_artifact(
    *,
    run_id: str,
    common: dict[str, Any],
    path_map: dict[str, str],
    filenames: dict[str, str],
    boundary_statements: list[str],
) -> dict[str, Any]:
    comparison_id = f"{run_id}-pt-ilc-registry"
    rows = [
        {
            "comparison_id": f"{run_id}-pt-demo-001",
            "comparison_version": WP6_COMPARISON_VERSION,
            "comparison_type": "PT",
            "scope_id": common["scope_id"],
            "decision_rule_id": common["decision_rule_id"],
            "status": "readiness_demo",
            "evidence_source": "simulated",
            "import_mode": "manual_fixture",
            "reviewer_only": True,
            "readiness_mapping_only": True,
            "not_real_acceptance_evidence": True,
            "not_ready_for_formal_claim": True,
            "primary_evidence_rewritten": False,
        },
        {
            "comparison_id": f"{run_id}-ilc-demo-001",
            "comparison_version": WP6_COMPARISON_VERSION,
            "comparison_type": "ILC",
            "scope_id": common["scope_id"],
            "decision_rule_id": common["decision_rule_id"],
            "status": "readiness_demo",
            "evidence_source": "simulated",
            "import_mode": "manual_fixture",
            "reviewer_only": True,
            "readiness_mapping_only": True,
            "not_real_acceptance_evidence": True,
            "not_ready_for_formal_claim": True,
            "primary_evidence_rewritten": False,
        },
    ]
    digest = {
        "comparison_id": comparison_id,
        "entry_count": len(rows),
        "comparison_types_present": ["PT", "ILC"],
        "all_simulated": True,
    }
    return _bundle(
        run_id=run_id,
        artifact_type="pt_ilc_registry",
        filename=filenames.get("pt_ilc_registry", PT_ILC_REGISTRY_FILENAME),
        markdown_filename=filenames.get("pt_ilc_registry_markdown", PT_ILC_REGISTRY_MARKDOWN_FILENAME),
        artifact_role="pt_ilc_registry",
        title_text="PT/ILC 注册表",
        reviewer_note=common["reviewer_note"],
        summary_text=f"PT/ILC 注册表: {len(rows)} 条就绪演示记录 (全部为模拟数据)",
        summary_lines=[f"共 {len(rows)} 条比对记录", "全部为 Step 2 就绪演示, 非真实外部比对结果"],
        detail_lines=[
            f"comparison_id: {comparison_id}",
            f"scope_id: {common['scope_id']}",
            f"decision_rule_id: {common['decision_rule_id']}",
            "所有记录 evidence_source=simulated",
        ],
        artifact_paths=_base_artifact_paths(path_map, "pt_ilc_registry", "comparison_evidence_pack", "comparison_rollup"),
        body={
            "comparison_id": comparison_id,
            "comparison_version": WP6_COMPARISON_VERSION,
            "rows": rows,
            "scope_id": common["scope_id"],
            "decision_rule_id": common["decision_rule_id"],
            "linked_uncertainty_case_ids": [],
            "linked_method_confirmation_protocol_ids": [],
            "linked_software_validation_release_ids": [],
            "reference_asset_refs": [],
            "certificate_lifecycle_refs": [],
            "source_files": [],
            "import_mode": "manual_fixture",
            "reviewer_only": True,
            "readiness_mapping_only": True,
            "not_real_acceptance_evidence": True,
            "not_ready_for_formal_claim": True,
            "primary_evidence_rewritten": False,
            "limitation_note": common["limitation_note"],
            "non_claim_note": common["non_claim_note"],
        },
        digest=digest,
        boundary_statements=boundary_statements,
        evidence_categories=["recognition_readiness", "pt_ilc_comparison"],
    )


# ---------------------------------------------------------------------------
# 2) external_comparison_importer
# ---------------------------------------------------------------------------

def _importer_artifact(
    *,
    run_id: str,
    common: dict[str, Any],
    path_map: dict[str, str],
    filenames: dict[str, str],
    boundary_statements: list[str],
) -> dict[str, Any]:
    importer_id = f"{run_id}-comparison-importer"
    supported_formats = ["json", "csv", "markdown", "artifact_sidecar"]
    digest = {
        "importer_id": importer_id,
        "supported_formats": supported_formats,
        "network_access": False,
        "offline_only": True,
    }
    return _bundle(
        run_id=run_id,
        artifact_type="external_comparison_importer",
        filename=filenames.get("external_comparison_importer", EXTERNAL_COMPARISON_IMPORTER_FILENAME),
        markdown_filename=filenames.get("external_comparison_importer_markdown", EXTERNAL_COMPARISON_IMPORTER_MARKDOWN_FILENAME),
        artifact_role="external_comparison_importer",
        title_text="外部比对导入器",
        reviewer_note=common["reviewer_note"],
        summary_text="Step 2 离线导入器: 仅支持本地文件, 不联网",
        summary_lines=[
            "支持格式: " + ", ".join(supported_formats),
            "仅从本地 artifact / fixture / 用户文件导入",
            "不连接任何真实第三方系统",
        ],
        detail_lines=[
            f"importer_id: {importer_id}",
            "import_mode: local_file_only",
            "network_access: false",
            "所有导入结果默认 evidence_source=simulated",
            "缺字段/旧schema/legacy payload: 保守降级 + placeholder",
        ],
        artifact_paths=_base_artifact_paths(path_map, "external_comparison_importer", "pt_ilc_registry"),
        body={
            "importer_id": importer_id,
            "supported_formats": supported_formats,
            "supported_import_modes": list(IMPORT_MODES),
            "network_access": False,
            "offline_only": True,
            "default_evidence_source": "simulated",
            "schema_validation": "minimal",
            "legacy_compatibility": True,
            "missing_field_policy": "conservative_placeholder",
            "invalid_input_policy": "reviewer_friendly_warning",
            "reviewer_only": True,
            "readiness_mapping_only": True,
            "not_real_acceptance_evidence": True,
            "not_ready_for_formal_claim": True,
            "primary_evidence_rewritten": False,
            "limitation_note": common["limitation_note"],
            "non_claim_note": common["non_claim_note"],
        },
        digest=digest,
        boundary_statements=boundary_statements,
        evidence_categories=["recognition_readiness", "pt_ilc_comparison"],
    )


# ---------------------------------------------------------------------------
# 3) comparison_evidence_pack
# ---------------------------------------------------------------------------

def _comparison_evidence_pack_artifact(
    *,
    run_id: str,
    common: dict[str, Any],
    path_map: dict[str, str],
    filenames: dict[str, str],
    boundary_statements: list[str],
) -> dict[str, Any]:
    pack_id = f"{run_id}-comparison-evidence-pack"
    # Link to upstream WP artifacts
    scope_linkage = {"scope_id": common["scope_id"], "scope_name": common["scope_name"]}
    dr_linkage = {"decision_rule_id": common["decision_rule_id"]}
    unc_linkage = {"linked_uncertainty_case_ids": []}
    mc_linkage = {"linked_method_confirmation_protocol_ids": []}
    sv_linkage = {"linked_software_validation_release_ids": []}
    ref_linkage = {"reference_asset_refs": []}
    cert_linkage = {"certificate_lifecycle_refs": []}

    coverage_summary = {
        "scope_linked": True,
        "decision_rule_linked": True,
        "uncertainty_linked": False,
        "method_confirmation_linked": False,
        "software_validation_linked": False,
        "reference_assets_linked": False,
        "certificates_linked": False,
        "overall_coverage": "partial_readiness_only",
    }
    gap_summary = [
        "未关联不确定度预算案例",
        "未关联方法确认协议",
        "未关联软件验证发布",
        "未关联参考标准资产",
        "未关联证书生命周期",
        "当前仅为就绪映射, 不能形成正式比对结论",
    ]
    reviewer_actions = [
        "确认比对范围与认可范围一致",
        "补充不确定度预算关联",
        "补充方法确认关联",
        "补充软件验证关联",
        "补充标准器/证书关联",
        "在进入Step 3前不得形成正式对外一致性声明",
    ]
    digest = {
        "pack_id": pack_id,
        "coverage": coverage_summary,
        "gap_count": len(gap_summary),
        "reviewer_action_count": len(reviewer_actions),
    }
    return _bundle(
        run_id=run_id,
        artifact_type="comparison_evidence_pack",
        filename=filenames.get("comparison_evidence_pack", COMPARISON_EVIDENCE_PACK_FILENAME),
        markdown_filename=filenames.get("comparison_evidence_pack_markdown", COMPARISON_EVIDENCE_PACK_MARKDOWN_FILENAME),
        artifact_role="comparison_evidence_pack",
        title_text="比对证据包",
        reviewer_note=common["reviewer_note"],
        summary_text="比对证据包: 就绪映射 + 审阅摘要, 非正式比对结论",
        summary_lines=[
            f"范围关联: {common['scope_id']}",
            f"决策规则关联: {common['decision_rule_id']}",
            f"覆盖度: {coverage_summary['overall_coverage']}",
            f"缺口: {len(gap_summary)} 项",
            f"审阅动作: {len(reviewer_actions)} 项",
        ],
        detail_lines=[
            f"pack_id: {pack_id}",
            "当前仅表达 readiness mapping / reviewer summary",
            "不能表达真实对外一致性结论",
            *[f"gap: {g}" for g in gap_summary],
        ],
        artifact_paths=_base_artifact_paths(path_map, "comparison_evidence_pack", "pt_ilc_registry", "scope_comparison_view", "comparison_digest", "comparison_rollup"),
        body={
            "pack_id": pack_id,
            "comparison_overview": {"comparison_type": "readiness_demo", "status": "readiness_mapping_only"},
            "imported_source_summary": {"source_count": 0, "all_simulated": True},
            "scope_linkage": scope_linkage,
            "decision_rule_linkage": dr_linkage,
            "uncertainty_linkage": unc_linkage,
            "method_confirmation_linkage": mc_linkage,
            "software_validation_linkage": sv_linkage,
            "reference_asset_linkage": ref_linkage,
            "certificate_lifecycle_linkage": cert_linkage,
            "coverage_summary": coverage_summary,
            "gap_summary": gap_summary,
            "reviewer_actions": reviewer_actions,
            "non_claim_note": common["non_claim_note"],
            "reviewer_only": True,
            "readiness_mapping_only": True,
            "not_real_acceptance_evidence": True,
            "not_ready_for_formal_claim": True,
            "primary_evidence_rewritten": False,
            "limitation_note": common["limitation_note"],
        },
        digest=digest,
        boundary_statements=boundary_statements,
        evidence_categories=["recognition_readiness", "pt_ilc_comparison"],
    )


# ---------------------------------------------------------------------------
# 4) scope_comparison_view
# ---------------------------------------------------------------------------

def _scope_comparison_view_artifact(
    *,
    run_id: str,
    common: dict[str, Any],
    path_map: dict[str, str],
    filenames: dict[str, str],
    boundary_statements: list[str],
) -> dict[str, Any]:
    view_id = f"{run_id}-scope-comparison-view"
    digest = {
        "view_id": view_id,
        "scope_id": common["scope_id"],
        "comparison_entries": 2,
        "all_simulated": True,
    }
    return _bundle(
        run_id=run_id,
        artifact_type="scope_comparison_view",
        filename=filenames.get("scope_comparison_view", SCOPE_COMPARISON_VIEW_FILENAME),
        markdown_filename=filenames.get("scope_comparison_view_markdown", SCOPE_COMPARISON_VIEW_MARKDOWN_FILENAME),
        artifact_role="scope_comparison_view",
        title_text="范围比对视图",
        reviewer_note=common["reviewer_note"],
        summary_text=f"范围 {common['scope_id']} 的比对视图: 2 条就绪演示",
        summary_lines=[
            f"scope_id: {common['scope_id']}",
            f"decision_rule_id: {common['decision_rule_id']}",
            "2 条比对记录 (PT + ILC 就绪演示)",
            "全部为模拟数据, 非正式比对结论",
        ],
        detail_lines=[
            f"view_id: {view_id}",
            "当前仅展示就绪映射摘要",
            "不展示真实外部比对结果",
        ],
        artifact_paths=_base_artifact_paths(path_map, "scope_comparison_view", "comparison_evidence_pack"),
        body={
            "view_id": view_id,
            "scope_id": common["scope_id"],
            "scope_name": common["scope_name"],
            "decision_rule_id": common["decision_rule_id"],
            "comparison_entries": [
                {"comparison_type": "PT", "status": "readiness_demo", "evidence_source": "simulated"},
                {"comparison_type": "ILC", "status": "readiness_demo", "evidence_source": "simulated"},
            ],
            "linked_uncertainty_case_ids": [],
            "linked_method_confirmation_protocol_ids": [],
            "linked_software_validation_release_ids": [],
            "current_evidence_coverage": "partial_readiness_only",
            "gaps": ["未关联不确定度/方法确认/软件验证/标准器/证书"],
            "reviewer_actions": ["补充上游WP关联", "不得形成正式对外一致性声明"],
            "reviewer_only": True,
            "readiness_mapping_only": True,
            "not_real_acceptance_evidence": True,
            "not_ready_for_formal_claim": True,
            "primary_evidence_rewritten": False,
            "non_claim_note": common["non_claim_note"],
            "limitation_note": common["limitation_note"],
        },
        digest=digest,
        boundary_statements=boundary_statements,
        evidence_categories=["recognition_readiness", "pt_ilc_comparison"],
    )


# ---------------------------------------------------------------------------
# 5) comparison_digest
# ---------------------------------------------------------------------------

def _comparison_digest_artifact(
    *,
    run_id: str,
    common: dict[str, Any],
    evidence_pack: dict[str, Any],
    path_map: dict[str, str],
    filenames: dict[str, str],
    boundary_statements: list[str],
) -> dict[str, Any]:
    digest_id = f"{run_id}-comparison-digest"
    ep_digest = dict(evidence_pack.get("digest") or {})
    digest = {
        "digest_id": digest_id,
        "scope_id": common["scope_id"],
        "decision_rule_id": common["decision_rule_id"],
        "coverage": ep_digest.get("coverage", {}),
        "gap_count": ep_digest.get("gap_count", 0),
        "reviewer_action_count": ep_digest.get("reviewer_action_count", 0),
        "all_simulated": True,
    }
    return _bundle(
        run_id=run_id,
        artifact_type="comparison_digest",
        filename=filenames.get("comparison_digest", COMPARISON_DIGEST_FILENAME),
        markdown_filename=filenames.get("comparison_digest_markdown", COMPARISON_DIGEST_MARKDOWN_FILENAME),
        artifact_role="comparison_digest",
        title_text="比对摘要",
        reviewer_note=common["reviewer_note"],
        summary_text="比对摘要: 就绪映射状态汇总",
        summary_lines=[
            f"scope_id: {common['scope_id']}",
            f"缺口: {digest['gap_count']} 项",
            f"审阅动作: {digest['reviewer_action_count']} 项",
        ],
        detail_lines=[f"digest_id: {digest_id}", "全部为 Step 2 就绪映射摘要"],
        artifact_paths=_base_artifact_paths(path_map, "comparison_digest", "comparison_evidence_pack"),
        body={
            "digest_id": digest_id,
            "scope_id": common["scope_id"],
            "decision_rule_id": common["decision_rule_id"],
            "coverage_summary": ep_digest.get("coverage", {}),
            "gap_count": digest["gap_count"],
            "reviewer_action_count": digest["reviewer_action_count"],
            "reviewer_only": True,
            "readiness_mapping_only": True,
            "not_real_acceptance_evidence": True,
            "not_ready_for_formal_claim": True,
            "primary_evidence_rewritten": False,
            "non_claim_note": common["non_claim_note"],
            "limitation_note": common["limitation_note"],
        },
        digest=digest,
        boundary_statements=boundary_statements,
        evidence_categories=["recognition_readiness", "pt_ilc_comparison"],
    )


# ---------------------------------------------------------------------------
# 6) comparison_rollup
# ---------------------------------------------------------------------------

def _comparison_rollup_artifact(
    *,
    run_id: str,
    common: dict[str, Any],
    evidence_pack: dict[str, Any],
    comparison_digest: dict[str, Any],
    path_map: dict[str, str],
    filenames: dict[str, str],
    boundary_statements: list[str],
) -> dict[str, Any]:
    rollup_id = f"{run_id}-comparison-rollup"
    ep_digest = dict(evidence_pack.get("digest") or {})
    cd_digest = dict(comparison_digest.get("digest") or {})
    summary_display = "PT/ILC 比对就绪映射 (Step 2 reviewer-only)"
    digest = {
        "rollup_id": rollup_id,
        "scope_id": common["scope_id"],
        "decision_rule_id": common["decision_rule_id"],
        "comparison_overview_summary": summary_display,
        "all_simulated": True,
    }
    return _bundle(
        run_id=run_id,
        artifact_type="comparison_rollup",
        filename=filenames.get("comparison_rollup", COMPARISON_ROLLUP_FILENAME),
        markdown_filename=filenames.get("comparison_rollup_markdown", COMPARISON_ROLLUP_MARKDOWN_FILENAME),
        artifact_role="comparison_rollup",
        title_text="比对汇总",
        reviewer_note=common["reviewer_note"],
        summary_text=summary_display,
        summary_lines=[
            summary_display,
            f"scope_id: {common['scope_id']}",
            f"decision_rule_id: {common['decision_rule_id']}",
            "所有比对数据均为模拟/就绪演示",
            "不能形成正式对外一致性声明",
        ],
        detail_lines=[
            f"rollup_id: {rollup_id}",
            "repository/gateway: file_artifact_first / file_backed_default",
            "db_ready_stub: enabled=false, not_in_default_chain=true",
            "primary_evidence_rewritten=false",
            common["limitation_note"],
        ],
        artifact_paths=_base_artifact_paths(path_map, "comparison_rollup", "comparison_digest", "comparison_evidence_pack"),
        body={
            "rollup_id": rollup_id,
            "scope_id": common["scope_id"],
            "decision_rule_id": common["decision_rule_id"],
            "rollup_summary_display": summary_display,
            "linked_surface_visibility": ["results", "review_center", "workbench", "historical_artifacts"],
            "rollup_scope": "run-dir",
            "artifact_count": 6,
            "legacy_placeholder_used": False,
            "missing_artifact_types": [],
            "db_ready_stub": {
                "enabled": False,
                "mode": "db_ready_stub",
                "default_path": False,
                "requires_explicit_injection": True,
                "not_in_default_chain": True,
            },
            "reviewer_only": True,
            "readiness_mapping_only": True,
            "not_real_acceptance_evidence": True,
            "not_ready_for_formal_claim": True,
            "primary_evidence_rewritten": False,
            "non_claim_note": common["non_claim_note"],
            "limitation_note": common["limitation_note"],
        },
        digest=digest,
        boundary_statements=boundary_statements,
        evidence_categories=["recognition_readiness", "pt_ilc_comparison"],
    )


# ---------------------------------------------------------------------------
# importer functions (local file only)
# ---------------------------------------------------------------------------

def import_comparison_from_json(file_path: Path) -> dict[str, Any]:
    """Import comparison data from a local JSON file.

    Step 2 only — offline, no network access.
    Returns a normalized comparison payload with evidence_source=simulated.
    """
    if not file_path.exists():
        return {
            "status": "warning",
            "warning_type": "file_not_found",
            "message": f"文件不存在: {file_path}",
            "imported_data": {},
            "evidence_source": "simulated",
            "reviewer_only": True,
            "readiness_mapping_only": True,
            "not_real_acceptance_evidence": True,
        }
    try:
        payload = json.loads(file_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {
            "status": "warning",
            "warning_type": "json_parse_error",
            "message": f"JSON解析失败: {exc}",
            "imported_data": {},
            "evidence_source": "simulated",
            "reviewer_only": True,
            "readiness_mapping_only": True,
            "not_real_acceptance_evidence": True,
        }
    if not isinstance(payload, dict):
        return {
            "status": "warning",
            "warning_type": "invalid_schema",
            "message": "JSON根元素不是dict",
            "imported_data": {},
            "evidence_source": "simulated",
            "reviewer_only": True,
            "readiness_mapping_only": True,
            "not_real_acceptance_evidence": True,
        }
    # Normalize: ensure required fields with conservative defaults
    normalized = _normalize_comparison_payload(payload, source_file=str(file_path))
    return {
        "status": "ok",
        "warning_type": None,
        "message": "导入成功 (simulated, reviewer-only)",
        "imported_data": normalized,
        "evidence_source": "simulated",
        "import_mode": "local_json",
        "source_file": str(file_path),
        "reviewer_only": True,
        "readiness_mapping_only": True,
        "not_real_acceptance_evidence": True,
        "not_ready_for_formal_claim": True,
        "primary_evidence_rewritten": False,
    }


def import_comparison_from_csv(file_path: Path) -> dict[str, Any]:
    """Import comparison data from a local CSV file.

    Step 2 only — offline, no network access.
    Returns a normalized comparison payload with evidence_source=simulated.
    """
    if not file_path.exists():
        return {
            "status": "warning",
            "warning_type": "file_not_found",
            "message": f"文件不存在: {file_path}",
            "imported_data": {},
            "evidence_source": "simulated",
            "reviewer_only": True,
            "readiness_mapping_only": True,
            "not_real_acceptance_evidence": True,
        }
    try:
        text = file_path.read_text(encoding="utf-8")
        reader = csv.DictReader(io.StringIO(text))
        rows = [dict(row) for row in reader]
    except Exception as exc:
        return {
            "status": "warning",
            "warning_type": "csv_parse_error",
            "message": f"CSV解析失败: {exc}",
            "imported_data": {},
            "evidence_source": "simulated",
            "reviewer_only": True,
            "readiness_mapping_only": True,
            "not_real_acceptance_evidence": True,
        }
    normalized_rows = [_normalize_comparison_payload(row, source_file=str(file_path)) for row in rows]
    return {
        "status": "ok",
        "warning_type": None,
        "message": f"导入 {len(normalized_rows)} 条记录 (simulated, reviewer-only)",
        "imported_data": {"rows": normalized_rows, "row_count": len(normalized_rows)},
        "evidence_source": "simulated",
        "import_mode": "local_csv",
        "source_file": str(file_path),
        "reviewer_only": True,
        "readiness_mapping_only": True,
        "not_real_acceptance_evidence": True,
        "not_ready_for_formal_claim": True,
        "primary_evidence_rewritten": False,
    }


def _normalize_comparison_payload(payload: dict[str, Any], *, source_file: str = "") -> dict[str, Any]:
    """Normalize a comparison payload with conservative defaults for missing fields."""
    comparison_type = _safe_str(payload.get("comparison_type"))
    if comparison_type and comparison_type not in COMPARISON_TYPES:
        comparison_type = "readiness_demo"

    return {
        "comparison_id": _safe_str(payload.get("comparison_id") or "unknown"),
        "comparison_version": _safe_str(payload.get("comparison_version") or WP6_COMPARISON_VERSION),
        "comparison_type": comparison_type or "readiness_demo",
        "scope_id": _safe_str(payload.get("scope_id") or ""),
        "decision_rule_id": _safe_str(payload.get("decision_rule_id") or ""),
        "status": _safe_str(payload.get("status") or "imported_readiness_demo"),
        "evidence_source": "simulated",
        "import_mode": _safe_str(payload.get("import_mode") or "local_file"),
        "source_file": source_file,
        "linked_uncertainty_case_ids": _safe_list(payload.get("linked_uncertainty_case_ids")),
        "linked_method_confirmation_protocol_ids": _safe_list(payload.get("linked_method_confirmation_protocol_ids")),
        "linked_software_validation_release_ids": _safe_list(payload.get("linked_software_validation_release_ids")),
        "reference_asset_refs": _safe_list(payload.get("reference_asset_refs")),
        "certificate_lifecycle_refs": _safe_list(payload.get("certificate_lifecycle_refs")),
        "reviewer_only": True,
        "readiness_mapping_only": True,
        "not_real_acceptance_evidence": True,
        "not_ready_for_formal_claim": True,
        "primary_evidence_rewritten": False,
        "limitation_note": _LIMITATION_NOTE,
        "non_claim_note": _NON_CLAIM_NOTE,
        # Preserve any extra fields from the original payload
        "extra_fields": {k: v for k, v in payload.items() if k not in {
            "comparison_id", "comparison_version", "comparison_type", "scope_id",
            "decision_rule_id", "status", "evidence_source", "import_mode",
            "linked_uncertainty_case_ids", "linked_method_confirmation_protocol_ids",
            "linked_software_validation_release_ids", "reference_asset_refs",
            "certificate_lifecycle_refs",
        }},
    }


# ---------------------------------------------------------------------------
# main entry: build_wp6_artifacts
# ---------------------------------------------------------------------------

def build_wp6_artifacts(
    *,
    run_id: str,
    scope_definition_pack: dict[str, Any],
    decision_rule_profile: dict[str, Any],
    reference_asset_registry: dict[str, Any],
    certificate_lifecycle_summary: dict[str, Any],
    pre_run_readiness_gate: dict[str, Any],
    uncertainty_report_pack: dict[str, Any],
    uncertainty_rollup: dict[str, Any],
    method_confirmation_protocol: dict[str, Any],
    verification_digest: dict[str, Any],
    software_validation_rollup: dict[str, Any],
    path_map: dict[str, str],
    filenames: dict[str, str],
    boundary_statements: list[str],
) -> dict[str, dict[str, Any]]:
    """Build WP6 artifacts: PT/ILC + comparison evidence pack + reviewer navigation.

    Step 2 only — reviewer-facing / readiness-mapping-only.
    """
    common = _common_context(
        run_id=run_id,
        scope_definition_pack=scope_definition_pack,
        decision_rule_profile=decision_rule_profile,
        reference_asset_registry=reference_asset_registry,
        certificate_lifecycle_summary=certificate_lifecycle_summary,
        pre_run_readiness_gate=pre_run_readiness_gate,
        uncertainty_report_pack=uncertainty_report_pack,
        uncertainty_rollup=uncertainty_rollup,
        method_confirmation_protocol=method_confirmation_protocol,
        verification_digest=verification_digest,
        software_validation_rollup=software_validation_rollup,
        path_map=path_map,
    )
    registry = _pt_ilc_registry_artifact(
        run_id=run_id, common=common, path_map=path_map,
        filenames=filenames, boundary_statements=boundary_statements,
    )
    importer = _importer_artifact(
        run_id=run_id, common=common, path_map=path_map,
        filenames=filenames, boundary_statements=boundary_statements,
    )
    evidence_pack = _comparison_evidence_pack_artifact(
        run_id=run_id, common=common, path_map=path_map,
        filenames=filenames, boundary_statements=boundary_statements,
    )
    scope_view = _scope_comparison_view_artifact(
        run_id=run_id, common=common, path_map=path_map,
        filenames=filenames, boundary_statements=boundary_statements,
    )
    comp_digest = _comparison_digest_artifact(
        run_id=run_id, common=common, evidence_pack=evidence_pack,
        path_map=path_map, filenames=filenames, boundary_statements=boundary_statements,
    )
    rollup = _comparison_rollup_artifact(
        run_id=run_id, common=common, evidence_pack=evidence_pack,
        comparison_digest=comp_digest,
        path_map=path_map, filenames=filenames, boundary_statements=boundary_statements,
    )
    return {
        "pt_ilc_registry": registry,
        "external_comparison_importer": importer,
        "comparison_evidence_pack": evidence_pack,
        "scope_comparison_view": scope_view,
        "comparison_digest": comp_digest,
        "comparison_rollup": rollup,
    }


# ---------------------------------------------------------------------------
# Step 2 closeout digest — aggregates WP1–WP6 readiness status
# ---------------------------------------------------------------------------

STEP2_CLOSEOUT_SCHEMA_VERSION = "step2-closeout-digest-v1"


# ---------------------------------------------------------------------------
# Shared helpers for Step 2 boundary classification
# ---------------------------------------------------------------------------

_SIMULATED_ONLY_SIGNALS = (
    "evidence_source",       # == "simulated"
    "not_real_acceptance_evidence",  # is True
    "not_ready_for_formal_claim",    # is True
    "reviewer_only",         # is True
    "readiness_mapping_only",  # is True
)


def _extract_boundary_flags(payload: dict[str, Any]) -> dict[str, Any]:
    """Extract Step 2 boundary flags from a payload, searching multiple nesting levels.

    Searches: payload top-level, payload["raw"], payload["digest"],
    payload["digest"]["raw"], and one level into common nested keys
    (bundle, artifact, imported_data, rollup, summary, data) for their raw or dict.
    """
    flags: dict[str, Any] = {}
    candidates: list[dict[str, Any]] = []

    # Level 0: top-level
    if isinstance(payload, dict):
        candidates.append(payload)

    # Level 1: raw
    raw = payload.get("raw") if isinstance(payload, dict) else None
    if isinstance(raw, dict):
        candidates.append(raw)

    # Level 1: digest
    digest = payload.get("digest") if isinstance(payload, dict) else None
    if isinstance(digest, dict):
        candidates.append(digest)
        # Level 2: digest.raw
        digest_raw = digest.get("raw")
        if isinstance(digest_raw, dict):
            candidates.append(digest_raw)

    # Level 1: common nested containers
    for nested_key in ("bundle", "artifact", "imported_data", "rollup", "summary", "data"):
        nested = payload.get(nested_key) if isinstance(payload, dict) else None
        if isinstance(nested, dict):
            candidates.append(nested)
            nested_raw = nested.get("raw")
            if isinstance(nested_raw, dict):
                candidates.append(nested_raw)

    for c in candidates:
        for signal in _SIMULATED_ONLY_SIGNALS:
            if signal in c and signal not in flags:
                flags[signal] = c[signal]

    return flags


def _classify_step2_payload_status(payload: dict[str, Any]) -> str:
    """Conservatively classify a WP payload's Step 2 status.

    Returns one of:
    - "not_available": payload is empty or available==False
    - "simulated_readiness_only": any simulated-only boundary signal detected
    - "available": payload exists with no simulated-only signals (rare in Step 2)
    """
    if not payload:
        return "not_available"

    # Check available flag at top level or in raw
    available = payload.get("available")
    if available is None:
        raw = payload.get("raw")
        if isinstance(raw, dict):
            available = raw.get("available")
    if available is False:
        return "not_available"

    # Extract boundary flags from all nesting levels
    flags = _extract_boundary_flags(payload)

    # If any simulated-only signal is present, classify as simulated_readiness_only
    if flags.get("evidence_source") == "simulated":
        return "simulated_readiness_only"
    if flags.get("not_real_acceptance_evidence") is True:
        return "simulated_readiness_only"
    if flags.get("not_ready_for_formal_claim") is True:
        return "simulated_readiness_only"
    if flags.get("reviewer_only") is True:
        return "simulated_readiness_only"
    if flags.get("readiness_mapping_only") is True:
        return "simulated_readiness_only"

    return "available"


def build_step2_closeout_digest(
    *,
    run_id: str,
    scope_definition_pack: dict[str, Any],
    decision_rule_profile: dict[str, Any],
    reference_asset_registry: dict[str, Any],
    certificate_lifecycle_summary: dict[str, Any],
    pre_run_readiness_gate: dict[str, Any],
    uncertainty_report_pack: dict[str, Any],
    uncertainty_rollup: dict[str, Any],
    method_confirmation_protocol: dict[str, Any],
    verification_digest: dict[str, Any],
    software_validation_rollup: dict[str, Any],
    comparison_rollup: dict[str, Any],
    boundary_statements: list[str],
) -> dict[str, Any]:
    """Build a Step 2 closeout digest aggregating WP1–WP6 readiness status.

    This is a **reviewer-facing readiness summary only** — not a formal claim.
    """
    wp_status: dict[str, str] = {}
    for label, payload in [
        ("WP1_scope", scope_definition_pack),
        ("WP1_decision_rule", decision_rule_profile),
        ("WP1_reference_asset", reference_asset_registry),
        ("WP1_certificate", certificate_lifecycle_summary),
        ("WP1_pre_run_gate", pre_run_readiness_gate),
        ("WP3_uncertainty", uncertainty_report_pack),
        ("WP3_uncertainty_rollup", uncertainty_rollup),
        ("WP4_method_confirmation", method_confirmation_protocol),
        ("WP4_verification", verification_digest),
        ("WP5_software_validation", software_validation_rollup),
        ("WP6_comparison", comparison_rollup),
    ]:
        wp_status[label] = _classify_step2_payload_status(payload)

    all_simulated = all(
        v in ("simulated_readiness_only", "not_available") for v in wp_status.values()
    )

    raw = {
        "schema_version": STEP2_CLOSEOUT_SCHEMA_VERSION,
        "artifact_type": "step2_closeout_digest",
        "generated_at": _now_iso(),
        "run_id": run_id,
        "evidence_source": "simulated",
        "evidence_state": "reviewer_readiness_only",
        "not_real_acceptance_evidence": True,
        "ready_for_readiness_mapping": True,
        "not_ready_for_formal_claim": True,
        "primary_evidence_rewritten": False,
        "readiness_mapping_only": True,
        "reviewer_only": True,
        "wp_status": wp_status,
        "all_simulated": all_simulated,
        "scope_summary": str(
            scope_definition_pack.get("raw", scope_definition_pack).get("scope_id", "--")
        ),
        "decision_rule_summary": str(
            decision_rule_profile.get("raw", decision_rule_profile).get("decision_rule_id", "--")
        ),
        "uncertainty_status": wp_status.get("WP3_uncertainty", "--"),
        "method_confirmation_status": wp_status.get("WP4_method_confirmation", "--"),
        "software_validation_status": wp_status.get("WP5_software_validation", "--"),
        "comparison_status": wp_status.get("WP6_comparison", "--"),
        "pre_run_gate_status": wp_status.get("WP1_pre_run_gate", "--"),
        "non_claim_note": (
            "Step 2 closeout digest is reviewer-facing readiness summary only. "
            "All evidence is simulated. This is not a formal claim, accreditation, "
            "or acceptance declaration."
        ),
        "limitation_note": "Aggregates Step 2 simulation-only readiness; does not close formal evidence.",
        "reviewer_note": "Step 2 阶段收口摘要：仅用于 reviewer 就绪映射，不构成正式声明。",
        "boundary_statements": list(boundary_statements),
    }

    digest = {
        "all_simulated": all_simulated,
        "wp_count": len(wp_status),
        "simulated_count": sum(
            1 for v in wp_status.values() if v == "simulated_readiness_only"
        ),
        "boundary_digest": "Step 2 readiness-mapping-only closeout",
    }

    review_surface = {
        "title": "Step 2 阶段收口摘要",
        "title_en": "Step 2 Closeout Digest",
        "summary": f"WP1–WP6 就绪状态聚合 (共{len(wp_status)}项)",
        "summary_en": f"WP1–WP6 readiness status aggregation ({len(wp_status)} items)",
        "all_simulated": all_simulated,
        "non_claim": True,
    }

    return {
        "available": True,
        "artifact_type": "step2_closeout_digest",
        "filename": "step2_closeout_digest.json",
        "markdown_filename": "step2_closeout_digest.md",
        "raw": raw,
        "digest": digest,
        "review_surface": review_surface,
    }
