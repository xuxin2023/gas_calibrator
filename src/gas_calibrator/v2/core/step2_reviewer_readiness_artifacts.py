from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


STEP2_CLOSEOUT_DIGEST_FILENAME = "step2_closeout_digest.json"
EVIDENCE_COVERAGE_MATRIX_FILENAME = "evidence_coverage_matrix.json"
RESULT_TRACEABILITY_TREE_FILENAME = "result_traceability_tree.json"
EVIDENCE_LINEAGE_INDEX_FILENAME = "evidence_lineage_index.json"
REVIEWER_ANCHOR_NAVIGATION_FILENAME = "reviewer_anchor_navigation.json"
AI_RUN_SUMMARY_FILENAME = "ai_run_summary.md"

STEP2_CLOSEOUT_DIGEST_ARTIFACT_TYPE = "step2_closeout_digest"
EVIDENCE_COVERAGE_MATRIX_ARTIFACT_TYPE = "evidence_coverage_matrix"
RESULT_TRACEABILITY_TREE_ARTIFACT_TYPE = "result_traceability_tree"
EVIDENCE_LINEAGE_INDEX_ARTIFACT_TYPE = "evidence_lineage_index"
REVIEWER_ANCHOR_NAVIGATION_ARTIFACT_TYPE = "reviewer_anchor_navigation"
AI_RUN_SUMMARY_ARTIFACT_TYPE = "ai_run_summary"

STEP2_REVIEWER_READINESS_SCHEMA_VERSION = "step2-reviewer-readiness-v1"

_BOUNDARY_FIELDS: dict[str, Any] = {
    "reviewer_only": True,
    "readiness_mapping_only": True,
    "not_real_acceptance_evidence": True,
    "not_ready_for_formal_claim": True,
    "file_artifact_first_preserved": True,
    "main_chain_dependency": False,
}
_BOUNDARY_SUMMARY = (
    "reviewer_only=true | readiness_mapping_only=true | "
    "not_real_acceptance_evidence=true | not_ready_for_formal_claim=true | "
    "file_artifact_first_preserved=true | main_chain_dependency=false"
)


def build_step2_reviewer_readiness_artifacts(
    *,
    run_id: str = "",
    run_dir: str | Path | None = None,
    summary: dict[str, Any] | None = None,
    manifest: dict[str, Any] | None = None,
    results: dict[str, Any] | None = None,
    scope_definition_pack: dict[str, Any] | None = None,
    decision_rule_profile: dict[str, Any] | None = None,
    conformity_statement_profile: dict[str, Any] | None = None,
    reference_asset_registry: dict[str, Any] | None = None,
    certificate_lifecycle_summary: dict[str, Any] | None = None,
    uncertainty_report_pack: dict[str, Any] | None = None,
    uncertainty_rollup: dict[str, Any] | None = None,
    method_confirmation_protocol: dict[str, Any] | None = None,
    verification_rollup: dict[str, Any] | None = None,
    software_validation_traceability_matrix: dict[str, Any] | None = None,
    release_manifest: dict[str, Any] | None = None,
    comparison_evidence_pack: dict[str, Any] | None = None,
    comparison_rollup: dict[str, Any] | None = None,
    stage3_standards_alignment_matrix: dict[str, Any] | None = None,
    run_metadata_profile: dict[str, Any] | None = None,
    operator_authorization_profile: dict[str, Any] | None = None,
    training_record: dict[str, Any] | None = None,
    sop_version_binding: dict[str, Any] | None = None,
    qc_flag_catalog: dict[str, Any] | None = None,
    recovery_action_log: dict[str, Any] | None = None,
    reviewer_dual_check_placeholder: dict[str, Any] | None = None,
    sidecar_index_summary: dict[str, Any] | None = None,
    review_copilot_payload: dict[str, Any] | None = None,
    model_governance_summary: dict[str, Any] | None = None,
    existing_ai_run_summary_text: str = "",
) -> dict[str, Any]:
    normalized_run_id = str(run_id or "").strip()
    normalized_run_dir = str(Path(run_dir)) if run_dir is not None else ""
    summary_payload = dict(summary or {})
    manifest_payload = dict(manifest or {})
    results_payload = dict(results or {})
    scope_payload = dict(scope_definition_pack or {})
    decision_payload = dict(decision_rule_profile or {})
    conformity_payload = dict(
        conformity_statement_profile
        or dict(decision_payload.get("conformity_statement_profile") or {})
        or {}
    )
    reference_payload = dict(reference_asset_registry or {})
    certificate_payload = dict(certificate_lifecycle_summary or {})
    uncertainty_pack = dict(uncertainty_report_pack or {})
    uncertainty_rollup_payload = dict(uncertainty_rollup or {})
    method_payload = dict(method_confirmation_protocol or {})
    verification_payload = dict(verification_rollup or {})
    software_validation_payload = dict(software_validation_traceability_matrix or {})
    release_manifest_payload = dict(release_manifest or {})
    comparison_payload = dict(comparison_evidence_pack or {})
    comparison_rollup_payload = dict(comparison_rollup or {})
    standards_matrix_payload = dict(stage3_standards_alignment_matrix or {})
    human_governance_payloads = {
        "run_metadata_profile": dict(run_metadata_profile or {}),
        "operator_authorization_profile": dict(operator_authorization_profile or {}),
        "training_record": dict(training_record or {}),
        "sop_version_binding": dict(sop_version_binding or {}),
        "qc_flag_catalog": dict(qc_flag_catalog or {}),
        "recovery_action_log": dict(recovery_action_log or {}),
        "reviewer_dual_check_placeholder": dict(reviewer_dual_check_placeholder or {}),
    }
    sidecar_payloads = {
        "sidecar_index_summary": dict(sidecar_index_summary or {}),
        "review_copilot_payload": dict(review_copilot_payload or {}),
        "model_governance_summary": dict(model_governance_summary or {}),
    }

    scope_context = _build_scope_context(
        scope_definition_pack=scope_payload,
        decision_rule_profile=decision_payload,
        conformity_statement_profile=conformity_payload,
    )
    uncertainty_method_context = _build_uncertainty_method_context(
        uncertainty_report_pack=uncertainty_pack,
        uncertainty_rollup=uncertainty_rollup_payload,
        method_confirmation_protocol=method_payload,
        verification_rollup=verification_payload,
    )
    coverage_matrix = build_evidence_coverage_matrix(
        run_id=normalized_run_id,
        run_dir=normalized_run_dir,
        scope_context=scope_context,
        standards_alignment_matrix=standards_matrix_payload,
        software_validation_traceability_matrix=software_validation_payload,
        release_manifest=release_manifest_payload,
        comparison_evidence_pack=comparison_payload,
        human_governance_payloads=human_governance_payloads,
    )
    traceability_tree = build_result_traceability_tree(
        run_id=normalized_run_id,
        run_dir=normalized_run_dir,
        summary=summary_payload,
        manifest=manifest_payload,
        results=results_payload,
        scope_context=scope_context,
        uncertainty_method_context=uncertainty_method_context,
        reference_asset_registry=reference_payload,
        certificate_lifecycle_summary=certificate_payload,
        release_manifest=release_manifest_payload,
        comparison_evidence_pack=comparison_payload,
        human_governance_payloads=human_governance_payloads,
    )
    lineage_index = build_evidence_lineage_index(
        run_id=normalized_run_id,
        run_dir=normalized_run_dir,
        coverage_matrix=coverage_matrix,
        traceability_tree=traceability_tree,
        scope_definition_pack=scope_payload,
        decision_rule_profile=decision_payload,
        uncertainty_report_pack=uncertainty_pack,
        uncertainty_rollup=uncertainty_rollup_payload,
        method_confirmation_protocol=method_payload,
        verification_rollup=verification_payload,
        software_validation_traceability_matrix=software_validation_payload,
        release_manifest=release_manifest_payload,
        comparison_evidence_pack=comparison_payload,
        human_governance_payloads=human_governance_payloads,
    )
    reviewer_anchor_navigation = build_reviewer_anchor_navigation(
        run_id=normalized_run_id,
        run_dir=normalized_run_dir,
        coverage_matrix=coverage_matrix,
        traceability_tree=traceability_tree,
        lineage_index=lineage_index,
        scope_context=scope_context,
        uncertainty_method_context=uncertainty_method_context,
    )
    ai_run_summary = build_ai_run_summary_artifact(
        run_id=normalized_run_id,
        run_dir=normalized_run_dir,
        sidecar_index_summary=sidecar_payloads["sidecar_index_summary"],
        review_copilot_payload=sidecar_payloads["review_copilot_payload"],
        model_governance_summary=sidecar_payloads["model_governance_summary"],
        existing_markdown_text=existing_ai_run_summary_text,
    )
    step2_closeout_digest = build_step2_closeout_digest(
        run_id=normalized_run_id,
        run_dir=normalized_run_dir,
        scope_context=scope_context,
        uncertainty_method_context=uncertainty_method_context,
        coverage_matrix=coverage_matrix,
        traceability_tree=traceability_tree,
        lineage_index=lineage_index,
        reviewer_anchor_navigation=reviewer_anchor_navigation,
        ai_run_summary=ai_run_summary,
        human_governance_payloads=human_governance_payloads,
        sidecar_payloads=sidecar_payloads,
        comparison_rollup=comparison_rollup_payload,
    )
    return {
        "step2_closeout_digest": step2_closeout_digest,
        "evidence_coverage_matrix": coverage_matrix,
        "result_traceability_tree": traceability_tree,
        "evidence_lineage_index": lineage_index,
        "reviewer_anchor_navigation": reviewer_anchor_navigation,
        "ai_run_summary_payload": ai_run_summary,
        "ai_run_summary_markdown": str(ai_run_summary.get("markdown") or ""),
    }


def build_evidence_coverage_matrix(
    *,
    run_id: str,
    run_dir: str,
    scope_context: dict[str, Any],
    standards_alignment_matrix: dict[str, Any] | None,
    software_validation_traceability_matrix: dict[str, Any] | None,
    release_manifest: dict[str, Any] | None,
    comparison_evidence_pack: dict[str, Any] | None,
    human_governance_payloads: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    standards_payload = dict(standards_alignment_matrix or {})
    rows = [
        _normalize_coverage_row(
            row,
            index=index,
            scope_context=scope_context,
            software_validation_traceability_matrix=dict(software_validation_traceability_matrix or {}),
            release_manifest=dict(release_manifest or {}),
            comparison_evidence_pack=dict(comparison_evidence_pack or {}),
            human_governance_payloads=human_governance_payloads,
        )
        for index, row in enumerate(list(standards_payload.get("rows") or []), start=1)
        if isinstance(row, dict)
    ]
    if not rows:
        rows = _fallback_coverage_rows(
            scope_context=scope_context,
            software_validation_traceability_matrix=dict(software_validation_traceability_matrix or {}),
            release_manifest=dict(release_manifest or {}),
            comparison_evidence_pack=dict(comparison_evidence_pack or {}),
            human_governance_payloads=human_governance_payloads,
        )
    standard_families = _dedupe(row.get("standard_family") for row in rows)
    evidence_categories = _dedupe(
        category
        for row in rows
        for category in list(row.get("required_evidence_categories") or [])
    )
    readiness_statuses = _dedupe(row.get("readiness_status") for row in rows)
    missing_rows = [row for row in rows if bool(row.get("missing_coverage"))]
    gapped_rows = [row for row in rows if bool(row.get("blockers_or_gaps"))]
    digest = {
        "summary": (
            f"evidence coverage matrix | families {len(standard_families)} | "
            f"rows {len(rows)} | missing {len(missing_rows)} | gaps {len(gapped_rows)}"
        ),
        "standard_family_summary": " | ".join(standard_families) if standard_families else "--",
        "evidence_category_summary": " | ".join(evidence_categories) if evidence_categories else "--",
        "readiness_status_summary": " | ".join(readiness_statuses) if readiness_statuses else "--",
        "missing_evidence_summary": "; ".join(
            str(row.get("topic_or_control_object") or row.get("standard_family") or "").strip()
            for row in missing_rows[:4]
        )
        or "none",
        "top_gaps_summary": "; ".join(
            str(row.get("gap_note") or "").strip()
            for row in gapped_rows[:4]
            if str(row.get("gap_note") or "").strip()
        )
        or "none",
    }
    artifact_paths = {
        EVIDENCE_COVERAGE_MATRIX_ARTIFACT_TYPE: _artifact_output_path(run_dir, EVIDENCE_COVERAGE_MATRIX_FILENAME),
    }
    return {
        "schema_version": STEP2_REVIEWER_READINESS_SCHEMA_VERSION,
        "artifact_type": EVIDENCE_COVERAGE_MATRIX_ARTIFACT_TYPE,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "run_id": run_id,
        "run_dir": run_dir,
        "mapping_scope": "family_topic_control_object_level_only",
        "rows": rows,
        "standard_family": list(standard_families),
        "evidence_categories": list(evidence_categories),
        "digest": digest,
        "artifact_paths": artifact_paths,
        "review_surface": {
            "summary_text": digest["summary"],
            "summary_lines": [
                digest["summary"],
                f"scope_id: {scope_context.get('scope_id') or '--'}",
                f"decision_rule_id: {scope_context.get('decision_rule_id') or '--'}",
                f"missing coverage: {len(missing_rows)}",
                f"blockers / gaps: {len(gapped_rows)}",
            ],
            "anchor_id": "evidence-coverage-matrix",
            "anchor_label": "证据覆盖矩阵",
            "standard_family_filters": list(standard_families),
            "evidence_category_filters": list(evidence_categories),
            "readiness_status_filters": list(readiness_statuses),
            "missing_coverage_filters": _dedupe(
                row.get("missing_coverage_filter") for row in rows
            ),
            "gap_filters": _dedupe(row.get("gap_filter") for row in rows),
            "anchor_filters": _dedupe(
                ["evidence-coverage-matrix"]
                + [row.get("anchor_id") for row in rows]
            ),
            "anchor_rows": [
                {
                    "anchor_id": str(row.get("anchor_id") or ""),
                    "anchor_label": str(
                        row.get("anchor_label")
                        or row.get("topic_or_control_object")
                        or row.get("standard_family")
                        or ""
                    ),
                }
                for row in rows
                if str(row.get("anchor_id") or "").strip()
            ],
        },
        "evidence_source": "simulated",
        "non_claim": [
            "readiness mapping only",
            "family/topic/control-object level only",
            "not formal claim",
        ],
        **_BOUNDARY_FIELDS,
    }


def build_result_traceability_tree(
    *,
    run_id: str,
    run_dir: str,
    summary: dict[str, Any] | None,
    manifest: dict[str, Any] | None,
    results: dict[str, Any] | None,
    scope_context: dict[str, Any],
    uncertainty_method_context: dict[str, Any],
    reference_asset_registry: dict[str, Any] | None,
    certificate_lifecycle_summary: dict[str, Any] | None,
    release_manifest: dict[str, Any] | None,
    comparison_evidence_pack: dict[str, Any] | None,
    human_governance_payloads: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    summary_payload = dict(summary or {})
    manifest_payload = dict(manifest or {})
    results_payload = dict(results or {})
    reference_payload = dict(reference_asset_registry or {})
    certificate_payload = dict(certificate_lifecycle_summary or {})
    release_payload = dict(release_manifest or {})
    comparison_payload = dict(comparison_evidence_pack or {})
    samples = [dict(item) for item in list(results_payload.get("samples") or []) if isinstance(item, dict)]
    point_summaries = [
        dict(item) for item in list(results_payload.get("point_summaries") or []) if isinstance(item, dict)
    ]
    root_node_key = f"result:{run_id or Path(run_dir).name or 'current'}"
    nodes: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []

    def add_node(*, node_key: str, node_type: str, label: str, summary_text: str = "", artifact_key: str = "", artifact_path: str = "") -> None:
        if any(str(node.get("node_key") or "") == node_key for node in nodes):
            return
        nodes.append(
            {
                "node_key": node_key,
                "node_type": node_type,
                "label": label,
                "summary": summary_text,
                "artifact_key": artifact_key,
                "artifact_path": artifact_path,
            }
        )

    def add_edge(source: str, target: str, relationship: str) -> None:
        if not source or not target:
            return
        edge = {"from": source, "to": target, "relationship": relationship}
        if edge not in edges:
            edges.append(edge)

    result_summary = str(
        dict(summary_payload.get("stats") or {}).get("sample_count")
        or len(samples)
        or len(point_summaries)
    )
    add_node(
        node_key=root_node_key,
        node_type="result",
        label="result",
        summary_text=f"run {run_id or '--'} | points {result_summary}",
        artifact_key="results_json",
        artifact_path=_artifact_output_path(run_dir, "results.json"),
    )
    fit_node_key = f"fit:{run_id or 'current'}:reviewer"
    fit_summary = str(
        uncertainty_method_context.get("calculation_chain_summary")
        or uncertainty_method_context.get("protocol_overview_summary")
        or "reviewer fit rollup"
    )
    add_node(
        node_key=fit_node_key,
        node_type="fit",
        label="fit",
        summary_text=fit_summary,
        artifact_key="uncertainty_report_pack",
        artifact_path=_artifact_path_from_payload(
            uncertainty_method_context.get("uncertainty_report_pack_path"),
            run_dir,
            "uncertainty_report_pack.json",
        ),
    )
    add_edge(root_node_key, fit_node_key, "aggregates_fit")

    point_keys: list[str] = []
    point_indexes = set()
    for sample in samples:
        point = dict(sample.get("point") or {})
        point_index = int(point.get("index") or len(point_keys) + 1)
        point_indexes.add(point_index)
        point_key = f"point:{point_index}"
        point_keys.append(point_key)
        add_node(
            node_key=point_key,
            node_type="point",
            label=f"point {point_index}",
            summary_text=(
                f"route {point.get('route') or '--'} | "
                f"temperature {point.get('temperature_c') or '--'} | "
                f"co2 {point.get('co2_ppm') or '--'}"
            ),
            artifact_key="results_json",
            artifact_path=_artifact_output_path(run_dir, "results.json"),
        )
        add_edge(root_node_key, point_key, "contains_point")
        add_edge(point_key, fit_node_key, "feeds_fit")
    for item in point_summaries:
        point = dict(item.get("point") or {})
        point_index = int(point.get("index") or len(point_indexes) + 1)
        if point_index in point_indexes:
            continue
        point_key = f"point:{point_index}"
        add_node(
            node_key=point_key,
            node_type="point",
            label=f"point {point_index}",
            summary_text=str(dict(item.get("stats") or {}).get("reason") or "point summary"),
            artifact_key="results_json",
            artifact_path=_artifact_output_path(run_dir, "results.json"),
        )
        add_edge(root_node_key, point_key, "contains_point")
        add_edge(point_key, fit_node_key, "feeds_fit")

    algorithm_version = str(
        summary_payload.get("software_build_id")
        or summary_payload.get("software_version")
        or manifest_payload.get("software_build_id")
        or manifest_payload.get("software_version")
        or "unknown"
    ).strip()
    algorithm_node_key = f"algorithm:{algorithm_version or 'unknown'}"
    add_node(
        node_key=algorithm_node_key,
        node_type="algorithm_version",
        label="algorithm_version",
        summary_text=algorithm_version or "--",
        artifact_key="summary_json",
        artifact_path=_artifact_output_path(run_dir, "summary.json"),
    )
    add_edge(fit_node_key, algorithm_node_key, "uses_algorithm")

    method_node_key = f"method:{uncertainty_method_context.get('method_confirmation_protocol_id') or 'protocol'}"
    add_node(
        node_key=method_node_key,
        node_type="method_confirmation_protocol",
        label="method_confirmation_protocol",
        summary_text=str(uncertainty_method_context.get("protocol_overview_summary") or "--"),
        artifact_key="method_confirmation_protocol",
        artifact_path=_artifact_path_from_payload(
            uncertainty_method_context.get("method_confirmation_protocol_path"),
            run_dir,
            "method_confirmation_protocol.json",
        ),
    )
    add_edge(fit_node_key, method_node_key, "validated_by_method")

    uncertainty_case_id = str(uncertainty_method_context.get("uncertainty_case_id") or "").strip() or "uncertainty-case"
    uncertainty_node_key = f"uncertainty:{uncertainty_case_id}"
    add_node(
        node_key=uncertainty_node_key,
        node_type="uncertainty_case",
        label="uncertainty_case",
        summary_text=str(uncertainty_method_context.get("uncertainty_overview_summary") or uncertainty_case_id),
        artifact_key="uncertainty_report_pack",
        artifact_path=_artifact_path_from_payload(
            uncertainty_method_context.get("uncertainty_report_pack_path"),
            run_dir,
            "uncertainty_report_pack.json",
        ),
    )
    add_edge(fit_node_key, uncertainty_node_key, "bounded_by_uncertainty_case")

    decision_node_key = f"decision:{scope_context.get('decision_rule_id') or 'reviewer-rule'}"
    add_node(
        node_key=decision_node_key,
        node_type="decision_rule_profile",
        label="decision_rule_profile",
        summary_text=str(scope_context.get("decision_rule_id") or "--"),
        artifact_key="decision_rule_profile",
        artifact_path=_artifact_path_from_payload(
            scope_context.get("decision_rule_path"),
            run_dir,
            "decision_rule_profile.json",
        ),
    )
    add_edge(fit_node_key, decision_node_key, "evaluated_by_decision_rule")

    scope_node_key = f"scope:{scope_context.get('scope_id') or 'reviewer-scope'}"
    add_node(
        node_key=scope_node_key,
        node_type="scope_definition_pack",
        label="scope_definition_pack",
        summary_text=str(scope_context.get("scope_name") or scope_context.get("scope_id") or "--"),
        artifact_key="scope_definition_pack",
        artifact_path=_artifact_path_from_payload(
            scope_context.get("scope_path"),
            run_dir,
            "scope_definition_pack.json",
        ),
    )
    add_edge(decision_node_key, scope_node_key, "applies_within_scope")

    reference_node_key = "reference_asset_registry"
    add_node(
        node_key=reference_node_key,
        node_type="reference_asset_registry",
        label="reference_asset_registry",
        summary_text=str(
            dict(reference_payload.get("digest") or {}).get("summary")
            or dict(reference_payload.get("digest") or {}).get("asset_count_summary")
            or "reference assets"
        ),
        artifact_key="reference_asset_registry",
        artifact_path=_artifact_path_from_payload(
            dict(reference_payload.get("artifact_paths") or {}).get("reference_asset_registry"),
            run_dir,
            "reference_asset_registry.json",
        ),
    )
    add_edge(scope_node_key, reference_node_key, "binds_reference_assets")

    certificate_node_key = "certificate_lifecycle"
    add_node(
        node_key=certificate_node_key,
        node_type="certificate_lifecycle",
        label="certificate_lifecycle",
        summary_text=str(
            dict(certificate_payload.get("digest") or {}).get("summary")
            or dict(certificate_payload.get("digest") or {}).get("certificate_validity_summary")
            or "certificate lifecycle"
        ),
        artifact_key="certificate_lifecycle_summary",
        artifact_path=_artifact_path_from_payload(
            dict(certificate_payload.get("artifact_paths") or {}).get("certificate_lifecycle_summary"),
            run_dir,
            "certificate_lifecycle_summary.json",
        ),
    )
    add_edge(reference_node_key, certificate_node_key, "proven_by_certificate_lifecycle")

    release_node_key = f"release:{str(release_payload.get('release_id') or release_payload.get('run_id') or run_id or 'current')}"
    add_node(
        node_key=release_node_key,
        node_type="software_validation_release_manifest",
        label="software_validation release manifest",
        summary_text=str(
            dict(release_payload.get("digest") or {}).get("summary")
            or dict(release_payload.get("digest") or {}).get("current_coverage_summary")
            or "release manifest"
        ),
        artifact_key="release_manifest",
        artifact_path=_artifact_path_from_payload(
            dict(release_payload.get("artifact_paths") or {}).get("release_manifest"),
            run_dir,
            "release_manifest.json",
        ),
    )
    add_edge(fit_node_key, release_node_key, "implemented_by_release_manifest")

    comparison_rows = list(comparison_payload.get("comparison_rows") or [])
    if comparison_payload or comparison_rows:
        comparison_node_key = f"comparison:{str(comparison_payload.get('pack_id') or run_id or 'current')}"
        add_node(
            node_key=comparison_node_key,
            node_type="comparison_evidence",
            label="comparison evidence",
            summary_text=str(
                comparison_payload.get("comparison_overview_summary")
                or dict(comparison_payload.get("digest") or {}).get("comparison_overview_summary")
                or "comparison evidence"
            ),
            artifact_key="comparison_evidence_pack",
            artifact_path=_artifact_path_from_payload(
                dict(comparison_payload.get("artifact_paths") or {}).get("comparison_evidence_pack"),
                run_dir,
                "comparison_evidence_pack.json",
            ),
        )
        add_edge(fit_node_key, comparison_node_key, "benchmarked_by_comparison")

    governance_node_key = f"governance:{run_id or 'current'}"
    governance_summary = _human_governance_summary(human_governance_payloads)
    add_node(
        node_key=governance_node_key,
        node_type="human_governance_bundle",
        label="human governance bundle",
        summary_text=governance_summary,
        artifact_key="run_metadata_profile",
        artifact_path=_artifact_path_from_payload(
            dict(human_governance_payloads.get("run_metadata_profile") or {}).get("path"),
            run_dir,
            "run_metadata_profile.json",
        ),
    )
    add_edge(root_node_key, governance_node_key, "governed_by_human_bundle")

    for artifact_key in (
        "operator_authorization_profile",
        "sop_version_binding",
        "reviewer_dual_check_placeholder",
    ):
        artifact_payload = dict(human_governance_payloads.get(artifact_key) or {})
        node_key = f"governance:{artifact_key}"
        add_node(
            node_key=node_key,
            node_type=artifact_key,
            label=artifact_key,
            summary_text=str(
                artifact_payload.get("summary_line")
                or dict(artifact_payload.get("digest") or {}).get("summary")
                or artifact_key
            ),
            artifact_key=artifact_key,
            artifact_path=_artifact_path_from_payload(
                artifact_payload.get("path")
                or dict(artifact_payload.get("artifact_paths") or {}).get(artifact_key),
                run_dir,
                f"{artifact_key}.json",
            ),
        )
        add_edge(governance_node_key, node_key, "includes")

    digest = {
        "summary": (
            f"result traceability tree | nodes {len(nodes)} | edges {len(edges)} | "
            f"points {len([node for node in nodes if node.get('node_type') == 'point'])}"
        ),
        "algorithm_version_summary": algorithm_version or "--",
        "uncertainty_case_summary": uncertainty_case_id or "--",
        "scope_decision_summary": (
            f"{scope_context.get('scope_id') or '--'} | {scope_context.get('decision_rule_id') or '--'}"
        ),
        "release_manifest_summary": str(
            dict(release_payload.get("digest") or {}).get("summary") or "release manifest"
        ),
        "human_governance_summary": governance_summary,
    }
    artifact_paths = {
        RESULT_TRACEABILITY_TREE_ARTIFACT_TYPE: _artifact_output_path(run_dir, RESULT_TRACEABILITY_TREE_FILENAME),
    }
    linked_node_keys = [str(node.get("node_key") or "") for node in nodes if str(node.get("node_key") or "").strip()]
    return {
        "schema_version": STEP2_REVIEWER_READINESS_SCHEMA_VERSION,
        "artifact_type": RESULT_TRACEABILITY_TREE_ARTIFACT_TYPE,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "run_id": run_id,
        "run_dir": run_dir,
        "root_node_key": root_node_key,
        "nodes": nodes,
        "edges": edges,
        "digest": digest,
        "artifact_paths": artifact_paths,
        "review_surface": {
            "summary_text": digest["summary"],
            "summary_lines": [
                digest["summary"],
                f"algorithm_version: {digest['algorithm_version_summary']}",
                f"uncertainty_case: {digest['uncertainty_case_summary']}",
                f"scope_decision: {digest['scope_decision_summary']}",
                f"human_governance: {digest['human_governance_summary']}",
            ],
            "anchor_id": "result-traceability-tree",
            "anchor_label": "结果溯源树",
            "evidence_category_filters": [
                "result_traceability",
                "scope_decision",
                "uncertainty_method",
                "software_validation",
                "comparison",
                "human_governance",
            ],
            "linked_traceability_node_keys": linked_node_keys,
            "linked_traceability_nodes": [
                str(node.get("label") or node.get("node_key") or "")
                for node in nodes
            ],
        },
        "evidence_source": "simulated",
        **_BOUNDARY_FIELDS,
    }


def build_evidence_lineage_index(
    *,
    run_id: str,
    run_dir: str,
    coverage_matrix: dict[str, Any],
    traceability_tree: dict[str, Any],
    scope_definition_pack: dict[str, Any] | None,
    decision_rule_profile: dict[str, Any] | None,
    uncertainty_report_pack: dict[str, Any] | None,
    uncertainty_rollup: dict[str, Any] | None,
    method_confirmation_protocol: dict[str, Any] | None,
    verification_rollup: dict[str, Any] | None,
    software_validation_traceability_matrix: dict[str, Any] | None,
    release_manifest: dict[str, Any] | None,
    comparison_evidence_pack: dict[str, Any] | None,
    human_governance_payloads: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    node_index = {
        str(node.get("node_key") or ""): dict(node)
        for node in list(traceability_tree.get("nodes") or [])
        if isinstance(node, dict) and str(node.get("node_key") or "").strip()
    }
    lineage_rows = []
    payloads: list[tuple[str, dict[str, Any], list[str]]] = [
        ("scope_definition_pack", dict(scope_definition_pack or {}), ["scope_definition_pack"]),
        ("decision_rule_profile", dict(decision_rule_profile or {}), ["decision_rule_profile"]),
        ("uncertainty_report_pack", dict(uncertainty_report_pack or {}), ["uncertainty_case"]),
        ("uncertainty_rollup", dict(uncertainty_rollup or {}), ["uncertainty_case"]),
        ("method_confirmation_protocol", dict(method_confirmation_protocol or {}), ["method_confirmation_protocol"]),
        ("verification_rollup", dict(verification_rollup or {}), ["method_confirmation_protocol", "uncertainty_case"]),
        (
            "software_validation_traceability_matrix",
            dict(software_validation_traceability_matrix or {}),
            ["software_validation_release_manifest"],
        ),
        ("release_manifest", dict(release_manifest or {}), ["software_validation_release_manifest"]),
        ("comparison_evidence_pack", dict(comparison_evidence_pack or {}), ["comparison_evidence"]),
    ]
    payloads.extend(
        (artifact_key, dict(payload or {}), ["human_governance_bundle"])
        for artifact_key, payload in human_governance_payloads.items()
    )
    for artifact_key, payload, node_types in payloads:
        if not payload:
            continue
        linked_node_keys = _linked_node_keys_for_payload(
            payload,
            node_index=node_index,
            preferred_node_types=node_types,
        )
        lineage_rows.append(
            {
                "artifact_key": artifact_key,
                "artifact_type": str(payload.get("artifact_type") or artifact_key),
                "artifact_path": _artifact_path_from_payload(
                    payload.get("path")
                    or dict(payload.get("artifact_paths") or {}).get(artifact_key),
                    run_dir,
                    f"{artifact_key}.json",
                ),
                "summary": str(
                    payload.get("summary_line")
                    or dict(payload.get("digest") or {}).get("summary")
                    or artifact_key
                ),
                "anchor_id": str(
                    dict(payload.get("review_surface") or {}).get("anchor_id")
                    or payload.get("anchor_id")
                    or artifact_key.replace("_", "-")
                ),
                "standard_family_filters": list(
                    dict(payload.get("review_surface") or {}).get("standard_family_filters") or []
                ),
                "evidence_category_filters": list(
                    dict(payload.get("review_surface") or {}).get("evidence_category_filters")
                    or payload.get("evidence_categories")
                    or []
                ),
                "readiness_status": str(
                    payload.get("readiness_status")
                    or payload.get("overall_status")
                    or dict(payload.get("digest") or {}).get("readiness_status_summary")
                    or "reviewer_only"
                ),
                "linked_traceability_node_keys": linked_node_keys,
                "linked_traceability_nodes": [
                    str(node_index.get(key, {}).get("label") or key)
                    for key in linked_node_keys
                ],
            }
        )
    lineage_rows.append(
        {
            "artifact_key": EVIDENCE_COVERAGE_MATRIX_ARTIFACT_TYPE,
            "artifact_type": EVIDENCE_COVERAGE_MATRIX_ARTIFACT_TYPE,
            "artifact_path": _artifact_output_path(run_dir, EVIDENCE_COVERAGE_MATRIX_FILENAME),
            "summary": str(dict(coverage_matrix.get("digest") or {}).get("summary") or "coverage matrix"),
            "anchor_id": str(dict(coverage_matrix.get("review_surface") or {}).get("anchor_id") or "evidence-coverage-matrix"),
            "standard_family_filters": list(coverage_matrix.get("standard_family") or []),
            "evidence_category_filters": list(coverage_matrix.get("evidence_categories") or []),
            "readiness_status": str(
                dict(coverage_matrix.get("digest") or {}).get("readiness_status_summary") or "coverage_indexed"
            ),
            "linked_traceability_node_keys": [
                str(traceability_tree.get("root_node_key") or "")
            ]
            if str(traceability_tree.get("root_node_key") or "").strip()
            else [],
            "linked_traceability_nodes": [
                str(traceability_tree.get("root_node_key") or "")
            ]
            if str(traceability_tree.get("root_node_key") or "").strip()
            else [],
        }
    )
    digest = {
        "summary": (
            f"evidence lineage index | artifacts {len(lineage_rows)} | "
            f"traceability nodes {len(node_index)}"
        ),
        "coverage_summary": str(dict(coverage_matrix.get("digest") or {}).get("summary") or "--"),
        "next_required_artifacts_summary": "; ".join(
            str(row.get("artifact_key") or "")
            for row in lineage_rows
            if not list(row.get("linked_traceability_node_keys") or [])
        )
        or "none",
    }
    artifact_paths = {
        EVIDENCE_LINEAGE_INDEX_ARTIFACT_TYPE: _artifact_output_path(run_dir, EVIDENCE_LINEAGE_INDEX_FILENAME),
    }
    return {
        "schema_version": STEP2_REVIEWER_READINESS_SCHEMA_VERSION,
        "artifact_type": EVIDENCE_LINEAGE_INDEX_ARTIFACT_TYPE,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "run_id": run_id,
        "run_dir": run_dir,
        "rows": lineage_rows,
        "digest": digest,
        "artifact_paths": artifact_paths,
        "review_surface": {
            "summary_text": digest["summary"],
            "summary_lines": [digest["summary"], f"coverage: {digest['coverage_summary']}"],
            "anchor_id": "evidence-lineage-index",
            "anchor_label": "证据谱系索引",
        },
        "evidence_source": "simulated",
        **_BOUNDARY_FIELDS,
    }


def build_reviewer_anchor_navigation(
    *,
    run_id: str,
    run_dir: str,
    coverage_matrix: dict[str, Any],
    traceability_tree: dict[str, Any],
    lineage_index: dict[str, Any],
    scope_context: dict[str, Any],
    uncertainty_method_context: dict[str, Any],
) -> dict[str, Any]:
    anchors: list[dict[str, Any]] = [
        {
            "anchor_id": "evidence-coverage-matrix",
            "anchor_label": "证据覆盖矩阵",
            "group": "coverage",
            "artifact_key": EVIDENCE_COVERAGE_MATRIX_ARTIFACT_TYPE,
            "artifact_path": _artifact_output_path(run_dir, EVIDENCE_COVERAGE_MATRIX_FILENAME),
            "standard_family_filters": list(coverage_matrix.get("standard_family") or []),
            "evidence_category_filters": list(coverage_matrix.get("evidence_categories") or []),
            "readiness_status_filters": list(
                dict(coverage_matrix.get("review_surface") or {}).get("readiness_status_filters") or []
            ),
            "missing_coverage_filters": list(
                dict(coverage_matrix.get("review_surface") or {}).get("missing_coverage_filters") or []
            ),
            "gap_filters": list(dict(coverage_matrix.get("review_surface") or {}).get("gap_filters") or []),
        },
        {
            "anchor_id": "result-traceability-tree",
            "anchor_label": "结果溯源树",
            "group": "traceability",
            "artifact_key": RESULT_TRACEABILITY_TREE_ARTIFACT_TYPE,
            "artifact_path": _artifact_output_path(run_dir, RESULT_TRACEABILITY_TREE_FILENAME),
            "evidence_category_filters": ["result_traceability", "scope_decision", "uncertainty_method"],
        },
        {
            "anchor_id": "evidence-lineage-index",
            "anchor_label": "证据谱系索引",
            "group": "lineage",
            "artifact_key": EVIDENCE_LINEAGE_INDEX_ARTIFACT_TYPE,
            "artifact_path": _artifact_output_path(run_dir, EVIDENCE_LINEAGE_INDEX_FILENAME),
            "evidence_category_filters": ["lineage", "result_traceability"],
        },
    ]
    for row in list(coverage_matrix.get("rows") or []):
        if not isinstance(row, dict):
            continue
        anchor_id = str(row.get("anchor_id") or "").strip()
        if not anchor_id:
            continue
        anchors.append(
            {
                "anchor_id": anchor_id,
                "anchor_label": str(
                    row.get("anchor_label")
                    or row.get("topic_or_control_object")
                    or row.get("standard_family")
                    or anchor_id
                ),
                "group": "coverage_row",
                "artifact_key": EVIDENCE_COVERAGE_MATRIX_ARTIFACT_TYPE,
                "artifact_path": _artifact_output_path(run_dir, EVIDENCE_COVERAGE_MATRIX_FILENAME),
                "standard_family_filters": [str(row.get("standard_family") or "").strip()],
                "evidence_category_filters": list(row.get("required_evidence_categories") or []),
                "readiness_status_filters": [str(row.get("readiness_status") or "").strip()],
                "missing_coverage_filters": [str(row.get("missing_coverage_filter") or "").strip()],
                "gap_filters": [str(row.get("gap_filter") or "").strip()],
            }
        )
    artifact_paths = {
        REVIEWER_ANCHOR_NAVIGATION_ARTIFACT_TYPE: _artifact_output_path(run_dir, REVIEWER_ANCHOR_NAVIGATION_FILENAME),
    }
    return {
        "schema_version": STEP2_REVIEWER_READINESS_SCHEMA_VERSION,
        "artifact_type": REVIEWER_ANCHOR_NAVIGATION_ARTIFACT_TYPE,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "run_id": run_id,
        "run_dir": run_dir,
        "anchors": anchors,
        "filter_options": {
            "anchor_ids": [str(item.get("anchor_id") or "") for item in anchors if str(item.get("anchor_id") or "").strip()],
            "standard_families": list(coverage_matrix.get("standard_family") or []),
            "evidence_categories": list(coverage_matrix.get("evidence_categories") or []),
            "readiness_statuses": list(
                dict(coverage_matrix.get("review_surface") or {}).get("readiness_status_filters") or []
            ),
            "missing_coverage": list(
                dict(coverage_matrix.get("review_surface") or {}).get("missing_coverage_filters") or []
            ),
            "gaps": list(dict(coverage_matrix.get("review_surface") or {}).get("gap_filters") or []),
        },
        "scope_context": {
            "scope_id": str(scope_context.get("scope_id") or ""),
            "decision_rule_id": str(scope_context.get("decision_rule_id") or ""),
        },
        "uncertainty_method_context": {
            "uncertainty_case_id": str(uncertainty_method_context.get("uncertainty_case_id") or ""),
            "method_confirmation_protocol_id": str(
                uncertainty_method_context.get("method_confirmation_protocol_id") or ""
            ),
            "verification_rollup_id": str(uncertainty_method_context.get("verification_rollup_id") or ""),
        },
        "traceability_root_node_key": str(traceability_tree.get("root_node_key") or ""),
        "lineage_index_artifact_count": len(list(lineage_index.get("rows") or [])),
        "artifact_paths": artifact_paths,
        "review_surface": {
            "summary_text": f"reviewer anchor navigation | anchors {len(anchors)}",
            "summary_lines": [f"anchors: {len(anchors)}", f"scope: {scope_context.get('scope_id') or '--'}"],
            "anchor_id": "reviewer-anchor-navigation",
            "anchor_label": "评审锚点导航",
        },
        "evidence_source": "simulated",
        **_BOUNDARY_FIELDS,
    }


def build_ai_run_summary_artifact(
    *,
    run_id: str,
    run_dir: str,
    sidecar_index_summary: dict[str, Any] | None,
    review_copilot_payload: dict[str, Any] | None,
    model_governance_summary: dict[str, Any] | None,
    existing_markdown_text: str = "",
) -> dict[str, Any]:
    sidecar_payload = dict(sidecar_index_summary or {})
    copilot_payload = dict(review_copilot_payload or {})
    governance_payload = dict(model_governance_summary or {})
    summary_line = str(
        sidecar_payload.get("summary_line")
        or copilot_payload.get("summary_line")
        or copilot_payload.get("risk_summary")
        or governance_payload.get("summary_line")
        or "AI reviewer sidecar summary"
    ).strip()
    summary_lines = _dedupe(
        [
            summary_line,
            str(copilot_payload.get("risk_summary") or "").strip(),
            str(governance_payload.get("summary_line") or "").strip(),
            str(governance_payload.get("release_status") or "").strip(),
        ]
    )
    boundary_lines = [
        "advisory_only = true",
        "reviewer_only = true",
        "not_formal_metrology_conclusion = true",
        "not_real_acceptance_evidence = true",
        "not_ready_for_formal_claim = true",
        "main_chain_dependency = false",
    ]
    generated_markdown = "\n".join(
        [
            "# AI 运行摘要",
            "",
            f"- run_id: {run_id or '--'}",
            *[f"- {line}" for line in summary_lines],
            "",
            "## 边界",
            "",
            *[f"- {line}" for line in boundary_lines],
            "",
        ]
    ).strip() + "\n"
    markdown_text = str(existing_markdown_text or "").strip()
    if markdown_text:
        lowered = markdown_text.lower()
        if (
            "advisory_only" not in lowered
            or "reviewer_only" not in lowered
            or "not_formal_metrology_conclusion" not in lowered
        ):
            markdown_text = generated_markdown
        else:
            markdown_text = markdown_text + ("\n" if not markdown_text.endswith("\n") else "")
    else:
        markdown_text = generated_markdown
    artifact_paths = {
        AI_RUN_SUMMARY_ARTIFACT_TYPE: _artifact_output_path(run_dir, AI_RUN_SUMMARY_FILENAME),
    }
    digest = {
        "summary": summary_line or "AI reviewer sidecar summary",
        "boundary_summary": " | ".join(boundary_lines),
        "review_mode_summary": "advisory_only | reviewer_only | not_formal_metrology_conclusion",
    }
    return {
        "schema_version": STEP2_REVIEWER_READINESS_SCHEMA_VERSION,
        "artifact_type": AI_RUN_SUMMARY_ARTIFACT_TYPE,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "run_id": run_id,
        "run_dir": run_dir,
        "summary_line": summary_line,
        "summary_lines": summary_lines,
        "boundary_lines": boundary_lines,
        "digest": digest,
        "artifact_paths": artifact_paths,
        "markdown": markdown_text,
        "review_surface": {
            "summary_text": summary_line or "AI reviewer sidecar summary",
            "summary_lines": [
                summary_line or "AI reviewer sidecar summary",
                "advisory_only = true",
                "reviewer_only = true",
                "not_formal_metrology_conclusion = true",
            ],
            "detail_lines": list(summary_lines),
            "anchor_id": "ai-run-summary",
            "anchor_label": "AI 运行摘要",
            "phase_filters": ["step2_tail_stage3_bridge"],
            "artifact_role_filters": ["diagnostic_analysis"],
            "evidence_category_filters": ["sidecar_ai_surface", "reviewer_summary"],
            "boundary_filters": list(boundary_lines),
            "evidence_source_filters": ["simulated"],
        },
        "evidence_categories": ["sidecar_ai_surface", "reviewer_summary"],
        "advisory_only": True,
        "reviewer_only": True,
        "not_formal_metrology_conclusion": True,
        "not_real_acceptance_evidence": True,
        "not_ready_for_formal_claim": True,
        "not_device_control": True,
        "not_coefficient_writeback": True,
        "evidence_source": "simulated",
        "main_chain_dependency": False,
        "file_artifact_first_preserved": True,
    }


def build_step2_closeout_digest(
    *,
    run_id: str,
    run_dir: str,
    scope_context: dict[str, Any],
    uncertainty_method_context: dict[str, Any],
    coverage_matrix: dict[str, Any],
    traceability_tree: dict[str, Any],
    lineage_index: dict[str, Any],
    reviewer_anchor_navigation: dict[str, Any],
    ai_run_summary: dict[str, Any],
    human_governance_payloads: dict[str, dict[str, Any]],
    sidecar_payloads: dict[str, dict[str, Any]],
    comparison_rollup: dict[str, Any] | None,
) -> dict[str, Any]:
    coverage_digest = dict(coverage_matrix.get("digest") or {})
    traceability_digest = dict(traceability_tree.get("digest") or {})
    lineage_digest = dict(lineage_index.get("digest") or {})
    comparison_rollup_payload = dict(comparison_rollup or {})
    sidecar_summary = str(
        dict(sidecar_payloads.get("sidecar_index_summary") or {}).get("summary_line")
        or dict(sidecar_payloads.get("review_copilot_payload") or {}).get("risk_summary")
        or dict(sidecar_payloads.get("model_governance_summary") or {}).get("summary_line")
        or ""
    ).strip()
    human_summary = _human_governance_summary(human_governance_payloads)
    blocker_items = []
    warning_items = []
    formal_gap_items = []
    missing_coverage_summary = str(coverage_digest.get("missing_evidence_summary") or "").strip()
    top_gaps_summary = str(coverage_digest.get("top_gaps_summary") or "").strip()
    if missing_coverage_summary and missing_coverage_summary != "none":
        blocker_items.append(missing_coverage_summary)
    if top_gaps_summary and top_gaps_summary != "none":
        formal_gap_items.append(top_gaps_summary)
    if not human_summary or "missing" in human_summary.lower():
        warning_items.append(human_summary or "human governance bundle still reviewer-placeholder only")
    artifact_paths = {
        STEP2_CLOSEOUT_DIGEST_ARTIFACT_TYPE: _artifact_output_path(run_dir, STEP2_CLOSEOUT_DIGEST_FILENAME),
        EVIDENCE_COVERAGE_MATRIX_ARTIFACT_TYPE: _artifact_output_path(run_dir, EVIDENCE_COVERAGE_MATRIX_FILENAME),
        RESULT_TRACEABILITY_TREE_ARTIFACT_TYPE: _artifact_output_path(run_dir, RESULT_TRACEABILITY_TREE_FILENAME),
        EVIDENCE_LINEAGE_INDEX_ARTIFACT_TYPE: _artifact_output_path(run_dir, EVIDENCE_LINEAGE_INDEX_FILENAME),
        REVIEWER_ANCHOR_NAVIGATION_ARTIFACT_TYPE: _artifact_output_path(run_dir, REVIEWER_ANCHOR_NAVIGATION_FILENAME),
        AI_RUN_SUMMARY_ARTIFACT_TYPE: _artifact_output_path(run_dir, AI_RUN_SUMMARY_FILENAME),
    }
    digest = {
        "summary": (
            f"step2 closeout digest | scope {scope_context.get('scope_id') or '--'} | "
            f"decision {scope_context.get('decision_rule_id') or '--'} | "
            f"coverage {coverage_digest.get('summary') or '--'}"
        ),
        "scope_overview_summary": str(scope_context.get("scope_id") or "--"),
        "decision_rule_summary": str(scope_context.get("decision_rule_id") or "--"),
        "limitation_note": str(scope_context.get("limitation_note") or "--"),
        "non_claim_note": str(scope_context.get("non_claim_note") or "--"),
        "uncertainty_case_summary": str(uncertainty_method_context.get("uncertainty_case_id") or "--"),
        "method_confirmation_summary": str(
            uncertainty_method_context.get("method_confirmation_protocol_id") or "--"
        ),
        "verification_rollup_summary": str(
            uncertainty_method_context.get("verification_rollup_id") or "--"
        ),
        "coverage_summary": str(coverage_digest.get("summary") or "--"),
        "traceability_summary": str(traceability_digest.get("summary") or "--"),
        "lineage_summary": str(lineage_digest.get("summary") or "--"),
        "human_governance_summary": human_summary,
        "sidecar_summary": sidecar_summary or str(ai_run_summary.get("summary_line") or "--"),
        "comparison_summary": str(
            comparison_rollup_payload.get("rollup_summary_display")
            or comparison_rollup_payload.get("comparison_overview_summary")
            or "--"
        ),
        "readiness_status_summary": str(coverage_digest.get("readiness_status_summary") or "--"),
        "missing_evidence_summary": missing_coverage_summary or "none",
        "top_gaps_summary": top_gaps_summary or "none",
        "blocker_summary": "; ".join(blocker_items) or "none",
        "warning_summary": "; ".join(warning_items) or "none",
        "formal_gap_summary": "; ".join(formal_gap_items) or "none",
        "reviewer_anchor_summary": f"anchors {len(list(reviewer_anchor_navigation.get('anchors') or []))}",
    }
    return {
        "schema_version": STEP2_REVIEWER_READINESS_SCHEMA_VERSION,
        "artifact_type": STEP2_CLOSEOUT_DIGEST_ARTIFACT_TYPE,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "run_id": run_id,
        "run_dir": run_dir,
        "summary_line": str(digest.get("summary") or ""),
        "summary_lines": [
            str(digest.get("summary") or ""),
            f"scope_id: {digest.get('scope_overview_summary') or '--'}",
            f"decision_rule_id: {digest.get('decision_rule_summary') or '--'}",
            f"uncertainty_case: {digest.get('uncertainty_case_summary') or '--'}",
            f"traceability: {digest.get('traceability_summary') or '--'}",
        ],
        "digest": digest,
        "blocker_items": blocker_items,
        "warning_items": warning_items,
        "formal_gap_items": formal_gap_items,
        "artifact_paths": artifact_paths,
        "review_surface": {
            "summary_text": str(digest.get("summary") or ""),
            "summary_lines": [
                str(digest.get("summary") or ""),
                f"limitation_note: {digest.get('limitation_note') or '--'}",
                f"non_claim_note: {digest.get('non_claim_note') or '--'}",
                f"missing_evidence: {digest.get('missing_evidence_summary') or '--'}",
                f"formal_gap: {digest.get('formal_gap_summary') or '--'}",
            ],
            "anchor_id": "step2-closeout-digest",
            "anchor_label": "Step 2 收尾摘要",
        },
        "boundary_summary": _BOUNDARY_SUMMARY,
        "evidence_source": "simulated",
        **_BOUNDARY_FIELDS,
    }


def _build_scope_context(
    *,
    scope_definition_pack: dict[str, Any],
    decision_rule_profile: dict[str, Any],
    conformity_statement_profile: dict[str, Any],
) -> dict[str, Any]:
    scope_payload = dict(scope_definition_pack or {})
    decision_payload = dict(decision_rule_profile or {})
    conformity_payload = dict(conformity_statement_profile or {})
    return {
        "scope_id": str(
            scope_payload.get("scope_id")
            or dict(scope_payload.get("scope_export_pack") or {}).get("scope_id")
            or ""
        ).strip(),
        "scope_name": str(
            scope_payload.get("scope_name")
            or dict(scope_payload.get("scope_export_pack") or {}).get("scope_name")
            or ""
        ).strip(),
        "decision_rule_id": str(decision_payload.get("decision_rule_id") or "").strip(),
        "limitation_note": str(
            decision_payload.get("limitation_note")
            or conformity_payload.get("limitation_note")
            or ""
        ).strip(),
        "non_claim_note": str(
            decision_payload.get("non_claim_note")
            or conformity_payload.get("non_claim_note")
            or ""
        ).strip(),
        "scope_path": _artifact_path_from_payload(
            dict(scope_payload.get("artifact_paths") or {}).get("scope_definition_pack"),
            "",
            "scope_definition_pack.json",
        ),
        "decision_rule_path": _artifact_path_from_payload(
            dict(decision_payload.get("artifact_paths") or {}).get("decision_rule_profile"),
            "",
            "decision_rule_profile.json",
        ),
    }


def _build_uncertainty_method_context(
    *,
    uncertainty_report_pack: dict[str, Any],
    uncertainty_rollup: dict[str, Any],
    method_confirmation_protocol: dict[str, Any],
    verification_rollup: dict[str, Any],
) -> dict[str, Any]:
    uncertainty_payload = dict(uncertainty_report_pack or {})
    uncertainty_rollup_payload = dict(uncertainty_rollup or {})
    method_payload = dict(method_confirmation_protocol or {})
    verification_payload = dict(verification_rollup or {})
    return {
        "uncertainty_case_id": str(
            uncertainty_payload.get("uncertainty_case_id")
            or uncertainty_rollup_payload.get("uncertainty_case_id")
            or method_payload.get("uncertainty_case_id")
            or verification_payload.get("uncertainty_case_id")
            or ""
        ).strip(),
        "method_confirmation_protocol_id": str(
            method_payload.get("method_confirmation_protocol_id")
            or method_payload.get("protocol_id")
            or verification_payload.get("method_confirmation_protocol_id")
            or ""
        ).strip(),
        "verification_rollup_id": str(
            verification_payload.get("verification_rollup_id")
            or verification_payload.get("verification_digest_id")
            or ""
        ).strip(),
        "protocol_overview_summary": str(
            method_payload.get("protocol_overview")
            or dict(method_payload.get("digest") or {}).get("protocol_overview_summary")
            or ""
        ).strip(),
        "uncertainty_overview_summary": str(
            dict(uncertainty_payload.get("digest") or {}).get("uncertainty_overview_summary")
            or dict(uncertainty_rollup_payload.get("digest") or {}).get("uncertainty_overview_summary")
            or ""
        ).strip(),
        "calculation_chain_summary": str(
            uncertainty_payload.get("calculation_chain_summary")
            or uncertainty_rollup_payload.get("calculation_chain_summary")
            or dict(uncertainty_payload.get("digest") or {}).get("calculation_chain_summary")
            or ""
        ).strip(),
        "method_confirmation_protocol_path": _artifact_path_from_payload(
            dict(method_payload.get("artifact_paths") or {}).get("method_confirmation_protocol"),
            "",
            "method_confirmation_protocol.json",
        ),
        "uncertainty_report_pack_path": _artifact_path_from_payload(
            dict(uncertainty_payload.get("artifact_paths") or {}).get("uncertainty_report_pack"),
            "",
            "uncertainty_report_pack.json",
        ),
    }


def _normalize_coverage_row(
    row: dict[str, Any],
    *,
    index: int,
    scope_context: dict[str, Any],
    software_validation_traceability_matrix: dict[str, Any],
    release_manifest: dict[str, Any],
    comparison_evidence_pack: dict[str, Any],
    human_governance_payloads: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    payload = dict(row or {})
    linked_existing_artifacts = _dedupe(payload.get("linked_existing_artifacts") or [])
    if "release_manifest" not in linked_existing_artifacts and release_manifest:
        linked_existing_artifacts.append("release_manifest")
    if comparison_evidence_pack and "comparison_evidence_pack" not in linked_existing_artifacts:
        linked_existing_artifacts.append("comparison_evidence_pack")
    if any(human_governance_payloads.values()) and "human_governance_bundle" not in linked_existing_artifacts:
        linked_existing_artifacts.append("human_governance_bundle")
    current_coverage = _dedupe(payload.get("current_evidence_coverage") or [])
    missing_coverage = len(current_coverage) < len(list(payload.get("required_evidence_categories") or []))
    gap_note = str(payload.get("gap_note") or "").strip()
    blockers_or_gaps = missing_coverage or bool(gap_note)
    mapping_id = str(payload.get("mapping_id") or f"coverage-row-{index}").strip() or f"coverage-row-{index}"
    standard_family = str(payload.get("standard_family") or payload.get("standard_id_or_family") or "--").strip()
    readiness_status = str(payload.get("readiness_status") or "mapping_ready_evidence_pending").strip()
    row_payload = {
        "mapping_id": mapping_id,
        "standard_family": standard_family,
        "topic_or_control_object": str(payload.get("topic_or_control_object") or "--"),
        "applicability": str(
            payload.get("applicability")
            or "Step 2 reviewer/readiness mapping only"
        ),
        "linked_existing_artifacts": linked_existing_artifacts,
        "required_evidence_categories": _dedupe(payload.get("required_evidence_categories") or []),
        "current_evidence_coverage": current_coverage,
        "readiness_status": readiness_status,
        "gap_note": gap_note or "none",
        "non_claim": list(payload.get("non_claim") or ["readiness mapping only"]),
        "digest": str(payload.get("digest") or "--"),
        "missing_coverage": missing_coverage,
        "blockers_or_gaps": blockers_or_gaps,
        "missing_coverage_filter": "missing_coverage:yes" if missing_coverage else "missing_coverage:no",
        "gap_filter": "gaps:present" if blockers_or_gaps else "gaps:none",
        "anchor_id": f"coverage:{mapping_id}",
        "anchor_label": f"{standard_family} / {str(payload.get('topic_or_control_object') or mapping_id)}",
        "scope_id": str(scope_context.get("scope_id") or ""),
        "decision_rule_id": str(scope_context.get("decision_rule_id") or ""),
        "limitation_note": str(scope_context.get("limitation_note") or ""),
        "non_claim_note": str(scope_context.get("non_claim_note") or ""),
    }
    if software_validation_traceability_matrix:
        row_payload["traceability_id"] = str(
            software_validation_traceability_matrix.get("traceability_id") or ""
        )
    return row_payload


def _fallback_coverage_rows(
    *,
    scope_context: dict[str, Any],
    software_validation_traceability_matrix: dict[str, Any],
    release_manifest: dict[str, Any],
    comparison_evidence_pack: dict[str, Any],
    human_governance_payloads: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    rows = [
        {
            "mapping_id": "scope_decision",
            "standard_family": "Step 2 reviewer readiness",
            "topic_or_control_object": "scope / decision / non-claim boundary",
            "applicability": "Step 2 reviewer/readiness mapping only",
            "linked_existing_artifacts": ["scope_definition_pack", "decision_rule_profile"],
            "required_evidence_categories": ["scope_decision"],
            "current_evidence_coverage": _dedupe(
                [
                    scope_context.get("scope_id"),
                    scope_context.get("decision_rule_id"),
                    scope_context.get("limitation_note"),
                    scope_context.get("non_claim_note"),
                ]
            ),
            "readiness_status": "mapping_ready",
            "gap_note": "none",
            "non_claim": ["readiness mapping only"],
            "digest": f"{scope_context.get('scope_id') or '--'} | {scope_context.get('decision_rule_id') or '--'}",
        }
    ]
    if software_validation_traceability_matrix or release_manifest:
        rows.append(
            {
                "mapping_id": "software_validation",
                "standard_family": "Step 2 reviewer readiness",
                "topic_or_control_object": "software validation / release manifest linkage",
                "applicability": "Step 2 reviewer/readiness mapping only",
                "linked_existing_artifacts": ["software_validation_traceability_matrix", "release_manifest"],
                "required_evidence_categories": ["software_validation"],
                "current_evidence_coverage": _dedupe(
                    [
                        dict(software_validation_traceability_matrix.get("digest") or {}).get("summary"),
                        dict(release_manifest.get("digest") or {}).get("summary"),
                    ]
                ),
                "readiness_status": "mapping_ready_evidence_pending",
                "gap_note": "family/topic readiness only; no clause-level claim",
                "non_claim": ["readiness mapping only"],
                "digest": "software validation linkage",
            }
        )
    if comparison_evidence_pack:
        rows.append(
            {
                "mapping_id": "comparison",
                "standard_family": "Step 2 reviewer readiness",
                "topic_or_control_object": "comparison evidence linkage",
                "applicability": "Step 2 reviewer/readiness mapping only",
                "linked_existing_artifacts": ["comparison_evidence_pack"],
                "required_evidence_categories": ["comparison"],
                "current_evidence_coverage": _dedupe(
                    [
                        comparison_evidence_pack.get("comparison_overview_summary"),
                        dict(comparison_evidence_pack.get("digest") or {}).get("comparison_overview_summary"),
                    ]
                ),
                "readiness_status": "mapping_ready_evidence_pending",
                "gap_note": "comparison remains reviewer-only and non-claim",
                "non_claim": ["readiness mapping only"],
                "digest": "comparison linkage",
            }
        )
    if any(human_governance_payloads.values()):
        rows.append(
            {
                "mapping_id": "human_governance",
                "standard_family": "Step 2 reviewer readiness",
                "topic_or_control_object": "operator / SOP / reviewer placeholder",
                "applicability": "Step 2 reviewer/readiness mapping only",
                "linked_existing_artifacts": [
                    key for key, payload in human_governance_payloads.items() if payload
                ],
                "required_evidence_categories": ["human_governance"],
                "current_evidence_coverage": _dedupe(
                    dict(payload.get("digest") or {}).get("summary") or payload.get("summary_line")
                    for payload in human_governance_payloads.values()
                    if payload
                ),
                "readiness_status": "reviewer_placeholder_ready",
                "gap_note": "placeholder-only governance; no formal approval chain",
                "non_claim": ["reviewer only"],
                "digest": _human_governance_summary(human_governance_payloads),
            }
        )
    return [
        _normalize_coverage_row(
            row,
            index=index,
            scope_context=scope_context,
            software_validation_traceability_matrix=software_validation_traceability_matrix,
            release_manifest=release_manifest,
            comparison_evidence_pack=comparison_evidence_pack,
            human_governance_payloads=human_governance_payloads,
        )
        for index, row in enumerate(rows, start=1)
    ]


def _linked_node_keys_for_payload(
    payload: dict[str, Any],
    *,
    node_index: dict[str, dict[str, Any]],
    preferred_node_types: list[str],
) -> list[str]:
    rows: list[str] = []
    scope_id = str(payload.get("scope_id") or payload.get("linked_scope_id") or "").strip()
    decision_rule_id = str(
        payload.get("decision_rule_id")
        or payload.get("linked_decision_rule_id")
        or ""
    ).strip()
    uncertainty_case_id = str(
        payload.get("uncertainty_case_id")
        or dict(payload.get("digest") or {}).get("uncertainty_case_summary")
        or ""
    ).strip()
    if scope_id:
        candidate = f"scope:{scope_id}"
        if candidate in node_index:
            rows.append(candidate)
    if decision_rule_id:
        candidate = f"decision:{decision_rule_id}"
        if candidate in node_index:
            rows.append(candidate)
    if uncertainty_case_id:
        candidate = f"uncertainty:{uncertainty_case_id}"
        if candidate in node_index:
            rows.append(candidate)
    for preferred_type in preferred_node_types:
        for key, node in node_index.items():
            if str(node.get("node_type") or "") == preferred_type and key not in rows:
                rows.append(key)
    return rows


def _human_governance_summary(payloads: dict[str, dict[str, Any]]) -> str:
    rows = _dedupe(
        dict(payload.get("digest") or {}).get("summary") or payload.get("summary_line")
        for payload in payloads.values()
        if isinstance(payload, dict) and payload
    )
    return " | ".join(rows) if rows else "human governance placeholder-only"


def _artifact_output_path(run_dir: str, filename: str) -> str:
    if not run_dir:
        return filename
    return str(Path(run_dir) / filename)


def _artifact_path_from_payload(value: Any, run_dir: str, default_filename: str) -> str:
    text = str(value or "").strip()
    if text:
        candidate = Path(text)
        if candidate.is_absolute() or not run_dir:
            return str(candidate)
        return str(Path(run_dir) / candidate)
    return _artifact_output_path(run_dir, default_filename)


def _dedupe(values: Iterable[Any]) -> list[str]:
    rows: list[str] = []
    for value in list(values or []):
        text = str(value or "").strip()
        if text and text not in rows:
            rows.append(text)
    return rows
