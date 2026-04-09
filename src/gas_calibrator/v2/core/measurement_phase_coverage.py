from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable

from .models import SamplingResult
from .multi_source_stability import CANONICAL_BOUNDARY_STATEMENTS, SIGNAL_GROUP_CHANNELS, SIGNAL_GROUP_ORDER


MEASUREMENT_PHASE_COVERAGE_REPORT_FILENAME = "measurement_phase_coverage_report.json"
MEASUREMENT_PHASE_COVERAGE_REPORT_MARKDOWN_FILENAME = "measurement_phase_coverage_report.md"

_PHASE_DEFINITIONS = (
    {
        "phase_name": "ambient_diagnostic",
        "route_family": "ambient",
        "policy_version": "shadow_ambient_v1",
        "fallback_evidence_source": "model_only",
        "reviewer_note": "Ambient/diagnostic phase readiness coverage for simulation-only reviewer evidence.",
    },
    {
        "phase_name": "preseal",
        "route_family": "water",
        "policy_version": "shadow_water_v1",
        "fallback_evidence_source": "model_only",
        "reviewer_note": "Water-route preseal phase coverage for Step 2 tail / Stage 3 bridge review.",
    },
    {
        "phase_name": "preseal",
        "route_family": "gas",
        "policy_version": "shadow_gas_v1",
        "fallback_evidence_source": "model_only",
        "reviewer_note": "Gas-route preseal phase coverage for Step 2 tail / Stage 3 bridge review.",
    },
    {
        "phase_name": "pressure_stable",
        "route_family": "water",
        "policy_version": "shadow_water_v1",
        "fallback_evidence_source": "model_only",
        "reviewer_note": "Water-route pressure-stable phase coverage for simulation-only shadow review.",
    },
    {
        "phase_name": "pressure_stable",
        "route_family": "gas",
        "policy_version": "shadow_gas_v1",
        "fallback_evidence_source": "model_only",
        "reviewer_note": "Gas-route pressure-stable phase coverage for simulation-only shadow review.",
    },
    {
        "phase_name": "sample_ready",
        "route_family": "ambient",
        "policy_version": "shadow_ambient_v1",
        "fallback_evidence_source": "model_only",
        "reviewer_note": "Ambient sample-ready coverage uses simulation outputs only and is not a real acceptance path.",
    },
    {
        "phase_name": "sample_ready",
        "route_family": "water",
        "policy_version": "shadow_water_v1",
        "fallback_evidence_source": "model_only",
        "reviewer_note": "Water sample-ready coverage uses shadow decisions only and cannot replace metrology validation.",
    },
    {
        "phase_name": "sample_ready",
        "route_family": "gas",
        "policy_version": "shadow_gas_v1",
        "fallback_evidence_source": "model_only",
        "reviewer_note": "Gas sample-ready coverage uses shadow decisions only and cannot replace metrology validation.",
    },
    {
        "phase_name": "recovery_retry",
        "route_family": "system",
        "policy_version": "controlled_flex_v1",
        "fallback_evidence_source": "test_only",
        "reviewer_note": "Recovery/retry coverage is currently model/test oriented unless a simulated trace captures it explicitly.",
    },
)

_PHASE_ACTIONS: dict[str, tuple[str, ...]] = {
    "ambient_diagnostic": ("ambient", "diagnostic", "wait_temperature", "wait_humidity"),
    "preseal": ("set_h2o_path", "set_co2_valves", "wait_route_ready", "wait_dewpoint", "wait_route_soak", "seal_route"),
    "pressure_stable": ("set_pressure", "wait_post_pressure"),
    "sample_ready": ("sample_start", "sample_end"),
    "recovery_retry": ("retry", "recovery", "abort", "fault_capture", "safe_recovery"),
}

_PAYLOAD_COMPLETE_BUCKET = "actual_simulated_run_with_payload_complete"
_PAYLOAD_PARTIAL_BUCKET = "actual_simulated_run_with_payload_partial"
_PAYLOAD_BACKED_BUCKETS = {_PAYLOAD_COMPLETE_BUCKET, _PAYLOAD_PARTIAL_BUCKET}

_READINESS_ARTIFACT_ANCHORS: dict[str, dict[str, str]] = {
    "scope_definition_pack": {
        "artifact_type": "scope_definition_pack",
        "anchor_id": "scope-definition-pack",
        "anchor_label": "Scope definition pack",
    },
    "scope_readiness_summary": {
        "artifact_type": "scope_readiness_summary",
        "anchor_id": "scope-readiness-summary",
        "anchor_label": "Scope readiness summary",
    },
    "decision_rule_profile": {
        "artifact_type": "decision_rule_profile",
        "anchor_id": "decision-rule-profile",
        "anchor_label": "Decision rule profile",
    },
    "certificate_readiness_summary": {
        "artifact_type": "certificate_readiness_summary",
        "anchor_id": "certificate-readiness-summary",
        "anchor_label": "Certificate readiness summary",
    },
    "reference_asset_registry": {
        "artifact_type": "reference_asset_registry",
        "anchor_id": "reference-asset-registry",
        "anchor_label": "Reference asset registry",
    },
    "metrology_traceability_stub": {
        "artifact_type": "metrology_traceability_stub",
        "anchor_id": "metrology-traceability-stub",
        "anchor_label": "Metrology traceability stub",
    },
    "uncertainty_budget_stub": {
        "artifact_type": "uncertainty_budget_stub",
        "anchor_id": "uncertainty-budget-stub",
        "anchor_label": "Uncertainty budget stub",
    },
    "method_confirmation_protocol": {
        "artifact_type": "method_confirmation_protocol",
        "anchor_id": "method-confirmation-protocol",
        "anchor_label": "Method confirmation protocol",
    },
    "method_confirmation_matrix": {
        "artifact_type": "method_confirmation_matrix",
        "anchor_id": "method-confirmation-matrix",
        "anchor_label": "Method confirmation matrix",
    },
    "uncertainty_method_readiness_summary": {
        "artifact_type": "uncertainty_method_readiness_summary",
        "anchor_id": "uncertainty-method-readiness-summary",
        "anchor_label": "Uncertainty / method readiness summary",
    },
    "software_validation_traceability_matrix": {
        "artifact_type": "software_validation_traceability_matrix",
        "anchor_id": "software-validation-traceability-matrix",
        "anchor_label": "Software validation traceability matrix",
    },
    "release_validation_manifest": {
        "artifact_type": "release_validation_manifest",
        "anchor_id": "release-validation-manifest",
        "anchor_label": "Release validation manifest",
    },
    "audit_readiness_digest": {
        "artifact_type": "audit_readiness_digest",
        "anchor_id": "audit-readiness-digest",
        "anchor_label": "Audit readiness digest",
    },
}

_PHASE_READINESS_ARTIFACT_TYPES: dict[str, tuple[str, ...]] = {
    "ambient_diagnostic": ("scope_definition_pack", "scope_readiness_summary", "decision_rule_profile"),
    "preseal": (
        "scope_definition_pack",
        "scope_readiness_summary",
        "decision_rule_profile",
        "metrology_traceability_stub",
        "uncertainty_budget_stub",
        "method_confirmation_protocol",
        "method_confirmation_matrix",
        "uncertainty_method_readiness_summary",
    ),
    "pressure_stable": (
        "reference_asset_registry",
        "certificate_readiness_summary",
        "metrology_traceability_stub",
        "uncertainty_budget_stub",
        "method_confirmation_matrix",
        "uncertainty_method_readiness_summary",
    ),
    "sample_ready": (
        "scope_definition_pack",
        "scope_readiness_summary",
        "decision_rule_profile",
        "method_confirmation_protocol",
        "method_confirmation_matrix",
    ),
    "recovery_retry": (
        "software_validation_traceability_matrix",
        "release_validation_manifest",
        "audit_readiness_digest",
    ),
}

_PHASE_READINESS_IMPACT_AREAS: dict[str, str] = {
    "ambient_diagnostic": "scope / decision",
    "preseal": "scope / method / uncertainty / traceability",
    "pressure_stable": "uncertainty / traceability / certificate",
    "sample_ready": "scope / method",
    "recovery_retry": "software validation / audit",
}

_PHASE_READINESS_DIMENSIONS: dict[str, tuple[str, ...]] = {
    "ambient_diagnostic": ("scope", "decision rule"),
    "preseal": ("scope", "decision rule", "method confirmation", "uncertainty inputs", "traceability stub"),
    "pressure_stable": ("reference asset / certificate", "traceability", "uncertainty / method"),
    "sample_ready": ("scope", "method confirmation"),
    "recovery_retry": ("software validation", "audit"),
}

_PHASE_GAP_NAVIGATION_PROFILES: dict[tuple[str, str], dict[str, Any]] = {
    ("water", "preseal"): {
        "linked_method_confirmation_items": [
            "Water preseal window definition",
            "Water route conditioning repeatability",
            "Water preseal release criteria",
        ],
        "linked_uncertainty_inputs": [
            "Humidity reference window",
            "Preseal pressure term",
            "Preseal temperature term",
        ],
        "linked_traceability_stub_nodes": [
            "Humidity reference chain",
            "Dew-point reference link",
            "Preseal conditioning window stub",
        ],
        "gap_classification": "conditioning_window_partial_payload",
        "gap_severity": "high",
        "reviewer_next_step_digest": (
            "Confirm the water preseal window method first, then add the preseal pressure/temperature "
            "uncertainty inputs, then tie the humidity and dew-point references into the traceability stub "
            "while keeping preseal partial explicit as payload-partial."
        ),
    },
    ("gas", "preseal"): {
        "linked_method_confirmation_items": [
            "Gas preseal window definition",
            "Gas route conditioning repeatability",
            "Gas preseal release criteria",
        ],
        "linked_uncertainty_inputs": [
            "Reference gas window",
            "Preseal pressure term",
            "Preseal temperature term",
        ],
        "linked_traceability_stub_nodes": [
            "Standard gas chain",
            "Pressure reference link",
            "Preseal conditioning window stub",
        ],
        "gap_classification": "conditioning_window_partial_payload",
        "gap_severity": "high",
        "reviewer_next_step_digest": (
            "Confirm the gas preseal window method first, then add the preseal pressure/temperature "
            "uncertainty inputs, then tie the standard-gas and pressure references into the traceability stub "
            "while keeping preseal partial explicit as payload-partial."
        ),
    },
    ("water", "pressure_stable"): {
        "linked_method_confirmation_items": ["Water pressure stabilization hold confirmation"],
        "linked_uncertainty_inputs": ["Humidity reference", "Pressure reference", "Temperature reference"],
        "linked_traceability_stub_nodes": [
            "Humidity reference chain",
            "Dew-point reference link",
            "Pressure reference link",
        ],
        "gap_classification": "payload_complete_synthetic_reviewer_anchor",
        "gap_severity": "info",
        "reviewer_next_step_digest": (
            "Use the water pressure-stable payload as the synthetic reviewer anchor, then keep certificate and "
            "traceability closure in readiness-only artifacts until released reference evidence exists."
        ),
    },
    ("gas", "pressure_stable"): {
        "linked_method_confirmation_items": ["Gas pressure stabilization hold confirmation"],
        "linked_uncertainty_inputs": ["Reference gas value", "Pressure reference", "Temperature reference"],
        "linked_traceability_stub_nodes": [
            "Standard gas chain",
            "Pressure reference link",
            "Temperature reference link",
        ],
        "gap_classification": "payload_complete_synthetic_reviewer_anchor",
        "gap_severity": "info",
        "reviewer_next_step_digest": (
            "Use the gas pressure-stable payload as the synthetic reviewer anchor, then keep certificate and "
            "traceability closure in readiness-only artifacts until released reference evidence exists."
        ),
    },
}


def build_measurement_phase_coverage_report(
    *,
    run_id: str,
    samples: Iterable[SamplingResult],
    point_summaries: Iterable[dict[str, Any]] | None = None,
    route_trace_events: Iterable[dict[str, Any]] | None = None,
    multi_source_stability_evidence: dict[str, Any] | None = None,
    state_transition_evidence: dict[str, Any] | None = None,
    artifact_paths: dict[str, Any] | None = None,
    synthetic_trace_provenance: dict[str, Any] | None = None,
) -> dict[str, Any]:
    sample_rows = [sample for sample in list(samples or []) if isinstance(sample, SamplingResult)]
    summary_rows = [dict(item) for item in list(point_summaries or []) if isinstance(item, dict)]
    trace_rows = [dict(item) for item in list(route_trace_events or []) if isinstance(item, dict)]
    stability_raw = dict((multi_source_stability_evidence or {}).get("raw") or multi_source_stability_evidence or {})
    transition_raw = dict((state_transition_evidence or {}).get("raw") or state_transition_evidence or {})
    artifact_path_map = {
        "measurement_phase_coverage_report": str(
            dict(artifact_paths or {}).get("measurement_phase_coverage_report")
            or MEASUREMENT_PHASE_COVERAGE_REPORT_FILENAME
        ),
        "measurement_phase_coverage_report_markdown": str(
            dict(artifact_paths or {}).get("measurement_phase_coverage_report_markdown")
            or MEASUREMENT_PHASE_COVERAGE_REPORT_MARKDOWN_FILENAME
        ),
        "multi_source_stability_evidence": str(
            dict(artifact_paths or {}).get("multi_source_stability_evidence")
            or "multi_source_stability_evidence.json"
        ),
        "state_transition_evidence": str(
            dict(artifact_paths or {}).get("state_transition_evidence")
            or "state_transition_evidence.json"
        ),
        "simulation_evidence_sidecar_bundle": str(
            dict(artifact_paths or {}).get("simulation_evidence_sidecar_bundle")
            or "simulation_evidence_sidecar_bundle.json"
        ),
    }

    configured_routes = _configured_route_families(sample_rows, summary_rows, trace_rows)
    sample_groups = _sample_groups(sample_rows)
    trace_groups = _trace_groups(trace_rows)
    stability_groups = _stability_groups(stability_raw)
    transition_groups = _transition_groups(transition_raw)
    phase_rows: list[dict[str, Any]] = []
    for definition in _PHASE_DEFINITIONS:
        row = _build_phase_row(
            definition=definition,
            configured_routes=configured_routes,
            sample_groups=sample_groups,
            trace_groups=trace_groups,
            stability_groups=stability_groups,
            transition_groups=transition_groups,
            artifact_paths=artifact_path_map,
        )
        phase_rows.append(row)
    phase_rows = _enrich_phase_rows_for_reviewer_guidance(phase_rows)

    payload_complete_count = sum(1 for row in phase_rows if row.get("coverage_bucket") == _PAYLOAD_COMPLETE_BUCKET)
    payload_partial_count = sum(1 for row in phase_rows if row.get("coverage_bucket") == _PAYLOAD_PARTIAL_BUCKET)
    payload_backed_count = payload_complete_count + payload_partial_count
    trace_only_count = sum(1 for row in phase_rows if row.get("coverage_bucket") == "trace_only_not_evaluated")
    model_only_count = sum(1 for row in phase_rows if row.get("coverage_bucket") == "model_only")
    test_only_count = sum(1 for row in phase_rows if row.get("coverage_bucket") == "test_only")
    gap_count = sum(1 for row in phase_rows if row.get("coverage_bucket") == "gap")
    routes = _dedupe(row.get("route_family") for row in phase_rows)
    phases = _dedupe(row.get("phase_name") for row in phase_rows)
    signal_families = _dedupe(
        group_name
        for row in phase_rows
        for group_name, payload in dict(row.get("signal_group_coverage") or {}).items()
        if str(dict(payload).get("coverage_status") or "") not in {"", "gap"}
    )
    evidence_sources = _dedupe(row.get("coverage_bucket") or row.get("evidence_source") for row in phase_rows)
    decision_results = _dedupe(row.get("decision_result") for row in phase_rows)
    policy_versions = _dedupe(row.get("policy_version") for row in phase_rows)
    payload_phase_summary = " | ".join(
        f"{row['route_family']}/{row['phase_name']}"
        for row in phase_rows
        if row.get("coverage_bucket") in _PAYLOAD_BACKED_BUCKETS
    ) or "no payload-backed simulated phase evidence"
    payload_complete_phase_summary = " | ".join(
        f"{row['route_family']}/{row['phase_name']}"
        for row in phase_rows
        if row.get("coverage_bucket") == _PAYLOAD_COMPLETE_BUCKET
    ) or "no payload-complete simulated phase evidence"
    payload_partial_phase_summary = " | ".join(
        f"{row['route_family']}/{row['phase_name']}"
        for row in phase_rows
        if row.get("coverage_bucket") == _PAYLOAD_PARTIAL_BUCKET
    ) or "no payload-partial simulated phase evidence"
    actual_summary = " | ".join(
        f"{row['route_family']}/{row['phase_name']}"
        for row in phase_rows
        if row.get("coverage_bucket") in _PAYLOAD_BACKED_BUCKETS
    ) or "no sample-backed simulated phase evidence"
    trace_only_summary = " | ".join(
        f"{row['route_family']}/{row['phase_name']}"
        for row in phase_rows
        if row.get("coverage_bucket") == "trace_only_not_evaluated"
    ) or "no trace-only phase buckets"
    gap_summary = " | ".join(
        f"{row['route_family']}/{row['phase_name']}"
        for row in phase_rows
        if row.get("coverage_bucket") in {"gap", "model_only", "test_only"}
    ) or "no phase coverage gaps"
    coverage_summary = " | ".join(
        f"{row['route_family']}/{row['phase_name']}={row['coverage_bucket']}"
        for row in phase_rows
    )
    coverage_display_summary = " | ".join(
        f"{row['route_family']}/{row['phase_name']}={row['coverage_bucket_display']}"
        for row in phase_rows
    )
    payload_completeness_summary = " | ".join(
        f"{key} {value}"
        for key, value in _count_rows_by_key(phase_rows, "payload_completeness").items()
    ) or "--"
    provenance_summary = " | ".join(
        f"{key} {value}"
        for key, value in _count_rows_by_key(phase_rows, "evidence_provenance").items()
    ) or "--"
    readiness_impact_summary = " | ".join(
        f"{row['route_family']}/{row['phase_name']}: {row['readiness_impact_digest']}"
        for row in phase_rows
        if row.get("coverage_bucket") != _PAYLOAD_COMPLETE_BUCKET
    ) or "no open readiness impacts from measurement-core phases"
    blocker_summary = " | ".join(
        _dedupe(
            f"{row['route_family']}/{row['phase_name']}: {', '.join(list(row.get('blockers') or []))}"
            for row in phase_rows
            if row.get("coverage_bucket") != _PAYLOAD_COMPLETE_BUCKET
            and list(row.get("blockers") or [])
        )
    ) or "no reviewer blockers recorded"
    next_required_artifacts_summary = " | ".join(
        _dedupe(
            artifact_name
            for row in phase_rows
            if row.get("coverage_bucket") != _PAYLOAD_COMPLETE_BUCKET
            for artifact_name in list(row.get("next_required_artifacts") or [])
        )
    ) or "no next artifact escalation recorded"
    linked_readiness_summary = " | ".join(
        _dedupe(
            str(ref.get("anchor_label") or ref.get("artifact_type") or "").strip()
            for row in phase_rows
            for ref in list(row.get("linked_readiness_artifact_refs") or [])
            if isinstance(ref, dict)
        )
    ) or "no linked readiness anchors"
    preseal_partial_guidance_summary = " | ".join(
        _dedupe(
            str(row.get("reviewer_guidance_digest") or "").strip()
            for row in phase_rows
            if str(row.get("phase_name") or "").strip() == "preseal"
            and str(row.get("coverage_bucket") or "").strip() == _PAYLOAD_PARTIAL_BUCKET
        )
    ) or "no preseal payload-partial reviewer guidance recorded"
    linked_method_confirmation_summary = " | ".join(
        _dedupe(
            (
                f"{row['route_family']}/{row['phase_name']}: "
                f"{', '.join(list(row.get('linked_method_confirmation_items') or []))}"
            )
            for row in phase_rows
            if list(row.get("linked_method_confirmation_items") or [])
            and row.get("coverage_bucket") != _PAYLOAD_COMPLETE_BUCKET
        )
    ) or "no linked method confirmation items recorded"
    linked_uncertainty_input_summary = " | ".join(
        _dedupe(
            (
                f"{row['route_family']}/{row['phase_name']}: "
                f"{', '.join(list(row.get('linked_uncertainty_inputs') or []))}"
            )
            for row in phase_rows
            if list(row.get("linked_uncertainty_inputs") or [])
            and row.get("coverage_bucket") != _PAYLOAD_COMPLETE_BUCKET
        )
    ) or "no linked uncertainty inputs recorded"
    linked_traceability_stub_summary = " | ".join(
        _dedupe(
            (
                f"{row['route_family']}/{row['phase_name']}: "
                f"{', '.join(list(row.get('linked_traceability_stub_nodes') or []))}"
            )
            for row in phase_rows
            if list(row.get("linked_traceability_stub_nodes") or [])
            and row.get("coverage_bucket") != _PAYLOAD_COMPLETE_BUCKET
        )
    ) or "no linked traceability stub nodes recorded"
    gap_index_summary = " | ".join(
        _dedupe(
            (
                f"{row['route_family']}/{row['phase_name']}: "
                f"{str(row.get('gap_classification') or '--')} / {str(row.get('gap_severity') or '--')}"
            )
            for row in phase_rows
            if str(row.get("gap_classification") or "").strip()
            and row.get("coverage_bucket") != _PAYLOAD_COMPLETE_BUCKET
        )
    ) or "no gap index recorded"
    reviewer_next_step_summary = " | ".join(
        _dedupe(
            str(row.get("reviewer_next_step_digest") or "").strip()
            for row in phase_rows
            if str(row.get("reviewer_next_step_digest") or "").strip()
            and row.get("coverage_bucket") != _PAYLOAD_COMPLETE_BUCKET
        )
    ) or "no reviewer next-step guidance recorded"
    phase_contrast_summary = _phase_contrast_summary(phase_rows)
    digest = {
        "summary": (
            "Step 2 tail / Stage 3 bridge | measurement phase coverage | "
            f"payload-complete {payload_complete_count} | payload-partial {payload_partial_count} | "
            f"trace-only {trace_only_count} | model-only {model_only_count} | test-only {test_only_count} | gap {gap_count}"
        ),
        "actual_phase_summary": actual_summary,
        "payload_phase_summary": payload_phase_summary,
        "payload_complete_phase_summary": payload_complete_phase_summary,
        "payload_partial_phase_summary": payload_partial_phase_summary,
        "trace_only_phase_summary": trace_only_summary,
        "coverage_summary": coverage_summary,
        "coverage_display_summary": coverage_display_summary,
        "payload_completeness_summary": payload_completeness_summary,
        "provenance_summary": provenance_summary,
        "gap_summary": gap_summary,
        "readiness_impact_summary": readiness_impact_summary,
        "blocker_summary": blocker_summary,
        "next_required_artifacts_summary": next_required_artifacts_summary,
        "linked_readiness_summary": linked_readiness_summary,
        "preseal_partial_guidance_summary": preseal_partial_guidance_summary,
        "linked_method_confirmation_summary": linked_method_confirmation_summary,
        "linked_uncertainty_input_summary": linked_uncertainty_input_summary,
        "linked_traceability_stub_summary": linked_traceability_stub_summary,
        "gap_index_summary": gap_index_summary,
        "reviewer_next_step_summary": reviewer_next_step_summary,
        "phase_contrast_summary": phase_contrast_summary,
        "boundary_summary": " | ".join(CANONICAL_BOUNDARY_STATEMENTS),
    }
    review_surface = {
        "title_text": "Measurement Phase Coverage Report",
        "role_text": "diagnostic_analysis",
        "reviewer_note": (
            "Step 2 tail / Stage 3 bridge reviewer evidence for richer simulation coverage only. "
            "This is readiness mapping for measurement-core evidence and not a runtime control surface."
        ),
        "summary_text": digest["summary"],
        "summary_lines": [
            digest["summary"],
            f"payload-backed phases: {payload_phase_summary}",
            f"payload-complete phases: {payload_complete_phase_summary}",
            f"payload-partial phases: {payload_partial_phase_summary}",
            f"trace-only phases: {trace_only_summary}",
            f"coverage digest: {coverage_display_summary}",
            f"payload completeness: {payload_completeness_summary}",
            f"phase gaps: {gap_summary}",
            f"blockers: {blocker_summary}",
            f"next artifacts: {next_required_artifacts_summary}",
            f"preseal partial guidance: {preseal_partial_guidance_summary}",
            f"linked method confirmation items: {linked_method_confirmation_summary}",
            f"linked uncertainty inputs: {linked_uncertainty_input_summary}",
            f"linked traceability stub nodes: {linked_traceability_stub_summary}",
            f"reviewer next steps: {reviewer_next_step_summary}",
            f"phase contrast: {phase_contrast_summary}",
        ],
        "detail_lines": [
            f"route families: {', '.join(routes) or '--'}",
            f"phase buckets: {', '.join(phases) or '--'}",
            f"provenance summary: {provenance_summary}",
            f"linked readiness anchors: {linked_readiness_summary}",
            f"readiness impact: {readiness_impact_summary}",
            f"gap index: {gap_index_summary}",
            *[
                f"{str(row.get('phase_route_key') or '--')} guidance: {str(row.get('reviewer_guidance_digest') or '--')}"
                for row in phase_rows
                if str(row.get("reviewer_guidance_digest") or "").strip()
                and str(row.get("phase_name") or "").strip() in {"preseal", "pressure_stable"}
            ],
            *[
                f"{str(row.get('phase_route_key') or '--')} comparison: {str(row.get('comparison_digest') or '--')}"
                for row in phase_rows
                if str(row.get("comparison_digest") or "").strip()
            ],
            f"synthetic provenance: {dict(synthetic_trace_provenance or {}).get('summary', 'simulation trace only')}",
            *[f"boundary: {line}" for line in CANONICAL_BOUNDARY_STATEMENTS],
        ],
        "anchor_id": "measurement-phase-coverage-report",
        "anchor_label": "Measurement phase coverage report",
        "phase_filters": phases,
        "route_filters": routes,
        "signal_family_filters": signal_families,
        "decision_result_filters": decision_results,
        "policy_version_filters": policy_versions,
        "boundary_filters": list(CANONICAL_BOUNDARY_STATEMENTS),
        "evidence_source_filters": evidence_sources,
        "linked_anchor_refs": _dedupe(
            str(ref.get("anchor_id") or "")
            for row in phase_rows
            for ref in list(row.get("linked_readiness_artifact_refs") or [])
            if isinstance(ref, dict)
        ),
        "artifact_paths": dict(artifact_path_map),
    }
    linked_artifact_refs = _dedupe_artifact_refs(
        [
            {
                "artifact_type": artifact_name,
                "path": str(path_value or ""),
            }
            for artifact_name, path_value in artifact_path_map.items()
            if str(path_value or "").strip()
        ]
        + [
            dict(ref)
            for row in phase_rows
            for ref in list(row.get("linked_readiness_artifact_refs") or [])
            if isinstance(ref, dict)
        ]
    )
    raw = {
        "schema_version": "1.1",
        "artifact_type": "measurement_phase_coverage_report",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "run_id": str(run_id or ""),
        "artifact_role": "diagnostic_analysis",
        "evidence_source": "simulated",
        "not_real_acceptance_evidence": True,
        "boundary_statements": list(CANONICAL_BOUNDARY_STATEMENTS),
        "phase_rows": phase_rows,
        "phase_index": {str(row.get("phase_route_key") or ""): dict(row) for row in phase_rows},
        "synthetic_trace_provenance": dict(synthetic_trace_provenance or {}),
        "digest": digest,
        "review_surface": review_surface,
        "artifact_paths": artifact_path_map,
        "linked_artifact_refs": linked_artifact_refs,
        "linked_measurement_phases": [
            f"{str(row.get('route_family') or '').strip()}/{str(row.get('phase_name') or '').strip()}".strip("/")
            for row in phase_rows
            if str(row.get("route_family") or "").strip() and str(row.get("phase_name") or "").strip()
        ],
        "linked_measurement_gaps": [
            {
                "phase_route_key": str(row.get("phase_route_key") or "").strip(),
                "route_phase": f"{str(row.get('route_family') or '').strip()}/{str(row.get('phase_name') or '').strip()}".strip("/"),
                "gap_classification": str(row.get("gap_classification") or "").strip(),
                "gap_severity": str(row.get("gap_severity") or "").strip(),
                "missing_signal_layers": list(row.get("missing_signal_layers") or []),
                "gap_reason": str(row.get("missing_reason_digest") or "").strip(),
                "linked_method_confirmation_items": list(row.get("linked_method_confirmation_items") or []),
                "linked_uncertainty_inputs": list(row.get("linked_uncertainty_inputs") or []),
                "linked_traceability_nodes": list(row.get("linked_traceability_stub_nodes") or []),
                "reviewer_next_step_digest": str(row.get("reviewer_next_step_digest") or "").strip(),
            }
            for row in phase_rows
            if str(row.get("coverage_bucket") or "").strip() != _PAYLOAD_COMPLETE_BUCKET
        ],
        "linked_method_confirmation_items": _dedupe(
            item
            for row in phase_rows
            for item in list(row.get("linked_method_confirmation_items") or [])
        ),
        "linked_uncertainty_inputs": _dedupe(
            item
            for row in phase_rows
            for item in list(row.get("linked_uncertainty_inputs") or [])
        ),
        "linked_traceability_nodes": _dedupe(
            item
            for row in phase_rows
            for item in list(row.get("linked_traceability_stub_nodes") or [])
        ),
        "reviewer_next_step_digest": reviewer_next_step_summary,
        "next_required_artifacts": _dedupe(
            artifact_name
            for row in phase_rows
            for artifact_name in list(row.get("next_required_artifacts") or [])
        ),
        "overall_status": (
            "diagnostic_only"
            if (payload_backed_count + trace_only_count) == 0
            else "degraded"
            if (payload_partial_count + trace_only_count + model_only_count + test_only_count + gap_count)
            else "passed"
        ),
    }
    return {
        "available": True,
        "artifact_type": "measurement_phase_coverage_report",
        "filename": MEASUREMENT_PHASE_COVERAGE_REPORT_FILENAME,
        "markdown_filename": MEASUREMENT_PHASE_COVERAGE_REPORT_MARKDOWN_FILENAME,
        "raw": raw,
        "markdown": _render_markdown(raw=raw),
        "digest": digest,
    }


def _build_phase_row(
    *,
    definition: dict[str, Any],
    configured_routes: set[str],
    sample_groups: dict[tuple[str, str], list[SamplingResult]],
    trace_groups: dict[tuple[str, str], list[dict[str, Any]]],
    stability_groups: dict[tuple[str, str], dict[str, Any]],
    transition_groups: dict[tuple[str, str], dict[str, Any]],
    artifact_paths: dict[str, str],
) -> dict[str, Any]:
    phase_name = str(definition.get("phase_name") or "")
    route_family = str(definition.get("route_family") or "")
    key = (route_family, phase_name)
    sample_rows = list(sample_groups.get(key) or [])
    trace_rows = list(trace_groups.get(key) or [])
    stability_row = dict(stability_groups.get(key) or {})
    if not stability_row and route_family == "ambient" and phase_name == "ambient_diagnostic":
        stability_row = dict(stability_groups.get(("ambient", "sample_ready")) or {})
    transition_row = dict(transition_groups.get(key) or {})
    actual_run_evidence_present = bool(sample_rows or trace_rows or stability_row or transition_row)
    fallback_source = str(definition.get("fallback_evidence_source") or "model_only")
    route_in_scope = route_family in configured_routes or route_family == "system"
    signal_group_coverage, available_channels, missing_channels = _phase_signal_coverage(
        phase_name=phase_name,
        route_family=route_family,
        sample_rows=sample_rows,
    )
    available_signal_layers = [
        group_name
        for group_name, payload in signal_group_coverage.items()
        if str(dict(payload).get("coverage_status") or "") in {"complete", "partial"}
    ]
    missing_signal_layers = [
        group_name
        for group_name, payload in signal_group_coverage.items()
        if str(dict(payload).get("coverage_status") or "") == "gap"
    ]
    payload_completeness = _payload_completeness(
        sample_rows=sample_rows,
        signal_group_coverage=signal_group_coverage,
        actual_run_evidence_present=actual_run_evidence_present,
    )
    coverage_bucket = _coverage_bucket(
        actual_run_evidence_present=actual_run_evidence_present,
        sample_rows=sample_rows,
        payload_completeness=payload_completeness,
        fallback_source=fallback_source,
        route_in_scope=route_in_scope,
        route_family=route_family,
    )
    evidence_provenance = _evidence_provenance(
        sample_rows=sample_rows,
        trace_rows=trace_rows,
        stability_row=stability_row,
        transition_row=transition_row,
        coverage_bucket=coverage_bucket,
    )
    missing_layer_reasons = _missing_layer_reasons(
        phase_name=phase_name,
        signal_group_coverage=signal_group_coverage,
        sample_rows=sample_rows,
        actual_run_evidence_present=actual_run_evidence_present,
        coverage_bucket=coverage_bucket,
    )
    decision_result = str(
        stability_row.get("decision_result")
        or transition_row.get("decision_result")
        or ("trace_only_no_shadow_window" if actual_run_evidence_present and not sample_rows else f"{coverage_bucket}_coverage")
    )
    hold_time_summary = _hold_time_summary(stability_row, actual_run_evidence_present=actual_run_evidence_present)
    summary = (
        f"{route_family}/{phase_name} | {coverage_bucket} | payload {payload_completeness} | "
        f"decision {decision_result} | hold {hold_time_summary}"
    )
    anchor_id = f"measurement-phase-{route_family}-{phase_name.replace('_', '-')}"
    linked_readiness_artifact_refs = _phase_readiness_artifact_refs(phase_name)
    next_required_artifacts = [
        str(ref.get("artifact_type") or "").strip()
        for ref in linked_readiness_artifact_refs
        if isinstance(ref, dict) and str(ref.get("artifact_type") or "").strip()
    ]
    linked_readiness_summary = " | ".join(
        _dedupe(
            str(ref.get("anchor_label") or ref.get("artifact_type") or "").strip()
            for ref in linked_readiness_artifact_refs
            if isinstance(ref, dict)
        )
    ) or "--"
    linked_method_confirmation_items = _phase_linked_method_confirmation_items(
        route_family=route_family,
        phase_name=phase_name,
        coverage_bucket=coverage_bucket,
    )
    linked_uncertainty_inputs = _phase_linked_uncertainty_inputs(
        route_family=route_family,
        phase_name=phase_name,
        coverage_bucket=coverage_bucket,
    )
    linked_traceability_stub_nodes = _phase_linked_traceability_stub_nodes(
        route_family=route_family,
        phase_name=phase_name,
        coverage_bucket=coverage_bucket,
    )
    gap_classification = _phase_gap_classification(
        route_family=route_family,
        phase_name=phase_name,
        coverage_bucket=coverage_bucket,
        payload_completeness=payload_completeness,
    )
    gap_severity = _phase_gap_severity(
        route_family=route_family,
        phase_name=phase_name,
        coverage_bucket=coverage_bucket,
        payload_completeness=payload_completeness,
    )
    reviewer_next_step_digest = _phase_reviewer_next_step_digest(
        route_family=route_family,
        phase_name=phase_name,
        coverage_bucket=coverage_bucket,
        payload_completeness=payload_completeness,
        linked_method_confirmation_items=linked_method_confirmation_items,
        linked_uncertainty_inputs=linked_uncertainty_inputs,
        linked_traceability_stub_nodes=linked_traceability_stub_nodes,
    )
    blockers = _phase_blockers(
        phase_name=phase_name,
        payload_completeness=payload_completeness,
        actual_run_evidence_present=actual_run_evidence_present,
        missing_signal_layers=missing_signal_layers,
        coverage_bucket=coverage_bucket,
        linked_method_confirmation_items=linked_method_confirmation_items,
        linked_uncertainty_inputs=linked_uncertainty_inputs,
        linked_traceability_stub_nodes=linked_traceability_stub_nodes,
    )
    impacted_readiness_dimensions = _phase_readiness_dimensions(phase_name)
    missing_reason_digest = _missing_reason_digest(missing_layer_reasons)
    readiness_impact_digest = _phase_readiness_impact_digest(
        phase_name=phase_name,
        payload_completeness=payload_completeness,
        impacted_readiness_dimensions=impacted_readiness_dimensions,
        missing_signal_layers=missing_signal_layers,
        coverage_bucket=coverage_bucket,
    )
    phase_boundary_digest = _phase_boundary_digest(
        phase_name=phase_name,
        coverage_bucket=coverage_bucket,
        payload_completeness=payload_completeness,
    )
    reviewer_guidance_digest = _phase_reviewer_guidance_digest(
        route_family=route_family,
        phase_name=phase_name,
        coverage_bucket=coverage_bucket,
        available_signal_layers=available_signal_layers,
        missing_signal_layers=missing_signal_layers,
        missing_reason_digest=missing_reason_digest,
        readiness_impact_digest=readiness_impact_digest,
        linked_method_confirmation_items=linked_method_confirmation_items,
        linked_uncertainty_inputs=linked_uncertainty_inputs,
        linked_traceability_stub_nodes=linked_traceability_stub_nodes,
        blockers=blockers,
        next_required_artifacts=next_required_artifacts,
        reviewer_next_step_digest=reviewer_next_step_digest,
        phase_boundary_digest=phase_boundary_digest,
    )
    linked_artifact_refs = [
        {
            "artifact_type": artifact_name,
            "path": str(path_value or ""),
        }
        for artifact_name, path_value in dict(
            {
                "multi_source_stability_evidence": str(artifact_paths.get("multi_source_stability_evidence") or ""),
                "state_transition_evidence": str(artifact_paths.get("state_transition_evidence") or ""),
                "simulation_evidence_sidecar_bundle": str(artifact_paths.get("simulation_evidence_sidecar_bundle") or ""),
            }
        ).items()
        if str(path_value or "").strip()
    ] + [dict(item) for item in linked_readiness_artifact_refs]
    return {
        "phase_name": phase_name,
        "route_family": route_family,
        "phase_route_key": f"{route_family}:{phase_name}",
        "anchor_id": anchor_id,
        "anchor_label": f"{route_family}/{phase_name} phase coverage",
        "actual_run_evidence_present": actual_run_evidence_present,
        "evidence_source": coverage_bucket,
        "coverage_bucket": coverage_bucket,
        "coverage_bucket_display": _coverage_bucket_display(coverage_bucket),
        "payload_completeness": payload_completeness,
        "signal_group_coverage": signal_group_coverage,
        "available_signal_layers": available_signal_layers,
        "missing_signal_layers": missing_signal_layers,
        "missing_layer_reasons": missing_layer_reasons,
        "missing_reason_digest": missing_reason_digest,
        "available_channels": available_channels,
        "missing_channels": missing_channels,
        "policy_version": str(
            stability_row.get("policy_version")
            or definition.get("policy_version")
            or transition_row.get("policy_version")
            or "--"
        ),
        "evidence_provenance": evidence_provenance,
        "boundary_digest": " | ".join(CANONICAL_BOUNDARY_STATEMENTS),
        "non_claim_digest": (
            "simulation / synthetic reviewer evidence only | "
            "not real acceptance | not live gate | not compliance claim | not accreditation claim"
        ),
        "decision_result": decision_result,
        "decision_summary": str(
            stability_row.get("decision_result")
            or transition_row.get("decision_result")
            or (
                "actual simulated payload-complete coverage"
                if coverage_bucket == _PAYLOAD_COMPLETE_BUCKET
                else "actual simulated payload-partial coverage"
                if coverage_bucket == _PAYLOAD_PARTIAL_BUCKET
                else "actual simulated payload coverage"
                if sample_rows
                else "actual trace without payload coverage"
                if actual_run_evidence_present
                else f"{coverage_bucket} phase coverage"
            )
        ),
        "hold_time_summary": hold_time_summary,
        "impacted_readiness_dimensions": impacted_readiness_dimensions,
        "readiness_impact_digest": readiness_impact_digest,
        "phase_boundary_digest": phase_boundary_digest,
        "gap_classification": gap_classification,
        "gap_severity": gap_severity,
        "linked_method_confirmation_items": linked_method_confirmation_items,
        "linked_uncertainty_inputs": linked_uncertainty_inputs,
        "linked_traceability_stub_nodes": linked_traceability_stub_nodes,
        "blockers": blockers,
        "next_required_artifacts": next_required_artifacts,
        "linked_readiness_artifact_refs": linked_readiness_artifact_refs,
        "linked_readiness_summary": linked_readiness_summary,
        "linked_artifacts": {
            "multi_source_stability_evidence": str(artifact_paths.get("multi_source_stability_evidence") or ""),
            "state_transition_evidence": str(artifact_paths.get("state_transition_evidence") or ""),
            "simulation_evidence_sidecar_bundle": str(artifact_paths.get("simulation_evidence_sidecar_bundle") or ""),
        },
        "linked_artifact_refs": linked_artifact_refs,
        "reviewer_next_step_digest": reviewer_next_step_digest,
        "reviewer_guidance_digest": reviewer_guidance_digest,
        "reviewer_note": str(definition.get("reviewer_note") or ""),
        "digest": summary,
    }


def _configured_route_families(
    samples: list[SamplingResult],
    point_summaries: list[dict[str, Any]],
    route_trace_events: list[dict[str, Any]],
) -> set[str]:
    rows = {
        _route_family_from_sample(sample)
        for sample in samples
    }
    rows.update(
        _route_family(
            str(dict(item.get("point") or {}).get("route") or ""),
            pressure_mode=str(dict(item.get("point") or {}).get("pressure_mode") or ""),
        )
        for item in point_summaries
        if isinstance(item, dict)
    )
    rows.update(_route_family_from_trace(event) for event in route_trace_events)
    return {str(item).strip() for item in rows if str(item).strip()}


def _sample_groups(samples: list[SamplingResult]) -> dict[tuple[str, str], list[SamplingResult]]:
    rows: dict[tuple[str, str], list[SamplingResult]] = {}
    for sample in samples:
        key = (_route_family_from_sample(sample), _phase_name_from_sample(sample))
        rows.setdefault(key, []).append(sample)
    return rows


def _trace_groups(route_trace_events: list[dict[str, Any]]) -> dict[tuple[str, str], list[dict[str, Any]]]:
    rows: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for event in route_trace_events:
        for phase_name in _phase_names_from_trace(event):
            key = (_route_family_from_trace(event), phase_name)
            rows.setdefault(key, []).append(dict(event))
    return rows


def _stability_groups(stability_raw: dict[str, Any]) -> dict[tuple[str, str], dict[str, Any]]:
    rows: dict[tuple[str, str], dict[str, Any]] = {}
    for decision in list(stability_raw.get("stability_decisions") or []):
        if not isinstance(decision, dict):
            continue
        route_family = str(decision.get("route_family") or "").strip()
        phase_policy = str(decision.get("phase_policy") or "").strip()
        if route_family and phase_policy:
            rows[(route_family, phase_policy)] = dict(decision)
    return rows


def _transition_groups(transition_raw: dict[str, Any]) -> dict[tuple[str, str], dict[str, Any]]:
    rows: dict[tuple[str, str], dict[str, Any]] = {}
    for decision in list(transition_raw.get("phase_decision_logs") or []):
        if not isinstance(decision, dict):
            continue
        route_family = str(decision.get("route_family") or "").strip()
        phase_policy = str(decision.get("phase_policy") or "").strip()
        if route_family and phase_policy:
            rows[(route_family, phase_policy)] = dict(decision)
    return rows


def _phase_signal_coverage(
    *,
    phase_name: str,
    route_family: str,
    sample_rows: list[SamplingResult],
) -> tuple[dict[str, dict[str, Any]], list[str], list[str]]:
    if not sample_rows:
        coverage = {
            group_name: {
                "coverage_status": "gap",
                "available_channels": [],
                "missing_channels": _required_channels_for_phase(group_name, route_family, phase_name),
            }
            for group_name in SIGNAL_GROUP_ORDER
        }
        return coverage, [], _dedupe(
            channel
            for group_name in SIGNAL_GROUP_ORDER
            for channel in coverage[group_name]["missing_channels"]
        )

    coverage: dict[str, dict[str, Any]] = {}
    available_all: list[str] = []
    missing_all: list[str] = []
    for group_name in SIGNAL_GROUP_ORDER:
        expected = _required_channels_for_phase(group_name, route_family, phase_name)
        available = [
            channel
            for channel in SIGNAL_GROUP_CHANNELS[group_name]
            if any(_has_value(getattr(sample, channel, None)) for sample in sample_rows)
        ]
        if not expected:
            status = "gap"
            missing = []
        else:
            missing = [channel for channel in expected if channel not in set(available)]
            if len(available) >= len(expected):
                status = "complete"
            elif available:
                status = "partial"
            else:
                status = "gap"
        coverage[group_name] = {
            "coverage_status": status,
            "available_channels": available,
            "missing_channels": missing,
        }
        available_all.extend(available)
        missing_all.extend(missing)
    return coverage, _dedupe(available_all), _dedupe(missing_all)


def _required_channels_for_phase(group_name: str, route_family: str, phase_name: str) -> list[str]:
    if route_family == "water":
        requirements = {
            "reference": ["temperature_c", "dew_point_c", "pressure_hpa"],
            "analyzer_raw": ["h2o_ratio_raw", "h2o_signal", "ref_signal"],
            "output": ["h2o_mmol", "h2o_ratio_f"],
            "data_quality": ["frame_has_data", "frame_usable", "stability_time_s"],
        }
        return list(requirements.get(group_name, []))
    if route_family == "ambient":
        requirements = {
            "reference": ["temperature_c", "pressure_hpa"],
            "analyzer_raw": ["ref_signal"],
            "output": ["co2_ppm"],
            "data_quality": ["frame_has_data", "frame_usable"],
        }
        return list(requirements.get(group_name, []))
    if route_family == "system":
        requirements = {
            "reference": ["temperature_c", "pressure_hpa"],
            "analyzer_raw": ["ref_signal", "frame_status"],
            "output": ["co2_ppm"],
            "data_quality": ["frame_has_data", "frame_usable", "point_phase"],
        }
        if phase_name == "recovery_retry":
            return list(requirements.get(group_name, []))
        return []
    requirements = {
        "reference": ["temperature_c", "pressure_hpa"],
        "analyzer_raw": ["co2_ratio_raw", "co2_signal", "ref_signal"],
        "output": ["co2_ppm", "co2_ratio_f"],
        "data_quality": ["frame_has_data", "frame_usable", "stability_time_s"],
    }
    return list(requirements.get(group_name, []))


def _hold_time_summary(stability_row: dict[str, Any], *, actual_run_evidence_present: bool) -> str:
    if stability_row:
        if stability_row.get("hold_time_met") is True:
            return "hold_time_met"
        if stability_row.get("hold_time_met") is False:
            return "hold_time_gap"
    return "trace_only_not_evaluated" if actual_run_evidence_present else "not_applicable"


def _payload_completeness(
    *,
    sample_rows: list[SamplingResult],
    signal_group_coverage: dict[str, dict[str, Any]],
    actual_run_evidence_present: bool,
) -> str:
    if not sample_rows:
        return "trace_only" if actual_run_evidence_present else "not_available"
    statuses = [
        str(dict(signal_group_coverage.get(group_name) or {}).get("coverage_status") or "gap")
        for group_name in SIGNAL_GROUP_ORDER
    ]
    if statuses and all(status == "complete" for status in statuses):
        return "complete"
    if any(status in {"complete", "partial"} for status in statuses):
        return "partial"
    return "minimal"


def _coverage_bucket(
    *,
    actual_run_evidence_present: bool,
    sample_rows: list[SamplingResult],
    payload_completeness: str,
    fallback_source: str,
    route_in_scope: bool,
    route_family: str,
) -> str:
    if sample_rows:
        return _PAYLOAD_COMPLETE_BUCKET if payload_completeness == "complete" else _PAYLOAD_PARTIAL_BUCKET
    if actual_run_evidence_present:
        return "trace_only_not_evaluated"
    if route_family == "system":
        return fallback_source
    if route_in_scope:
        return fallback_source
    return "gap"


def _evidence_provenance(
    *,
    sample_rows: list[SamplingResult],
    trace_rows: list[dict[str, Any]],
    stability_row: dict[str, Any],
    transition_row: dict[str, Any],
    coverage_bucket: str,
) -> str:
    if sample_rows:
        sample_tags = " ".join(
            str(
                getattr(sample, "frame_status", "")
                or getattr(sample, "point_tag", "")
                or getattr(sample, "analyzer_id", "")
                or ""
            )
            for sample in sample_rows
        ).lower()
        return "synthetic_sample_payload" if "synthetic" in sample_tags else "actual_simulated_payload"
    if trace_rows or stability_row or transition_row:
        trace_text = " ".join(
            str(item.get(key) or "")
            for item in trace_rows
            for key in ("message", "result", "action", "point_tag")
        ).lower()
        return "synthetic_trace_only" if "synthetic" in trace_text else "simulated_trace_only"
    if coverage_bucket == "model_only":
        return "model_only"
    if coverage_bucket == "test_only":
        return "test_only"
    return "gap"


def _missing_layer_reasons(
    *,
    phase_name: str,
    signal_group_coverage: dict[str, dict[str, Any]],
    sample_rows: list[SamplingResult],
    actual_run_evidence_present: bool,
    coverage_bucket: str,
) -> dict[str, str]:
    rows: dict[str, str] = {}
    for group_name, payload in signal_group_coverage.items():
        if str(dict(payload).get("coverage_status") or "") != "gap":
            continue
        if sample_rows:
            rows[group_name] = _sample_backed_missing_layer_reason(
                phase_name=phase_name,
                layer_name=group_name,
            )
        elif actual_run_evidence_present:
            rows[group_name] = "phase currently has trace bucket only; no simulated sample payload captured for this layer"
        elif coverage_bucket in {"model_only", "test_only"}:
            rows[group_name] = f"{coverage_bucket} coverage only; this layer has not been promoted into simulated payload evidence"
        else:
            rows[group_name] = "no simulated evidence captured for this layer"
    return rows


def _phase_name_from_sample(sample: SamplingResult) -> str:
    route_family = _route_family_from_sample(sample)
    phase_text = str(getattr(sample, "point_phase", "") or "").strip().lower()
    if "recovery" in phase_text or "retry" in phase_text or "abort" in phase_text:
        return "recovery_retry"
    if route_family == "ambient":
        return "sample_ready" if "sample" in phase_text else "ambient_diagnostic"
    if "preseal" in phase_text or "seal" in phase_text:
        return "preseal"
    if "pressure" in phase_text:
        return "pressure_stable"
    return "sample_ready"


def _phase_names_from_trace(event: dict[str, Any]) -> list[str]:
    text = " ".join(
        str(event.get(key) or "")
        for key in ("action", "message", "route", "point_tag", "result")
    ).strip().lower()
    route_family = _route_family_from_trace(event)
    rows: list[str] = []
    if route_family == "ambient":
        rows.append("ambient_diagnostic")
    if any(token in text for token in _PHASE_ACTIONS["recovery_retry"]):
        rows.append("recovery_retry")
    if any(token in text for token in _PHASE_ACTIONS["pressure_stable"]):
        rows.append("pressure_stable")
    if any(token in text for token in _PHASE_ACTIONS["sample_ready"]):
        rows.append("sample_ready")
    if any(token in text for token in _PHASE_ACTIONS["preseal"]):
        rows.append("preseal")
    return _dedupe(rows)


def _route_family_from_trace(event: dict[str, Any]) -> str:
    route = str(event.get("route") or "").strip().lower()
    if route == "h2o":
        return "water"
    if "ambient" in route:
        return "ambient"
    if route:
        return "gas"
    text = " ".join(str(event.get(key) or "") for key in ("action", "message", "point_tag")).lower()
    if "ambient" in text or "diagnostic" in text:
        return "ambient"
    return "system" if any(token in text for token in _PHASE_ACTIONS["recovery_retry"]) else "gas"


def _route_family_from_sample(sample: SamplingResult) -> str:
    point = getattr(sample, "point", None)
    route = str(getattr(point, "route", "") or "").strip().lower()
    pressure_mode = str(getattr(point, "effective_pressure_mode", "") or "").strip().lower()
    return _route_family(route, pressure_mode=pressure_mode)


def _route_family(route: str, *, pressure_mode: str = "") -> str:
    route_text = str(route or "").strip().lower()
    pressure_text = str(pressure_mode or "").strip().lower()
    if route_text == "h2o":
        return "water"
    if pressure_text == "ambient_open" or "ambient" in route_text:
        return "ambient"
    return "gas" if route_text else "system"


def _has_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True


def _render_markdown(*, raw: dict[str, Any]) -> str:
    review_surface = dict(raw.get("review_surface") or {})
    phase_rows = [dict(item) for item in list(raw.get("phase_rows") or []) if isinstance(item, dict)]
    lines = [
        "# Measurement Phase Coverage Report",
        "",
        f"- title: {review_surface.get('title_text', '--')}",
        f"- role: {review_surface.get('role_text', '--')}",
        f"- reviewer_note: {review_surface.get('reviewer_note', '--')}",
        "",
        "## Boundary",
        "",
        *[f"- {line}" for line in CANONICAL_BOUNDARY_STATEMENTS],
        "",
        "## Phase Coverage",
        "",
    ]
    for row in phase_rows:
        lines.append(
            f"- {row.get('route_family', '--')}/{row.get('phase_name', '--')}: "
            f"{row.get('coverage_bucket', '--')} | payload {row.get('payload_completeness', '--')} | "
            f"provenance {row.get('evidence_provenance', '--')} | decision {row.get('decision_result', '--')} | "
            f"hold {row.get('hold_time_summary', '--')}"
        )
        lines.append(
            f"  layers available {', '.join(list(row.get('available_signal_layers') or [])[:6]) or '--'} | "
            f"missing {', '.join(list(row.get('missing_signal_layers') or [])[:6]) or '--'}"
        )
        lines.append(
            f"  channels available {', '.join(list(row.get('available_channels') or [])[:6]) or '--'} | "
            f"missing {', '.join(list(row.get('missing_channels') or [])[:6]) or '--'}"
        )
        lines.append(
            f"  readiness impact {row.get('readiness_impact_digest', '--')} | "
            f"next artifacts {', '.join(list(row.get('next_required_artifacts') or [])[:6]) or '--'}"
        )
        lines.append(
            f"  blockers {', '.join(list(row.get('blockers') or [])[:6]) or '--'} | "
            f"non-claim {row.get('non_claim_digest', '--')}"
        )
        lines.append(
            f"  method items {', '.join(list(row.get('linked_method_confirmation_items') or [])[:6]) or '--'} | "
            f"uncertainty inputs {', '.join(list(row.get('linked_uncertainty_inputs') or [])[:6]) or '--'}"
        )
        lines.append(
            f"  traceability nodes {', '.join(list(row.get('linked_traceability_stub_nodes') or [])[:6]) or '--'} | "
            f"gap {row.get('gap_classification', '--')} / {row.get('gap_severity', '--')}"
        )
        lines.append(f"  reviewer next step {row.get('reviewer_next_step_digest', '--')}")
    lines.extend(
        [
            "",
            "## Artifact Paths",
            "",
            f"- json: {dict(raw.get('artifact_paths') or {}).get('measurement_phase_coverage_report', '--')}",
            f"- markdown: {dict(raw.get('artifact_paths') or {}).get('measurement_phase_coverage_report_markdown', '--')}",
            "",
        ]
    )
    return "\n".join(lines).strip() + "\n"


def _dedupe(values: Iterable[Any]) -> list[str]:
    rows: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if text and text not in rows:
            rows.append(text)
    return rows


def _count_rows_by_key(rows: list[dict[str, Any]], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        text = str(dict(row or {}).get(key) or "").strip()
        if not text:
            continue
        counts[text] = int(counts.get(text, 0) or 0) + 1
    return counts


def _dedupe_artifact_refs(rows: list[dict[str, Any]]) -> list[dict[str, str]]:
    deduped: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    for item in rows:
        payload = dict(item or {})
        key = (
            str(payload.get("artifact_type") or "").strip(),
            str(payload.get("path") or "").strip(),
            str(payload.get("anchor_id") or "").strip(),
        )
        if not any(key):
            continue
        if key in seen:
            continue
        seen.add(key)
        deduped.append(payload)
    return deduped


def _sample_backed_missing_layer_reason(*, phase_name: str, layer_name: str) -> str:
    if str(phase_name or "").strip() == "preseal" and str(layer_name or "").strip() == "output":
        return (
            "preseal remains a conditioning window, so released measurement output stays intentionally open in "
            "synthetic reviewer evidence"
        )
    if str(phase_name or "").strip() == "preseal" and str(layer_name or "").strip() == "analyzer_raw":
        return (
            "preseal does not claim a released analyzer frame; promoting this layer would overstate setup evidence"
        )
    if str(phase_name or "").strip() == "preseal":
        return (
            "preseal synthetic payload is intentionally scoped to setup / conditioning evidence, so this layer stays "
            "open until later released measurement phases"
        )
    return "synthetic/actual simulated payload for this layer is still missing required channels"


def _phase_readiness_dimensions(phase_name: str) -> list[str]:
    return list(_PHASE_READINESS_DIMENSIONS.get(str(phase_name or "").strip(), ("readiness",)))


def _missing_reason_digest(missing_layer_reasons: dict[str, str]) -> str:
    return " | ".join(
        f"{layer_name}: {reason}"
        for layer_name, reason in dict(missing_layer_reasons or {}).items()
        if str(layer_name).strip() and str(reason).strip()
    ) or "--"


def _phase_boundary_digest(*, phase_name: str, coverage_bucket: str, payload_completeness: str) -> str:
    phase_name = str(phase_name or "").strip()
    if phase_name == "preseal":
        return (
            "preseal partial is an honesty boundary for setup / conditioning evidence and does not imply released "
            "measurement output"
        )
    if phase_name == "pressure_stable" and coverage_bucket == _PAYLOAD_COMPLETE_BUCKET:
        return "pressure-stable complete remains synthetic reviewer evidence only and does not imply real acceptance"
    if payload_completeness == "trace_only":
        return "trace-only phase evidence remains reviewer-only until payload-backed layers are promoted"
    return "measurement-core reviewer evidence stays within Step 2 simulation-only boundaries"


def _phase_navigation_profile(route_family: str, phase_name: str) -> dict[str, Any]:
    return dict(_PHASE_GAP_NAVIGATION_PROFILES.get((str(route_family or "").strip(), str(phase_name or "").strip())) or {})


def _phase_linked_method_confirmation_items(*, route_family: str, phase_name: str, coverage_bucket: str) -> list[str]:
    profile = _phase_navigation_profile(route_family, phase_name)
    return list(profile.get("linked_method_confirmation_items") or [])


def _phase_linked_uncertainty_inputs(*, route_family: str, phase_name: str, coverage_bucket: str) -> list[str]:
    profile = _phase_navigation_profile(route_family, phase_name)
    return list(profile.get("linked_uncertainty_inputs") or [])


def _phase_linked_traceability_stub_nodes(*, route_family: str, phase_name: str, coverage_bucket: str) -> list[str]:
    profile = _phase_navigation_profile(route_family, phase_name)
    return list(profile.get("linked_traceability_stub_nodes") or [])


def _phase_gap_classification(
    *,
    route_family: str,
    phase_name: str,
    coverage_bucket: str,
    payload_completeness: str,
) -> str:
    profile = _phase_navigation_profile(route_family, phase_name)
    if profile and str(profile.get("gap_classification") or "").strip():
        return str(profile.get("gap_classification") or "").strip()
    if coverage_bucket == _PAYLOAD_COMPLETE_BUCKET:
        return "payload_complete_synthetic_reviewer_anchor"
    if payload_completeness == "trace_only":
        return "trace_only_reviewer_gap"
    if coverage_bucket in {"model_only", "test_only", "gap"}:
        return f"{coverage_bucket}_reviewer_gap"
    return "payload_partial_reviewer_gap"


def _phase_gap_severity(
    *,
    route_family: str,
    phase_name: str,
    coverage_bucket: str,
    payload_completeness: str,
) -> str:
    profile = _phase_navigation_profile(route_family, phase_name)
    if profile and str(profile.get("gap_severity") or "").strip():
        return str(profile.get("gap_severity") or "").strip()
    if coverage_bucket == _PAYLOAD_COMPLETE_BUCKET:
        return "info"
    if payload_completeness == "trace_only":
        return "medium"
    if coverage_bucket in {"model_only", "test_only", "gap"}:
        return "medium"
    return "high"


def _phase_reviewer_next_step_digest(
    *,
    route_family: str,
    phase_name: str,
    coverage_bucket: str,
    payload_completeness: str,
    linked_method_confirmation_items: list[str],
    linked_uncertainty_inputs: list[str],
    linked_traceability_stub_nodes: list[str],
) -> str:
    profile = _phase_navigation_profile(route_family, phase_name)
    if profile and str(profile.get("reviewer_next_step_digest") or "").strip():
        return str(profile.get("reviewer_next_step_digest") or "").strip()
    if coverage_bucket == _PAYLOAD_COMPLETE_BUCKET:
        return "Keep this phase linked to readiness artifacts as synthetic reviewer evidence only."
    if payload_completeness == "trace_only":
        return "Promote this trace-only phase into payload-backed reviewer evidence before closing downstream readiness gaps."
    parts = []
    if linked_method_confirmation_items:
        parts.append(f"confirm method items: {', '.join(linked_method_confirmation_items)}")
    if linked_uncertainty_inputs:
        parts.append(f"add uncertainty inputs: {', '.join(linked_uncertainty_inputs)}")
    if linked_traceability_stub_nodes:
        parts.append(f"anchor traceability nodes: {', '.join(linked_traceability_stub_nodes)}")
    return " | ".join(parts) or "Keep the current reviewer boundary explicit and document the next missing artifact."


def _phase_reviewer_guidance_digest(
    *,
    route_family: str,
    phase_name: str,
    coverage_bucket: str,
    available_signal_layers: list[str],
    missing_signal_layers: list[str],
    missing_reason_digest: str,
    readiness_impact_digest: str,
    linked_method_confirmation_items: list[str],
    linked_uncertainty_inputs: list[str],
    linked_traceability_stub_nodes: list[str],
    blockers: list[str],
    next_required_artifacts: list[str],
    reviewer_next_step_digest: str,
    phase_boundary_digest: str,
) -> str:
    route_phase = f"{str(route_family or '').strip()}/{str(phase_name or '').strip()}".strip("/")
    available_text = ", ".join(list(available_signal_layers or [])) or "--"
    missing_text = ", ".join(list(missing_signal_layers or [])) or "--"
    method_text = ", ".join(list(linked_method_confirmation_items or [])) or "--"
    uncertainty_text = ", ".join(list(linked_uncertainty_inputs or [])) or "--"
    traceability_text = ", ".join(list(linked_traceability_stub_nodes or [])) or "--"
    blocker_text = " | ".join(list(blockers or [])) or "--"
    next_text = " | ".join(list(next_required_artifacts or [])) or "--"
    bucket_text = _coverage_bucket_display(coverage_bucket)
    return (
        f"{route_phase}={bucket_text} | available {available_text} | missing {missing_text} | "
        f"reason {missing_reason_digest} | impact {readiness_impact_digest} | method {method_text} | "
        f"uncertainty {uncertainty_text} | traceability {traceability_text} | blockers {blocker_text} | "
        f"next {next_text} | reviewer next step {reviewer_next_step_digest} | boundary {phase_boundary_digest}"
    )


def _enrich_phase_rows_for_reviewer_guidance(phase_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = [dict(item) for item in list(phase_rows or []) if isinstance(item, dict)]
    by_key = {
        (str(row.get("route_family") or "").strip(), str(row.get("phase_name") or "").strip()): row
        for row in rows
    }
    for row in rows:
        route_family = str(row.get("route_family") or "").strip()
        phase_name = str(row.get("phase_name") or "").strip()
        if phase_name != "preseal":
            continue
        comparison_row = by_key.get((route_family, "pressure_stable"))
        comparison_digest = _phase_comparison_digest(row=row, comparison_row=comparison_row)
        if comparison_digest:
            row["comparison_digest"] = comparison_digest
    for row in rows:
        if str(row.get("phase_name") or "").strip() != "pressure_stable":
            continue
        comparison_row = by_key.get((str(row.get("route_family") or "").strip(), "preseal"))
        comparison_digest = _phase_comparison_digest(row=comparison_row, comparison_row=row)
        if comparison_digest:
            row["comparison_digest"] = comparison_digest
    return rows


def _phase_comparison_digest(*, row: dict[str, Any] | None, comparison_row: dict[str, Any] | None) -> str:
    phase_row = dict(row or {})
    stable_row = dict(comparison_row or {})
    if not phase_row or not stable_row:
        return ""
    if str(phase_row.get("phase_name") or "").strip() != "preseal":
        return ""
    if str(phase_row.get("coverage_bucket") or "").strip() != _PAYLOAD_PARTIAL_BUCKET:
        return ""
    if str(stable_row.get("phase_name") or "").strip() != "pressure_stable":
        return ""
    if str(stable_row.get("coverage_bucket") or "").strip() != _PAYLOAD_COMPLETE_BUCKET:
        return ""
    route_family = str(phase_row.get("route_family") or "").strip() or str(stable_row.get("route_family") or "").strip()
    preseal_missing = ", ".join(list(phase_row.get("missing_signal_layers") or [])) or "--"
    stable_available = ", ".join(list(stable_row.get("available_signal_layers") or [])) or "--"
    preseal_method = ", ".join(list(phase_row.get("linked_method_confirmation_items") or [])) or "--"
    preseal_uncertainty = ", ".join(list(phase_row.get("linked_uncertainty_inputs") or [])) or "--"
    preseal_traceability = ", ".join(list(phase_row.get("linked_traceability_stub_nodes") or [])) or "--"
    stable_method = ", ".join(list(stable_row.get("linked_method_confirmation_items") or [])) or "--"
    stable_uncertainty = ", ".join(list(stable_row.get("linked_uncertainty_inputs") or [])) or "--"
    stable_traceability = ", ".join(list(stable_row.get("linked_traceability_stub_nodes") or [])) or "--"
    return (
        f"{route_family} route: preseal stays payload-partial because missing {preseal_missing}; "
        f"same-route pressure_stable reaches payload-complete because {stable_available} are all available; "
        f"preseal keeps method {preseal_method}, uncertainty {preseal_uncertainty}, and traceability "
        f"{preseal_traceability} open, while pressure_stable already exposes method {stable_method}, "
        f"uncertainty {stable_uncertainty}, and traceability {stable_traceability} as synthetic reviewer linkage"
    )


def _phase_contrast_summary(phase_rows: list[dict[str, Any]]) -> str:
    explicit = " | ".join(
        _dedupe(str(row.get("comparison_digest") or "").strip() for row in list(phase_rows or []) if isinstance(row, dict))
    )
    if explicit:
        return explicit
    preseal_row = next(
        (
            dict(row)
            for row in list(phase_rows or [])
            if isinstance(row, dict)
            and str(row.get("phase_name") or "").strip() == "preseal"
            and str(row.get("coverage_bucket") or "").strip() == _PAYLOAD_PARTIAL_BUCKET
        ),
        {},
    )
    pressure_row = next(
        (
            dict(row)
            for row in list(phase_rows or [])
            if isinstance(row, dict)
            and str(row.get("phase_name") or "").strip() == "pressure_stable"
            and str(row.get("coverage_bucket") or "").strip() == _PAYLOAD_COMPLETE_BUCKET
        ),
        {},
    )
    if preseal_row and pressure_row:
        preseal_missing = ", ".join(list(preseal_row.get("missing_signal_layers") or [])) or "--"
        pressure_available = ", ".join(list(pressure_row.get("available_signal_layers") or [])) or "--"
        return (
            "preseal stays payload-partial because setup / conditioning evidence still keeps "
            f"{preseal_missing} explicit; pressure_stable can reach payload-complete once {pressure_available} are all "
            "available and can therefore anchor richer method / uncertainty / traceability reviewer linkage"
        )
    return "no complete-vs-partial phase contrast recorded"


def _coverage_bucket_display(bucket: str) -> str:
    mapping = {
        _PAYLOAD_COMPLETE_BUCKET: "payload_complete",
        _PAYLOAD_PARTIAL_BUCKET: "payload_partial",
        "trace_only_not_evaluated": "trace_only",
        "model_only": "model_only",
        "test_only": "test_only",
        "gap": "gap",
    }
    return str(mapping.get(str(bucket or "").strip(), bucket or "")).strip() or "gap"


def _phase_readiness_artifact_refs(phase_name: str) -> list[dict[str, str]]:
    refs: list[dict[str, str]] = []
    for artifact_type in _PHASE_READINESS_ARTIFACT_TYPES.get(str(phase_name or "").strip(), ()):
        payload = dict(_READINESS_ARTIFACT_ANCHORS.get(artifact_type) or {})
        if payload:
            refs.append(payload)
    return refs


def _phase_readiness_impact_digest(
    *,
    phase_name: str,
    payload_completeness: str,
    impacted_readiness_dimensions: list[str],
    missing_signal_layers: list[str],
    coverage_bucket: str,
) -> str:
    impact_area = _PHASE_READINESS_IMPACT_AREAS.get(str(phase_name or "").strip(), "readiness")
    dimension_text = ", ".join(list(impacted_readiness_dimensions or [])) or impact_area
    if coverage_bucket == _PAYLOAD_COMPLETE_BUCKET:
        return f"{dimension_text} linkage is available from synthetic payload-backed reviewer evidence"
    if payload_completeness == "partial":
        return (
            f"{dimension_text} remains open because payload is partial and "
            f"missing layers stay explicit: {', '.join(missing_signal_layers) or '--'}"
        )
    if payload_completeness == "trace_only":
        return f"{dimension_text} remains open because this phase is still trace-only and not payload-evaluated"
    if coverage_bucket in {"model_only", "test_only", "gap"}:
        return f"{dimension_text} remains open because this phase has only {coverage_bucket} reviewer coverage"
    return f"{dimension_text} remains open because payload evidence is not complete"


def _phase_blockers(
    *,
    phase_name: str,
    payload_completeness: str,
    actual_run_evidence_present: bool,
    missing_signal_layers: list[str],
    coverage_bucket: str,
    linked_method_confirmation_items: list[str],
    linked_uncertainty_inputs: list[str],
    linked_traceability_stub_nodes: list[str],
) -> list[str]:
    blockers = []
    if payload_completeness == "partial":
        blockers.append(
            "payload stays partial so reviewer evidence cannot be overstated as phase-complete measurement evidence"
        )
    elif payload_completeness == "trace_only":
        blockers.append("phase is still trace-only; simulated payload layers have not been promoted yet")
    elif coverage_bucket in {"model_only", "test_only", "gap"}:
        blockers.append(f"phase remains {coverage_bucket}; richer simulated payload evidence is still missing")
    elif actual_run_evidence_present and coverage_bucket != _PAYLOAD_COMPLETE_BUCKET:
        blockers.append("actual simulated evidence exists but payload completeness remains open")
    if missing_signal_layers:
        blockers.append(f"missing signal layers: {', '.join(missing_signal_layers)}")
    if linked_method_confirmation_items and coverage_bucket != _PAYLOAD_COMPLETE_BUCKET:
        blockers.append(f"linked method confirmation items remain open: {', '.join(linked_method_confirmation_items)}")
    if linked_uncertainty_inputs and coverage_bucket != _PAYLOAD_COMPLETE_BUCKET:
        blockers.append(f"linked uncertainty inputs remain open: {', '.join(linked_uncertainty_inputs)}")
    if linked_traceability_stub_nodes and coverage_bucket != _PAYLOAD_COMPLETE_BUCKET:
        blockers.append(f"linked traceability nodes remain stub-only: {', '.join(linked_traceability_stub_nodes)}")
    if str(phase_name or "").strip() == "preseal":
        blockers.append("preseal partial is an honesty boundary, not a measurement-core bug")
        blockers.append("preseal remains setup/conditioning evidence and does not imply released measurement output")
    return _dedupe(blockers)
