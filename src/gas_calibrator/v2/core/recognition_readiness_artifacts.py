from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable

from .models import SamplingResult

SCOPE_DEFINITION_PACK_FILENAME = "scope_definition_pack.json"
SCOPE_DEFINITION_PACK_MARKDOWN_FILENAME = "scope_definition_pack.md"
DECISION_RULE_PROFILE_FILENAME = "decision_rule_profile.json"
DECISION_RULE_PROFILE_MARKDOWN_FILENAME = "decision_rule_profile.md"
SCOPE_READINESS_SUMMARY_FILENAME = "scope_readiness_summary.json"
SCOPE_READINESS_SUMMARY_MARKDOWN_FILENAME = "scope_readiness_summary.md"
REFERENCE_ASSET_REGISTRY_FILENAME = "reference_asset_registry.json"
REFERENCE_ASSET_REGISTRY_MARKDOWN_FILENAME = "reference_asset_registry.md"
CERTIFICATE_READINESS_SUMMARY_FILENAME = "certificate_readiness_summary.json"
CERTIFICATE_READINESS_SUMMARY_MARKDOWN_FILENAME = "certificate_readiness_summary.md"
METROLOGY_TRACEABILITY_STUB_FILENAME = "metrology_traceability_stub.json"
METROLOGY_TRACEABILITY_STUB_MARKDOWN_FILENAME = "metrology_traceability_stub.md"
UNCERTAINTY_BUDGET_STUB_FILENAME = "uncertainty_budget_stub.json"
UNCERTAINTY_BUDGET_STUB_MARKDOWN_FILENAME = "uncertainty_budget_stub.md"
METHOD_CONFIRMATION_PROTOCOL_FILENAME = "method_confirmation_protocol.json"
METHOD_CONFIRMATION_PROTOCOL_MARKDOWN_FILENAME = "method_confirmation_protocol.md"
METHOD_CONFIRMATION_MATRIX_FILENAME = "method_confirmation_matrix.json"
METHOD_CONFIRMATION_MATRIX_MARKDOWN_FILENAME = "method_confirmation_matrix.md"
UNCERTAINTY_METHOD_READINESS_SUMMARY_FILENAME = "uncertainty_method_readiness_summary.json"
UNCERTAINTY_METHOD_READINESS_SUMMARY_MARKDOWN_FILENAME = "uncertainty_method_readiness_summary.md"
SOFTWARE_VALIDATION_TRACEABILITY_MATRIX_FILENAME = "software_validation_traceability_matrix.json"
SOFTWARE_VALIDATION_TRACEABILITY_MATRIX_MARKDOWN_FILENAME = "software_validation_traceability_matrix.md"
RELEASE_VALIDATION_MANIFEST_FILENAME = "release_validation_manifest.json"
RELEASE_VALIDATION_MANIFEST_MARKDOWN_FILENAME = "release_validation_manifest.md"
AUDIT_READINESS_DIGEST_FILENAME = "audit_readiness_digest.json"
AUDIT_READINESS_DIGEST_MARKDOWN_FILENAME = "audit_readiness_digest.md"

RECOGNITION_READINESS_SUMMARY_FILENAMES = (
    SCOPE_READINESS_SUMMARY_FILENAME,
    CERTIFICATE_READINESS_SUMMARY_FILENAME,
    UNCERTAINTY_METHOD_READINESS_SUMMARY_FILENAME,
    AUDIT_READINESS_DIGEST_FILENAME,
)

RECOGNITION_READINESS_BOUNDARY_STATEMENTS = [
    "Step 2 reviewer readiness only",
    "simulation / offline / headless only",
    "file-artifact-first reviewer evidence",
    "not real acceptance",
    "not compliance claim",
    "not accreditation claim",
    "cannot replace real metrology validation",
]

_RECOGNITION_ARTIFACT_ANCHORS: dict[str, dict[str, str]] = {
    "scope_definition_pack": {"anchor_id": "scope-definition-pack", "anchor_label": "Scope definition pack"},
    "decision_rule_profile": {"anchor_id": "decision-rule-profile", "anchor_label": "Decision rule profile"},
    "scope_readiness_summary": {"anchor_id": "scope-readiness-summary", "anchor_label": "Scope readiness summary"},
    "reference_asset_registry": {"anchor_id": "reference-asset-registry", "anchor_label": "Reference asset registry"},
    "certificate_readiness_summary": {
        "anchor_id": "certificate-readiness-summary",
        "anchor_label": "Certificate readiness summary",
    },
    "metrology_traceability_stub": {
        "anchor_id": "metrology-traceability-stub",
        "anchor_label": "Metrology traceability stub",
    },
    "uncertainty_budget_stub": {"anchor_id": "uncertainty-budget-stub", "anchor_label": "Uncertainty budget stub"},
    "method_confirmation_protocol": {
        "anchor_id": "method-confirmation-protocol",
        "anchor_label": "Method confirmation protocol",
    },
    "method_confirmation_matrix": {
        "anchor_id": "method-confirmation-matrix",
        "anchor_label": "Method confirmation matrix",
    },
    "uncertainty_method_readiness_summary": {
        "anchor_id": "uncertainty-method-readiness-summary",
        "anchor_label": "Uncertainty / method readiness summary",
    },
    "software_validation_traceability_matrix": {
        "anchor_id": "software-validation-traceability-matrix",
        "anchor_label": "Software validation traceability matrix",
    },
    "release_validation_manifest": {
        "anchor_id": "release-validation-manifest",
        "anchor_label": "Release validation manifest",
    },
    "audit_readiness_digest": {"anchor_id": "audit-readiness-digest", "anchor_label": "Audit readiness digest"},
    "measurement_phase_coverage_report": {
        "anchor_id": "measurement-phase-coverage-report",
        "anchor_label": "Measurement phase coverage report",
    },
    "multi_source_stability_evidence": {
        "anchor_id": "multi-source-stability-evidence",
        "anchor_label": "Multi-source stability evidence",
    },
    "state_transition_evidence": {
        "anchor_id": "state-transition-evidence",
        "anchor_label": "State transition evidence",
    },
    "simulation_evidence_sidecar_bundle": {
        "anchor_id": "simulation-evidence-sidecar-bundle",
        "anchor_label": "Simulation evidence sidecar bundle",
    },
}

_RECOGNITION_NEXT_ARTIFACT_DEFAULTS: dict[str, list[str]] = {
    "scope_definition_pack": ["decision_rule_profile", "scope_readiness_summary"],
    "decision_rule_profile": ["scope_readiness_summary", "method_confirmation_matrix"],
    "scope_readiness_summary": ["reference_asset_registry", "method_confirmation_matrix"],
    "reference_asset_registry": ["certificate_readiness_summary", "metrology_traceability_stub"],
    "certificate_readiness_summary": ["metrology_traceability_stub", "reference_asset_registry"],
    "metrology_traceability_stub": ["certificate_readiness_summary", "uncertainty_method_readiness_summary"],
    "uncertainty_budget_stub": ["method_confirmation_protocol", "uncertainty_method_readiness_summary"],
    "method_confirmation_protocol": ["method_confirmation_matrix", "uncertainty_method_readiness_summary"],
    "method_confirmation_matrix": ["uncertainty_method_readiness_summary", "software_validation_traceability_matrix"],
    "uncertainty_method_readiness_summary": ["certificate_readiness_summary", "audit_readiness_digest"],
    "software_validation_traceability_matrix": ["release_validation_manifest", "audit_readiness_digest"],
    "release_validation_manifest": ["audit_readiness_digest"],
    "audit_readiness_digest": ["release_validation_manifest"],
}

_RECOGNITION_BLOCKER_DEFAULTS: dict[str, list[str]] = {
    "scope_definition_pack": [
        "scope package remains reviewer-facing only",
        "formal scope approval chain is not closed",
    ],
    "decision_rule_profile": [
        "decision rule profile does not drive live gate",
        "release / accreditation semantics remain explicitly out of scope",
    ],
    "reference_asset_registry": [
        "reference registry is still a stub and not a released traceability chain",
        "certificate-backed asset closure is missing",
    ],
    "certificate_readiness_summary": [
        "certificate files and intermediate checks remain missing",
        "traceability chain stays reviewer-facing only",
    ],
    "metrology_traceability_stub": [
        "certificate-backed release chain is not closed",
        "traceability rows remain stub-only",
    ],
    "uncertainty_budget_stub": [
        "uncertainty sources are placeholders only",
        "simulation does not close released uncertainty budgets",
    ],
    "method_confirmation_protocol": [
        "protocol remains placeholder-only",
        "simulation does not close method confirmation evidence",
    ],
    "method_confirmation_matrix": [
        "matrix rows remain reviewer-only and not released method confirmation evidence",
    ],
    "uncertainty_method_readiness_summary": [
        "uncertainty / method readiness remains open until missing evidence is closed outside Step 2",
    ],
    "software_validation_traceability_matrix": [
        "software traceability matrix remains reviewer-facing only",
        "no live release qualification claim is produced here",
    ],
    "release_validation_manifest": [
        "artifact hash closure is still stub-only",
        "manifest is not a released validation record",
    ],
    "audit_readiness_digest": [
        "audit digest remains a reviewer traceability skeleton only",
        "no formal audit conclusion is produced here",
    ],
}

_RECOGNITION_MISSING_EVIDENCE_DEFAULTS: dict[str, list[str]] = {
    "scope_definition_pack": [
        "formal scope approval chain is not closed",
        "real acceptance evidence remains out of scope",
    ],
    "decision_rule_profile": [
        "no live decision gate is attached",
        "released decision-rule evidence remains out of scope",
    ],
    "reference_asset_registry": [
        "certificate files and intermediate checks are still missing",
    ],
    "certificate_readiness_summary": [
        "no released certificate files attached",
        "no intermediate check execution evidence attached",
    ],
    "metrology_traceability_stub": [
        "traceability chain is not backed by released certificates",
    ],
    "uncertainty_budget_stub": [
        "input uncertainties and combined budgets remain placeholders only",
    ],
    "method_confirmation_protocol": [
        "real method confirmation datasets remain out of scope",
    ],
    "method_confirmation_matrix": [
        "trace-only and partial measurement phases still require follow-up evidence",
    ],
    "uncertainty_method_readiness_summary": [
        "released uncertainty and method confirmation evidence is still missing",
    ],
    "software_validation_traceability_matrix": [
        "formal software qualification artifacts are not attached here",
    ],
    "release_validation_manifest": [
        "signed artifact-hash closure remains deferred",
    ],
    "audit_readiness_digest": [
        "formal audit closure remains out of scope",
    ],
}


def build_recognition_readiness_artifacts(
    *,
    run_id: str,
    samples: Iterable[SamplingResult],
    point_summaries: Iterable[dict[str, Any]] | None = None,
    versions: dict[str, Any] | None = None,
    acceptance_plan: dict[str, Any] | None = None,
    analytics_summary: dict[str, Any] | None = None,
    lineage_summary: dict[str, Any] | None = None,
    evidence_registry: dict[str, Any] | None = None,
    multi_source_stability_evidence: dict[str, Any] | None = None,
    state_transition_evidence: dict[str, Any] | None = None,
    simulation_evidence_sidecar_bundle: dict[str, Any] | None = None,
    measurement_phase_coverage_report: dict[str, Any] | None = None,
    artifact_paths: dict[str, Any] | None = None,
) -> dict[str, dict[str, Any]]:
    sample_rows = [sample for sample in list(samples or []) if isinstance(sample, SamplingResult)]
    point_rows = [dict(item) for item in list(point_summaries or []) if isinstance(item, dict)]
    version_payload = dict(versions or {})
    acceptance_payload = dict(acceptance_plan or {})
    analytics_payload = dict(analytics_summary or {})
    lineage_payload = dict(lineage_summary or {})
    evidence_registry_payload = dict(evidence_registry or {})
    stability_payload = dict((multi_source_stability_evidence or {}).get("raw") or multi_source_stability_evidence or {})
    transition_payload = dict((state_transition_evidence or {}).get("raw") or state_transition_evidence or {})
    sidecar_payload = dict(simulation_evidence_sidecar_bundle or {})
    phase_coverage_payload = dict((measurement_phase_coverage_report or {}).get("raw") or measurement_phase_coverage_report or {})
    path_map = _artifact_path_map(dict(artifact_paths or {}))

    route_families = _route_families(sample_rows, point_rows, phase_coverage_payload)
    payload_complete_phases = _phase_pairs(
        phase_coverage_payload,
        include={"actual_simulated_run_with_payload_complete"},
    )
    payload_partial_phases = _phase_pairs(
        phase_coverage_payload,
        include={"actual_simulated_run_with_payload_partial"},
    )
    payload_backed_phases = _dedupe([*payload_complete_phases, *payload_partial_phases])
    trace_only_phases = _phase_pairs(phase_coverage_payload, include={"trace_only_not_evaluated"})
    gap_phases = _phase_pairs(phase_coverage_payload, include={"model_only", "test_only", "gap"})
    phase_digest = dict(phase_coverage_payload.get("digest") or {})
    sample_digest = {
        "temperature_range": _range_text(_collect_numeric_values(sample_rows, point_rows, "temperature_c")),
        "pressure_range": _range_text(
            _collect_numeric_values(sample_rows, point_rows, "pressure_hpa", "pressure_gauge_hpa")
        ),
        "analyzers": sorted(
            {
                str(getattr(sample, "analyzer_id", "") or "").strip()
                for sample in sample_rows
                if str(getattr(sample, "analyzer_id", "") or "").strip()
            }
        ),
    }

    scope_definition_pack = _build_scope_definition_pack(
        run_id=run_id,
        route_families=route_families,
        payload_backed_phases=payload_backed_phases,
        trace_only_phases=trace_only_phases,
        gap_phases=gap_phases,
        sample_digest=sample_digest,
        version_payload=version_payload,
        acceptance_payload=acceptance_payload,
        path_map=path_map,
    )
    decision_rule_profile = _build_decision_rule_profile(
        run_id=run_id,
        version_payload=version_payload,
        acceptance_payload=acceptance_payload,
        analytics_payload=analytics_payload,
        phase_digest=phase_digest,
        path_map=path_map,
    )
    scope_readiness_summary = _build_scope_readiness_summary(
        run_id=run_id,
        scope_definition_pack=scope_definition_pack,
        decision_rule_profile=decision_rule_profile,
        payload_backed_phases=payload_backed_phases,
        trace_only_phases=trace_only_phases,
        gap_phases=gap_phases,
        phase_digest=phase_digest,
        path_map=path_map,
    )
    reference_asset_registry = _build_reference_asset_registry(
        run_id=run_id,
        sample_digest=sample_digest,
        payload_backed_phases=payload_backed_phases,
        path_map=path_map,
    )
    certificate_readiness_summary = _build_certificate_readiness_summary(
        run_id=run_id,
        reference_asset_registry=reference_asset_registry,
        path_map=path_map,
    )
    metrology_traceability_stub = _build_metrology_traceability_stub(
        run_id=run_id,
        reference_asset_registry=reference_asset_registry,
        certificate_readiness_summary=certificate_readiness_summary,
        path_map=path_map,
    )
    uncertainty_budget_stub = _build_uncertainty_budget_stub(
        run_id=run_id,
        route_families=route_families,
        payload_backed_phases=payload_backed_phases,
        trace_only_phases=trace_only_phases,
        path_map=path_map,
    )
    method_confirmation_protocol = _build_method_confirmation_protocol(
        run_id=run_id,
        route_families=route_families,
        path_map=path_map,
    )
    method_confirmation_matrix = _build_method_confirmation_matrix(
        run_id=run_id,
        payload_backed_phases=payload_backed_phases,
        trace_only_phases=trace_only_phases,
        gap_phases=gap_phases,
        path_map=path_map,
    )
    uncertainty_method_readiness_summary = _build_uncertainty_method_readiness_summary(
        run_id=run_id,
        uncertainty_budget_stub=uncertainty_budget_stub,
        method_confirmation_protocol=method_confirmation_protocol,
        method_confirmation_matrix=method_confirmation_matrix,
        path_map=path_map,
    )
    software_validation_traceability_matrix = _build_software_validation_traceability_matrix(
        run_id=run_id,
        version_payload=version_payload,
        lineage_payload=lineage_payload,
        evidence_registry_payload=evidence_registry_payload,
        stability_payload=stability_payload,
        transition_payload=transition_payload,
        phase_coverage_payload=phase_coverage_payload,
        sidecar_payload=sidecar_payload,
        path_map=path_map,
    )
    release_validation_manifest = _build_release_validation_manifest(
        run_id=run_id,
        version_payload=version_payload,
        lineage_payload=lineage_payload,
        software_validation_traceability_matrix=software_validation_traceability_matrix,
        path_map=path_map,
    )
    audit_readiness_digest = _build_audit_readiness_digest(
        run_id=run_id,
        software_validation_traceability_matrix=software_validation_traceability_matrix,
        release_validation_manifest=release_validation_manifest,
        path_map=path_map,
    )

    artifacts = {
        "scope_definition_pack": scope_definition_pack,
        "decision_rule_profile": decision_rule_profile,
        "scope_readiness_summary": scope_readiness_summary,
        "reference_asset_registry": reference_asset_registry,
        "certificate_readiness_summary": certificate_readiness_summary,
        "metrology_traceability_stub": metrology_traceability_stub,
        "uncertainty_budget_stub": uncertainty_budget_stub,
        "method_confirmation_protocol": method_confirmation_protocol,
        "method_confirmation_matrix": method_confirmation_matrix,
        "uncertainty_method_readiness_summary": uncertainty_method_readiness_summary,
        "software_validation_traceability_matrix": software_validation_traceability_matrix,
        "release_validation_manifest": release_validation_manifest,
        "audit_readiness_digest": audit_readiness_digest,
    }
    return _enrich_recognition_readiness_artifacts(
        artifacts=artifacts,
        phase_coverage_payload=phase_coverage_payload,
    )


def _build_scope_definition_pack(
    *,
    run_id: str,
    route_families: list[str],
    payload_backed_phases: list[str],
    trace_only_phases: list[str],
    gap_phases: list[str],
    sample_digest: dict[str, Any],
    version_payload: dict[str, Any],
    acceptance_payload: dict[str, Any],
    path_map: dict[str, str],
) -> dict[str, Any]:
    profiles = _dedupe(
        [
            str(version_payload.get("profile_version") or "").strip(),
            str(dict(acceptance_payload.get("readiness_summary") or {}).get("evidence_mode") or "").strip(),
            "measurement_trace_rich_v1",
        ]
    )
    raw = {
        "schema_version": "1.0",
        "artifact_type": "scope_definition_pack",
        "generated_at": _now_iso(),
        "run_id": run_id,
        "artifact_role": "execution_summary",
        "evidence_source": "simulated_protocol",
        "evidence_state": "reviewer_readiness_only",
        "not_real_acceptance_evidence": True,
        "boundary_statements": list(RECOGNITION_READINESS_BOUNDARY_STATEMENTS),
        "measurand_family": "dual",
        "route_applicability": route_families,
        "temperature_range": sample_digest["temperature_range"],
        "pressure_range": sample_digest["pressure_range"],
        "humidity_mode": "water + ambient simulation coverage",
        "analyzer_population_scope": sample_digest["analyzers"] or ["simulation_analyzer_population"],
        "applicable_profiles": profiles,
        "current_coverage": {
            "payload_backed_phases": payload_backed_phases,
            "trace_only_phases": trace_only_phases,
            "gap_phases": gap_phases,
        },
        "linked_artifacts": {
            "measurement_phase_coverage_report": path_map["measurement_phase_coverage_report"],
            "multi_source_stability_evidence": path_map["multi_source_stability_evidence"],
            "state_transition_evidence": path_map["state_transition_evidence"],
            "simulation_evidence_sidecar_bundle": path_map["simulation_evidence_sidecar_bundle"],
            "scope_readiness_summary": path_map["scope_readiness_summary"],
            "decision_rule_profile": path_map["decision_rule_profile"],
        },
        "readiness_status": "scope_skeleton_ready",
        "non_claim": [
            "scope package stub only",
            "not a formal scope statement",
            "not accreditation claim",
            "not real acceptance",
        ],
    }
    markdown = _render_markdown(
        "Scope Definition Pack",
        [
            f"- measurand_family: {raw['measurand_family']}",
            f"- route_applicability: {' | '.join(route_families) or '--'}",
            f"- temperature_range: {raw['temperature_range']}",
            f"- pressure_range: {raw['pressure_range']}",
            f"- humidity_mode: {raw['humidity_mode']}",
            f"- analyzer_population_scope: {' | '.join(raw['analyzer_population_scope'])}",
            f"- applicable_profiles: {' | '.join(raw['applicable_profiles']) or '--'}",
            f"- readiness_status: {raw['readiness_status']}",
            f"- payload_backed_phases: {' | '.join(payload_backed_phases) or '--'}",
            f"- trace_only_phases: {' | '.join(trace_only_phases) or '--'}",
            f"- gap_phases: {' | '.join(gap_phases) or '--'}",
            f"- non_claim: {' | '.join(raw['non_claim'])}",
        ],
    )
    return {
        "available": True,
        "artifact_type": "scope_definition_pack",
        "filename": SCOPE_DEFINITION_PACK_FILENAME,
        "markdown_filename": SCOPE_DEFINITION_PACK_MARKDOWN_FILENAME,
        "raw": raw,
        "markdown": markdown,
        "digest": {"summary": "scope definition pack / readiness stub"},
    }


def _build_decision_rule_profile(
    *,
    run_id: str,
    version_payload: dict[str, Any],
    acceptance_payload: dict[str, Any],
    analytics_payload: dict[str, Any],
    phase_digest: dict[str, Any],
    path_map: dict[str, str],
) -> dict[str, Any]:
    raw = {
        "schema_version": "1.0",
        "artifact_type": "decision_rule_profile",
        "generated_at": _now_iso(),
        "run_id": run_id,
        "artifact_role": "execution_summary",
        "evidence_source": "simulated_protocol",
        "evidence_state": "reviewer_readiness_only",
        "not_real_acceptance_evidence": True,
        "boundary_statements": list(RECOGNITION_READINESS_BOUNDARY_STATEMENTS),
        "rule_profile_id": "decision_rule_profile_v0",
        "version": "v0",
        "pass_warn_fail_semantics": {
            "pass": "simulation reviewer evidence is complete enough for dry-run scope discussion",
            "warn": "coverage exists but still contains trace-only or stubbed evidence",
            "fail": "current reviewer skeleton has hard gaps that block readiness sign-off",
        },
        "tolerance_source": {
            "source": "current analytics/qc thresholds + measurement-core reviewer digests",
            "profile_version": str(version_payload.get("profile_version") or "--"),
            "acceptance_scope": str(acceptance_payload.get("acceptance_scope") or "simulation_only"),
            "quality_summary": str(dict(analytics_payload.get("qc_overview") or {}).get("summary") or ""),
        },
        "evaluation_dimensions": [
            "measurement-core phase coverage",
            "shadow stability evidence",
            "controlled transition trace",
            "config safety / Step 2 dual-gate",
            "reviewer artifact completeness",
        ],
        "phase_digest": {
            "payload_phases": str(phase_digest.get("payload_phase_summary") or "--"),
            "trace_only": str(phase_digest.get("trace_only_phase_summary") or "--"),
            "coverage": str(phase_digest.get("coverage_summary") or "--"),
        },
        "current_stage_applicability": "Step 2 tail reviewer decision support only",
        "linked_artifacts": {
            "scope_definition_pack": path_map["scope_definition_pack"],
            "scope_readiness_summary": path_map["scope_readiness_summary"],
            "measurement_phase_coverage_report": path_map["measurement_phase_coverage_report"],
        },
        "non_claim": [
            "decision rule profile stub only",
            "not a release gate",
            "not live acceptance",
            "not compliance claim",
        ],
    }
    markdown = _render_markdown(
        "Decision Rule Profile",
        [
            f"- rule_profile_id: {raw['rule_profile_id']}",
            f"- version: {raw['version']}",
            f"- current_stage_applicability: {raw['current_stage_applicability']}",
            f"- tolerance_source: {raw['tolerance_source']['source']}",
            f"- evaluation_dimensions: {' | '.join(raw['evaluation_dimensions'])}",
            f"- payload_phases: {raw['phase_digest']['payload_phases']}",
            f"- trace_only: {raw['phase_digest']['trace_only']}",
            f"- coverage: {raw['phase_digest']['coverage']}",
            f"- non_claim: {' | '.join(raw['non_claim'])}",
        ],
    )
    return {
        "available": True,
        "artifact_type": "decision_rule_profile",
        "filename": DECISION_RULE_PROFILE_FILENAME,
        "markdown_filename": DECISION_RULE_PROFILE_MARKDOWN_FILENAME,
        "raw": raw,
        "markdown": markdown,
        "digest": {
            "summary": "decision rule profile / pass-warn-fail semantics / reviewer-only",
            "tolerance_source_summary": raw["tolerance_source"]["source"],
        },
    }


def _build_scope_readiness_summary(
    *,
    run_id: str,
    scope_definition_pack: dict[str, Any],
    decision_rule_profile: dict[str, Any],
    payload_backed_phases: list[str],
    trace_only_phases: list[str],
    gap_phases: list[str],
    phase_digest: dict[str, Any],
    path_map: dict[str, str],
) -> dict[str, Any]:
    current_coverage = [
        f"payload-backed simulated phases: {' | '.join(payload_backed_phases) or '--'}",
        f"trace-only phases: {' | '.join(trace_only_phases) or '--'}",
        f"coverage digest: {str(phase_digest.get('coverage_summary') or '--')}",
        f"decision rule digest: {str(dict(decision_rule_profile.get('digest') or {}).get('summary') or '--')}",
    ]
    missing_evidence = [
        *([f"trace-only phases remain: {' | '.join(trace_only_phases)}"] if trace_only_phases else []),
        *([f"coverage gaps remain: {' | '.join(gap_phases)}"] if gap_phases else []),
        "formal scope approval chain is not closed",
        "real acceptance and accreditation evidence remain out of scope",
    ]
    blockers = [
        "scope package is reviewer-facing only",
        "decision rule profile does not drive live gate",
        "certificate-backed reference chain is not closed",
    ]
    digest = {
        "summary": (
            "Step 2 reviewer readiness | scope package + decision rule profile | "
            f"payload-backed {len(payload_backed_phases)} | trace-only {len(trace_only_phases)} | gap {len(gap_phases)}"
        ),
        "current_coverage_summary": " | ".join(current_coverage),
        "missing_evidence_summary": " | ".join(missing_evidence),
        "blocker_summary": " | ".join(blockers),
        "decision_rule_profile_summary": str(dict(decision_rule_profile.get("digest") or {}).get("summary") or "--"),
    }
    return _summary_raw(
        run_id=run_id,
        artifact_type="scope_readiness_summary",
        overall_status="degraded" if (trace_only_phases or gap_phases) else "diagnostic_only",
        title_text="Scope Readiness Summary",
        reviewer_note=(
            "Step 2 reviewer-facing scope readiness summary only. "
            "This pack helps review dry-run scope applicability and decision-rule semantics without claiming formal scope approval."
        ),
        summary_text=digest["summary"],
        summary_lines=current_coverage,
        detail_lines=[
            f"missing evidence: {digest['missing_evidence_summary']}",
            f"blockers: {digest['blocker_summary']}",
            f"scope_definition_pack: {path_map['scope_definition_pack']}",
            f"decision_rule_profile: {path_map['decision_rule_profile']}",
            *[f"boundary: {line}" for line in RECOGNITION_READINESS_BOUNDARY_STATEMENTS],
        ],
        anchor_id="scope-readiness-summary",
        anchor_label="Scope readiness summary",
        evidence_categories=["recognition_readiness", "scope_readiness", "decision_rule_profile"],
        artifact_paths={
            "scope_definition_pack": path_map["scope_definition_pack"],
            "scope_definition_pack_markdown": path_map["scope_definition_pack_markdown"],
            "decision_rule_profile": path_map["decision_rule_profile"],
            "decision_rule_profile_markdown": path_map["decision_rule_profile_markdown"],
            "scope_readiness_summary": path_map["scope_readiness_summary"],
            "scope_readiness_summary_markdown": path_map["scope_readiness_summary_markdown"],
            "measurement_phase_coverage_report": path_map["measurement_phase_coverage_report"],
        },
        body={
            "current_coverage": current_coverage,
            "missing_evidence": missing_evidence,
            "blockers": blockers,
            "linked_artifacts": dict(scope_definition_pack.get("raw", {}).get("linked_artifacts") or {}),
            "decision_rule_profile_digest": dict(decision_rule_profile.get("digest") or {}),
        },
        digest=digest,
        filename=SCOPE_READINESS_SUMMARY_FILENAME,
        markdown_filename=SCOPE_READINESS_SUMMARY_MARKDOWN_FILENAME,
    )


def _build_reference_asset_registry(
    *,
    run_id: str,
    sample_digest: dict[str, Any],
    payload_backed_phases: list[str],
    path_map: dict[str, str],
) -> dict[str, Any]:
    analyzer_scope = " | ".join(sample_digest["analyzers"]) or "simulation_analyzer_population"
    assets = [
        _asset_row("sg-001", "standard_gas", "Standard gas / standard gas lot", "reference", current_stage_usage="registry_stub_only"),
        _asset_row("hg-001", "humidity_generator", "Humidity generator", "reference", current_stage_usage="registry_stub_only"),
        _asset_row("dp-001", "dew_point_meter", "Dew point meter", "reference", current_stage_usage="registry_stub_only"),
        _asset_row("pg-001", "digital_pressure_gauge", "Digital pressure gauge", "reference", current_stage_usage="registry_stub_only"),
        _asset_row("pc-001", "pressure_controller", "Pressure controller", "support", current_stage_usage="registry_stub_only"),
        _asset_row("tc-001", "temperature_chamber", "Temperature chamber", "support", current_stage_usage="registry_stub_only"),
        _asset_row("th-001", "thermometer", "Thermometer", "reference", current_stage_usage="registry_stub_only"),
        _asset_row(
            "dut-sim",
            "analyzer_under_test",
            f"Analyzer under test population / {analyzer_scope}",
            "dut",
            certificate_status="not_applicable_for_reference_chain",
            intermediate_check_status="not_applicable_for_reference_chain",
            current_stage_usage="simulation_dut_stub_only",
        ),
    ]
    raw = {
        "schema_version": "1.0",
        "artifact_type": "reference_asset_registry",
        "generated_at": _now_iso(),
        "run_id": run_id,
        "artifact_role": "execution_summary",
        "evidence_source": "simulated_protocol",
        "evidence_state": "reviewer_readiness_only",
        "not_real_acceptance_evidence": True,
        "boundary_statements": list(RECOGNITION_READINESS_BOUNDARY_STATEMENTS),
        "assets": assets,
        "payload_backed_phases": payload_backed_phases,
        "readiness_status": "reference_registry_stub_only",
        "linked_run_artifacts": {
            "measurement_phase_coverage_report": path_map["measurement_phase_coverage_report"],
            "scope_readiness_summary": path_map["scope_readiness_summary"],
            "certificate_readiness_summary": path_map["certificate_readiness_summary"],
        },
        "non_claim": [
            "reference registry stub only",
            "not a released metrology chain",
            "certificate evidence not closed",
        ],
    }
    markdown = _render_markdown(
        "Reference Asset Registry",
        [
            *[
                (
                    f"- {row['asset_id']}: {row['display_name']} | role={row['role']} | "
                    f"certificate_status={row['certificate_status']} | intermediate_check_status={row['intermediate_check_status']}"
                )
                for row in assets
            ],
            f"- readiness_status: {raw['readiness_status']}",
            f"- non_claim: {' | '.join(raw['non_claim'])}",
        ],
    )
    return {
        "available": True,
        "artifact_type": "reference_asset_registry",
        "filename": REFERENCE_ASSET_REGISTRY_FILENAME,
        "markdown_filename": REFERENCE_ASSET_REGISTRY_MARKDOWN_FILENAME,
        "raw": raw,
        "markdown": markdown,
        "digest": {
            "summary": "reference asset registry / reviewer readiness stub",
            "asset_count": len(assets),
        },
    }


def _build_certificate_readiness_summary(
    *,
    run_id: str,
    reference_asset_registry: dict[str, Any],
    path_map: dict[str, str],
) -> dict[str, Any]:
    assets = [dict(item) for item in list(dict(reference_asset_registry.get("raw") or {}).get("assets") or [])]
    missing_certificate_count = sum(
        1 for item in assets if str(item.get("certificate_status") or "").startswith("missing")
    )
    missing_intermediate_check_count = sum(
        1 for item in assets if str(item.get("intermediate_check_status") or "").startswith("missing")
    )
    current_coverage = [
        f"assets tracked: {len(assets)}",
        f"certificate missing: {missing_certificate_count}",
        f"intermediate check missing: {missing_intermediate_check_count}",
    ]
    missing_evidence = [
        "no released certificate files attached",
        "no intermediate check execution evidence attached",
        "traceability chain remains readiness-only",
    ]
    digest = {
        "summary": (
            "reference asset / certificate readiness | "
            f"assets {len(assets)} | certificate gaps {missing_certificate_count} | "
            f"intermediate-check gaps {missing_intermediate_check_count}"
        ),
        "current_coverage_summary": " | ".join(current_coverage),
        "missing_evidence_summary": " | ".join(missing_evidence),
    }
    return _summary_raw(
        run_id=run_id,
        artifact_type="certificate_readiness_summary",
        overall_status="degraded",
        title_text="Certificate Readiness Summary",
        reviewer_note=(
            "Reference asset / certificate readiness summary only. "
            "It shows registry coverage and gaps without turning missing certificates into pass results."
        ),
        summary_text=digest["summary"],
        summary_lines=current_coverage,
        detail_lines=[
            f"missing evidence: {digest['missing_evidence_summary']}",
            f"reference_asset_registry: {path_map['reference_asset_registry']}",
            f"metrology_traceability_stub: {path_map['metrology_traceability_stub']}",
            *[f"boundary: {line}" for line in RECOGNITION_READINESS_BOUNDARY_STATEMENTS],
        ],
        anchor_id="certificate-readiness-summary",
        anchor_label="Certificate readiness summary",
        evidence_categories=["recognition_readiness", "reference_asset_readiness", "certificate_readiness"],
        artifact_paths={
            "reference_asset_registry": path_map["reference_asset_registry"],
            "reference_asset_registry_markdown": path_map["reference_asset_registry_markdown"],
            "certificate_readiness_summary": path_map["certificate_readiness_summary"],
            "certificate_readiness_summary_markdown": path_map["certificate_readiness_summary_markdown"],
            "metrology_traceability_stub": path_map["metrology_traceability_stub"],
            "metrology_traceability_stub_markdown": path_map["metrology_traceability_stub_markdown"],
        },
        body={
            "current_coverage": current_coverage,
            "missing_evidence": missing_evidence,
            "asset_status_rows": assets,
        },
        digest=digest,
        filename=CERTIFICATE_READINESS_SUMMARY_FILENAME,
        markdown_filename=CERTIFICATE_READINESS_SUMMARY_MARKDOWN_FILENAME,
    )


def _build_metrology_traceability_stub(
    *,
    run_id: str,
    reference_asset_registry: dict[str, Any],
    certificate_readiness_summary: dict[str, Any],
    path_map: dict[str, str],
) -> dict[str, Any]:
    assets = [dict(item) for item in list(dict(reference_asset_registry.get("raw") or {}).get("assets") or [])]
    asset_by_type = {str(item.get("asset_type") or ""): dict(item) for item in assets}
    chain_rows = [
        {
            "control_object": "gas route / CO2",
            "reference_asset": str(dict(asset_by_type.get("standard_gas") or {}).get("display_name") or "standard gas"),
            "supporting_assets": [
                str(dict(asset_by_type.get("digital_pressure_gauge") or {}).get("display_name") or "digital pressure gauge"),
                str(dict(asset_by_type.get("thermometer") or {}).get("display_name") or "thermometer"),
            ],
            "readiness_status": "traceability_stub_only",
        },
        {
            "control_object": "water route / humidity",
            "reference_asset": str(dict(asset_by_type.get("humidity_generator") or {}).get("display_name") or "humidity generator"),
            "supporting_assets": [
                str(dict(asset_by_type.get("dew_point_meter") or {}).get("display_name") or "dew point meter"),
                str(dict(asset_by_type.get("temperature_chamber") or {}).get("display_name") or "temperature chamber"),
            ],
            "readiness_status": "traceability_stub_only",
        },
    ]
    raw = {
        "schema_version": "1.0",
        "artifact_type": "metrology_traceability_stub",
        "generated_at": _now_iso(),
        "run_id": run_id,
        "artifact_role": "execution_summary",
        "evidence_source": "simulated_protocol",
        "evidence_state": "reviewer_readiness_only",
        "not_real_acceptance_evidence": True,
        "boundary_statements": list(RECOGNITION_READINESS_BOUNDARY_STATEMENTS),
        "chain_rows": chain_rows,
        "certificate_readiness_summary": dict(certificate_readiness_summary.get("digest") or {}),
        "linked_artifacts": {
            "reference_asset_registry": path_map["reference_asset_registry"],
            "certificate_readiness_summary": path_map["certificate_readiness_summary"],
        },
        "readiness_status": "traceability_stub_only",
        "non_claim": [
            "traceability chain stub only",
            "certificate-backed release chain not closed",
            "not real metrology release evidence",
        ],
    }
    markdown = _render_markdown(
        "Metrology Traceability Stub",
        [
            *[
                (
                    f"- {row['control_object']}: {row['reference_asset']} | "
                    f"supporting_assets={' | '.join(row['supporting_assets'])} | readiness_status={row['readiness_status']}"
                )
                for row in chain_rows
            ],
            f"- certificate_readiness_summary: {str(dict(certificate_readiness_summary.get('digest') or {}).get('summary') or '--')}",
            f"- non_claim: {' | '.join(raw['non_claim'])}",
        ],
    )
    return {
        "available": True,
        "artifact_type": "metrology_traceability_stub",
        "filename": METROLOGY_TRACEABILITY_STUB_FILENAME,
        "markdown_filename": METROLOGY_TRACEABILITY_STUB_MARKDOWN_FILENAME,
        "raw": raw,
        "markdown": markdown,
        "digest": {"summary": "metrology traceability stub / reviewer readiness only"},
    }


def _build_uncertainty_budget_stub(
    *,
    run_id: str,
    route_families: list[str],
    payload_backed_phases: list[str],
    trace_only_phases: list[str],
    path_map: dict[str, str],
) -> dict[str, Any]:
    rows = [
        {
            "measurand": "CO2",
            "model_name": "co2_ratio_to_ppm",
            "input_quantities": ["reference gas value", "pressure", "temperature", "ratio response"],
            "sensitivity_placeholders": ["dc/dreference", "dc/dpressure", "dc/dtemperature", "dc/dratio"],
            "uncertainty_sources": ["reference standard", "pressure reference", "temperature reference", "fit residual"],
            "combined_uncertainty_status": "not_closed",
            "readiness_status": "stub_ready",
            "non_claim": "not a released uncertainty budget",
        },
        {
            "measurand": "H2O",
            "model_name": "h2o_ratio_to_ppm",
            "input_quantities": ["humidity reference", "pressure", "temperature", "ratio response"],
            "sensitivity_placeholders": ["dc/dhumidity", "dc/dpressure", "dc/dtemperature", "dc/dratio"],
            "uncertainty_sources": ["humidity standard", "dew point reference", "pressure reference", "fit residual"],
            "combined_uncertainty_status": "not_closed",
            "readiness_status": "stub_ready",
            "non_claim": "not a released uncertainty budget",
        },
    ]
    raw = {
        "schema_version": "1.0",
        "artifact_type": "uncertainty_budget_stub",
        "generated_at": _now_iso(),
        "run_id": run_id,
        "artifact_role": "execution_summary",
        "evidence_source": "simulated_protocol",
        "evidence_state": "reviewer_readiness_only",
        "not_real_acceptance_evidence": True,
        "boundary_statements": list(RECOGNITION_READINESS_BOUNDARY_STATEMENTS),
        "rows": rows,
        "route_families": route_families,
        "payload_backed_phases": payload_backed_phases,
        "trace_only_phases": trace_only_phases,
        "linked_artifacts": {
            "measurement_phase_coverage_report": path_map["measurement_phase_coverage_report"],
            "multi_source_stability_evidence": path_map["multi_source_stability_evidence"],
        },
        "readiness_status": "uncertainty_stub_ready",
        "non_claim": [
            "uncertainty stub only",
            "not a final uncertainty report",
            "simulation does not close uncertainty",
        ],
    }
    markdown = _render_markdown(
        "Uncertainty Budget Stub",
        [
            *[
                (
                    f"- {row['measurand']}: model={row['model_name']} | "
                    f"combined_uncertainty_status={row['combined_uncertainty_status']} | "
                    f"readiness_status={row['readiness_status']}"
                )
                for row in rows
            ],
            f"- route_families: {' | '.join(route_families) or '--'}",
            f"- payload_backed_phases: {' | '.join(payload_backed_phases) or '--'}",
            f"- trace_only_phases: {' | '.join(trace_only_phases) or '--'}",
            f"- non_claim: {' | '.join(raw['non_claim'])}",
        ],
    )
    return {
        "available": True,
        "artifact_type": "uncertainty_budget_stub",
        "filename": UNCERTAINTY_BUDGET_STUB_FILENAME,
        "markdown_filename": UNCERTAINTY_BUDGET_STUB_MARKDOWN_FILENAME,
        "raw": raw,
        "markdown": markdown,
        "digest": {"summary": "uncertainty budget stub / reviewer readiness only"},
    }


def _build_method_confirmation_protocol(
    *,
    run_id: str,
    route_families: list[str],
    path_map: dict[str, str],
) -> dict[str, Any]:
    checks = {
        "linearity": "simulation regression coverage only",
        "repeatability": "simulation repeat windows only",
        "drift": "analytics trend digest only",
        "influence_factors": "temperature / pressure / humidity placeholders only",
        "route_specific_checks": [f"{route}: placeholder route check" for route in route_families],
        "acceptance_contract_placeholder": "reviewer contract placeholder only",
    }
    raw = {
        "schema_version": "1.0",
        "artifact_type": "method_confirmation_protocol",
        "generated_at": _now_iso(),
        "run_id": run_id,
        "artifact_role": "execution_summary",
        "evidence_source": "simulated_protocol",
        "evidence_state": "reviewer_readiness_only",
        "not_real_acceptance_evidence": True,
        "boundary_statements": list(RECOGNITION_READINESS_BOUNDARY_STATEMENTS),
        **checks,
        "linked_artifacts": {
            "uncertainty_budget_stub": path_map["uncertainty_budget_stub"],
            "method_confirmation_matrix": path_map["method_confirmation_matrix"],
        },
        "non_claim": [
            "method confirmation protocol stub only",
            "not a closed method confirmation report",
            "simulation does not close method confirmation",
        ],
    }
    markdown = _render_markdown(
        "Method Confirmation Protocol",
        [
            f"- linearity: {checks['linearity']}",
            f"- repeatability: {checks['repeatability']}",
            f"- drift: {checks['drift']}",
            f"- influence_factors: {checks['influence_factors']}",
            f"- route_specific_checks: {' | '.join(checks['route_specific_checks']) or '--'}",
            f"- acceptance_contract_placeholder: {checks['acceptance_contract_placeholder']}",
            f"- non_claim: {' | '.join(raw['non_claim'])}",
        ],
    )
    return {
        "available": True,
        "artifact_type": "method_confirmation_protocol",
        "filename": METHOD_CONFIRMATION_PROTOCOL_FILENAME,
        "markdown_filename": METHOD_CONFIRMATION_PROTOCOL_MARKDOWN_FILENAME,
        "raw": raw,
        "markdown": markdown,
        "digest": {"summary": "method confirmation protocol / reviewer readiness only"},
    }


def _build_method_confirmation_matrix(
    *,
    run_id: str,
    payload_backed_phases: list[str],
    trace_only_phases: list[str],
    gap_phases: list[str],
    path_map: dict[str, str],
) -> dict[str, Any]:
    rows = [
        {
            "test_item": "linearity",
            "evidence_source": "simulation regression + measurement-core payload coverage",
            "current_coverage": "payload-backed sample_ready phases available",
            "missing_evidence": "released reference asset chain + real acceptance datasets",
            "next_required_artifact": "uncertainty_budget_stub",
        },
        {
            "test_item": "repeatability",
            "evidence_source": "multi_source_stability_evidence",
            "current_coverage": "shadow stability reviewer digest available",
            "missing_evidence": "real repeated reference runs",
            "next_required_artifact": "method_confirmation_protocol",
        },
        {
            "test_item": "drift",
            "evidence_source": "analytics_summary / trend_registry",
            "current_coverage": "trend digest available",
            "missing_evidence": "real time-separated calibration evidence",
            "next_required_artifact": "software_validation_traceability_matrix",
        },
        {
            "test_item": "route_specific_checks",
            "evidence_source": "measurement_phase_coverage_report",
            "current_coverage": f"payload-backed {len(payload_backed_phases)} | trace-only {len(trace_only_phases)} | gap {len(gap_phases)}",
            "missing_evidence": "trace-only and gap phases still need richer evidence or explicit gap closure",
            "next_required_artifact": "scope_readiness_summary",
        },
    ]
    raw = {
        "schema_version": "1.0",
        "artifact_type": "method_confirmation_matrix",
        "generated_at": _now_iso(),
        "run_id": run_id,
        "artifact_role": "execution_summary",
        "evidence_source": "simulated_protocol",
        "evidence_state": "reviewer_readiness_only",
        "not_real_acceptance_evidence": True,
        "boundary_statements": list(RECOGNITION_READINESS_BOUNDARY_STATEMENTS),
        "rows": rows,
        "payload_backed_phases": payload_backed_phases,
        "trace_only_phases": trace_only_phases,
        "gap_phases": gap_phases,
        "linked_artifacts": {
            "measurement_phase_coverage_report": path_map["measurement_phase_coverage_report"],
            "multi_source_stability_evidence": path_map["multi_source_stability_evidence"],
            "state_transition_evidence": path_map["state_transition_evidence"],
        },
        "non_claim": [
            "method confirmation matrix stub only",
            "not method confirmation closure",
        ],
    }
    markdown = _render_markdown(
        "Method Confirmation Matrix",
        [
            *[
                (
                    f"- {row['test_item']}: evidence_source={row['evidence_source']} | "
                    f"current_coverage={row['current_coverage']} | missing_evidence={row['missing_evidence']} | "
                    f"next_required_artifact={row['next_required_artifact']}"
                )
                for row in rows
            ],
            f"- non_claim: {' | '.join(raw['non_claim'])}",
        ],
    )
    return {
        "available": True,
        "artifact_type": "method_confirmation_matrix",
        "filename": METHOD_CONFIRMATION_MATRIX_FILENAME,
        "markdown_filename": METHOD_CONFIRMATION_MATRIX_MARKDOWN_FILENAME,
        "raw": raw,
        "markdown": markdown,
        "digest": {"summary": "method confirmation matrix / reviewer readiness only"},
    }


def _build_uncertainty_method_readiness_summary(
    *,
    run_id: str,
    uncertainty_budget_stub: dict[str, Any],
    method_confirmation_protocol: dict[str, Any],
    method_confirmation_matrix: dict[str, Any],
    path_map: dict[str, str],
) -> dict[str, Any]:
    matrix_rows = [dict(item) for item in list(dict(method_confirmation_matrix.get("raw") or {}).get("rows") or [])]
    missing_evidence = _dedupe(str(item.get("missing_evidence") or "").strip() for item in matrix_rows)
    digest = {
        "summary": (
            "uncertainty / method confirmation readiness | "
            f"matrix rows {len(matrix_rows)} | missing evidence {len(missing_evidence)}"
        ),
        "budget_summary": str(dict(uncertainty_budget_stub.get("digest") or {}).get("summary") or "--"),
        "protocol_summary": str(dict(method_confirmation_protocol.get("digest") or {}).get("summary") or "--"),
        "matrix_summary": str(dict(method_confirmation_matrix.get("digest") or {}).get("summary") or "--"),
        "missing_evidence_summary": " | ".join(missing_evidence),
    }
    return _summary_raw(
        run_id=run_id,
        artifact_type="uncertainty_method_readiness_summary",
        overall_status="degraded",
        title_text="Uncertainty / Method Readiness Summary",
        reviewer_note=(
            "Uncertainty + method confirmation readiness only. "
            "This summary keeps placeholder objects visible without presenting them as final metrology outputs."
        ),
        summary_text=digest["summary"],
        summary_lines=[
            f"uncertainty budget: {digest['budget_summary']}",
            f"method protocol: {digest['protocol_summary']}",
            f"method matrix: {digest['matrix_summary']}",
        ],
        detail_lines=[
            f"missing evidence: {digest['missing_evidence_summary']}",
            f"uncertainty_budget_stub: {path_map['uncertainty_budget_stub']}",
            f"method_confirmation_protocol: {path_map['method_confirmation_protocol']}",
            f"method_confirmation_matrix: {path_map['method_confirmation_matrix']}",
            *[f"boundary: {line}" for line in RECOGNITION_READINESS_BOUNDARY_STATEMENTS],
        ],
        anchor_id="uncertainty-method-readiness-summary",
        anchor_label="Uncertainty / method readiness summary",
        evidence_categories=["recognition_readiness", "uncertainty_readiness", "method_confirmation_readiness"],
        artifact_paths={
            "uncertainty_budget_stub": path_map["uncertainty_budget_stub"],
            "uncertainty_budget_stub_markdown": path_map["uncertainty_budget_stub_markdown"],
            "method_confirmation_protocol": path_map["method_confirmation_protocol"],
            "method_confirmation_protocol_markdown": path_map["method_confirmation_protocol_markdown"],
            "method_confirmation_matrix": path_map["method_confirmation_matrix"],
            "method_confirmation_matrix_markdown": path_map["method_confirmation_matrix_markdown"],
            "uncertainty_method_readiness_summary": path_map["uncertainty_method_readiness_summary"],
            "uncertainty_method_readiness_summary_markdown": path_map["uncertainty_method_readiness_summary_markdown"],
        },
        body={"missing_evidence": missing_evidence, "matrix_rows": matrix_rows},
        digest=digest,
        filename=UNCERTAINTY_METHOD_READINESS_SUMMARY_FILENAME,
        markdown_filename=UNCERTAINTY_METHOD_READINESS_SUMMARY_MARKDOWN_FILENAME,
    )


def _build_software_validation_traceability_matrix(
    *,
    run_id: str,
    version_payload: dict[str, Any],
    lineage_payload: dict[str, Any],
    evidence_registry_payload: dict[str, Any],
    stability_payload: dict[str, Any],
    transition_payload: dict[str, Any],
    phase_coverage_payload: dict[str, Any],
    sidecar_payload: dict[str, Any],
    path_map: dict[str, str],
) -> dict[str, Any]:
    rows = [
        {
            "requirement": "measurement-core richer trace reviewer evidence",
            "design": "multi_source_stability + controlled_state_machine_profile + measurement_phase_coverage",
            "tests": [
                "tests/v2/test_measurement_phase_coverage_report.py",
                "tests/v2/test_multi_source_stability.py",
                "tests/v2/test_controlled_state_machine_profile.py",
            ],
            "artifacts": [
                path_map["multi_source_stability_evidence"],
                path_map["state_transition_evidence"],
                path_map["measurement_phase_coverage_report"],
            ],
        },
        {
            "requirement": "reviewer discoverability across results / workbench / review_center",
            "design": "results_gateway + app_facade + device_workbench",
            "tests": [
                "tests/v2/test_results_gateway.py",
                "tests/v2/test_ui_v2_workbench_evidence.py",
                "tests/v2/test_ui_v2_review_center.py",
            ],
            "artifacts": [
                path_map["scope_readiness_summary"],
                path_map["certificate_readiness_summary"],
                path_map["uncertainty_method_readiness_summary"],
                path_map["audit_readiness_digest"],
            ],
        },
        {
            "requirement": "sidecar-ready but non-primary evidence contract",
            "design": "simulation_evidence_sidecar_bundle + release_validation_manifest",
            "tests": [
                "tests/v2/test_multi_source_stability.py",
                "tests/v2/test_results_gateway.py",
            ],
            "artifacts": [
                path_map["simulation_evidence_sidecar_bundle"],
                path_map["release_validation_manifest"],
            ],
        },
    ]
    raw = {
        "schema_version": "1.0",
        "artifact_type": "software_validation_traceability_matrix",
        "generated_at": _now_iso(),
        "run_id": run_id,
        "artifact_role": "execution_summary",
        "evidence_source": "simulated_protocol",
        "evidence_state": "reviewer_readiness_only",
        "not_real_acceptance_evidence": True,
        "boundary_statements": list(RECOGNITION_READINESS_BOUNDARY_STATEMENTS),
        "requirements_design_tests_artifacts": rows,
        "config_version": str(lineage_payload.get("config_version") or version_payload.get("config_version") or "--"),
        "profile_version": str(lineage_payload.get("profile_version") or version_payload.get("profile_version") or "--"),
        "points_version": str(lineage_payload.get("points_version") or version_payload.get("points_version") or "--"),
        "algorithm_version": str(version_payload.get("algorithm_version") or "--"),
        "evidence_source_summary": {
            "stability": str(stability_payload.get("evidence_source") or "simulated_protocol"),
            "transition": str(transition_payload.get("evidence_source") or "simulated_protocol"),
            "phase_coverage": str(phase_coverage_payload.get("evidence_source") or "simulated"),
            "sidecar": str(sidecar_payload.get("bundle_type") or "simulation_evidence_sidecar_bundle"),
        },
        "generated_from": {
            "evidence_registry": bool(evidence_registry_payload),
            "lineage_summary": bool(lineage_payload),
        },
        "non_claim": [
            "software traceability matrix only",
            "not a formal software qualification report",
        ],
    }
    markdown = _render_markdown(
        "Software Validation Traceability Matrix",
        [
            *[
                (
                    f"- requirement={row['requirement']} | design={row['design']} | "
                    f"tests={' | '.join(row['tests'])} | artifacts={' | '.join(row['artifacts'])}"
                )
                for row in rows
            ],
            f"- config_version: {raw['config_version']}",
            f"- profile_version: {raw['profile_version']}",
            f"- points_version: {raw['points_version']}",
            f"- algorithm_version: {raw['algorithm_version']}",
            f"- non_claim: {' | '.join(raw['non_claim'])}",
        ],
    )
    return {
        "available": True,
        "artifact_type": "software_validation_traceability_matrix",
        "filename": SOFTWARE_VALIDATION_TRACEABILITY_MATRIX_FILENAME,
        "markdown_filename": SOFTWARE_VALIDATION_TRACEABILITY_MATRIX_MARKDOWN_FILENAME,
        "raw": raw,
        "markdown": markdown,
        "digest": {"summary": "software validation traceability matrix / reviewer readiness only"},
    }


def _build_release_validation_manifest(
    *,
    run_id: str,
    version_payload: dict[str, Any],
    lineage_payload: dict[str, Any],
    software_validation_traceability_matrix: dict[str, Any],
    path_map: dict[str, str],
) -> dict[str, Any]:
    raw = {
        "schema_version": "1.0",
        "artifact_type": "release_validation_manifest",
        "generated_at": _now_iso(),
        "run_id": run_id,
        "artifact_role": "execution_summary",
        "evidence_source": "simulated_protocol",
        "evidence_state": "reviewer_readiness_only",
        "not_real_acceptance_evidence": True,
        "boundary_statements": list(RECOGNITION_READINESS_BOUNDARY_STATEMENTS),
        "config_version": str(lineage_payload.get("config_version") or version_payload.get("config_version") or "--"),
        "profile_version": str(lineage_payload.get("profile_version") or version_payload.get("profile_version") or "--"),
        "points_version": str(lineage_payload.get("points_version") or version_payload.get("points_version") or "--"),
        "algorithm_version": str(version_payload.get("algorithm_version") or "--"),
        "generated_from_run_id": run_id,
        "artifact_hash_summary": {
            "status": "stub_only",
            "summary": "file-artifact chain is present; signed hash closure is deferred",
        },
        "linked_artifacts": {
            "software_validation_traceability_matrix": path_map["software_validation_traceability_matrix"],
            "audit_readiness_digest": path_map["audit_readiness_digest"],
        },
        "non_claim": [
            "release manifest stub only",
            "not a released validation record",
        ],
    }
    markdown = _render_markdown(
        "Release Validation Manifest",
        [
            f"- config_version: {raw['config_version']}",
            f"- profile_version: {raw['profile_version']}",
            f"- points_version: {raw['points_version']}",
            f"- algorithm_version: {raw['algorithm_version']}",
            f"- generated_from_run_id: {run_id}",
            f"- artifact_hash_summary: {raw['artifact_hash_summary']['summary']}",
            f"- software_validation_traceability_matrix: {path_map['software_validation_traceability_matrix']}",
            f"- non_claim: {' | '.join(raw['non_claim'])}",
        ],
    )
    return {
        "available": True,
        "artifact_type": "release_validation_manifest",
        "filename": RELEASE_VALIDATION_MANIFEST_FILENAME,
        "markdown_filename": RELEASE_VALIDATION_MANIFEST_MARKDOWN_FILENAME,
        "raw": raw,
        "markdown": markdown,
        "digest": {"summary": "release validation manifest / reviewer readiness only"},
    }


def _build_audit_readiness_digest(
    *,
    run_id: str,
    software_validation_traceability_matrix: dict[str, Any],
    release_validation_manifest: dict[str, Any],
    path_map: dict[str, str],
) -> dict[str, Any]:
    trace_rows = [
        dict(item)
        for item in list(dict(software_validation_traceability_matrix.get("raw") or {}).get("requirements_design_tests_artifacts") or [])
    ]
    digest = {
        "summary": (
            "software validation / audit readiness | "
            f"trace rows {len(trace_rows)} | file-artifact-first reviewer digest"
        ),
        "traceability_summary": str(dict(software_validation_traceability_matrix.get("digest") or {}).get("summary") or "--"),
        "release_manifest_summary": str(dict(release_validation_manifest.get("digest") or {}).get("summary") or "--"),
        "artifact_hash_summary": str(
            dict(dict(release_validation_manifest.get("raw") or {}).get("artifact_hash_summary") or {}).get("summary")
            or "--"
        ),
    }
    return _summary_raw(
        run_id=run_id,
        artifact_type="audit_readiness_digest",
        overall_status="diagnostic_only",
        title_text="Audit Readiness Digest",
        reviewer_note=(
            "Software validation / audit readiness digest only. "
            "This is a reviewer-facing traceability skeleton and not a formal audit conclusion."
        ),
        summary_text=digest["summary"],
        summary_lines=[
            f"software traceability: {digest['traceability_summary']}",
            f"release manifest: {digest['release_manifest_summary']}",
            f"artifact hash summary: {digest['artifact_hash_summary']}",
        ],
        detail_lines=[
            f"software_validation_traceability_matrix: {path_map['software_validation_traceability_matrix']}",
            f"release_validation_manifest: {path_map['release_validation_manifest']}",
            *[f"boundary: {line}" for line in RECOGNITION_READINESS_BOUNDARY_STATEMENTS],
        ],
        anchor_id="audit-readiness-digest",
        anchor_label="Audit readiness digest",
        evidence_categories=["recognition_readiness", "software_validation", "audit_readiness"],
        artifact_paths={
            "software_validation_traceability_matrix": path_map["software_validation_traceability_matrix"],
            "software_validation_traceability_matrix_markdown": path_map["software_validation_traceability_matrix_markdown"],
            "release_validation_manifest": path_map["release_validation_manifest"],
            "release_validation_manifest_markdown": path_map["release_validation_manifest_markdown"],
            "audit_readiness_digest": path_map["audit_readiness_digest"],
            "audit_readiness_digest_markdown": path_map["audit_readiness_digest_markdown"],
        },
        body={"trace_rows": trace_rows},
        digest=digest,
        filename=AUDIT_READINESS_DIGEST_FILENAME,
        markdown_filename=AUDIT_READINESS_DIGEST_MARKDOWN_FILENAME,
    )


def _summary_raw(
    *,
    run_id: str,
    artifact_type: str,
    overall_status: str,
    title_text: str,
    reviewer_note: str,
    summary_text: str,
    summary_lines: list[str],
    detail_lines: list[str],
    anchor_id: str,
    anchor_label: str,
    evidence_categories: list[str],
    artifact_paths: dict[str, str],
    body: dict[str, Any],
    digest: dict[str, Any],
    filename: str,
    markdown_filename: str,
) -> dict[str, Any]:
    raw = {
        "schema_version": "1.0",
        "artifact_type": artifact_type,
        "generated_at": _now_iso(),
        "run_id": run_id,
        "artifact_role": "diagnostic_analysis",
        "evidence_source": "simulated_protocol",
        "evidence_state": "reviewer_readiness_only",
        "not_real_acceptance_evidence": True,
        "boundary_statements": list(RECOGNITION_READINESS_BOUNDARY_STATEMENTS),
        "overall_status": overall_status,
        "non_claim": list(RECOGNITION_READINESS_BOUNDARY_STATEMENTS),
        "digest": dict(digest),
        "review_surface": {
            "title_text": title_text,
            "role_text": "diagnostic_analysis",
            "reviewer_note": reviewer_note,
            "summary_text": summary_text,
            "summary_lines": [line for line in summary_lines if str(line).strip()],
            "detail_lines": [line for line in detail_lines if str(line).strip()],
            "anchor_id": anchor_id,
            "anchor_label": anchor_label,
            "phase_filters": ["step2_tail_recognition_ready"],
            "route_filters": [],
            "signal_family_filters": [],
            "decision_result_filters": [],
            "policy_version_filters": [],
            "boundary_filters": list(RECOGNITION_READINESS_BOUNDARY_STATEMENTS),
            "evidence_source_filters": ["simulated_protocol", "reviewer_readiness_only"],
            "artifact_paths": dict(artifact_paths),
        },
        "artifact_paths": dict(artifact_paths),
        "evidence_categories": list(evidence_categories),
        **body,
    }
    markdown = _render_markdown(
        title_text,
        [
            f"- summary: {summary_text}",
            *[f"- {line}" for line in summary_lines if str(line).strip()],
            *[f"- {line}" for line in detail_lines if str(line).strip()],
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


def _enrich_recognition_readiness_artifacts(
    *,
    artifacts: dict[str, dict[str, Any]],
    phase_coverage_payload: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    enriched: dict[str, dict[str, Any]] = {}
    for artifact_name, artifact_payload in artifacts.items():
        payload = dict(artifact_payload or {})
        raw = dict(payload.get("raw") or {})
        if not raw:
            enriched[artifact_name] = payload
            continue
        raw = _enrich_recognition_readiness_artifact(
            raw=raw,
            phase_coverage_payload=phase_coverage_payload,
        )
        payload["raw"] = raw
        payload["digest"] = dict(raw.get("digest") or payload.get("digest") or {})
        payload["markdown"] = _append_readiness_markdown_section(str(payload.get("markdown") or ""), raw=raw)
        enriched[artifact_name] = payload
    return enriched


def _enrich_recognition_readiness_artifact(
    *,
    raw: dict[str, Any],
    phase_coverage_payload: dict[str, Any],
) -> dict[str, Any]:
    artifact_type = str(raw.get("artifact_type") or "").strip()
    review_surface = dict(raw.get("review_surface") or {})
    anchor_meta = _artifact_anchor(artifact_type)
    anchor_id = str(raw.get("anchor_id") or review_surface.get("anchor_id") or anchor_meta.get("anchor_id") or "").strip()
    anchor_label = str(
        raw.get("anchor_label") or review_surface.get("anchor_label") or anchor_meta.get("anchor_label") or artifact_type
    ).strip()
    linked_artifact_map = dict(raw.get("linked_artifacts") or raw.get("linked_run_artifacts") or {})
    linked_artifact_refs = [dict(item) for item in list(raw.get("linked_artifact_refs") or []) if isinstance(item, dict)]
    if not linked_artifact_refs:
        linked_artifact_refs = _artifact_refs_from_map(linked_artifact_map)
    linked_measurement_phase_artifacts = _measurement_phase_refs_for_artifact(
        phase_coverage_payload=phase_coverage_payload,
        artifact_type=artifact_type,
    )
    missing_evidence = _normalize_text_list(
        raw.get("missing_evidence")
        or _RECOGNITION_MISSING_EVIDENCE_DEFAULTS.get(artifact_type)
        or []
    )
    blockers = _normalize_text_list(
        raw.get("blockers")
        or _RECOGNITION_BLOCKER_DEFAULTS.get(artifact_type)
        or []
    )
    next_required_artifacts = _normalize_text_list(
        raw.get("next_required_artifacts")
        or _RECOGNITION_NEXT_ARTIFACT_DEFAULTS.get(artifact_type)
        or []
    )
    readiness_status = str(raw.get("readiness_status") or "").strip() or f"{artifact_type}_readiness_stub"
    linked_measurement_phase_summary = _phase_route_summary(linked_measurement_phase_artifacts)
    linked_measurement_gap_summary = _linked_measurement_gap_summary(linked_measurement_phase_artifacts)
    preseal_partial_gap_summary = _preseal_partial_gap_summary(
        artifact_type=artifact_type,
        linked_measurement_phase_artifacts=linked_measurement_phase_artifacts,
    )
    linked_artifact_summary = " | ".join(
        _dedupe(str(item.get("artifact_type") or item.get("anchor_label") or "").strip() for item in linked_artifact_refs)
    )
    boundary_digest = " | ".join(
        str(item).strip() for item in list(raw.get("boundary_statements") or []) if str(item).strip()
    )
    non_claim_digest = " | ".join(str(item).strip() for item in list(raw.get("non_claim") or []) if str(item).strip())
    digest = dict(raw.get("digest") or {})
    digest["readiness_status"] = readiness_status
    if missing_evidence:
        digest["missing_evidence_summary"] = " | ".join(missing_evidence)
    if blockers:
        digest["blocker_summary"] = " | ".join(blockers)
    if next_required_artifacts:
        digest["next_required_artifacts_summary"] = " | ".join(next_required_artifacts)
    if linked_measurement_phase_summary:
        digest["linked_measurement_phase_summary"] = linked_measurement_phase_summary
    if linked_measurement_gap_summary:
        digest["linked_measurement_gap_summary"] = linked_measurement_gap_summary
    if preseal_partial_gap_summary:
        digest["preseal_partial_gap_summary"] = preseal_partial_gap_summary
    if linked_artifact_summary:
        digest["linked_artifact_summary"] = linked_artifact_summary
    if boundary_digest:
        digest["boundary_digest"] = boundary_digest
    if non_claim_digest:
        digest["non_claim_digest"] = non_claim_digest
    raw["anchor_id"] = anchor_id
    raw["anchor_label"] = anchor_label
    raw["linked_artifact_refs"] = linked_artifact_refs
    raw["linked_measurement_phase_artifacts"] = linked_measurement_phase_artifacts
    raw["linked_measurement_gap_summary"] = linked_measurement_gap_summary
    raw["preseal_partial_gap_summary"] = preseal_partial_gap_summary
    raw["missing_evidence"] = missing_evidence
    raw["blockers"] = blockers
    raw["next_required_artifacts"] = next_required_artifacts
    raw["readiness_status"] = readiness_status
    raw["boundary_digest"] = boundary_digest
    raw["non_claim_digest"] = non_claim_digest
    raw["digest"] = digest
    if review_surface:
        review_surface["anchor_id"] = anchor_id
        review_surface["anchor_label"] = anchor_label
        review_surface["phase_filters"] = _dedupe(
            [*list(review_surface.get("phase_filters") or []), *[str(item.get("phase_name") or "") for item in linked_measurement_phase_artifacts]]
        )
        review_surface["route_filters"] = _dedupe(
            [*list(review_surface.get("route_filters") or []), *[str(item.get("route_family") or "") for item in linked_measurement_phase_artifacts]]
        )
        review_surface["anchor_refs"] = _dedupe(
            [
                *list(review_surface.get("anchor_refs") or []),
                *[str(item.get("anchor_id") or "") for item in linked_artifact_refs],
                *[str(item.get("anchor_id") or "") for item in linked_measurement_phase_artifacts],
            ]
        )
        review_surface["summary_lines"] = _merge_unique_lines(
            list(review_surface.get("summary_lines") or []),
            [
                f"readiness status: {readiness_status}",
                f"linked measurement phases: {linked_measurement_phase_summary}" if linked_measurement_phase_summary else "",
                f"linked measurement gaps: {linked_measurement_gap_summary}" if linked_measurement_gap_summary else "",
                f"next required artifacts: {' | '.join(next_required_artifacts)}" if next_required_artifacts else "",
            ],
        )
        review_surface["detail_lines"] = _merge_unique_lines(
            list(review_surface.get("detail_lines") or []),
            [
                f"linked artifacts: {linked_artifact_summary}" if linked_artifact_summary else "",
                f"linked measurement phases: {linked_measurement_phase_summary}" if linked_measurement_phase_summary else "",
                f"linked measurement gaps: {linked_measurement_gap_summary}" if linked_measurement_gap_summary else "",
                f"preseal partial gap: {preseal_partial_gap_summary}" if preseal_partial_gap_summary else "",
                f"missing evidence: {' | '.join(missing_evidence)}" if missing_evidence else "",
                f"blockers: {' | '.join(blockers)}" if blockers else "",
                f"next required artifacts: {' | '.join(next_required_artifacts)}" if next_required_artifacts else "",
                f"non-claim digest: {non_claim_digest}" if non_claim_digest else "",
            ],
        )
        raw["review_surface"] = review_surface
    return raw


def _artifact_anchor(artifact_type: str) -> dict[str, str]:
    return dict(_RECOGNITION_ARTIFACT_ANCHORS.get(str(artifact_type or "").strip()) or {})


def _artifact_refs_from_map(linked_artifact_map: dict[str, Any]) -> list[dict[str, str]]:
    refs: list[dict[str, str]] = []
    for artifact_type, path in dict(linked_artifact_map or {}).items():
        path_text = str(path or "").strip()
        anchor_meta = _artifact_anchor(str(artifact_type or "").strip())
        refs.append(
            {
                "artifact_type": str(artifact_type or "").strip(),
                "path": path_text,
                "anchor_id": str(anchor_meta.get("anchor_id") or "").strip(),
                "anchor_label": str(anchor_meta.get("anchor_label") or str(artifact_type or "")).strip(),
            }
        )
    return refs


def _measurement_phase_refs_for_artifact(
    *,
    phase_coverage_payload: dict[str, Any],
    artifact_type: str,
) -> list[dict[str, Any]]:
    phase_rows = [dict(item) for item in list(phase_coverage_payload.get("phase_rows") or []) if isinstance(item, dict)]
    refs: list[dict[str, Any]] = []
    for row in phase_rows:
        linked_refs = [dict(item) for item in list(row.get("linked_readiness_artifact_refs") or []) if isinstance(item, dict)]
        if not any(str(item.get("artifact_type") or "").strip() == str(artifact_type or "").strip() for item in linked_refs):
            continue
        refs.append(
            {
                "artifact_type": "measurement_phase_coverage_report",
                "phase_route_key": str(row.get("phase_route_key") or "").strip(),
                "route_family": str(row.get("route_family") or "").strip(),
                "phase_name": str(row.get("phase_name") or "").strip(),
                "route_phase": (
                    f"{str(row.get('route_family') or '').strip()}/{str(row.get('phase_name') or '').strip()}".strip("/")
                ),
                "anchor_id": str(row.get("anchor_id") or "").strip(),
                "anchor_label": str(row.get("anchor_label") or "").strip(),
                "coverage_bucket": str(row.get("coverage_bucket") or "").strip(),
                "coverage_bucket_display": str(row.get("coverage_bucket_display") or row.get("coverage_bucket") or "").strip(),
                "payload_completeness": str(row.get("payload_completeness") or "").strip(),
                "available_signal_layers": list(row.get("available_signal_layers") or []),
                "missing_signal_layers": list(row.get("missing_signal_layers") or []),
                "missing_reason_digest": str(row.get("missing_reason_digest") or "").strip(),
                "evidence_provenance": str(row.get("evidence_provenance") or "").strip(),
                "readiness_impact_digest": str(row.get("readiness_impact_digest") or "").strip(),
                "linked_readiness_summary": str(row.get("linked_readiness_summary") or "").strip(),
                "blockers": list(row.get("blockers") or []),
                "next_required_artifacts": list(row.get("next_required_artifacts") or []),
                "reviewer_guidance_digest": str(row.get("reviewer_guidance_digest") or "").strip(),
                "comparison_digest": str(row.get("comparison_digest") or "").strip(),
            }
        )
    return refs


def _phase_route_summary(rows: list[dict[str, Any]]) -> str:
    return " | ".join(
        _dedupe(
            (
                f"{str(item.get('route_phase') or '').strip()}="
                f"{str(item.get('coverage_bucket_display') or item.get('coverage_bucket') or '').strip()}"
            )
            for item in rows
            if str(item.get("route_phase") or "").strip()
        )
    )


def _linked_measurement_gap_summary(rows: list[dict[str, Any]]) -> str:
    return " | ".join(
        _dedupe(
            (
                f"{str(item.get('route_phase') or '').strip()}: "
                f"{str(item.get('missing_reason_digest') or item.get('readiness_impact_digest') or '').strip()}"
            )
            for item in rows
            if str(item.get("route_phase") or "").strip()
            and (
                str(item.get("missing_reason_digest") or "").strip()
                or str(item.get("readiness_impact_digest") or "").strip()
            )
            and str(item.get("coverage_bucket") or "").strip() != "actual_simulated_run_with_payload_complete"
        )
    )


_RECOGNITION_PRESEAL_PARTIAL_HINTS: dict[str, str] = {
    "scope_definition_pack": (
        "scope boundary stays reviewer-only until preseal conditioning evidence can be tied to released output criteria"
    ),
    "decision_rule_profile": (
        "decision-rule wording remains constrained because preseal does not yet claim released measurement output"
    ),
    "scope_readiness_summary": "scope readiness cannot be promoted while preseal remains a partial conditioning window",
    "uncertainty_budget_stub": "uncertainty inputs stay stub-only while preseal output terms remain open",
    "method_confirmation_protocol": "method protocol steps stay reviewer-only while preseal remains setup evidence",
    "method_confirmation_matrix": "method rows stay open because preseal evidence does not yet close released output terms",
}


def _preseal_partial_gap_summary(
    *,
    artifact_type: str,
    linked_measurement_phase_artifacts: list[dict[str, Any]],
) -> str:
    hint = str(_RECOGNITION_PRESEAL_PARTIAL_HINTS.get(str(artifact_type or "").strip()) or "").strip()
    if not hint:
        return ""
    rows = [
        dict(item)
        for item in list(linked_measurement_phase_artifacts or [])
        if str(item.get("phase_name") or "").strip() == "preseal"
        and str(item.get("coverage_bucket") or "").strip() == "actual_simulated_run_with_payload_partial"
    ]
    if not rows:
        return ""
    return " | ".join(
        _dedupe(
            (
                f"{str(item.get('route_phase') or '').strip()}: "
                f"{str(item.get('missing_reason_digest') or item.get('reviewer_guidance_digest') or '').strip()} | "
                f"{hint}"
            )
            for item in rows
            if str(item.get("route_phase") or "").strip()
        )
    )


def _normalize_text_list(value: Any) -> list[str]:
    if isinstance(value, (list, tuple, set)):
        return _dedupe(str(item).strip() for item in value if str(item).strip())
    text = str(value or "").strip()
    return [text] if text else []


def _merge_unique_lines(existing: list[str], extra: list[str]) -> list[str]:
    return _dedupe([*list(existing or []), *[str(item).strip() for item in extra if str(item).strip()]])


def _append_readiness_markdown_section(markdown: str, *, raw: dict[str, Any]) -> str:
    base = str(markdown or "").rstrip()
    lines = [
        "",
        "## Readiness Linkage",
        "",
        f"- anchor_id: {str(raw.get('anchor_id') or '--')}",
        f"- readiness_status: {str(raw.get('readiness_status') or '--')}",
        f"- linked_artifact_refs: {' | '.join(_dedupe(str(item.get('artifact_type') or '') for item in list(raw.get('linked_artifact_refs') or []) if isinstance(item, dict))) or '--'}",
        f"- linked_measurement_phases: {_phase_route_summary(list(raw.get('linked_measurement_phase_artifacts') or [])) or '--'}",
        f"- linked_measurement_gaps: {str(raw.get('linked_measurement_gap_summary') or '--')}",
        f"- preseal_partial_gap: {str(raw.get('preseal_partial_gap_summary') or '--')}",
        f"- missing_evidence: {' | '.join(list(raw.get('missing_evidence') or [])) or '--'}",
        f"- blockers: {' | '.join(list(raw.get('blockers') or [])) or '--'}",
        f"- next_required_artifacts: {' | '.join(list(raw.get('next_required_artifacts') or [])) or '--'}",
        f"- boundary_digest: {str(raw.get('boundary_digest') or '--')}",
        f"- non_claim_digest: {str(raw.get('non_claim_digest') or '--')}",
    ]
    return (base + "\n" + "\n".join(lines).rstrip() + "\n").lstrip()


def _asset_row(
    asset_id: str,
    asset_type: str,
    display_name: str,
    role: str,
    *,
    certificate_status: str = "missing_certificate",
    expiry_status: str = "unknown_expiry",
    intermediate_check_status: str = "missing_intermediate_check",
    current_stage_usage: str = "registry_stub_only",
) -> dict[str, Any]:
    return {
        "asset_id": asset_id,
        "asset_type": asset_type,
        "display_name": display_name,
        "role": role,
        "vendor": "",
        "model": "",
        "serial": "",
        "certificate_id": "",
        "certificate_status": certificate_status,
        "expiry_status": expiry_status,
        "intermediate_check_status": intermediate_check_status,
        "current_stage_usage": current_stage_usage,
        "linked_run_artifacts": [],
        "readiness_status": "gap" if certificate_status.startswith("missing") else "stub_only",
        "non_claim": "readiness registry only",
    }


def _artifact_path_map(artifact_paths: dict[str, Any]) -> dict[str, str]:
    new_paths = {
        "scope_definition_pack": SCOPE_DEFINITION_PACK_FILENAME,
        "scope_definition_pack_markdown": SCOPE_DEFINITION_PACK_MARKDOWN_FILENAME,
        "decision_rule_profile": DECISION_RULE_PROFILE_FILENAME,
        "decision_rule_profile_markdown": DECISION_RULE_PROFILE_MARKDOWN_FILENAME,
        "scope_readiness_summary": SCOPE_READINESS_SUMMARY_FILENAME,
        "scope_readiness_summary_markdown": SCOPE_READINESS_SUMMARY_MARKDOWN_FILENAME,
        "reference_asset_registry": REFERENCE_ASSET_REGISTRY_FILENAME,
        "reference_asset_registry_markdown": REFERENCE_ASSET_REGISTRY_MARKDOWN_FILENAME,
        "certificate_readiness_summary": CERTIFICATE_READINESS_SUMMARY_FILENAME,
        "certificate_readiness_summary_markdown": CERTIFICATE_READINESS_SUMMARY_MARKDOWN_FILENAME,
        "metrology_traceability_stub": METROLOGY_TRACEABILITY_STUB_FILENAME,
        "metrology_traceability_stub_markdown": METROLOGY_TRACEABILITY_STUB_MARKDOWN_FILENAME,
        "uncertainty_budget_stub": UNCERTAINTY_BUDGET_STUB_FILENAME,
        "uncertainty_budget_stub_markdown": UNCERTAINTY_BUDGET_STUB_MARKDOWN_FILENAME,
        "method_confirmation_protocol": METHOD_CONFIRMATION_PROTOCOL_FILENAME,
        "method_confirmation_protocol_markdown": METHOD_CONFIRMATION_PROTOCOL_MARKDOWN_FILENAME,
        "method_confirmation_matrix": METHOD_CONFIRMATION_MATRIX_FILENAME,
        "method_confirmation_matrix_markdown": METHOD_CONFIRMATION_MATRIX_MARKDOWN_FILENAME,
        "uncertainty_method_readiness_summary": UNCERTAINTY_METHOD_READINESS_SUMMARY_FILENAME,
        "uncertainty_method_readiness_summary_markdown": UNCERTAINTY_METHOD_READINESS_SUMMARY_MARKDOWN_FILENAME,
        "software_validation_traceability_matrix": SOFTWARE_VALIDATION_TRACEABILITY_MATRIX_FILENAME,
        "software_validation_traceability_matrix_markdown": SOFTWARE_VALIDATION_TRACEABILITY_MATRIX_MARKDOWN_FILENAME,
        "release_validation_manifest": RELEASE_VALIDATION_MANIFEST_FILENAME,
        "release_validation_manifest_markdown": RELEASE_VALIDATION_MANIFEST_MARKDOWN_FILENAME,
        "audit_readiness_digest": AUDIT_READINESS_DIGEST_FILENAME,
        "audit_readiness_digest_markdown": AUDIT_READINESS_DIGEST_MARKDOWN_FILENAME,
    }
    merged = {key: str(artifact_paths.get(key) or value) for key, value in new_paths.items()}
    for key in (
        "multi_source_stability_evidence",
        "state_transition_evidence",
        "simulation_evidence_sidecar_bundle",
        "measurement_phase_coverage_report",
        "acceptance_plan",
        "analytics_summary",
        "lineage_summary",
        "evidence_registry",
    ):
        merged[key] = str(artifact_paths.get(key) or f"{key}.json")
    return merged


def _phase_pairs(payload: dict[str, Any], *, include: set[str]) -> list[str]:
    phase_rows = [dict(item) for item in list(payload.get("phase_rows") or []) if isinstance(item, dict)]
    return [
        f"{str(item.get('route_family') or '').strip()}/{str(item.get('phase_name') or '').strip()}"
        for item in phase_rows
        if str(item.get("coverage_bucket") or "").strip() in include
    ]


def _route_families(
    samples: list[SamplingResult],
    point_summaries: list[dict[str, Any]],
    phase_coverage_payload: dict[str, Any],
) -> list[str]:
    rows = {_route_family_from_value(getattr(getattr(sample, "point", None), "route", "")) for sample in samples}
    rows.update(_route_family_from_value(dict(item.get("point") or {}).get("route", "")) for item in point_summaries)
    rows.update(str(item.get("route_family") or "").strip() for item in list(phase_coverage_payload.get("phase_rows") or []))
    return _dedupe(item for item in rows if str(item).strip())


def _route_family_from_value(value: Any) -> str:
    route = str(value or "").strip().lower()
    if route in {"co2", "gas"}:
        return "gas"
    if route in {"h2o", "water"}:
        return "water"
    if route in {"ambient", "diagnostic"}:
        return "ambient"
    return route


def _collect_numeric_values(
    samples: list[SamplingResult],
    point_summaries: list[dict[str, Any]],
    *keys: str,
) -> list[float]:
    rows: list[float] = []
    for sample in samples:
        for key in keys:
            value = getattr(sample, key, None)
            if value in (None, "") and getattr(sample, "point", None) is not None:
                value = getattr(sample.point, key, None)
            parsed = _coerce_float(value)
            if parsed is not None:
                rows.append(parsed)
    for item in point_summaries:
        point = dict(item.get("point") or {})
        stats = dict(item.get("stats") or {})
        for key in keys:
            parsed = _coerce_float(point.get(key, stats.get(key)))
            if parsed is not None:
                rows.append(parsed)
    return rows


def _coerce_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _range_text(values: list[float]) -> str:
    if not values:
        return "readiness-gap"
    return f"{min(values):g} .. {max(values):g}"


def _dedupe(values: Iterable[Any]) -> list[str]:
    rows: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        rows.append(text)
    return rows


def _render_markdown(title: str, lines: list[str]) -> str:
    body = "\n".join(line for line in lines if str(line).strip())
    return f"# {title}\n\n{body}\n"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")
