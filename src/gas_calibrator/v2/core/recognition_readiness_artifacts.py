from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from .models import SamplingResult
from .phase_taxonomy_contract import (
    GAP_CLASSIFICATION_FAMILY,
    GAP_SEVERITY_FAMILY,
    TAXONOMY_CONTRACT_VERSION,
    normalize_phase_taxonomy_row,
    normalize_taxonomy_keys,
)
from .reviewer_fragments_contract import (
    BLOCKER_FRAGMENT_FAMILY,
    BOUNDARY_FRAGMENT_FAMILY,
    GAP_REASON_FRAGMENT_FAMILY,
    NON_CLAIM_FRAGMENT_FAMILY,
    READINESS_IMPACT_FRAGMENT_FAMILY,
    REVIEWER_FRAGMENTS_CONTRACT_VERSION,
    REVIEWER_NEXT_STEP_FRAGMENT_FAMILY,
    build_fragment_row,
    fragment_filter_rows_to_ids,
    fragment_rows_to_keys,
    fragment_rows_to_texts,
    fragment_summary,
    normalize_fragment_filter_rows,
    normalize_fragment_rows,
)

SCOPE_DEFINITION_PACK_FILENAME = "scope_definition_pack.json"
SCOPE_DEFINITION_PACK_MARKDOWN_FILENAME = "scope_definition_pack.md"
DECISION_RULE_PROFILE_FILENAME = "decision_rule_profile.json"
DECISION_RULE_PROFILE_MARKDOWN_FILENAME = "decision_rule_profile.md"
SCOPE_READINESS_SUMMARY_FILENAME = "scope_readiness_summary.json"
SCOPE_READINESS_SUMMARY_MARKDOWN_FILENAME = "scope_readiness_summary.md"
REFERENCE_ASSET_REGISTRY_FILENAME = "reference_asset_registry.json"
REFERENCE_ASSET_REGISTRY_MARKDOWN_FILENAME = "reference_asset_registry.md"
CERTIFICATE_LIFECYCLE_SUMMARY_FILENAME = "certificate_lifecycle_summary.json"
CERTIFICATE_LIFECYCLE_SUMMARY_MARKDOWN_FILENAME = "certificate_lifecycle_summary.md"
CERTIFICATE_READINESS_SUMMARY_FILENAME = "certificate_readiness_summary.json"
CERTIFICATE_READINESS_SUMMARY_MARKDOWN_FILENAME = "certificate_readiness_summary.md"
PRE_RUN_READINESS_GATE_FILENAME = "pre_run_readiness_gate.json"
PRE_RUN_READINESS_GATE_MARKDOWN_FILENAME = "pre_run_readiness_gate.md"
METROLOGY_TRACEABILITY_STUB_FILENAME = "metrology_traceability_stub.json"
METROLOGY_TRACEABILITY_STUB_MARKDOWN_FILENAME = "metrology_traceability_stub.md"
UNCERTAINTY_BUDGET_STUB_FILENAME = "uncertainty_budget_stub.json"
UNCERTAINTY_BUDGET_STUB_MARKDOWN_FILENAME = "uncertainty_budget_stub.md"
UNCERTAINTY_MODEL_FILENAME = "uncertainty_model.json"
UNCERTAINTY_MODEL_MARKDOWN_FILENAME = "uncertainty_model.md"
UNCERTAINTY_INPUT_SET_FILENAME = "uncertainty_input_set.json"
UNCERTAINTY_INPUT_SET_MARKDOWN_FILENAME = "uncertainty_input_set.md"
SENSITIVITY_COEFFICIENT_SET_FILENAME = "sensitivity_coefficient_set.json"
SENSITIVITY_COEFFICIENT_SET_MARKDOWN_FILENAME = "sensitivity_coefficient_set.md"
BUDGET_CASE_FILENAME = "budget_case.json"
BUDGET_CASE_MARKDOWN_FILENAME = "budget_case.md"
UNCERTAINTY_GOLDEN_CASES_FILENAME = "uncertainty_golden_cases.json"
UNCERTAINTY_GOLDEN_CASES_MARKDOWN_FILENAME = "uncertainty_golden_cases.md"
UNCERTAINTY_REPORT_PACK_FILENAME = "uncertainty_report_pack.json"
UNCERTAINTY_REPORT_PACK_MARKDOWN_FILENAME = "uncertainty_report_pack.md"
UNCERTAINTY_DIGEST_FILENAME = "uncertainty_digest.json"
UNCERTAINTY_DIGEST_MARKDOWN_FILENAME = "uncertainty_digest.md"
UNCERTAINTY_ROLLUP_FILENAME = "uncertainty_rollup.json"
UNCERTAINTY_ROLLUP_MARKDOWN_FILENAME = "uncertainty_rollup.md"
METHOD_CONFIRMATION_PROTOCOL_FILENAME = "method_confirmation_protocol.json"
METHOD_CONFIRMATION_PROTOCOL_MARKDOWN_FILENAME = "method_confirmation_protocol.md"
METHOD_CONFIRMATION_MATRIX_FILENAME = "method_confirmation_matrix.json"
METHOD_CONFIRMATION_MATRIX_MARKDOWN_FILENAME = "method_confirmation_matrix.md"
ROUTE_SPECIFIC_VALIDATION_MATRIX_FILENAME = "route_specific_validation_matrix.json"
ROUTE_SPECIFIC_VALIDATION_MATRIX_MARKDOWN_FILENAME = "route_specific_validation_matrix.md"
VALIDATION_RUN_SET_FILENAME = "validation_run_set.json"
VALIDATION_RUN_SET_MARKDOWN_FILENAME = "validation_run_set.md"
VERIFICATION_DIGEST_FILENAME = "verification_digest.json"
VERIFICATION_DIGEST_MARKDOWN_FILENAME = "verification_digest.md"
VERIFICATION_ROLLUP_FILENAME = "verification_rollup.json"
VERIFICATION_ROLLUP_MARKDOWN_FILENAME = "verification_rollup.md"
UNCERTAINTY_METHOD_READINESS_SUMMARY_FILENAME = "uncertainty_method_readiness_summary.json"
UNCERTAINTY_METHOD_READINESS_SUMMARY_MARKDOWN_FILENAME = "uncertainty_method_readiness_summary.md"
SOFTWARE_VALIDATION_TRACEABILITY_MATRIX_FILENAME = "software_validation_traceability_matrix.json"
SOFTWARE_VALIDATION_TRACEABILITY_MATRIX_MARKDOWN_FILENAME = "software_validation_traceability_matrix.md"
REQUIREMENT_DESIGN_CODE_TEST_LINKS_FILENAME = "requirement_design_code_test_links.json"
REQUIREMENT_DESIGN_CODE_TEST_LINKS_MARKDOWN_FILENAME = "requirement_design_code_test_links.md"
VALIDATION_EVIDENCE_INDEX_FILENAME = "validation_evidence_index.json"
VALIDATION_EVIDENCE_INDEX_MARKDOWN_FILENAME = "validation_evidence_index.md"
CHANGE_IMPACT_SUMMARY_FILENAME = "change_impact_summary.json"
CHANGE_IMPACT_SUMMARY_MARKDOWN_FILENAME = "change_impact_summary.md"
ROLLBACK_READINESS_SUMMARY_FILENAME = "rollback_readiness_summary.json"
ROLLBACK_READINESS_SUMMARY_MARKDOWN_FILENAME = "rollback_readiness_summary.md"
ARTIFACT_HASH_REGISTRY_FILENAME = "artifact_hash_registry.json"
ARTIFACT_HASH_REGISTRY_MARKDOWN_FILENAME = "artifact_hash_registry.md"
AUDIT_EVENT_STORE_FILENAME = "audit_event_store.json"
AUDIT_EVENT_STORE_MARKDOWN_FILENAME = "audit_event_store.md"
ENVIRONMENT_FINGERPRINT_FILENAME = "environment_fingerprint.json"
ENVIRONMENT_FINGERPRINT_MARKDOWN_FILENAME = "environment_fingerprint.md"
CONFIG_FINGERPRINT_FILENAME = "config_fingerprint.json"
CONFIG_FINGERPRINT_MARKDOWN_FILENAME = "config_fingerprint.md"
RELEASE_INPUT_DIGEST_FILENAME = "release_input_digest.json"
RELEASE_INPUT_DIGEST_MARKDOWN_FILENAME = "release_input_digest.md"
RELEASE_MANIFEST_FILENAME = "release_manifest.json"
RELEASE_MANIFEST_MARKDOWN_FILENAME = "release_manifest.md"
RELEASE_SCOPE_SUMMARY_FILENAME = "release_scope_summary.json"
RELEASE_SCOPE_SUMMARY_MARKDOWN_FILENAME = "release_scope_summary.md"
RELEASE_BOUNDARY_DIGEST_FILENAME = "release_boundary_digest.json"
RELEASE_BOUNDARY_DIGEST_MARKDOWN_FILENAME = "release_boundary_digest.md"
RELEASE_EVIDENCE_PACK_INDEX_FILENAME = "release_evidence_pack_index.json"
RELEASE_EVIDENCE_PACK_INDEX_MARKDOWN_FILENAME = "release_evidence_pack_index.md"
RELEASE_VALIDATION_MANIFEST_FILENAME = "release_validation_manifest.json"
RELEASE_VALIDATION_MANIFEST_MARKDOWN_FILENAME = "release_validation_manifest.md"
AUDIT_READINESS_DIGEST_FILENAME = "audit_readiness_digest.json"
AUDIT_READINESS_DIGEST_MARKDOWN_FILENAME = "audit_readiness_digest.md"
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

RECOGNITION_READINESS_SUMMARY_FILENAMES = (
    SCOPE_READINESS_SUMMARY_FILENAME,
    CERTIFICATE_LIFECYCLE_SUMMARY_FILENAME,
    CERTIFICATE_READINESS_SUMMARY_FILENAME,
    PRE_RUN_READINESS_GATE_FILENAME,
    UNCERTAINTY_REPORT_PACK_FILENAME,
    UNCERTAINTY_DIGEST_FILENAME,
    UNCERTAINTY_ROLLUP_FILENAME,
    ROUTE_SPECIFIC_VALIDATION_MATRIX_FILENAME,
    VALIDATION_RUN_SET_FILENAME,
    VERIFICATION_DIGEST_FILENAME,
    VERIFICATION_ROLLUP_FILENAME,
    UNCERTAINTY_METHOD_READINESS_SUMMARY_FILENAME,
    AUDIT_READINESS_DIGEST_FILENAME,
    COMPARISON_DIGEST_FILENAME,
    COMPARISON_ROLLUP_FILENAME,
)
from .software_validation_builder import build_software_validation_wp5_artifacts
from .uncertainty_builder import build_uncertainty_wp3_artifacts
from .wp6_builder import build_wp6_artifacts

RECOGNITION_READINESS_BOUNDARY_STATEMENTS = [
    "Step 2 reviewer readiness only",
    "simulation / offline / headless only",
    "file-artifact-first reviewer evidence",
    "not real acceptance",
    "not compliance claim",
    "not accreditation claim",
    "cannot replace real metrology validation",
]

METHOD_CONFIRMATION_VALIDATION_DIMENSIONS = [
    "linearity",
    "repeatability",
    "reproducibility",
    "drift",
    "temperature_effect",
    "pressure_effect",
    "route_switch_effect",
    "seal_ingress_sensitivity",
    "freshness_check",
    "writeback_verification",
]

_RECOGNITION_ARTIFACT_ANCHORS: dict[str, dict[str, str]] = {
    "scope_definition_pack": {"anchor_id": "scope-definition-pack", "anchor_label": "Scope definition pack"},
    "decision_rule_profile": {"anchor_id": "decision-rule-profile", "anchor_label": "Decision rule profile"},
    "scope_readiness_summary": {"anchor_id": "scope-readiness-summary", "anchor_label": "Scope readiness summary"},
    "reference_asset_registry": {"anchor_id": "reference-asset-registry", "anchor_label": "Reference asset registry"},
    "certificate_lifecycle_summary": {
        "anchor_id": "certificate-lifecycle-summary",
        "anchor_label": "Certificate lifecycle summary",
    },
    "certificate_readiness_summary": {
        "anchor_id": "certificate-readiness-summary",
        "anchor_label": "Certificate readiness summary",
    },
    "pre_run_readiness_gate": {
        "anchor_id": "pre-run-readiness-gate",
        "anchor_label": "Pre-run readiness gate",
    },
    "metrology_traceability_stub": {
        "anchor_id": "metrology-traceability-stub",
        "anchor_label": "Metrology traceability stub",
    },
    "uncertainty_model": {"anchor_id": "uncertainty-model", "anchor_label": "Uncertainty model"},
    "uncertainty_input_set": {
        "anchor_id": "uncertainty-input-set",
        "anchor_label": "Uncertainty input set",
    },
    "sensitivity_coefficient_set": {
        "anchor_id": "sensitivity-coefficient-set",
        "anchor_label": "Sensitivity coefficient set",
    },
    "budget_case": {"anchor_id": "budget-case", "anchor_label": "Budget case"},
    "uncertainty_golden_cases": {
        "anchor_id": "uncertainty-golden-cases",
        "anchor_label": "Uncertainty golden cases",
    },
    "uncertainty_report_pack": {
        "anchor_id": "uncertainty-report-pack",
        "anchor_label": "Uncertainty report pack",
    },
    "uncertainty_digest": {"anchor_id": "uncertainty-digest", "anchor_label": "Uncertainty digest"},
    "uncertainty_rollup": {"anchor_id": "uncertainty-rollup", "anchor_label": "Uncertainty rollup"},
    "uncertainty_budget_stub": {"anchor_id": "uncertainty-budget-stub", "anchor_label": "Uncertainty budget stub"},
    "method_confirmation_protocol": {
        "anchor_id": "method-confirmation-protocol",
        "anchor_label": "Method confirmation protocol",
    },
    "method_confirmation_matrix": {
        "anchor_id": "method-confirmation-matrix",
        "anchor_label": "Method confirmation matrix",
    },
    "route_specific_validation_matrix": {
        "anchor_id": "route-specific-validation-matrix",
        "anchor_label": "Route specific validation matrix",
    },
    "validation_run_set": {
        "anchor_id": "validation-run-set",
        "anchor_label": "Validation run set",
    },
    "verification_digest": {
        "anchor_id": "verification-digest",
        "anchor_label": "Verification digest",
    },
    "verification_rollup": {
        "anchor_id": "verification-rollup",
        "anchor_label": "Verification rollup",
    },
    "uncertainty_method_readiness_summary": {
        "anchor_id": "uncertainty-method-readiness-summary",
        "anchor_label": "Uncertainty / method readiness summary",
    },
    "software_validation_traceability_matrix": {
        "anchor_id": "software-validation-traceability-matrix",
        "anchor_label": "Software validation traceability matrix",
    },
    "requirement_design_code_test_links": {
        "anchor_id": "requirement-design-code-test-links",
        "anchor_label": "Requirement design code test links",
    },
    "validation_evidence_index": {
        "anchor_id": "validation-evidence-index",
        "anchor_label": "Validation evidence index",
    },
    "change_impact_summary": {"anchor_id": "change-impact-summary", "anchor_label": "Change impact summary"},
    "rollback_readiness_summary": {
        "anchor_id": "rollback-readiness-summary",
        "anchor_label": "Rollback readiness summary",
    },
    "artifact_hash_registry": {"anchor_id": "artifact-hash-registry", "anchor_label": "Artifact hash registry"},
    "audit_event_store": {"anchor_id": "audit-event-store", "anchor_label": "Audit event store"},
    "environment_fingerprint": {
        "anchor_id": "environment-fingerprint",
        "anchor_label": "Environment fingerprint",
    },
    "config_fingerprint": {"anchor_id": "config-fingerprint", "anchor_label": "Config fingerprint"},
    "release_input_digest": {"anchor_id": "release-input-digest", "anchor_label": "Release input digest"},
    "release_manifest": {"anchor_id": "release-manifest", "anchor_label": "Release manifest"},
    "release_scope_summary": {"anchor_id": "release-scope-summary", "anchor_label": "Release scope summary"},
    "release_boundary_digest": {
        "anchor_id": "release-boundary-digest",
        "anchor_label": "Release boundary digest",
    },
    "release_evidence_pack_index": {
        "anchor_id": "release-evidence-pack-index",
        "anchor_label": "Release evidence pack index",
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
    "pt_ilc_registry": {"anchor_id": "pt-ilc-registry", "anchor_label": "PT/ILC 注册表"},
    "external_comparison_importer": {
        "anchor_id": "external-comparison-importer",
        "anchor_label": "外部比对导入器",
    },
    "comparison_evidence_pack": {
        "anchor_id": "comparison-evidence-pack",
        "anchor_label": "比对证据包",
    },
    "scope_comparison_view": {
        "anchor_id": "scope-comparison-view",
        "anchor_label": "范围比对视图",
    },
    "comparison_digest": {"anchor_id": "comparison-digest", "anchor_label": "比对摘要"},
    "comparison_rollup": {"anchor_id": "comparison-rollup", "anchor_label": "比对汇总"},
}

_RECOGNITION_NEXT_ARTIFACT_DEFAULTS: dict[str, list[str]] = {
    "scope_definition_pack": ["decision_rule_profile", "scope_readiness_summary"],
    "decision_rule_profile": ["scope_readiness_summary", "method_confirmation_matrix"],
    "scope_readiness_summary": ["reference_asset_registry", "method_confirmation_matrix"],
    "reference_asset_registry": ["certificate_lifecycle_summary", "pre_run_readiness_gate"],
    "certificate_lifecycle_summary": ["certificate_readiness_summary", "pre_run_readiness_gate"],
    "certificate_readiness_summary": ["pre_run_readiness_gate", "metrology_traceability_stub"],
    "pre_run_readiness_gate": ["metrology_traceability_stub", "certificate_readiness_summary"],
    "metrology_traceability_stub": ["certificate_readiness_summary", "uncertainty_method_readiness_summary"],
    "uncertainty_model": ["uncertainty_input_set", "sensitivity_coefficient_set"],
    "uncertainty_input_set": ["sensitivity_coefficient_set", "budget_case"],
    "sensitivity_coefficient_set": ["budget_case", "uncertainty_report_pack"],
    "budget_case": ["uncertainty_golden_cases", "uncertainty_report_pack"],
    "uncertainty_golden_cases": ["uncertainty_report_pack", "uncertainty_digest"],
    "uncertainty_report_pack": ["uncertainty_digest", "uncertainty_rollup"],
    "uncertainty_digest": ["uncertainty_rollup", "uncertainty_method_readiness_summary"],
    "uncertainty_rollup": ["uncertainty_method_readiness_summary", "audit_readiness_digest"],
    "uncertainty_budget_stub": ["uncertainty_report_pack", "uncertainty_rollup"],
    "method_confirmation_protocol": ["route_specific_validation_matrix", "validation_run_set"],
    "method_confirmation_matrix": ["route_specific_validation_matrix", "verification_digest"],
    "route_specific_validation_matrix": ["validation_run_set", "verification_digest"],
    "validation_run_set": ["verification_digest", "verification_rollup"],
    "verification_digest": ["verification_rollup", "uncertainty_method_readiness_summary"],
    "verification_rollup": ["uncertainty_method_readiness_summary", "audit_readiness_digest"],
    "uncertainty_method_readiness_summary": ["certificate_readiness_summary", "audit_readiness_digest"],
    "software_validation_traceability_matrix": ["requirement_design_code_test_links", "release_manifest"],
    "requirement_design_code_test_links": ["validation_evidence_index", "release_manifest"],
    "validation_evidence_index": ["artifact_hash_registry", "release_manifest"],
    "change_impact_summary": ["rollback_readiness_summary", "release_manifest"],
    "rollback_readiness_summary": ["release_manifest", "audit_readiness_digest"],
    "artifact_hash_registry": ["audit_event_store", "release_manifest"],
    "audit_event_store": ["release_manifest", "audit_readiness_digest"],
    "environment_fingerprint": ["config_fingerprint", "release_input_digest"],
    "config_fingerprint": ["release_input_digest", "release_manifest"],
    "release_input_digest": ["artifact_hash_registry", "release_manifest"],
    "release_manifest": ["release_scope_summary", "release_boundary_digest"],
    "release_scope_summary": ["release_evidence_pack_index", "release_boundary_digest"],
    "release_boundary_digest": ["audit_readiness_digest", "release_evidence_pack_index"],
    "release_evidence_pack_index": ["audit_readiness_digest", "release_validation_manifest"],
    "release_validation_manifest": ["audit_readiness_digest"],
    "audit_readiness_digest": ["release_validation_manifest"],
    "pt_ilc_registry": ["external_comparison_importer", "comparison_evidence_pack"],
    "external_comparison_importer": ["comparison_evidence_pack", "scope_comparison_view"],
    "comparison_evidence_pack": ["scope_comparison_view", "comparison_digest"],
    "scope_comparison_view": ["comparison_digest", "comparison_rollup"],
    "comparison_digest": ["comparison_rollup", "audit_readiness_digest"],
    "comparison_rollup": ["audit_readiness_digest"],
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
    "certificate_lifecycle_summary": [
        "certificate lifecycle remains reviewer stub only",
        "lot binding / intermediate check / out-of-tolerance closure is not released for formal claim",
    ],
    "certificate_readiness_summary": [
        "certificate files and intermediate checks remain missing",
        "traceability chain stays reviewer-facing only",
    ],
    "pre_run_readiness_gate": [
        "pre-run gate is advisory only and cannot drive live equipment",
        "formal claim gate remains disabled in Step 2",
    ],
    "metrology_traceability_stub": [
        "certificate-backed release chain is not closed",
        "traceability rows remain stub-only",
    ],
    "uncertainty_model": [
        "uncertainty model remains skeleton-only",
        "scope/readiness mapping is available but formal metrology modeling is deferred",
    ],
    "uncertainty_input_set": [
        "input set rows remain simulated placeholders only",
        "released uncertainty input evidence is not attached",
    ],
    "sensitivity_coefficient_set": [
        "sensitivity coefficients are reviewer placeholders only",
        "writeback/rounding coefficients are not validated against real instruments",
    ],
    "budget_case": [
        "budget cases remain reviewer-only and cannot produce formal uncertainty declarations",
        "golden cases are examples and not accredited method evidence",
    ],
    "uncertainty_golden_cases": [
        "golden cases remain artifact-based reviewer examples only",
        "simulation does not create accredited uncertainty exemplars",
    ],
    "uncertainty_report_pack": [
        "report pack remains readiness mapping only",
        "no formal compliance or conformity gate is closed here",
    ],
    "uncertainty_digest": [
        "uncertainty digest remains reviewer-facing only",
        "top contributors are placeholder rankings and not released declarations",
    ],
    "uncertainty_rollup": [
        "rollup remains sidecar-first reviewer evidence",
        "default chain stays file-backed and non-claim only",
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
    "route_specific_validation_matrix": [
        "route matrix remains placeholder-only and cannot be interpreted as real method confirmation",
        "simulation/replay/parity evidence stays reviewer-facing only",
    ],
    "validation_run_set": [
        "validation run linkage stays file-artifact-first and reviewer-only",
        "golden linkage does not create released primary evidence",
    ],
    "verification_digest": [
        "verification digest summarizes reviewer gaps only and does not close formal method confirmation",
    ],
    "verification_rollup": [
        "verification rollup remains sidecar-first / reviewer-only",
        "formal compliance or accreditation claims stay disabled in Step 2",
    ],
    "uncertainty_method_readiness_summary": [
        "uncertainty / method readiness remains open until missing evidence is closed outside Step 2",
    ],
    "software_validation_traceability_matrix": [
        "software traceability matrix remains reviewer-facing only",
        "no live release qualification claim is produced here",
    ],
    "requirement_design_code_test_links": [
        "link rows remain reviewer-facing and do not create a formal software qualification report",
    ],
    "validation_evidence_index": [
        "evidence index remains file-backed and cannot be interpreted as formal approval evidence",
    ],
    "change_impact_summary": [
        "impact summary remains advisory and reviewer-facing only",
    ],
    "rollback_readiness_summary": [
        "rollback guidance is sidecar-first only and does not modify primary evidence",
    ],
    "artifact_hash_registry": [
        "hash registry remains a reviewer digest only and does not claim formal anti-tamper protection",
    ],
    "audit_event_store": [
        "event store remains reviewer-facing only and is not a formal audit ledger",
    ],
    "environment_fingerprint": [
        "environment fingerprint remains descriptive context only",
    ],
    "config_fingerprint": [
        "config fingerprint remains reviewer-facing only and does not replace released configuration governance",
    ],
    "release_input_digest": [
        "release input digest remains reviewer-facing only and cannot be used as formal approval",
    ],
    "release_manifest": [
        "release manifest is a Step 2 reviewer artifact only",
        "no real release approval or formal claim is produced here",
    ],
    "release_scope_summary": [
        "scope summary remains reviewer-facing only",
    ],
    "release_boundary_digest": [
        "boundary digest explicitly blocks formal claims in Step 2",
    ],
    "release_evidence_pack_index": [
        "evidence pack index is navigational only and not formal approval evidence",
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
    "certificate_lifecycle_summary": [
        "released lifecycle records and lot bindings remain incomplete",
        "internal/external lifecycle closure remains reviewer mapping only",
    ],
    "certificate_readiness_summary": [
        "no released certificate files attached",
        "no intermediate check execution evidence attached",
    ],
    "pre_run_readiness_gate": [
        "advisory gate cannot be used as formal compliance or acceptance evidence",
        "blocking items must still be closed outside Step 2",
    ],
    "metrology_traceability_stub": [
        "traceability chain is not backed by released certificates",
    ],
    "uncertainty_model": [
        "formal uncertainty model approval and released coefficient governance remain outside Step 2",
    ],
    "uncertainty_input_set": [
        "input quantity evidence remains simulated/example-only",
    ],
    "sensitivity_coefficient_set": [
        "sensitivity coefficients are placeholders and not a released solver output",
    ],
    "budget_case": [
        "combined/expanded uncertainty values remain placeholder examples only",
    ],
    "uncertainty_golden_cases": [
        "golden cases are reviewer-only examples and not recognition samples",
    ],
    "uncertainty_report_pack": [
        "report pack cannot be used for formal uncertainty or conformity claims",
    ],
    "uncertainty_digest": [
        "digest remains reviewer-facing only and does not close formal readiness",
    ],
    "uncertainty_rollup": [
        "rollup remains non-primary evidence and cannot replace formal uncertainty governance",
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
    "route_specific_validation_matrix": [
        "route specific dimensions remain placeholder/example rows only",
        "real method confirmation datasets and released closure records are still missing",
    ],
    "validation_run_set": [
        "linked run set remains reviewer-only and does not include released validation batches",
    ],
    "verification_digest": [
        "top gaps remain open and reviewer actions stay advisory only",
    ],
    "verification_rollup": [
        "readiness rollup remains non-claim and cannot replace formal verification closure",
    ],
    "uncertainty_method_readiness_summary": [
        "released uncertainty and method confirmation evidence is still missing",
    ],
    "software_validation_traceability_matrix": [
        "formal software qualification artifacts are not attached here",
    ],
    "requirement_design_code_test_links": [
        "formal requirement/design/code/test approval evidence remains out of scope",
    ],
    "validation_evidence_index": [
        "evidence index cannot be used as formal release approval or acceptance evidence",
    ],
    "change_impact_summary": [
        "impact summary remains reviewer-facing and not a formal change control record",
    ],
    "rollback_readiness_summary": [
        "rollback guidance remains sidecar-only and non-destructive",
    ],
    "artifact_hash_registry": [
        "hash registry remains file-backed only and not a formal anti-tamper system",
    ],
    "audit_event_store": [
        "event store remains reviewer-facing only and does not replace a formal audit ledger",
    ],
    "environment_fingerprint": [
        "environment fingerprint remains reviewer context only",
    ],
    "config_fingerprint": [
        "config fingerprint remains reviewer-facing only and does not replace released configuration governance",
    ],
    "release_input_digest": [
        "release input digest remains reviewer-facing only and not formal approval evidence",
    ],
    "release_manifest": [
        "manifest cannot be used for formal release approval or compliance claims",
    ],
    "release_scope_summary": [
        "scope summary remains reviewer-facing only and not a formal scope approval record",
    ],
    "release_boundary_digest": [
        "boundary digest exists to block formal claims in Step 2",
    ],
    "release_evidence_pack_index": [
        "evidence pack index is navigational only and not a formal approval pack",
    ],
    "release_validation_manifest": [
        "signed artifact-hash closure remains deferred",
    ],
    "audit_readiness_digest": [
        "formal audit closure remains out of scope",
    ],
    "pt_ilc_registry": [
        "PT/ILC registry is readiness-mapping-only and not a formal comparison record",
        "imported data is simulated and not from real PT/ILC participation",
    ],
    "external_comparison_importer": [
        "importer only supports local file sources, no network access",
        "all imported comparison data is marked simulated",
    ],
    "comparison_evidence_pack": [
        "evidence pack is reviewer-facing only and not a formal accreditation pack",
        "linked references are navigational, not approval chains",
    ],
    "scope_comparison_view": [
        "scope comparison view is readiness-mapping-only",
        "does not constitute formal scope equivalence",
    ],
    "comparison_digest": [
        "digest is sidecar-first reviewer evidence",
        "does not close formal comparison evidence",
    ],
    "comparison_rollup": [
        "rollup is reviewer-facing summary only",
        "does not constitute formal PT/ILC compliance claim",
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
    run_dir: str | Path | None = None,
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
    run_dir_path = Path(run_dir) if run_dir is not None else Path(str(run_id))
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
        "gas_or_humidity_range": {
            "co2_ppm_range": _range_text(_collect_numeric_values(sample_rows, point_rows, "co2_ppm")),
            "h2o_mmol_range": _range_text(_collect_numeric_values(sample_rows, point_rows, "h2o_mmol")),
            "humidity_pct_range": _range_text(_collect_numeric_values(sample_rows, point_rows, "humidity_pct")),
            "dew_point_c_range": _range_text(_collect_numeric_values(sample_rows, point_rows, "dew_point_c")),
        },
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
        route_families=route_families,
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
        scope_definition_pack=scope_definition_pack,
        decision_rule_profile=decision_rule_profile,
        sample_digest=sample_digest,
        payload_backed_phases=payload_backed_phases,
        path_map=path_map,
    )
    certificate_lifecycle_summary = _build_certificate_lifecycle_summary(
        run_id=run_id,
        scope_definition_pack=scope_definition_pack,
        decision_rule_profile=decision_rule_profile,
        reference_asset_registry=reference_asset_registry,
        path_map=path_map,
    )
    certificate_readiness_summary = _build_certificate_readiness_summary(
        run_id=run_id,
        reference_asset_registry=reference_asset_registry,
        certificate_lifecycle_summary=certificate_lifecycle_summary,
        path_map=path_map,
    )
    pre_run_readiness_gate = _build_pre_run_readiness_gate(
        run_id=run_id,
        scope_definition_pack=scope_definition_pack,
        decision_rule_profile=decision_rule_profile,
        reference_asset_registry=reference_asset_registry,
        certificate_lifecycle_summary=certificate_lifecycle_summary,
        certificate_readiness_summary=certificate_readiness_summary,
        path_map=path_map,
    )
    metrology_traceability_stub = _build_metrology_traceability_stub(
        run_id=run_id,
        reference_asset_registry=reference_asset_registry,
        certificate_readiness_summary=certificate_readiness_summary,
        path_map=path_map,
    )
    uncertainty_wp3_artifacts = build_uncertainty_wp3_artifacts(
        run_id=run_id,
        scope_definition_pack=scope_definition_pack,
        decision_rule_profile=decision_rule_profile,
        reference_asset_registry=reference_asset_registry,
        certificate_lifecycle_summary=certificate_lifecycle_summary,
        pre_run_readiness_gate=pre_run_readiness_gate,
        path_map=path_map,
        filenames={
            "uncertainty_model": UNCERTAINTY_MODEL_FILENAME,
            "uncertainty_model_markdown": UNCERTAINTY_MODEL_MARKDOWN_FILENAME,
            "uncertainty_input_set": UNCERTAINTY_INPUT_SET_FILENAME,
            "uncertainty_input_set_markdown": UNCERTAINTY_INPUT_SET_MARKDOWN_FILENAME,
            "sensitivity_coefficient_set": SENSITIVITY_COEFFICIENT_SET_FILENAME,
            "sensitivity_coefficient_set_markdown": SENSITIVITY_COEFFICIENT_SET_MARKDOWN_FILENAME,
            "budget_case": BUDGET_CASE_FILENAME,
            "budget_case_markdown": BUDGET_CASE_MARKDOWN_FILENAME,
            "uncertainty_golden_cases": UNCERTAINTY_GOLDEN_CASES_FILENAME,
            "uncertainty_golden_cases_markdown": UNCERTAINTY_GOLDEN_CASES_MARKDOWN_FILENAME,
            "uncertainty_report_pack": UNCERTAINTY_REPORT_PACK_FILENAME,
            "uncertainty_report_pack_markdown": UNCERTAINTY_REPORT_PACK_MARKDOWN_FILENAME,
            "uncertainty_digest": UNCERTAINTY_DIGEST_FILENAME,
            "uncertainty_digest_markdown": UNCERTAINTY_DIGEST_MARKDOWN_FILENAME,
            "uncertainty_rollup": UNCERTAINTY_ROLLUP_FILENAME,
            "uncertainty_rollup_markdown": UNCERTAINTY_ROLLUP_MARKDOWN_FILENAME,
            "uncertainty_budget_stub": UNCERTAINTY_BUDGET_STUB_FILENAME,
            "uncertainty_budget_stub_markdown": UNCERTAINTY_BUDGET_STUB_MARKDOWN_FILENAME,
        },
        boundary_statements=list(RECOGNITION_READINESS_BOUNDARY_STATEMENTS),
    )
    uncertainty_budget_stub = dict(uncertainty_wp3_artifacts.get("uncertainty_budget_stub") or {})
    method_confirmation_wp4_artifacts = build_method_confirmation_wp4_artifacts(
        run_id=run_id,
        scope_definition_pack=scope_definition_pack,
        decision_rule_profile=decision_rule_profile,
        reference_asset_registry=reference_asset_registry,
        certificate_lifecycle_summary=certificate_lifecycle_summary,
        pre_run_readiness_gate=pre_run_readiness_gate,
        budget_case=dict(uncertainty_wp3_artifacts.get("budget_case") or {}),
        uncertainty_golden_cases=dict(uncertainty_wp3_artifacts.get("uncertainty_golden_cases") or {}),
        uncertainty_report_pack=dict(uncertainty_wp3_artifacts.get("uncertainty_report_pack") or {}),
        uncertainty_digest=dict(uncertainty_wp3_artifacts.get("uncertainty_digest") or {}),
        uncertainty_rollup=dict(uncertainty_wp3_artifacts.get("uncertainty_rollup") or {}),
        uncertainty_budget_stub=uncertainty_budget_stub,
        route_families=route_families,
        payload_backed_phases=payload_backed_phases,
        trace_only_phases=trace_only_phases,
        gap_phases=gap_phases,
        path_map=path_map,
    )
    method_confirmation_protocol = dict(method_confirmation_wp4_artifacts.get("method_confirmation_protocol") or {})
    method_confirmation_matrix = dict(method_confirmation_wp4_artifacts.get("method_confirmation_matrix") or {})
    route_specific_validation_matrix = dict(
        method_confirmation_wp4_artifacts.get("route_specific_validation_matrix") or {}
    )
    validation_run_set = dict(method_confirmation_wp4_artifacts.get("validation_run_set") or {})
    verification_digest = dict(method_confirmation_wp4_artifacts.get("verification_digest") or {})
    verification_rollup = dict(method_confirmation_wp4_artifacts.get("verification_rollup") or {})
    uncertainty_method_readiness_summary = _build_uncertainty_method_readiness_summary(
        run_id=run_id,
        uncertainty_budget_stub=uncertainty_budget_stub,
        method_confirmation_protocol=method_confirmation_protocol,
        method_confirmation_matrix=method_confirmation_matrix,
        route_specific_validation_matrix=route_specific_validation_matrix,
        verification_digest=verification_digest,
        path_map=path_map,
    )
    software_validation_wp5_artifacts = build_software_validation_wp5_artifacts(
        run_id=run_id,
        run_dir=run_dir_path,
        path_map=path_map,
        filenames={
            "software_validation_traceability_matrix": SOFTWARE_VALIDATION_TRACEABILITY_MATRIX_FILENAME,
            "software_validation_traceability_matrix_markdown": SOFTWARE_VALIDATION_TRACEABILITY_MATRIX_MARKDOWN_FILENAME,
            "requirement_design_code_test_links": REQUIREMENT_DESIGN_CODE_TEST_LINKS_FILENAME,
            "requirement_design_code_test_links_markdown": REQUIREMENT_DESIGN_CODE_TEST_LINKS_MARKDOWN_FILENAME,
            "validation_evidence_index": VALIDATION_EVIDENCE_INDEX_FILENAME,
            "validation_evidence_index_markdown": VALIDATION_EVIDENCE_INDEX_MARKDOWN_FILENAME,
            "change_impact_summary": CHANGE_IMPACT_SUMMARY_FILENAME,
            "change_impact_summary_markdown": CHANGE_IMPACT_SUMMARY_MARKDOWN_FILENAME,
            "rollback_readiness_summary": ROLLBACK_READINESS_SUMMARY_FILENAME,
            "rollback_readiness_summary_markdown": ROLLBACK_READINESS_SUMMARY_MARKDOWN_FILENAME,
            "artifact_hash_registry": ARTIFACT_HASH_REGISTRY_FILENAME,
            "artifact_hash_registry_markdown": ARTIFACT_HASH_REGISTRY_MARKDOWN_FILENAME,
            "audit_event_store": AUDIT_EVENT_STORE_FILENAME,
            "audit_event_store_markdown": AUDIT_EVENT_STORE_MARKDOWN_FILENAME,
            "environment_fingerprint": ENVIRONMENT_FINGERPRINT_FILENAME,
            "environment_fingerprint_markdown": ENVIRONMENT_FINGERPRINT_MARKDOWN_FILENAME,
            "config_fingerprint": CONFIG_FINGERPRINT_FILENAME,
            "config_fingerprint_markdown": CONFIG_FINGERPRINT_MARKDOWN_FILENAME,
            "release_input_digest": RELEASE_INPUT_DIGEST_FILENAME,
            "release_input_digest_markdown": RELEASE_INPUT_DIGEST_MARKDOWN_FILENAME,
            "release_manifest": RELEASE_MANIFEST_FILENAME,
            "release_manifest_markdown": RELEASE_MANIFEST_MARKDOWN_FILENAME,
            "release_scope_summary": RELEASE_SCOPE_SUMMARY_FILENAME,
            "release_scope_summary_markdown": RELEASE_SCOPE_SUMMARY_MARKDOWN_FILENAME,
            "release_boundary_digest": RELEASE_BOUNDARY_DIGEST_FILENAME,
            "release_boundary_digest_markdown": RELEASE_BOUNDARY_DIGEST_MARKDOWN_FILENAME,
            "release_evidence_pack_index": RELEASE_EVIDENCE_PACK_INDEX_FILENAME,
            "release_evidence_pack_index_markdown": RELEASE_EVIDENCE_PACK_INDEX_MARKDOWN_FILENAME,
            "release_validation_manifest": RELEASE_VALIDATION_MANIFEST_FILENAME,
            "release_validation_manifest_markdown": RELEASE_VALIDATION_MANIFEST_MARKDOWN_FILENAME,
            "audit_readiness_digest": AUDIT_READINESS_DIGEST_FILENAME,
            "audit_readiness_digest_markdown": AUDIT_READINESS_DIGEST_MARKDOWN_FILENAME,
        },
        boundary_statements=list(RECOGNITION_READINESS_BOUNDARY_STATEMENTS),
        scope_definition_pack=scope_definition_pack,
        decision_rule_profile=decision_rule_profile,
        reference_asset_registry=reference_asset_registry,
        certificate_lifecycle_summary=certificate_lifecycle_summary,
        pre_run_readiness_gate=pre_run_readiness_gate,
        uncertainty_report_pack=dict(uncertainty_wp3_artifacts.get("uncertainty_report_pack") or {}),
        uncertainty_rollup=dict(uncertainty_wp3_artifacts.get("uncertainty_rollup") or {}),
        method_confirmation_protocol=method_confirmation_protocol,
        route_specific_validation_matrix=route_specific_validation_matrix,
        validation_run_set=validation_run_set,
        verification_digest=verification_digest,
        verification_rollup=verification_rollup,
        version_payload=version_payload,
        lineage_payload=lineage_payload,
        analytics_payload=analytics_payload,
    )

    wp6_artifacts = build_wp6_artifacts(
        run_id=run_id,
        scope_definition_pack=scope_definition_pack,
        decision_rule_profile=decision_rule_profile,
        reference_asset_registry=reference_asset_registry,
        certificate_lifecycle_summary=certificate_lifecycle_summary,
        pre_run_readiness_gate=pre_run_readiness_gate,
        uncertainty_report_pack=dict(uncertainty_wp3_artifacts.get("uncertainty_report_pack") or {}),
        uncertainty_rollup=dict(uncertainty_wp3_artifacts.get("uncertainty_rollup") or {}),
        method_confirmation_protocol=method_confirmation_protocol,
        verification_digest=verification_digest,
        software_validation_rollup=dict(software_validation_wp5_artifacts.get("software_validation_rollup") or {}),
        path_map=path_map,
        filenames={
            "pt_ilc_registry": PT_ILC_REGISTRY_FILENAME,
            "pt_ilc_registry_markdown": PT_ILC_REGISTRY_MARKDOWN_FILENAME,
            "external_comparison_importer": EXTERNAL_COMPARISON_IMPORTER_FILENAME,
            "external_comparison_importer_markdown": EXTERNAL_COMPARISON_IMPORTER_MARKDOWN_FILENAME,
            "comparison_evidence_pack": COMPARISON_EVIDENCE_PACK_FILENAME,
            "comparison_evidence_pack_markdown": COMPARISON_EVIDENCE_PACK_MARKDOWN_FILENAME,
            "scope_comparison_view": SCOPE_COMPARISON_VIEW_FILENAME,
            "scope_comparison_view_markdown": SCOPE_COMPARISON_VIEW_MARKDOWN_FILENAME,
            "comparison_digest": COMPARISON_DIGEST_FILENAME,
            "comparison_digest_markdown": COMPARISON_DIGEST_MARKDOWN_FILENAME,
            "comparison_rollup": COMPARISON_ROLLUP_FILENAME,
            "comparison_rollup_markdown": COMPARISON_ROLLUP_MARKDOWN_FILENAME,
        },
        boundary_statements=list(RECOGNITION_READINESS_BOUNDARY_STATEMENTS),
    )

    artifacts = {
        "scope_definition_pack": scope_definition_pack,
        "decision_rule_profile": decision_rule_profile,
        "scope_readiness_summary": scope_readiness_summary,
        "reference_asset_registry": reference_asset_registry,
        "certificate_lifecycle_summary": certificate_lifecycle_summary,
        "certificate_readiness_summary": certificate_readiness_summary,
        "pre_run_readiness_gate": pre_run_readiness_gate,
        "metrology_traceability_stub": metrology_traceability_stub,
        "uncertainty_model": dict(uncertainty_wp3_artifacts.get("uncertainty_model") or {}),
        "uncertainty_input_set": dict(uncertainty_wp3_artifacts.get("uncertainty_input_set") or {}),
        "sensitivity_coefficient_set": dict(uncertainty_wp3_artifacts.get("sensitivity_coefficient_set") or {}),
        "budget_case": dict(uncertainty_wp3_artifacts.get("budget_case") or {}),
        "uncertainty_golden_cases": dict(uncertainty_wp3_artifacts.get("uncertainty_golden_cases") or {}),
        "uncertainty_report_pack": dict(uncertainty_wp3_artifacts.get("uncertainty_report_pack") or {}),
        "uncertainty_digest": dict(uncertainty_wp3_artifacts.get("uncertainty_digest") or {}),
        "uncertainty_rollup": dict(uncertainty_wp3_artifacts.get("uncertainty_rollup") or {}),
        "uncertainty_budget_stub": uncertainty_budget_stub,
        "method_confirmation_protocol": method_confirmation_protocol,
        "method_confirmation_matrix": method_confirmation_matrix,
        "route_specific_validation_matrix": route_specific_validation_matrix,
        "validation_run_set": validation_run_set,
        "verification_digest": verification_digest,
        "verification_rollup": verification_rollup,
        "uncertainty_method_readiness_summary": uncertainty_method_readiness_summary,
        "software_validation_traceability_matrix": dict(software_validation_wp5_artifacts.get("software_validation_traceability_matrix") or {}),
        "requirement_design_code_test_links": dict(software_validation_wp5_artifacts.get("requirement_design_code_test_links") or {}),
        "validation_evidence_index": dict(software_validation_wp5_artifacts.get("validation_evidence_index") or {}),
        "change_impact_summary": dict(software_validation_wp5_artifacts.get("change_impact_summary") or {}),
        "rollback_readiness_summary": dict(software_validation_wp5_artifacts.get("rollback_readiness_summary") or {}),
        "artifact_hash_registry": dict(software_validation_wp5_artifacts.get("artifact_hash_registry") or {}),
        "audit_event_store": dict(software_validation_wp5_artifacts.get("audit_event_store") or {}),
        "environment_fingerprint": dict(software_validation_wp5_artifacts.get("environment_fingerprint") or {}),
        "config_fingerprint": dict(software_validation_wp5_artifacts.get("config_fingerprint") or {}),
        "release_input_digest": dict(software_validation_wp5_artifacts.get("release_input_digest") or {}),
        "release_manifest": dict(software_validation_wp5_artifacts.get("release_manifest") or {}),
        "release_scope_summary": dict(software_validation_wp5_artifacts.get("release_scope_summary") or {}),
        "release_boundary_digest": dict(software_validation_wp5_artifacts.get("release_boundary_digest") or {}),
        "release_evidence_pack_index": dict(software_validation_wp5_artifacts.get("release_evidence_pack_index") or {}),
        "release_validation_manifest": dict(software_validation_wp5_artifacts.get("release_validation_manifest") or {}),
        "audit_readiness_digest": dict(software_validation_wp5_artifacts.get("audit_readiness_digest") or {}),
        "pt_ilc_registry": dict(wp6_artifacts.get("pt_ilc_registry") or {}),
        "external_comparison_importer": dict(wp6_artifacts.get("external_comparison_importer") or {}),
        "comparison_evidence_pack": dict(wp6_artifacts.get("comparison_evidence_pack") or {}),
        "scope_comparison_view": dict(wp6_artifacts.get("scope_comparison_view") or {}),
        "comparison_digest": dict(wp6_artifacts.get("comparison_digest") or {}),
        "comparison_rollup": dict(wp6_artifacts.get("comparison_rollup") or {}),
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
    readiness_mapping_status = "ready_for_readiness_mapping"
    measurands = _dedupe(
        [
            "CO2" if "gas" in route_families else "",
            "H2O" if "water" in route_families else "",
            "Ambient diagnostic" if "ambient" in route_families else "",
        ]
    ) or ["CO2", "H2O"]
    standard_families = _dedupe(
        [
            "ISO/IEC 17025",
            "ISO gas standards" if "gas" in route_families else "",
            "WMO / GAW" if {"gas", "ambient"} & set(route_families) else "",
            "CNAS-CL01 / CNAS laboratory accreditation guidance",
        ]
    )
    required_evidence_categories = _dedupe(
        [
            "measurement_phase_payload",
            "measurement_phase_trace",
            "scope_package",
            "decision_rule_profile",
            "artifact_compatibility_sidecar",
            "reviewer_fragment_digest",
            "reference_chain_stub",
        ]
    )
    current_evidence_coverage = [
        f"payload-backed phases: {' | '.join(payload_backed_phases) or '--'}",
        f"trace-only phases: {' | '.join(trace_only_phases) or '--'}",
        f"gap phases: {' | '.join(gap_phases) or '--'}",
        f"compatibility sidecar path: {path_map['simulation_evidence_sidecar_bundle']}",
        f"measurement-phase coverage path: {path_map['measurement_phase_coverage_report']}",
    ]
    gap_note = (
        f"trace-only phases remain {' | '.join(trace_only_phases)}; "
        f"gap phases remain {' | '.join(gap_phases) or '--'}; "
        "formal scope approval and released reference chain stay outside Step 2."
    )
    limitation_note = (
        "Current scope pack is limited to simulation/offline/replay/parity/resilience reviewer mapping. "
        "It does not close released certificates, uncertainty, or formal metrology approval."
    )
    non_claim_note = (
        "Reviewer-only scope package; simulated/offline/shadow outputs cannot become formal compliance, "
        "accreditation, or final pass-fail metrology claims."
    )
    decision_rule_reference = {
        "decision_rule_id": "step2_readiness_reviewer_rule_v1",
        "artifact_path": path_map["decision_rule_profile"],
        "reviewer_gate": "reviewer_digest_only",
        "not_real_acceptance_evidence": True,
    }
    certificate_set = [
        {
            "asset_type": "standard_gas" if "gas" in route_families else "humidity_generator",
            "certificate_status": "reviewer_stub_only",
            "released_for_formal_claim": False,
        },
        {
            "asset_type": "pressure_reference",
            "certificate_status": "reviewer_stub_only",
            "released_for_formal_claim": False,
        },
        {
            "asset_type": "temperature_reference",
            "certificate_status": "reviewer_stub_only",
            "released_for_formal_claim": False,
        },
    ]
    artifact_paths = {
        "scope_definition_pack": path_map["scope_definition_pack"],
        "scope_definition_pack_markdown": path_map["scope_definition_pack_markdown"],
        "decision_rule_profile": path_map["decision_rule_profile"],
        "decision_rule_profile_markdown": path_map["decision_rule_profile_markdown"],
        "scope_readiness_summary": path_map["scope_readiness_summary"],
        "measurement_phase_coverage_report": path_map["measurement_phase_coverage_report"],
        "multi_source_stability_evidence": path_map["multi_source_stability_evidence"],
        "state_transition_evidence": path_map["state_transition_evidence"],
        "simulation_evidence_sidecar_bundle": path_map["simulation_evidence_sidecar_bundle"],
    }
    review_surface = _build_recognition_review_surface(
        title_text="Scope Definition Pack",
        reviewer_note=(
            "Step 2 scope package only supports reviewer-facing readiness mapping. "
            "It stays file-artifact-first, sidecar-first, and explicitly non-claim."
        ),
        summary_text="scope package / reviewer mapping / not ready for formal claim",
        summary_lines=[
            f"scope package: {readiness_mapping_status}",
            f"scope overview: {' / '.join(measurands)} | {' | '.join(route_families) or '--'} | simulation_offline_headless",
            f"decision rule: {decision_rule_reference['decision_rule_id']} | reviewer gate only",
            f"non-claim: {non_claim_note}",
        ],
        detail_lines=[
            f"standard family: {' | '.join(standard_families)}",
            f"current coverage: {' | '.join(current_evidence_coverage)}",
            f"required evidence categories: {' | '.join(required_evidence_categories)}",
            f"limitation: {limitation_note}",
            f"gap note: {gap_note}",
        ],
        anchor_id="scope-definition-pack",
        anchor_label="Scope definition pack",
        artifact_paths=artifact_paths,
        standard_family_filters=standard_families,
    )
    digest = {
        "summary": "scope package / readiness mapping ready / reviewer-only / formal claim not ready",
        "current_coverage_summary": " | ".join(current_evidence_coverage),
        "missing_evidence_summary": gap_note,
        "blocker_summary": "formal scope approval chain open | certificate-backed reference chain open",
        "reviewer_next_step_digest": "keep reviewer mapping explicit; regenerate reviewer/index sidecars only if compatibility suggests",
        "scope_overview_summary": f"{' / '.join(measurands)} | {' | '.join(route_families) or '--'} | simulation_offline_headless",
        "decision_rule_summary": f"{decision_rule_reference['decision_rule_id']} | reviewer gate only",
        "conformity_boundary_summary": non_claim_note,
        "standard_family_summary": " | ".join(standard_families),
        "required_evidence_categories_summary": " | ".join(required_evidence_categories),
    }
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
        "scope_id": f"{run_id}-step2-scope-package",
        "scope_name": "Step 2 simulation reviewer scope package",
        "scope_version": str(version_payload.get("profile_version") or version_payload.get("config_version") or "scope-v1"),
        "measurand": measurands,
        "route_type": list(route_families),
        "environment_mode": "simulation_offline_headless",
        "analyzer_model": sample_digest["analyzers"] or ["simulation_analyzer_population"],
        "measurand_family": "dual",
        "route_applicability": route_families,
        "temperature_range": sample_digest["temperature_range"],
        "pressure_range": sample_digest["pressure_range"],
        "gas_or_humidity_range": dict(sample_digest.get("gas_or_humidity_range") or {}),
        "humidity_mode": "water + ambient simulation coverage",
        "analyzer_population_scope": sample_digest["analyzers"] or ["simulation_analyzer_population"],
        "reference_chain": [
            {
                "reference_name": "standard_gas_or_humidity_source",
                "status": "reviewer_stub_only",
                "released_for_formal_claim": False,
            },
            {
                "reference_name": "pressure_reference",
                "status": "reviewer_stub_only",
                "released_for_formal_claim": False,
            },
            {
                "reference_name": "temperature_reference",
                "status": "reviewer_stub_only",
                "released_for_formal_claim": False,
            },
        ],
        "standard_gas_lot_policy": (
            "Sidecar-tracked placeholder only; lot-specific release policy remains outside Step 2 and cannot be used as a formal claim."
        ),
        "certificate_set": certificate_set,
        "method_version": str(version_payload.get("profile_version") or version_payload.get("config_version") or "--"),
        "algorithm_version": str(version_payload.get("algorithm_version") or "--"),
        "uncertainty_profile": {
            "profile_id": "uncertainty_stub_profile_v1",
            "status": "reviewer_stub_only",
            "linked_artifact": path_map["uncertainty_method_readiness_summary"],
            "not_real_acceptance_evidence": True,
        },
        "decision_rule_profile": dict(decision_rule_reference),
        "applicable_profiles": profiles,
        "linked_evidence_categories": required_evidence_categories,
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
        "artifact_paths": artifact_paths,
        "standard_family": standard_families,
        "topic_or_control_object": "scope boundary, route applicability, reference chain placeholder, and reviewer-facing readiness mapping",
        "linked_existing_artifacts": [
            "measurement_phase_coverage_report",
            "multi_source_stability_evidence",
            "state_transition_evidence",
            "simulation_evidence_sidecar_bundle",
            "scope_readiness_summary",
            "decision_rule_profile",
        ],
        "required_evidence_categories": required_evidence_categories,
        "current_evidence_coverage": current_evidence_coverage,
        "ready_for_readiness_mapping": True,
        "not_ready_for_formal_claim": True,
        "gap_note": gap_note,
        "limitation_note": limitation_note,
        "non_claim_note": non_claim_note,
        "scope_package": {
            "scope_id": f"{run_id}-step2-scope-package",
            "scope_name": "Step 2 simulation reviewer scope package",
            "scope_version": str(version_payload.get("profile_version") or version_payload.get("config_version") or "scope-v1"),
            "ready_for_readiness_mapping": True,
            "not_ready_for_formal_claim": True,
            "gap_note": gap_note,
            "limitation_note": limitation_note,
            "non_claim_note": non_claim_note,
        },
        "scope_export_pack": {
            "scope_id": f"{run_id}-step2-scope-package",
            "scope_name": "Step 2 simulation reviewer scope package",
            "scope_version": str(version_payload.get("profile_version") or version_payload.get("config_version") or "scope-v1"),
            "environment_mode": "simulation_offline_headless",
            "ready_for_readiness_mapping": True,
            "not_ready_for_formal_claim": True,
            "gap_note": gap_note,
            "limitation_note": limitation_note,
            "non_claim_note": non_claim_note,
        },
        "scope_overview": {
            "summary": f"{' / '.join(measurands)} | {' | '.join(route_families) or '--'} | simulation_offline_headless",
            "readiness_status": readiness_mapping_status,
            "decision_rule_id": decision_rule_reference["decision_rule_id"],
        },
        "readiness_status": readiness_mapping_status,
        "non_claim": [
            "scope package stub only",
            "not a formal scope statement",
            "not accreditation claim",
            "not real acceptance",
        ],
        "review_surface": review_surface,
        "digest": digest,
    }
    markdown = _render_markdown(
        "Scope Definition Pack",
        [
            f"- scope_id: {raw['scope_id']}",
            f"- scope_version: {raw['scope_version']}",
            f"- measurand: {' | '.join(raw['measurand'])}",
            f"- environment_mode: {raw['environment_mode']}",
            f"- measurand_family: {raw['measurand_family']}",
            f"- route_applicability: {' | '.join(route_families) or '--'}",
            f"- temperature_range: {raw['temperature_range']}",
            f"- pressure_range: {raw['pressure_range']}",
            f"- gas_or_humidity_range: {raw['gas_or_humidity_range']}",
            f"- humidity_mode: {raw['humidity_mode']}",
            f"- analyzer_population_scope: {' | '.join(raw['analyzer_population_scope'])}",
            f"- applicable_profiles: {' | '.join(raw['applicable_profiles']) or '--'}",
            f"- method_version: {raw['method_version']}",
            f"- algorithm_version: {raw['algorithm_version']}",
            f"- decision_rule_profile: {raw['decision_rule_profile']['decision_rule_id']}",
            f"- standard_family: {' | '.join(raw['standard_family'])}",
            f"- required_evidence_categories: {' | '.join(raw['required_evidence_categories'])}",
            f"- readiness_status: {raw['readiness_status']}",
            f"- limitation_note: {raw['limitation_note']}",
            f"- non_claim_note: {raw['non_claim_note']}",
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
        "digest": digest,
    }


def _build_decision_rule_profile(
    *,
    run_id: str,
    route_families: list[str],
    version_payload: dict[str, Any],
    acceptance_payload: dict[str, Any],
    analytics_payload: dict[str, Any],
    phase_digest: dict[str, Any],
    path_map: dict[str, str],
) -> dict[str, Any]:
    decision_rule_id = "step2_readiness_reviewer_rule_v1"
    standard_families = _dedupe(
        [
            "ISO/IEC 17025",
            "CNAS-CL01 / CNAS laboratory accreditation guidance",
            "ISO gas standards" if "gas" in route_families else "",
            "WMO / GAW" if {"gas", "ambient"} & set(route_families) else "",
        ]
    )
    required_evidence_categories = _dedupe(
        [
            "decision_rule_profile",
            "reviewer_digest",
            "uncertainty_stub",
            "traceability_stub",
            "artifact_compatibility_sidecar",
        ]
    )
    source_standard_or_method = _dedupe(
        [
            "Step 2 reviewer method skeleton",
            str(version_payload.get("profile_version") or "").strip(),
            str(dict(acceptance_payload.get("readiness_summary") or {}).get("evidence_mode") or "").strip(),
            "measurement phase coverage reviewer digest",
        ]
    )
    acceptance_limit = {
        "limit_id": "reviewer_readiness_placeholder_limit",
        "mode": "readiness_mapping_only",
        "value": None,
        "unit": "",
        "note": "Quantitative metrology acceptance limits remain deferred outside Step 2.",
    }
    reviewer_gate = {
        "mode": "reviewer_digest_only",
        "allow_outputs": ["readiness_mapping", "reviewer_digest"],
        "deny_outputs": [
            "formal_compliance_claim",
            "accreditation_claim",
            "final_pass_fail_metrology_conclusion",
        ],
        "real_acceptance_ready": False,
    }
    conformity_statement_profile = {
        "profile_id": "step2_reviewer_conformity_boundary_v1",
        "statement_template": (
            "Step 2 simulation/offline reviewer digest only supports readiness mapping and reviewer digest; "
            "it cannot be used for formal compliance claims, accreditation claims, or final metrology pass/fail conclusions."
        ),
        "allowed_statement_scope": ["readiness_mapping", "reviewer_digest"],
        "prohibited_statement_scope": list(reviewer_gate["deny_outputs"]),
        "reviewer_gate": dict(reviewer_gate),
    }
    acceptance_contract = {
        "contract_id": "step2_reviewer_acceptance_contract_v1",
        "repository_mode": "file_artifact_first",
        "gateway_mode": "file_backed_default",
        "primary_evidence_rewritten": False,
        "sidecar_only_chain": True,
        "non_primary_evidence_chain": True,
        "reviewer_gate": dict(reviewer_gate),
        "real_acceptance_dependency": "reserved_for_future_stage",
    }
    limitation_note = (
        "Decision-rule and conformity wording remains reviewer-facing only until real standards, released certificates, "
        "and uncertainty evidence are closed outside Step 2."
    )
    non_claim_note = (
        "Simulation/offline/shadow outputs remain reviewer-only. They cannot be promoted into formal compliance, "
        "accreditation, or final metrology pass-fail claims."
    )
    current_evidence_coverage = [
        f"payload phases: {str(phase_digest.get('payload_phase_summary') or '--')}",
        f"trace-only phases: {str(phase_digest.get('trace_only_phase_summary') or '--')}",
        f"compatibility sidecar path: {path_map['simulation_evidence_sidecar_bundle']}",
        f"scope pack path: {path_map['scope_definition_pack']}",
    ]
    gap_note = (
        "Formal acceptance limits, guard band release policy, and live decision gates remain out of scope. "
        "Only reviewer digest and readiness mapping outputs are permitted."
    )
    artifact_paths = {
        "decision_rule_profile": path_map["decision_rule_profile"],
        "decision_rule_profile_markdown": path_map["decision_rule_profile_markdown"],
        "scope_definition_pack": path_map["scope_definition_pack"],
        "scope_readiness_summary": path_map["scope_readiness_summary"],
        "measurement_phase_coverage_report": path_map["measurement_phase_coverage_report"],
        "simulation_evidence_sidecar_bundle": path_map["simulation_evidence_sidecar_bundle"],
    }
    review_surface = _build_recognition_review_surface(
        title_text="Decision Rule Profile",
        reviewer_note=(
            "Decision-rule and conformity profile stay in reviewer-only mode. "
            "They define scope/rule/statement placeholders without creating any live gate."
        ),
        summary_text="decision rule / conformity boundary / reviewer-only",
        summary_lines=[
            f"decision rule: {decision_rule_id}",
            "reviewer gate: reviewer digest only",
            "formal claim boundary: disabled in Step 2",
            f"non-claim: {non_claim_note}",
        ],
        detail_lines=[
            f"source standard or method: {' | '.join(source_standard_or_method)}",
            f"current coverage: {' | '.join(current_evidence_coverage)}",
            f"required evidence categories: {' | '.join(required_evidence_categories)}",
            f"limitation: {limitation_note}",
            f"gap note: {gap_note}",
        ],
        anchor_id="decision-rule-profile",
        anchor_label="Decision rule profile",
        artifact_paths=artifact_paths,
        standard_family_filters=standard_families,
    )
    digest = {
        "summary": "decision rule profile / conformity boundary / reviewer-only / live gate disabled",
        "current_coverage_summary": " | ".join(current_evidence_coverage),
        "missing_evidence_summary": gap_note,
        "blocker_summary": "live decision gate disabled | formal acceptance limits deferred | accreditation semantics out of scope",
        "reviewer_next_step_digest": "keep conformity wording reviewer-only and tie only to file-backed sidecars/indexes",
        "decision_rule_summary": f"{decision_rule_id} | reviewer gate only",
        "conformity_boundary_summary": non_claim_note,
        "standard_family_summary": " | ".join(standard_families),
        "required_evidence_categories_summary": " | ".join(required_evidence_categories),
        "tolerance_source_summary": "current analytics/qc thresholds + reviewer digests",
    }
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
        "decision_rule_id": decision_rule_id,
        "source_standard_or_method": source_standard_or_method,
        "acceptance_limit": acceptance_limit,
        "guard_band_policy": {
            "policy_id": "reviewer_guard_band_placeholder",
            "status": "placeholder_only",
            "note": "Guard band policy remains reserved for a future real metrology contract.",
        },
        "uncertainty_source_scope": {
            "linked_artifact": path_map["uncertainty_method_readiness_summary"],
            "scope": "reviewer_stub_only",
            "formal_release_ready": False,
        },
        "pass_fail_inconclusive_rule": {
            "pass": "ready_for_readiness_mapping",
            "fail": "reviewer_gap_blocks_mapping",
            "inconclusive": "reviewer_follow_up_required",
            "formal_pass_fail_metrology_conclusion_enabled": False,
        },
        "statement_template": conformity_statement_profile["statement_template"],
        "applicability_scope": {
            "route_type": list(route_families),
            "environment_mode": "simulation_offline_headless",
            "compatibility_reader_neutral": True,
        },
        "reviewer_gate": reviewer_gate,
        "exception_clause": (
            "Compatibility adapter, missing released certificates, or trace-only phases all keep the output in reviewer-only mode."
        ),
        "conformity_statement_profile": conformity_statement_profile,
        "acceptance_contract": acceptance_contract,
        "rule_profile_id": decision_rule_id,
        "version": "v1",
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
        "standard_family": standard_families,
        "topic_or_control_object": "decision rule wording, conformity boundary, reviewer gate, and sidecar-first acceptance contract skeleton",
        "linked_existing_artifacts": [
            "scope_definition_pack",
            "scope_readiness_summary",
            "measurement_phase_coverage_report",
            "uncertainty_method_readiness_summary",
            "simulation_evidence_sidecar_bundle",
        ],
        "required_evidence_categories": required_evidence_categories,
        "current_evidence_coverage": current_evidence_coverage,
        "ready_for_readiness_mapping": True,
        "not_ready_for_formal_claim": True,
        "gap_note": gap_note,
        "limitation_note": limitation_note,
        "non_claim_note": non_claim_note,
        "linked_artifacts": {
            "scope_definition_pack": path_map["scope_definition_pack"],
            "scope_readiness_summary": path_map["scope_readiness_summary"],
            "measurement_phase_coverage_report": path_map["measurement_phase_coverage_report"],
            "uncertainty_method_readiness_summary": path_map["uncertainty_method_readiness_summary"],
            "simulation_evidence_sidecar_bundle": path_map["simulation_evidence_sidecar_bundle"],
        },
        "artifact_paths": artifact_paths,
        "decision_rule_overview": {
            "summary": f"{decision_rule_id} | reviewer gate only | simulation_offline_headless",
            "reviewer_gate": "reviewer_digest_only",
            "formal_claim_ready": False,
        },
        "conformity_boundary": {
            "summary": non_claim_note,
            "reviewer_only": True,
            "formal_claim_ready": False,
            "prohibited_outputs": list(reviewer_gate["deny_outputs"]),
        },
        "readiness_status": "ready_for_readiness_mapping",
        "non_claim": [
            "decision rule profile stub only",
            "not a release gate",
            "not live acceptance",
            "not compliance claim",
        ],
        "review_surface": review_surface,
        "digest": digest,
    }
    markdown = _render_markdown(
        "Decision Rule Profile",
        [
            f"- decision_rule_id: {raw['decision_rule_id']}",
            f"- rule_profile_id: {raw['rule_profile_id']}",
            f"- version: {raw['version']}",
            f"- source_standard_or_method: {' | '.join(raw['source_standard_or_method'])}",
            f"- statement_template: {raw['statement_template']}",
            f"- reviewer_gate: {raw['reviewer_gate']['mode']}",
            f"- current_stage_applicability: {raw['current_stage_applicability']}",
            f"- tolerance_source: {raw['tolerance_source']['source']}",
            f"- evaluation_dimensions: {' | '.join(raw['evaluation_dimensions'])}",
            f"- payload_phases: {raw['phase_digest']['payload_phases']}",
            f"- trace_only: {raw['phase_digest']['trace_only']}",
            f"- coverage: {raw['phase_digest']['coverage']}",
            f"- standard_family: {' | '.join(raw['standard_family'])}",
            f"- required_evidence_categories: {' | '.join(raw['required_evidence_categories'])}",
            f"- limitation_note: {raw['limitation_note']}",
            f"- non_claim_note: {raw['non_claim_note']}",
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
        "digest": digest,
    }


def _build_recognition_review_surface(
    *,
    title_text: str,
    reviewer_note: str,
    summary_text: str,
    summary_lines: list[str],
    detail_lines: list[str],
    anchor_id: str,
    anchor_label: str,
    artifact_paths: dict[str, str],
    standard_family_filters: list[str] | None = None,
) -> dict[str, Any]:
    return {
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
        "standard_family_filters": list(standard_family_filters or []),
        "boundary_filter_rows": [],
        "boundary_filters": [],
        "non_claim_filter_rows": [],
        "non_claim_filters": [],
        "evidence_source_filters": ["simulated_protocol", "reviewer_readiness_only"],
        "artifact_paths": dict(artifact_paths),
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
    scope_definition_pack: dict[str, Any],
    decision_rule_profile: dict[str, Any],
    sample_digest: dict[str, Any],
    payload_backed_phases: list[str],
    path_map: dict[str, str],
) -> dict[str, Any]:
    scope_raw = dict(scope_definition_pack.get("raw") or {})
    decision_raw = dict(decision_rule_profile.get("raw") or {})
    scope_id = str(scope_raw.get("scope_id") or f"{run_id}-step2-scope-package")
    scope_name = str(scope_raw.get("scope_name") or "Step 2 simulation reviewer scope package")
    decision_rule_id = str(decision_raw.get("decision_rule_id") or "step2_readiness_reviewer_rule_v1")
    analyzer_scope = " | ".join(sample_digest.get("analyzers") or []) or "simulation_analyzer_population"
    linked_run_artifacts = [
        path_map["scope_definition_pack"],
        path_map["decision_rule_profile"],
        path_map["scope_readiness_summary"],
        path_map["measurement_phase_coverage_report"],
    ]
    assets = [
        _asset_row(
            asset_id="asset-standard-gas-sg-001",
            asset_name="标准气体 Lot SG-2026-CO2-01",
            asset_type="standard_gas",
            manufacturer="Reviewer Stub Gas Supply",
            model="CO2/H2O reference cylinder",
            serial_or_lot="LOT-SG-2026-CO2-01",
            role_in_reference_chain="primary_reference",
            measurand_scope=["CO2", "H2O"],
            route_scope=["gas", "water"],
            environment_scope=["simulation", "offline", "headless", "reviewer_only"],
            certificate_status="reviewer_stub_only",
            certificate_id="CERT-SG-2026-001",
            certificate_version="stub-v1",
            valid_from="2026-01-01",
            valid_to="2026-12-31",
            expiry_status="valid",
            intermediate_check_status="due_soon",
            intermediate_check_due="2026-05-01",
            last_check_at="2026-03-15",
            limitation_note="标准气 lot / 证书 / 使用批次仅作 reviewer mapping，不能替代真实 released 台账。",
            non_claim_note="标准气证书链未 released，不得形成 formal conformity / compliance claim。",
            reviewer_note="需补 lot binding、替代标准链审批与真实证书挂接。",
            linked_run_artifacts=linked_run_artifacts,
            linked_scope_ids=[scope_id],
            linked_scope_names=[scope_name],
            linked_decision_rule_ids=[decision_rule_id],
            lot_binding_required=True,
            lot_binding_status="missing_lot_binding",
            substitute_standard_chain_required=True,
            substitute_standard_chain_approval_status="missing_approval",
            readiness_status="warning_reviewer_attention",
        ),
        _asset_row(
            asset_id="asset-humidity-generator-hg-001",
            asset_name="湿度发生器 HG-STEP2-01",
            asset_type="humidity_generator",
            manufacturer="Reviewer Stub Climate Lab",
            model="HG-9000-sim",
            serial_or_lot="HG-STEP2-01",
            role_in_reference_chain="primary_reference",
            measurand_scope=["H2O", "humidity"],
            route_scope=["water"],
            environment_scope=["simulation", "offline", "headless"],
            active_state="inactive_certificate_expired",
            certificate_status="expired_certificate",
            certificate_id="CERT-HG-2024-017",
            certificate_version="stub-v1",
            valid_from="2025-04-01",
            valid_to="2026-03-31",
            expiry_status="expired",
            intermediate_check_status="pass",
            intermediate_check_due="2026-05-15",
            last_check_at="2026-03-10",
            limitation_note="当前仅展示 reviewer-facing 过期状态，不触发真实设备控制。",
            non_claim_note="证书有效期已过，当前结果不得作为 formal claim 证据。",
            reviewer_note="需补新证书或 reviewer-approved 替代链说明。",
            linked_run_artifacts=linked_run_artifacts,
            linked_scope_ids=[scope_id],
            linked_scope_names=[scope_name],
            linked_decision_rule_ids=[decision_rule_id],
            readiness_status="blocked_for_formal_claim",
        ),
        _asset_row(
            asset_id="asset-dewpoint-meter-dp-001",
            asset_name="露点仪 DP-STEP2-01",
            asset_type="dewpoint_meter",
            manufacturer="Reviewer Stub Climate Lab",
            model="DPM-750-sim",
            serial_or_lot="DP-STEP2-01",
            role_in_reference_chain="transfer_reference",
            measurand_scope=["dew_point", "H2O"],
            route_scope=["water"],
            environment_scope=["simulation", "offline", "headless"],
            certificate_status="missing_certificate",
            certificate_version="",
            expiry_status="missing",
            intermediate_check_status="missing_intermediate_check",
            intermediate_check_due="2026-04-20",
            limitation_note="当前为 reviewer-only stub，缺失 released certificate 文件。",
            non_claim_note="缺少证书与期间核查，不得进入 formal claim 口径。",
            reviewer_note="先补证书挂接，再补内部期间核查记录。",
            linked_run_artifacts=linked_run_artifacts,
            linked_scope_ids=[scope_id],
            linked_scope_names=[scope_name],
            linked_decision_rule_ids=[decision_rule_id],
            readiness_status="blocked_for_formal_claim",
            asset_type_aliases=["dew_point_meter"],
        ),
        _asset_row(
            asset_id="asset-pressure-gauge-pg-001",
            asset_name="数字压力计 PG-STEP2-01",
            asset_type="digital_pressure_gauge",
            manufacturer="Reviewer Stub Pressure Lab",
            model="DPG-2000-sim",
            serial_or_lot="PG-STEP2-01",
            role_in_reference_chain="transfer_reference",
            measurand_scope=["pressure"],
            route_scope=["gas", "water", "ambient"],
            environment_scope=["simulation", "offline", "headless"],
            certificate_status="reviewer_stub_only",
            certificate_id="CERT-PG-2026-021",
            certificate_version="stub-v1",
            valid_from="2026-01-15",
            valid_to="2026-10-01",
            expiry_status="valid",
            intermediate_check_status="overdue",
            intermediate_check_due="2026-04-01",
            last_check_at="2025-12-10",
            limitation_note="期间核查状态仅来自 reviewer stub，不回写真实仪表链路。",
            non_claim_note="期间核查逾期，不能支撑 formal claim。",
            reviewer_note="需要补内部期间核查记录并关联 decision rule dependency。",
            linked_run_artifacts=linked_run_artifacts,
            linked_scope_ids=[scope_id],
            linked_scope_names=[scope_name],
            linked_decision_rule_ids=[decision_rule_id],
            readiness_status="blocked_for_formal_claim",
        ),
        _asset_row(
            asset_id="asset-temp-chamber-tc-001",
            asset_name="温度箱 TC-STEP2-01",
            asset_type="temperature_chamber",
            manufacturer="Reviewer Stub Climate Lab",
            model="TC-600-sim",
            serial_or_lot="TC-STEP2-01",
            role_in_reference_chain="environment_support",
            measurand_scope=["temperature", "stability_environment"],
            route_scope=["gas", "water", "ambient"],
            environment_scope=["simulation", "offline", "headless"],
            quarantine_state="quarantined_pending_review",
            certificate_status="reviewer_stub_only",
            certificate_id="CERT-TC-2026-004",
            certificate_version="stub-v1",
            valid_from="2026-02-01",
            valid_to="2026-11-15",
            expiry_status="valid",
            intermediate_check_status="pass",
            intermediate_check_due="2026-06-01",
            last_check_at="2026-03-22",
            limitation_note="温度箱仅用于 reviewer-facing 环境链说明，不驱动真实环境控制。",
            non_claim_note="存在 out-of-tolerance 审核未关闭，不能用于 formal claim。",
            reviewer_note="需先关闭 OOT 事件，再讨论任何未来 formal path。",
            linked_run_artifacts=linked_run_artifacts,
            linked_scope_ids=[scope_id],
            linked_scope_names=[scope_name],
            linked_decision_rule_ids=[decision_rule_id],
            readiness_status="blocked_for_formal_claim",
        ),
        _asset_row(
            asset_id="asset-thermometer-th-001",
            asset_name="数字温度计 TH-STEP2-01",
            asset_type="digital_thermometer",
            manufacturer="Reviewer Stub Climate Lab",
            model="DT-250-sim",
            serial_or_lot="TH-STEP2-01",
            role_in_reference_chain="transfer_reference",
            measurand_scope=["temperature"],
            route_scope=["gas", "water", "ambient"],
            environment_scope=["simulation", "offline", "headless"],
            certificate_status="reviewer_stub_only",
            certificate_id="CERT-TH-2026-031",
            certificate_version="stub-v1",
            valid_from="2026-02-10",
            valid_to="2026-11-30",
            expiry_status="valid",
            intermediate_check_status="pass",
            intermediate_check_due="2026-06-15",
            last_check_at="2026-03-30",
            limitation_note="当前仅用于 readiness mapping 的温度参考链骨架。",
            non_claim_note="未释放到正式 metrology claim，仅可供 reviewer 视图引用。",
            reviewer_note="作为正向样例保留，用于展示 scope->asset traceability 完整关联。",
            linked_run_artifacts=linked_run_artifacts,
            linked_scope_ids=[scope_id],
            linked_scope_names=[scope_name],
            linked_decision_rule_ids=[decision_rule_id],
            readiness_status="ok_for_reviewer_mapping",
            asset_type_aliases=["thermometer"],
        ),
        _asset_row(
            asset_id="asset-pressure-controller-pc-001",
            asset_name="压力控制器 PC-STEP2-01",
            asset_type="pressure_controller",
            manufacturer="Reviewer Stub Pressure Lab",
            model="PC-880-sim",
            serial_or_lot="PC-STEP2-01",
            role_in_reference_chain="supporting_control",
            measurand_scope=["pressure", "pressure_stability"],
            route_scope=["gas", "water"],
            environment_scope=["simulation", "offline", "headless"],
            certificate_status="reviewer_stub_only",
            certificate_id="CERT-PC-2026-008",
            certificate_version="stub-v1",
            valid_from="2026-02-01",
            valid_to="2026-10-31",
            expiry_status="valid",
            intermediate_check_status="due_soon",
            intermediate_check_due="2026-04-25",
            last_check_at="2026-03-05",
            limitation_note="控制器状态仅作 artifact-based review，不构成 live control dependency。",
            non_claim_note="当前仍是 reviewer-only supporting asset，不能生成 formal compliance claim。",
            reviewer_note="建议与 pressure_stable phase 的 intermediate check 计划保持联动。",
            linked_run_artifacts=linked_run_artifacts,
            linked_scope_ids=[scope_id],
            linked_scope_names=[scope_name],
            linked_decision_rule_ids=[decision_rule_id],
            readiness_status="warning_reviewer_attention",
        ),
        _asset_row(
            asset_id="asset-aut-dut-sim",
            asset_name=f"被校分析仪群组 / {analyzer_scope}",
            asset_type="analyzer_under_test",
            manufacturer="Simulated DUT Population",
            model="AUT-simulation-bundle",
            serial_or_lot=analyzer_scope,
            role_in_reference_chain="analyzer_under_test",
            measurand_scope=["CO2", "H2O", "ambient_diagnostic"],
            route_scope=["gas", "water", "ambient"],
            environment_scope=["simulation", "offline", "headless"],
            certificate_status="not_applicable_for_reference_chain",
            expiry_status="not_applicable",
            intermediate_check_status="not_applicable_for_reference_chain",
            limitation_note="DUT 仅为 simulation population，不进入真实控制链。",
            non_claim_note="DUT 群组只用于 reviewer-facing readiness mapping，不代表真实 bench acceptance。",
            reviewer_note="保持与 reference assets 分离，避免误解为 released reference chain 成员。",
            linked_run_artifacts=linked_run_artifacts,
            linked_scope_ids=[scope_id],
            linked_scope_names=[scope_name],
            linked_decision_rule_ids=[decision_rule_id],
            readiness_status="ready_for_readiness_mapping",
        ),
    ]
    certificate_attention_count = sum(
        1
        for item in assets
        if str(item.get("certificate_status") or "") in {"missing_certificate", "expired_certificate", "reviewer_stub_only"}
    )
    intermediate_attention_count = sum(
        1
        for item in assets
        if str(item.get("intermediate_check_status") or "") in {"missing_intermediate_check", "overdue", "due_soon"}
    )
    lot_binding_gap_count = sum(
        1 for item in assets if bool(item.get("lot_binding_required")) and str(item.get("lot_binding_status") or "") != "approved"
    )
    scope_reference_assets = [
        f"{item['asset_name']} ({item['asset_type']})"
        for item in assets
        if str(item.get("asset_type") or "") != "analyzer_under_test"
    ]
    decision_rule_dependencies = [
        "certificate lifecycle summary",
        "standard gas lot binding",
        "pressure / temperature intermediate checks",
        "reviewer-only substitute standard approval trail",
    ]
    asset_readiness_overview = (
        f"assets {len(assets)} | certificate attention {certificate_attention_count} | "
        f"intermediate-check attention {intermediate_attention_count} | lot-binding gaps {lot_binding_gap_count}"
    )
    current_coverage = [
        f"scope linkage: {scope_name}",
        f"decision rule linkage: {decision_rule_id}",
        f"payload-backed phases: {' | '.join(payload_backed_phases) or '--'}",
        f"tracked asset types: {' | '.join(_dedupe(item['asset_type'] for item in assets))}",
    ]
    missing_evidence = [
        "released asset ledger / database intake remains out of default chain",
        "standard gas lot binding is not closed",
        "some certificates and intermediate checks remain stub / missing / expired",
    ]
    blockers = [
        "registry is reviewer-only and not a released traceability ledger",
        "formal claim release flags remain false for all Step 2 assets",
    ]
    digest = {
        "summary": (
            "reference asset registry | reviewer-only / simulated ledger | "
            f"assets {len(assets)} | certificate attention {certificate_attention_count}"
        ),
        "scope_overview_summary": scope_name,
        "decision_rule_summary": f"{decision_rule_id} | reviewer dependency mapping only",
        "conformity_boundary_summary": "reference assets stay reviewer-only / simulated and cannot become formal claim evidence.",
        "asset_readiness_overview": asset_readiness_overview,
        "scope_reference_assets_summary": " | ".join(scope_reference_assets),
        "decision_rule_dependency_summary": " | ".join(decision_rule_dependencies),
        "current_coverage_summary": " | ".join(current_coverage),
        "missing_evidence_summary": " | ".join(missing_evidence),
        "blocker_summary": " | ".join(blockers),
        "reviewer_next_step_digest": "keep registry file-backed, then map lifecycle / gate sidecars without introducing default DB or live equipment dependency.",
        "non_claim_digest": "reviewer-only registry; simulated/offline assets are not real acceptance evidence.",
    }
    bundle = _summary_raw(
        run_id=run_id,
        artifact_type="reference_asset_registry",
        overall_status="degraded",
        title_text="Reference Asset Registry",
        reviewer_note=(
            "Machine-readable reviewer-facing reference asset ledger only. "
            "It is file-backed, simulated/stub compatible, and intentionally not a released real metrology asset chain."
        ),
        summary_text=digest["summary"],
        summary_lines=[
            f"asset readiness overview: {asset_readiness_overview}",
            f"scope-linked assets: {digest['scope_reference_assets_summary']}",
            f"decision-rule dependencies: {digest['decision_rule_dependency_summary']}",
        ],
        detail_lines=[
            f"current coverage: {digest['current_coverage_summary']}",
            f"missing evidence: {digest['missing_evidence_summary']}",
            f"blockers: {digest['blocker_summary']}",
            f"scope_definition_pack: {path_map['scope_definition_pack']}",
            f"decision_rule_profile: {path_map['decision_rule_profile']}",
            *[f"boundary: {line}" for line in RECOGNITION_READINESS_BOUNDARY_STATEMENTS],
        ],
        anchor_id="reference-asset-registry",
        anchor_label="Reference asset registry",
        evidence_categories=["recognition_readiness", "reference_asset_registry", "asset_readiness"],
        artifact_paths={
            "scope_definition_pack": path_map["scope_definition_pack"],
            "decision_rule_profile": path_map["decision_rule_profile"],
            "scope_readiness_summary": path_map["scope_readiness_summary"],
            "reference_asset_registry": path_map["reference_asset_registry"],
            "reference_asset_registry_markdown": path_map["reference_asset_registry_markdown"],
            "certificate_lifecycle_summary": path_map["certificate_lifecycle_summary"],
            "pre_run_readiness_gate": path_map["pre_run_readiness_gate"],
            "measurement_phase_coverage_report": path_map["measurement_phase_coverage_report"],
        },
        body={
            "scope_id": scope_id,
            "scope_name": scope_name,
            "decision_rule_id": decision_rule_id,
            "assets": assets,
            "scope_reference_assets": scope_reference_assets,
            "decision_rule_dependencies": decision_rule_dependencies,
            "current_evidence_coverage": current_coverage,
            "missing_evidence": missing_evidence,
            "blockers": blockers,
            "reviewer_stub_only": True,
            "readiness_mapping_only": True,
            "not_released_for_formal_claim": True,
            "not_ready_for_formal_claim": True,
            "ready_for_readiness_mapping": True,
            "not_in_default_chain": False,
            "primary_evidence_rewritten": False,
            "linked_artifacts": {
                "scope_definition_pack": path_map["scope_definition_pack"],
                "decision_rule_profile": path_map["decision_rule_profile"],
                "scope_readiness_summary": path_map["scope_readiness_summary"],
                "certificate_lifecycle_summary": path_map["certificate_lifecycle_summary"],
                "pre_run_readiness_gate": path_map["pre_run_readiness_gate"],
                "measurement_phase_coverage_report": path_map["measurement_phase_coverage_report"],
            },
            "limitation_note": (
                "Registry is intentionally file-artifact-first and reviewer-facing in Step 2. "
                "It does not introduce a default database dependency or a hard dependency for live equipment control."
            ),
            "non_claim_note": (
                "All registry rows remain simulated / stub / reviewer-only and cannot be interpreted as real acceptance, "
                "formal compliance, or accreditation evidence."
            ),
            "reviewer_note": "Use this ledger to map scope/decision dependencies only; keep formal claim release flags false.",
        },
        digest=digest,
        filename=REFERENCE_ASSET_REGISTRY_FILENAME,
        markdown_filename=REFERENCE_ASSET_REGISTRY_MARKDOWN_FILENAME,
    )
    raw = dict(bundle.get("raw") or {})
    raw["artifact_role"] = "execution_summary"
    raw["evidence_source"] = "simulated"
    raw["review_surface"] = {
        **dict(raw.get("review_surface") or {}),
        "role_text": "execution_summary",
        "evidence_source_filters": ["simulated", "reviewer_readiness_only"],
    }
    bundle["raw"] = raw
    bundle["digest"] = dict(raw.get("digest") or {})
    bundle["markdown"] = _render_markdown(
        "Reference Asset Registry",
        [
            f"- scope_id: {scope_id}",
            f"- decision_rule_id: {decision_rule_id}",
            f"- asset_readiness_overview: {asset_readiness_overview}",
            *[
                (
                    f"- {row['asset_id']}: {row['asset_name']} | asset_type={row['asset_type']} | "
                    f"certificate_status={row['certificate_status']} | intermediate_check_status={row['intermediate_check_status']} | "
                    f"lot_binding_status={row['lot_binding_status']} | ready_for_readiness_mapping={row['ready_for_readiness_mapping']}"
                )
                for row in assets
            ],
            f"- non_claim_note: {str(raw.get('non_claim_note') or '--')}",
        ],
    )
    return bundle


def _build_certificate_lifecycle_summary(
    *,
    run_id: str,
    scope_definition_pack: dict[str, Any],
    decision_rule_profile: dict[str, Any],
    reference_asset_registry: dict[str, Any],
    path_map: dict[str, str],
) -> dict[str, Any]:
    scope_raw = dict(scope_definition_pack.get("raw") or {})
    decision_raw = dict(decision_rule_profile.get("raw") or {})
    scope_name = str(scope_raw.get("scope_name") or "Step 2 simulation reviewer scope package")
    decision_rule_id = str(decision_raw.get("decision_rule_id") or "step2_readiness_reviewer_rule_v1")
    assets = [dict(item) for item in list(dict(reference_asset_registry.get("raw") or {}).get("assets") or [])]
    certificate_rows = [
        {
            "certificate_id": str(item.get("certificate_id") or f"UNBOUND-{item['asset_id']}"),
            "certificate_version": str(item.get("certificate_version") or "missing"),
            "asset_id": str(item.get("asset_id") or ""),
            "asset_name": str(item.get("asset_name") or ""),
            "asset_type": str(item.get("asset_type") or ""),
            "certificate_status": str(item.get("certificate_status") or ""),
            "valid_from": str(item.get("valid_from") or ""),
            "valid_to": str(item.get("valid_to") or ""),
            "certificate_origin": (
                "external_calibration"
                if str(item.get("asset_type") or "") != "analyzer_under_test"
                else "not_applicable"
            ),
            "reviewer_stub_only": True,
            "readiness_mapping_only": True,
            "not_released_for_formal_claim": True,
            "evidence_source": "simulated",
            "not_real_acceptance_evidence": True,
        }
        for item in assets
    ]
    lot_bindings = [
        {
            "binding_id": "lot-binding-sg-001",
            "asset_id": "asset-standard-gas-sg-001",
            "asset_type": "standard_gas",
            "lot_id": "LOT-SG-2026-CO2-01",
            "certificate_id": "CERT-SG-2026-001",
            "use_batch_id": "USE-BATCH-SIM-20260410-A",
            "binding_status": "missing_binding_approval",
            "reviewer_stub_only": True,
            "readiness_mapping_only": True,
            "not_released_for_formal_claim": True,
            "evidence_source": "simulated",
            "not_real_acceptance_evidence": True,
        }
    ]
    intermediate_check_plans = [
        {
            "plan_id": "icp-pressure-stable-001",
            "asset_id": "asset-pressure-gauge-pg-001",
            "plan_name": "压力计期间核查计划",
            "activity_origin": "internal_intermediate_check",
            "due_at": "2026-04-01",
            "status": "overdue",
            "scope_link": scope_name,
            "decision_rule_dependency": decision_rule_id,
            "reviewer_stub_only": True,
            "readiness_mapping_only": True,
            "not_released_for_formal_claim": True,
            "evidence_source": "simulated",
            "not_real_acceptance_evidence": True,
        },
        {
            "plan_id": "icp-standard-gas-001",
            "asset_id": "asset-standard-gas-sg-001",
            "plan_name": "标准气使用批次期间核查计划",
            "activity_origin": "internal_intermediate_check",
            "due_at": "2026-05-01",
            "status": "scheduled",
            "scope_link": scope_name,
            "decision_rule_dependency": decision_rule_id,
            "reviewer_stub_only": True,
            "readiness_mapping_only": True,
            "not_released_for_formal_claim": True,
            "evidence_source": "simulated",
            "not_real_acceptance_evidence": True,
        },
    ]
    intermediate_check_records = [
        {
            "record_id": "icr-thermometer-001",
            "asset_id": "asset-thermometer-th-001",
            "activity_origin": "internal_intermediate_check",
            "performed_at": "2026-03-30",
            "status": "pass",
            "reviewer_stub_only": True,
            "readiness_mapping_only": True,
            "not_released_for_formal_claim": True,
            "evidence_source": "simulated",
            "not_real_acceptance_evidence": True,
        },
        {
            "record_id": "icr-humidity-generator-001",
            "asset_id": "asset-humidity-generator-hg-001",
            "activity_origin": "external_calibration",
            "performed_at": "2025-04-01",
            "status": "expired",
            "reviewer_stub_only": True,
            "readiness_mapping_only": True,
            "not_released_for_formal_claim": True,
            "evidence_source": "simulated",
            "not_real_acceptance_evidence": True,
        },
    ]
    out_of_tolerance_events = [
        {
            "event_id": "oot-temp-chamber-001",
            "asset_id": "asset-temp-chamber-tc-001",
            "activity_origin": "internal_intermediate_check",
            "detected_at": "2026-04-07",
            "event_status": "open",
            "disposition": "reviewer_hold_only",
            "impact_scope": ["water", "ambient"],
            "reviewer_stub_only": True,
            "readiness_mapping_only": True,
            "not_released_for_formal_claim": True,
            "evidence_source": "simulated",
            "not_real_acceptance_evidence": True,
        }
    ]
    missing_certificate_count = sum(
        1 for row in certificate_rows if str(row.get("certificate_status") or "") in {"missing_certificate", "expired_certificate"}
    )
    lot_binding_gap_count = sum(1 for row in lot_bindings if str(row.get("binding_status") or "") != "approved")
    overdue_check_count = sum(1 for row in intermediate_check_plans if str(row.get("status") or "") == "overdue")
    oot_open_count = sum(1 for row in out_of_tolerance_events if str(row.get("event_status") or "") == "open")
    lifecycle_overview = (
        f"certificates {len(certificate_rows)} | lot bindings {len(lot_bindings)} | "
        f"intermediate plans {len(intermediate_check_plans)} | OOT open {oot_open_count}"
    )
    digest = {
        "summary": (
            "certificate lifecycle | reviewer stub only | "
            f"missing certificate/expiry {missing_certificate_count} | lot-binding gaps {lot_binding_gap_count} | "
            f"overdue checks {overdue_check_count} | open OOT {oot_open_count}"
        ),
        "scope_overview_summary": scope_name,
        "decision_rule_summary": f"{decision_rule_id} | lifecycle dependency mapping",
        "conformity_boundary_summary": "certificate lifecycle is reviewer-facing only and not released for formal claim.",
        "certificate_lifecycle_overview": lifecycle_overview,
        "current_coverage_summary": (
            f"scope linkage: {scope_name} | decision rule dependency: {decision_rule_id} | "
            f"certificate rows: {len(certificate_rows)} | internal/external activities separated: yes"
        ),
        "missing_evidence_summary": (
            "lot binding approval remains missing | expired / missing certificates remain in reviewer stub state | "
            "out-of-tolerance closure remains open"
        ),
        "blocker_summary": (
            "certificate lifecycle is not released for formal claim | reviewer stub records cannot replace external "
            "certificate files or internal signed checks"
        ),
        "reviewer_next_step_digest": "close lot binding, overdue checks, and open OOT items before discussing any future formal path.",
        "non_claim_digest": "lifecycle rows are simulated reviewer artifacts and cannot be treated as real acceptance evidence.",
    }
    bundle = _summary_raw(
        run_id=run_id,
        artifact_type="certificate_lifecycle_summary",
        overall_status="failed" if (missing_certificate_count or lot_binding_gap_count or oot_open_count) else "diagnostic_only",
        title_text="Certificate Lifecycle Summary",
        reviewer_note=(
            "Certificate lifecycle skeleton only. "
            "It keeps external certificates, internal intermediate checks, lot bindings, and out-of-tolerance events separate for reviewer mapping."
        ),
        summary_text=digest["summary"],
        summary_lines=[
            f"certificate lifecycle overview: {lifecycle_overview}",
            f"current coverage: {digest['current_coverage_summary']}",
        ],
        detail_lines=[
            f"missing evidence: {digest['missing_evidence_summary']}",
            f"blockers: {digest['blocker_summary']}",
            f"reference_asset_registry: {path_map['reference_asset_registry']}",
            *[f"boundary: {line}" for line in RECOGNITION_READINESS_BOUNDARY_STATEMENTS],
        ],
        anchor_id="certificate-lifecycle-summary",
        anchor_label="Certificate lifecycle summary",
        evidence_categories=["recognition_readiness", "certificate_lifecycle", "readiness_mapping"],
        artifact_paths={
            "scope_definition_pack": path_map["scope_definition_pack"],
            "decision_rule_profile": path_map["decision_rule_profile"],
            "reference_asset_registry": path_map["reference_asset_registry"],
            "certificate_lifecycle_summary": path_map["certificate_lifecycle_summary"],
            "certificate_lifecycle_summary_markdown": path_map["certificate_lifecycle_summary_markdown"],
            "certificate_readiness_summary": path_map["certificate_readiness_summary"],
            "pre_run_readiness_gate": path_map["pre_run_readiness_gate"],
        },
        body={
            "certificate_rows": certificate_rows,
            "lot_bindings": lot_bindings,
            "intermediate_check_plans": intermediate_check_plans,
            "intermediate_check_records": intermediate_check_records,
            "out_of_tolerance_events": out_of_tolerance_events,
            "reviewer_stub_only": True,
            "readiness_mapping_only": True,
            "not_released_for_formal_claim": True,
            "not_ready_for_formal_claim": True,
            "ready_for_readiness_mapping": True,
            "primary_evidence_rewritten": False,
            "linked_artifacts": {
                "scope_definition_pack": path_map["scope_definition_pack"],
                "decision_rule_profile": path_map["decision_rule_profile"],
                "reference_asset_registry": path_map["reference_asset_registry"],
                "certificate_readiness_summary": path_map["certificate_readiness_summary"],
                "pre_run_readiness_gate": path_map["pre_run_readiness_gate"],
            },
            "limitation_note": (
                "Lifecycle data is intentionally reviewer_stub_only / readiness_mapping_only. "
                "It preserves file-artifact-first ingest and avoids a default DB or real enforcement path."
            ),
            "non_claim_note": (
                "All lifecycle rows are simulated reviewer artifacts and are not released for formal claim, "
                "accreditation, or real acceptance conclusions."
            ),
        },
        digest=digest,
        filename=CERTIFICATE_LIFECYCLE_SUMMARY_FILENAME,
        markdown_filename=CERTIFICATE_LIFECYCLE_SUMMARY_MARKDOWN_FILENAME,
    )
    raw = dict(bundle.get("raw") or {})
    raw["evidence_source"] = "simulated"
    raw["review_surface"] = {
        **dict(raw.get("review_surface") or {}),
        "evidence_source_filters": ["simulated", "reviewer_readiness_only"],
    }
    bundle["raw"] = raw
    bundle["digest"] = dict(raw.get("digest") or {})
    bundle["markdown"] = _render_markdown(
        "Certificate Lifecycle Summary",
        [
            f"- certificate_lifecycle_overview: {lifecycle_overview}",
            *[
                (
                    f"- certificate: {row['asset_name']} | certificate_status={row['certificate_status']} | "
                    f"valid_to={row['valid_to'] or '--'} | origin={row['certificate_origin']}"
                )
                for row in certificate_rows
            ],
            *[
                (
                    f"- lot_binding: {row['lot_id']} | binding_status={row['binding_status']} | "
                    f"use_batch_id={row['use_batch_id']}"
                )
                for row in lot_bindings
            ],
            f"- non_claim_note: {str(raw.get('non_claim_note') or '--')}",
        ],
    )
    return bundle


def _build_certificate_readiness_summary(
    *,
    run_id: str,
    reference_asset_registry: dict[str, Any],
    certificate_lifecycle_summary: dict[str, Any],
    path_map: dict[str, str],
) -> dict[str, Any]:
    assets = [dict(item) for item in list(dict(reference_asset_registry.get("raw") or {}).get("assets") or [])]
    lifecycle_raw = dict(certificate_lifecycle_summary.get("raw") or {})
    out_of_tolerance_events = [
        dict(item) for item in list(lifecycle_raw.get("out_of_tolerance_events") or []) if isinstance(item, dict)
    ]
    lot_bindings = [dict(item) for item in list(lifecycle_raw.get("lot_bindings") or []) if isinstance(item, dict)]
    missing_certificate_count = sum(
        1
        for item in assets
        if str(item.get("certificate_status") or "") in {"missing_certificate", "expired_certificate"}
    )
    missing_intermediate_check_count = sum(
        1
        for item in assets
        if str(item.get("intermediate_check_status") or "") in {"missing_intermediate_check", "overdue"}
    )
    lot_binding_gap_count = sum(
        1 for item in lot_bindings if str(item.get("binding_status") or "") != "approved"
    )
    oot_open_count = sum(
        1 for item in out_of_tolerance_events if str(item.get("event_status") or "") == "open"
    )
    current_coverage = [
        f"assets tracked: {len(assets)}",
        f"certificate missing or expired: {missing_certificate_count}",
        f"intermediate check missing or overdue: {missing_intermediate_check_count}",
        f"lot binding gaps: {lot_binding_gap_count}",
        f"open out-of-tolerance: {oot_open_count}",
    ]
    missing_evidence = [
        "no released certificate files attached",
        "no intermediate check execution evidence attached",
        "lot binding and substitute approval remain reviewer-only",
        "traceability chain remains readiness-only",
    ]
    digest = {
        "summary": (
            "reference asset / certificate readiness | "
            f"assets {len(assets)} | certificate gaps {missing_certificate_count} | "
            f"intermediate-check gaps {missing_intermediate_check_count} | "
            f"lot-binding gaps {lot_binding_gap_count} | open OOT {oot_open_count}"
        ),
        "asset_readiness_overview": str(
            dict(reference_asset_registry.get("digest") or {}).get("asset_readiness_overview")
            or f"assets {len(assets)}"
        ),
        "certificate_lifecycle_overview": str(
            dict(certificate_lifecycle_summary.get("digest") or {}).get("certificate_lifecycle_overview")
            or f"lot bindings {len(lot_bindings)}"
        ),
        "current_coverage_summary": " | ".join(current_coverage),
        "missing_evidence_summary": " | ".join(missing_evidence),
        "blocker_summary": (
            "certificate / intermediate check / lot-binding gaps remain open; "
            "current output stays reviewer-only and not released for formal claim"
        ),
        "reviewer_next_step_digest": (
            "use certificate lifecycle + pre-run gate to explain gaps clearly, but keep all outputs reviewer-only."
        ),
    }
    bundle = _summary_raw(
        run_id=run_id,
        artifact_type="certificate_readiness_summary",
        overall_status="failed" if (missing_certificate_count or missing_intermediate_check_count or oot_open_count) else "degraded",
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
            "certificate_lifecycle_summary": path_map["certificate_lifecycle_summary"],
            "certificate_lifecycle_summary_markdown": path_map["certificate_lifecycle_summary_markdown"],
            "certificate_readiness_summary": path_map["certificate_readiness_summary"],
            "certificate_readiness_summary_markdown": path_map["certificate_readiness_summary_markdown"],
            "pre_run_readiness_gate": path_map["pre_run_readiness_gate"],
            "metrology_traceability_stub": path_map["metrology_traceability_stub"],
            "metrology_traceability_stub_markdown": path_map["metrology_traceability_stub_markdown"],
        },
        body={
            "current_coverage": current_coverage,
            "missing_evidence": missing_evidence,
            "asset_status_rows": assets,
            "lot_binding_rows": lot_bindings,
            "out_of_tolerance_events": out_of_tolerance_events,
            "ready_for_readiness_mapping": True,
            "not_ready_for_formal_claim": True,
            "reviewer_stub_only": True,
            "readiness_mapping_only": True,
            "not_released_for_formal_claim": True,
            "primary_evidence_rewritten": False,
            "linked_artifacts": {
                "reference_asset_registry": path_map["reference_asset_registry"],
                "certificate_lifecycle_summary": path_map["certificate_lifecycle_summary"],
                "pre_run_readiness_gate": path_map["pre_run_readiness_gate"],
                "metrology_traceability_stub": path_map["metrology_traceability_stub"],
            },
        },
        digest=digest,
        filename=CERTIFICATE_READINESS_SUMMARY_FILENAME,
        markdown_filename=CERTIFICATE_READINESS_SUMMARY_MARKDOWN_FILENAME,
    )
    raw = dict(bundle.get("raw") or {})
    raw["evidence_source"] = "simulated"
    raw["review_surface"] = {
        **dict(raw.get("review_surface") or {}),
        "evidence_source_filters": ["simulated", "reviewer_readiness_only"],
    }
    bundle["raw"] = raw
    bundle["digest"] = dict(raw.get("digest") or {})
    return bundle


def _build_pre_run_readiness_gate(
    *,
    run_id: str,
    scope_definition_pack: dict[str, Any],
    decision_rule_profile: dict[str, Any],
    reference_asset_registry: dict[str, Any],
    certificate_lifecycle_summary: dict[str, Any],
    certificate_readiness_summary: dict[str, Any],
    path_map: dict[str, str],
) -> dict[str, Any]:
    scope_raw = dict(scope_definition_pack.get("raw") or {})
    decision_raw = dict(decision_rule_profile.get("raw") or {})
    scope_name = str(scope_raw.get("scope_name") or "Step 2 simulation reviewer scope package")
    decision_rule_id = str(decision_raw.get("decision_rule_id") or "step2_readiness_reviewer_rule_v1")
    assets = [dict(item) for item in list(dict(reference_asset_registry.get("raw") or {}).get("assets") or [])]
    lifecycle_raw = dict(certificate_lifecycle_summary.get("raw") or {})
    lot_bindings = [dict(item) for item in list(lifecycle_raw.get("lot_bindings") or []) if isinstance(item, dict)]
    out_of_tolerance_events = [
        dict(item) for item in list(lifecycle_raw.get("out_of_tolerance_events") or []) if isinstance(item, dict)
    ]
    certificate_missing_assets = [
        str(item.get("asset_name") or item.get("asset_id") or "")
        for item in assets
        if str(item.get("certificate_status") or "") == "missing_certificate"
    ]
    expired_assets = [
        str(item.get("asset_name") or item.get("asset_id") or "")
        for item in assets
        if str(item.get("certificate_status") or "") == "expired_certificate"
    ]
    intermediate_check_attention_assets = [
        str(item.get("asset_name") or item.get("asset_id") or "")
        for item in assets
        if str(item.get("intermediate_check_status") or "") in {"missing_intermediate_check", "overdue"}
    ]
    lot_binding_missing_assets = [
        str(item.get("lot_id") or item.get("asset_id") or "")
        for item in lot_bindings
        if str(item.get("binding_status") or "") != "approved"
    ]
    substitute_approval_missing_assets = [
        str(item.get("asset_name") or item.get("asset_id") or "")
        for item in assets
        if bool(item.get("substitute_standard_chain_required"))
        and str(item.get("substitute_standard_chain_approval_status") or "") != "approved"
    ]
    oot_open_assets = [
        str(item.get("asset_id") or "")
        for item in out_of_tolerance_events
        if str(item.get("event_status") or "") == "open"
    ]
    association_gaps = [
        str(item.get("asset_name") or item.get("asset_id") or "")
        for item in assets
        if str(item.get("asset_type") or "") != "analyzer_under_test"
        and (
            not list(item.get("linked_scope_ids") or [])
            or not list(item.get("linked_decision_rule_ids") or [])
        )
    ]
    scope_reference_assets = [
        f"{item['asset_name']} ({item['asset_type']})"
        for item in assets
        if str(item.get("asset_type") or "") != "analyzer_under_test"
    ]
    decision_rule_dependencies = [
        "released certificates",
        "standard gas lot binding",
        "intermediate check plans / records",
        "out-of-tolerance closure",
        "substitute standard approval trail",
    ]
    blocking_items = _normalize_text_list(
        [
            ("certificate missing: " + " | ".join(certificate_missing_assets)) if certificate_missing_assets else "",
            ("certificate expired: " + " | ".join(expired_assets)) if expired_assets else "",
            (
                "intermediate check not ready: " + " | ".join(intermediate_check_attention_assets)
                if intermediate_check_attention_assets
                else ""
            ),
            ("lot binding missing: " + " | ".join(lot_binding_missing_assets)) if lot_binding_missing_assets else "",
            ("out-of-tolerance open: " + " | ".join(oot_open_assets)) if oot_open_assets else "",
        ]
    )
    warning_items = _normalize_text_list(
        [
            (
                "substitute standard approval missing: " + " | ".join(substitute_approval_missing_assets)
                if substitute_approval_missing_assets
                else ""
            ),
            (
                "scope / decision / asset association incomplete: " + " | ".join(association_gaps)
                if association_gaps
                else ""
            ),
            "gate is reviewer-facing advisory only and cannot drive equipment",
        ]
    )
    reviewer_actions = _normalize_text_list(
        [
            "补齐缺失或过期证书，仅用于 reviewer mapping 后续闭环计划。",
            "补 lot binding / use batch / 替代标准审批 sidecar，不引入默认 DB 主链。",
            "关闭期间核查逾期与 OOT reviewer digest，再讨论未来 formal path。",
            "保持 current run 仅为 readiness mapping，不形成 formal compliance / acceptance claim。",
        ]
    )
    gate_status = (
        "blocked_for_formal_claim"
        if blocking_items
        else ("warning_reviewer_attention" if warning_items else "ok_for_reviewer_mapping")
    )
    overall_status = "failed" if blocking_items else ("degraded" if warning_items else "diagnostic_only")
    asset_overview = str(
        dict(reference_asset_registry.get("digest") or {}).get("asset_readiness_overview")
        or f"assets {len(assets)}"
    )
    lifecycle_overview = str(
        dict(certificate_lifecycle_summary.get("digest") or {}).get("certificate_lifecycle_overview")
        or "--"
    )
    digest = {
        "summary": (
            "pre-run readiness gate | reviewer advisory only | "
            f"gate_status {gate_status} | readiness mapping only"
        ),
        "scope_overview_summary": scope_name,
        "decision_rule_summary": f"{decision_rule_id} | reviewer mapping dependencies",
        "conformity_boundary_summary": (
            "current run can support readiness mapping only; it cannot support formal claim, compliance, or real acceptance."
        ),
        "asset_readiness_overview": asset_overview,
        "certificate_lifecycle_overview": lifecycle_overview,
        "pre_run_gate_status": gate_status,
        "scope_reference_assets_summary": " | ".join(scope_reference_assets),
        "decision_rule_dependency_summary": " | ".join(decision_rule_dependencies),
        "current_coverage_summary": (
            "certificate / validity / intermediate check / lot binding / substitute approval / association integrity reviewed"
        ),
        "missing_evidence_summary": (
            "released certificates, approved lot bindings, closed OOT, and signed internal checks remain incomplete"
        ),
        "blocker_summary": " | ".join(blocking_items) or "no blocking items",
        "warning_summary": " | ".join(warning_items) or "no warning items",
        "reviewer_action_summary": " | ".join(reviewer_actions),
        "reviewer_next_step_digest": reviewer_actions[0] if reviewer_actions else "--",
        "next_required_artifacts_summary": "certificate_lifecycle_summary | pre_run_readiness_gate | metrology_traceability_stub",
        "non_claim_digest": (
            "advisory gate only; Step 2 gate results are simulated reviewer evidence and not real acceptance evidence."
        ),
    }
    bundle = _summary_raw(
        run_id=run_id,
        artifact_type="pre_run_readiness_gate",
        overall_status=overall_status,
        title_text="Pre-run Readiness Gate",
        reviewer_note=(
            "Step 2 pre-run readiness gate is advisory and artifact-based only. "
            "It is reviewer-facing, cannot drive live equipment, and cannot become a formal compliance or real acceptance gate."
        ),
        summary_text=digest["summary"],
        summary_lines=[
            f"gate status: {gate_status}",
            f"asset readiness overview: {asset_overview}",
            f"certificate lifecycle overview: {lifecycle_overview}",
        ],
        detail_lines=[
            f"blocking items: {digest['blocker_summary']}",
            f"warning items: {digest['warning_summary']}",
            f"reviewer actions: {digest['reviewer_action_summary']}",
            *[f"boundary: {line}" for line in RECOGNITION_READINESS_BOUNDARY_STATEMENTS],
        ],
        anchor_id="pre-run-readiness-gate",
        anchor_label="Pre-run readiness gate",
        evidence_categories=["recognition_readiness", "pre_run_gate", "reviewer_mapping"],
        artifact_paths={
            "scope_definition_pack": path_map["scope_definition_pack"],
            "decision_rule_profile": path_map["decision_rule_profile"],
            "reference_asset_registry": path_map["reference_asset_registry"],
            "certificate_lifecycle_summary": path_map["certificate_lifecycle_summary"],
            "certificate_readiness_summary": path_map["certificate_readiness_summary"],
            "pre_run_readiness_gate": path_map["pre_run_readiness_gate"],
            "pre_run_readiness_gate_markdown": path_map["pre_run_readiness_gate_markdown"],
        },
        body={
            "gate_status": gate_status,
            "blocking_items": blocking_items,
            "warning_items": warning_items,
            "reviewer_actions": reviewer_actions,
            "checks": [
                {
                    "check_id": "certificate_missing",
                    "status": "blocked" if certificate_missing_assets else "pass",
                    "summary": " | ".join(certificate_missing_assets) or "no missing certificate assets",
                },
                {
                    "check_id": "certificate_validity",
                    "status": "blocked" if expired_assets else "pass",
                    "summary": " | ".join(expired_assets) or "no expired assets",
                },
                {
                    "check_id": "intermediate_check",
                    "status": "blocked" if intermediate_check_attention_assets else "pass",
                    "summary": " | ".join(intermediate_check_attention_assets) or "all tracked assets have non-blocking check state",
                },
                {
                    "check_id": "out_of_tolerance",
                    "status": "blocked" if oot_open_assets else "pass",
                    "summary": " | ".join(oot_open_assets) or "no open OOT events",
                },
                {
                    "check_id": "lot_binding",
                    "status": "blocked" if lot_binding_missing_assets else "pass",
                    "summary": " | ".join(lot_binding_missing_assets) or "lot binding complete",
                },
                {
                    "check_id": "substitute_standard_chain_approval",
                    "status": "warning" if substitute_approval_missing_assets else "pass",
                    "summary": " | ".join(substitute_approval_missing_assets) or "substitute approvals complete",
                },
                {
                    "check_id": "scope_decision_asset_integrity",
                    "status": "warning" if association_gaps else "pass",
                    "summary": " | ".join(association_gaps) or "scope / decision / asset linkage complete",
                },
            ],
            "readiness_status": "ready_for_readiness_mapping",
            "non_claim_note": (
                "Current run is limited to readiness mapping; Step 2 gate output is advisory only and cannot be used as a formal claim gate."
            ),
            "not_ready_for_formal_claim": True,
            "ready_for_readiness_mapping": True,
            "reviewer_stub_only": True,
            "readiness_mapping_only": True,
            "not_released_for_formal_claim": True,
            "primary_evidence_rewritten": False,
            "scope_reference_assets": scope_reference_assets,
            "decision_rule_dependencies": decision_rule_dependencies,
            "linked_artifacts": {
                "scope_definition_pack": path_map["scope_definition_pack"],
                "decision_rule_profile": path_map["decision_rule_profile"],
                "reference_asset_registry": path_map["reference_asset_registry"],
                "certificate_lifecycle_summary": path_map["certificate_lifecycle_summary"],
                "certificate_readiness_summary": path_map["certificate_readiness_summary"],
            },
            "limitation_note": (
                "This gate is reviewer-facing / advisory / artifact-based only. "
                "It cannot open COM, drive devices, or enforce live equipment control."
            ),
        },
        digest=digest,
        filename=PRE_RUN_READINESS_GATE_FILENAME,
        markdown_filename=PRE_RUN_READINESS_GATE_MARKDOWN_FILENAME,
    )
    raw = dict(bundle.get("raw") or {})
    raw["evidence_source"] = "simulated"
    raw["review_surface"] = {
        **dict(raw.get("review_surface") or {}),
        "evidence_source_filters": ["simulated", "reviewer_readiness_only"],
    }
    bundle["raw"] = raw
    bundle["digest"] = dict(raw.get("digest") or {})
    bundle["markdown"] = _render_markdown(
        "Pre-run Readiness Gate",
        [
            f"- gate_status: {gate_status}",
            *[f"- blocking_item: {item}" for item in blocking_items],
            *[f"- warning_item: {item}" for item in warning_items],
            *[f"- reviewer_action: {item}" for item in reviewer_actions],
            f"- non_claim_note: {str(raw.get('non_claim_note') or '--')}",
        ],
    )
    return bundle


def _build_metrology_traceability_stub(
    *,
    run_id: str,
    reference_asset_registry: dict[str, Any],
    certificate_readiness_summary: dict[str, Any],
    path_map: dict[str, str],
) -> dict[str, Any]:
    assets = [dict(item) for item in list(dict(reference_asset_registry.get("raw") or {}).get("assets") or [])]
    standard_gas_asset = _find_asset_by_types(assets, "standard_gas")
    pressure_gauge_asset = _find_asset_by_types(assets, "digital_pressure_gauge")
    thermometer_asset = _find_asset_by_types(assets, "digital_thermometer", "thermometer")
    humidity_generator_asset = _find_asset_by_types(assets, "humidity_generator")
    dewpoint_asset = _find_asset_by_types(assets, "dewpoint_meter", "dew_point_meter")
    temperature_chamber_asset = _find_asset_by_types(assets, "temperature_chamber")
    chain_rows = [
        {
            "control_object": "gas route / CO2",
            "reference_asset": str(dict(standard_gas_asset).get("display_name") or "standard gas"),
            "supporting_assets": [
                str(dict(pressure_gauge_asset).get("display_name") or "digital pressure gauge"),
                str(dict(thermometer_asset).get("display_name") or "digital thermometer"),
            ],
            "readiness_status": "traceability_stub_only",
        },
        {
            "control_object": "water route / humidity",
            "reference_asset": str(dict(humidity_generator_asset).get("display_name") or "humidity generator"),
            "supporting_assets": [
                str(dict(dewpoint_asset).get("display_name") or "dew point meter"),
                str(dict(temperature_chamber_asset).get("display_name") or "temperature chamber"),
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


def build_method_confirmation_wp4_artifacts(
    *,
    run_id: str,
    scope_definition_pack: dict[str, Any],
    decision_rule_profile: dict[str, Any],
    reference_asset_registry: dict[str, Any],
    certificate_lifecycle_summary: dict[str, Any],
    pre_run_readiness_gate: dict[str, Any],
    budget_case: dict[str, Any],
    uncertainty_golden_cases: dict[str, Any],
    uncertainty_report_pack: dict[str, Any],
    uncertainty_digest: dict[str, Any],
    uncertainty_rollup: dict[str, Any],
    uncertainty_budget_stub: dict[str, Any],
    route_families: list[str],
    payload_backed_phases: list[str],
    trace_only_phases: list[str],
    gap_phases: list[str],
    path_map: dict[str, str],
) -> dict[str, dict[str, Any]]:
    scope_raw = dict(scope_definition_pack.get("raw") or {})
    decision_raw = dict(decision_rule_profile.get("raw") or {})
    reference_raw = dict(reference_asset_registry.get("raw") or {})
    certificate_raw = dict(certificate_lifecycle_summary.get("raw") or {})
    gate_raw = dict(pre_run_readiness_gate.get("raw") or {})
    budget_raw = dict(budget_case.get("raw") or {})
    golden_raw = dict(uncertainty_golden_cases.get("raw") or {})
    report_raw = dict(uncertainty_report_pack.get("raw") or {})
    digest_raw = dict(uncertainty_digest.get("raw") or {})
    rollup_raw = dict(uncertainty_rollup.get("raw") or {})
    scope_id = str(scope_raw.get("scope_id") or f"{run_id}-step2-scope-package")
    scope_name = str(scope_raw.get("scope_name") or "Step 2 simulation reviewer scope package")
    decision_rule_id = str(decision_raw.get("decision_rule_id") or "step2_readiness_reviewer_rule_v1")
    protocol_id = f"{run_id}-method-confirmation-protocol"
    protocol_version = "v1.2-step2-reviewer"
    validation_matrix_version = "v1.2-step2-validation-matrix"
    limitation_note = "当前仅生成 simulation-only / reviewer-facing / file-artifact-first 的方法确认骨架。"
    non_claim_note = "当前仅用于 readiness mapping，不构成 formal claim，也不是 real acceptance evidence。"
    reviewer_note = "当前协议、矩阵与 verification 仅面向 reviewer / engineer / quality 视图。"
    budget_cases = [dict(item) for item in list(budget_raw.get("budget_case") or budget_raw.get("budget_cases") or [])]
    golden_cases = [dict(item) for item in list(golden_raw.get("golden_cases") or [])]
    reference_assets = [dict(item) for item in list(reference_raw.get("assets") or [])]
    certificate_rows = [
        dict(item)
        for item in list(
            certificate_raw.get("certificate_rows")
            or certificate_raw.get("certificates")
            or certificate_raw.get("certificate_lifecycle_rows")
            or []
        )
    ]
    route_profiles = _method_confirmation_route_profiles(
        run_id=run_id,
        protocol_id=protocol_id,
        protocol_version=protocol_version,
        scope_id=scope_id,
        decision_rule_id=decision_rule_id,
        route_families=route_families,
        budget_cases=budget_cases,
        golden_cases=golden_cases,
        reference_assets=reference_assets,
        certificate_rows=certificate_rows,
        payload_backed_phases=payload_backed_phases,
        trace_only_phases=trace_only_phases,
        gap_phases=gap_phases,
        validation_matrix_version=validation_matrix_version,
    )
    validation_rows: list[dict[str, Any]] = []
    route_rollups: list[dict[str, Any]] = []
    validation_runs: list[dict[str, Any]] = []
    for profile in route_profiles:
        rows = _method_confirmation_validation_rows(profile=profile)
        validation_rows.extend(rows)
        route_rollups.append(_method_confirmation_route_rollup(profile=profile, rows=rows))
        validation_runs.append(
            _method_confirmation_validation_run(
                run_id=run_id,
                protocol_id=protocol_id,
                protocol_version=protocol_version,
                path_map=path_map,
                gate_raw=gate_raw,
                profile=profile,
            )
        )
    current_coverage = [
        str(item.get("current_evidence_coverage") or "").strip()
        for item in route_rollups
        if str(item.get("current_evidence_coverage") or "").strip()
    ]
    top_gaps = _dedupe(str(item.get("gap_note") or "").strip() for item in validation_rows if str(item.get("gap_note") or "").strip())[
        :6
    ]
    reviewer_actions = _dedupe(
        str(item.get("reviewer_action") or "").strip()
        for item in validation_rows
        if str(item.get("reviewer_action") or "").strip()
    )[:6]
    protocol_overview = (
        f"protocol {protocol_id} | scope {scope_id} | decision rule {decision_rule_id} | "
        f"routes {len(route_profiles)} | dimensions {len(METHOD_CONFIRMATION_VALIDATION_DIMENSIONS)}"
    )
    matrix_completeness_summary = " | ".join(
        str(item.get("matrix_completeness_summary") or "").strip()
        for item in route_rollups
        if str(item.get("matrix_completeness_summary") or "").strip()
    )
    current_coverage_summary = " | ".join(current_coverage)
    top_gaps_summary = " | ".join(top_gaps)
    reviewer_action_summary = " | ".join(reviewer_actions)
    readiness_status_summary = "ready_for_readiness_mapping | reviewer-only | simulated"
    scope_reference_assets_summary = " | ".join(
        _dedupe(str(item.get("asset_id") or "").strip() for item in reference_assets if str(item.get("asset_id") or "").strip())
    )
    decision_rule_dependency_summary = (
        f"{decision_rule_id} | uncertainty {str(report_raw.get('report_rule') or 'step2_readiness_mapping_only')}"
    )
    required_evidence_categories_summary = "scope / decision rule / reference assets / certificate lifecycle / pre-run gate / uncertainty"
    artifact_paths = _method_confirmation_artifact_paths(path_map)
    common_body = _method_confirmation_common_body(
        protocol_id=protocol_id,
        protocol_version=protocol_version,
        scope_id=scope_id,
        decision_rule_id=decision_rule_id,
        uncertainty_case_id=str(route_profiles[0].get("uncertainty_case_id") or "multi-route-readiness-set")
        if route_profiles
        else "multi-route-readiness-set",
        validation_matrix_version=validation_matrix_version,
        limitation_note=limitation_note,
        non_claim_note=non_claim_note,
        reviewer_note=reviewer_note,
        path_map=path_map,
    )
    base_digest = _method_confirmation_digest(
        summary="方法确认骨架 / reviewer-only / readiness mapping only",
        scope_name=scope_name,
        decision_rule_id=decision_rule_id,
        current_coverage_summary=current_coverage_summary,
        top_gaps_summary=top_gaps_summary,
        reviewer_action_summary=reviewer_action_summary,
        non_claim_note=non_claim_note,
        protocol_overview_summary=protocol_overview,
        matrix_completeness_summary=matrix_completeness_summary,
        readiness_status_summary=readiness_status_summary,
        scope_reference_assets_summary=scope_reference_assets_summary,
        decision_rule_dependency_summary=decision_rule_dependency_summary,
        required_evidence_categories_summary=required_evidence_categories_summary,
    )
    return _build_method_confirmation_wp4_bundle_set(
        run_id=run_id,
        base_digest=base_digest,
        common_body=common_body,
        route_profiles=route_profiles,
        validation_rows=validation_rows,
        route_rollups=route_rollups,
        validation_runs=validation_runs,
        protocol_overview=protocol_overview,
        matrix_completeness_summary=matrix_completeness_summary,
        current_coverage=current_coverage,
        current_coverage_summary=current_coverage_summary,
        top_gaps=top_gaps,
        top_gaps_summary=top_gaps_summary,
        reviewer_actions=reviewer_actions,
        reviewer_action_summary=reviewer_action_summary,
        readiness_status_summary=readiness_status_summary,
        non_claim_note=non_claim_note,
        artifact_paths=artifact_paths,
        path_map=path_map,
        rollup_raw=rollup_raw,
        digest_raw=digest_raw,
        uncertainty_budget_stub=uncertainty_budget_stub,
    )


def _method_confirmation_artifact_paths(path_map: dict[str, str]) -> dict[str, str]:
    return {
        "method_confirmation_protocol": path_map["method_confirmation_protocol"],
        "method_confirmation_protocol_markdown": path_map["method_confirmation_protocol_markdown"],
        "method_confirmation_matrix": path_map["method_confirmation_matrix"],
        "method_confirmation_matrix_markdown": path_map["method_confirmation_matrix_markdown"],
        "route_specific_validation_matrix": path_map["route_specific_validation_matrix"],
        "route_specific_validation_matrix_markdown": path_map["route_specific_validation_matrix_markdown"],
        "validation_run_set": path_map["validation_run_set"],
        "validation_run_set_markdown": path_map["validation_run_set_markdown"],
        "verification_digest": path_map["verification_digest"],
        "verification_digest_markdown": path_map["verification_digest_markdown"],
        "verification_rollup": path_map["verification_rollup"],
        "verification_rollup_markdown": path_map["verification_rollup_markdown"],
        "reference_asset_registry": path_map["reference_asset_registry"],
        "certificate_lifecycle_summary": path_map["certificate_lifecycle_summary"],
        "pre_run_readiness_gate": path_map["pre_run_readiness_gate"],
        "uncertainty_budget_stub": path_map["uncertainty_budget_stub"],
        "budget_case": path_map["budget_case"],
        "uncertainty_golden_cases": path_map["uncertainty_golden_cases"],
        "uncertainty_report_pack": path_map["uncertainty_report_pack"],
        "uncertainty_digest": path_map["uncertainty_digest"],
        "uncertainty_rollup": path_map["uncertainty_rollup"],
    }


def _method_confirmation_common_body(
    *,
    protocol_id: str,
    protocol_version: str,
    scope_id: str,
    decision_rule_id: str,
    uncertainty_case_id: str,
    validation_matrix_version: str,
    limitation_note: str,
    non_claim_note: str,
    reviewer_note: str,
    path_map: dict[str, str],
) -> dict[str, Any]:
    return {
        "protocol_id": protocol_id,
        "protocol_version": protocol_version,
        "scope_id": scope_id,
        "decision_rule_id": decision_rule_id,
        "uncertainty_case_id": uncertainty_case_id,
        "measurand": "multi-route",
        "route_type": "mixed",
        "environment_mode": "simulation_only",
        "analyzer_model": "step2-reviewer-placeholder",
        "validation_matrix_version": validation_matrix_version,
        "validation_dimensions": list(METHOD_CONFIRMATION_VALIDATION_DIMENSIONS),
        "validation_status": "reviewer_placeholder_only",
        "reviewer_only": True,
        "readiness_mapping_only": True,
        "ready_for_readiness_mapping": True,
        "not_real_acceptance_evidence": True,
        "not_ready_for_formal_claim": True,
        "primary_evidence_rewritten": False,
        "limitation_note": limitation_note,
        "non_claim_note": non_claim_note,
        "reviewer_note": reviewer_note,
        "required_evidence_categories": [
            "scope_definition_pack",
            "decision_rule_profile",
            "reference_asset_registry",
            "certificate_lifecycle_summary",
            "pre_run_readiness_gate",
            "uncertainty_report_pack",
            "verification_digest",
        ],
        "linked_artifacts": {
            "scope_definition_pack": path_map["scope_definition_pack"],
            "decision_rule_profile": path_map["decision_rule_profile"],
            "reference_asset_registry": path_map["reference_asset_registry"],
            "certificate_lifecycle_summary": path_map["certificate_lifecycle_summary"],
            "pre_run_readiness_gate": path_map["pre_run_readiness_gate"],
            "uncertainty_budget_stub": path_map["uncertainty_budget_stub"],
            "budget_case": path_map["budget_case"],
            "uncertainty_golden_cases": path_map["uncertainty_golden_cases"],
            "uncertainty_report_pack": path_map["uncertainty_report_pack"],
            "uncertainty_digest": path_map["uncertainty_digest"],
            "uncertainty_rollup": path_map["uncertainty_rollup"],
        },
    }


def _method_confirmation_digest(
    *,
    summary: str,
    scope_name: str,
    decision_rule_id: str,
    current_coverage_summary: str,
    top_gaps_summary: str,
    reviewer_action_summary: str,
    non_claim_note: str,
    protocol_overview_summary: str,
    matrix_completeness_summary: str,
    readiness_status_summary: str,
    scope_reference_assets_summary: str,
    decision_rule_dependency_summary: str,
    required_evidence_categories_summary: str,
) -> dict[str, Any]:
    return {
        "summary": summary,
        "scope_overview_summary": scope_name,
        "decision_rule_summary": f"{decision_rule_id} | reviewer dependency mapping only",
        "conformity_boundary_summary": non_claim_note,
        "current_coverage_summary": current_coverage_summary,
        "missing_evidence_summary": top_gaps_summary,
        "reviewer_action_summary": reviewer_action_summary,
        "reviewer_next_step_digest": reviewer_action_summary,
        "non_claim_digest": non_claim_note,
        "protocol_overview_summary": protocol_overview_summary,
        "matrix_completeness_summary": matrix_completeness_summary,
        "current_evidence_coverage_summary": current_coverage_summary,
        "top_gaps_summary": top_gaps_summary,
        "readiness_status_summary": readiness_status_summary,
        "scope_reference_assets_summary": scope_reference_assets_summary,
        "decision_rule_dependency_summary": decision_rule_dependency_summary,
        "required_evidence_categories_summary": required_evidence_categories_summary,
        "warning_summary": "reviewer-only / simulated / placeholder evidence rows",
    }


def _method_confirmation_bundle(
    *,
    run_id: str,
    artifact_type: str,
    filename: str,
    markdown_filename: str,
    artifact_role: str,
    title_text: str,
    summary_text: str,
    summary_lines: list[str],
    detail_lines: list[str],
    artifact_paths: dict[str, str],
    digest: dict[str, Any],
    body: dict[str, Any],
) -> dict[str, Any]:
    anchor = _artifact_anchor(artifact_type)
    raw = {
        "schema_version": "step2-method-confirmation-wp4-v1",
        "artifact_type": artifact_type,
        "generated_at": _now_iso(),
        "run_id": run_id,
        "artifact_role": artifact_role,
        "evidence_source": "simulated",
        "evidence_state": "reviewer_readiness_only",
        "not_real_acceptance_evidence": True,
        "boundary_statements": list(RECOGNITION_READINESS_BOUNDARY_STATEMENTS),
        "anchor_id": str(anchor.get("anchor_id") or ""),
        "anchor_label": str(anchor.get("anchor_label") or artifact_type),
        "artifact_paths": dict(artifact_paths),
        "digest": dict(digest),
        "review_surface": {
            "title_text": title_text,
            "role_text": artifact_role,
            "reviewer_note": str(body.get("reviewer_note") or ""),
            "summary_text": summary_text,
            "summary_lines": [line for line in summary_lines if str(line).strip()],
            "detail_lines": [line for line in detail_lines if str(line).strip()],
            "anchor_id": str(anchor.get("anchor_id") or ""),
            "anchor_label": str(anchor.get("anchor_label") or artifact_type),
            "phase_filters": ["step2_tail_recognition_ready"],
            "route_filters": [str(item.get("route_family") or "") for item in list(body.get("route_specific_protocols") or [])],
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
        "evidence_categories": ["recognition_readiness", "method_confirmation_readiness", "verification_readiness"],
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


def _build_method_confirmation_wp4_bundle_set(
    *,
    run_id: str,
    base_digest: dict[str, Any],
    common_body: dict[str, Any],
    route_profiles: list[dict[str, Any]],
    validation_rows: list[dict[str, Any]],
    route_rollups: list[dict[str, Any]],
    validation_runs: list[dict[str, Any]],
    protocol_overview: str,
    matrix_completeness_summary: str,
    current_coverage: list[str],
    current_coverage_summary: str,
    top_gaps: list[str],
    top_gaps_summary: str,
    reviewer_actions: list[str],
    reviewer_action_summary: str,
    readiness_status_summary: str,
    non_claim_note: str,
    artifact_paths: dict[str, str],
    path_map: dict[str, str],
    rollup_raw: dict[str, Any],
    digest_raw: dict[str, Any],
    uncertainty_budget_stub: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    matrix_body = {
        **common_body,
        "route_specific_protocols": route_profiles,
        "matrix_rows": validation_rows,
        "rows": validation_rows,
        "route_rollups": route_rollups,
        "current_evidence_coverage": current_coverage,
        "top_gaps": top_gaps,
        "reviewer_actions": reviewer_actions,
    }
    verification_body = {
        **common_body,
        "protocol_overview": protocol_overview,
        "route_rollups": route_rollups,
        "matrix_completeness_summary": matrix_completeness_summary,
        "current_evidence_coverage": current_coverage,
        "top_gaps": top_gaps,
        "reviewer_actions": reviewer_actions,
        "readiness_status": "ready_for_readiness_mapping",
    }
    protocol = _method_confirmation_bundle(
        run_id=run_id,
        artifact_type="method_confirmation_protocol",
        filename=METHOD_CONFIRMATION_PROTOCOL_FILENAME,
        markdown_filename=METHOD_CONFIRMATION_PROTOCOL_MARKDOWN_FILENAME,
        artifact_role="execution_summary",
        title_text="Method Confirmation Protocol",
        summary_text="方法确认协议骨架，仅用于 reviewer 侧 readiness mapping。",
        summary_lines=[
            f"protocol overview: {protocol_overview}",
            f"matrix completeness: {matrix_completeness_summary}",
            f"current evidence coverage: {current_coverage_summary}",
            f"reviewer actions: {reviewer_action_summary}",
        ],
        detail_lines=[
            f"linked uncertainty budget stub: {str(dict(uncertainty_budget_stub.get('digest') or {}).get('summary') or '--')}",
            f"linked route matrix: {path_map['route_specific_validation_matrix']}",
            f"linked verification digest: {path_map['verification_digest']}",
            f"non-claim: {non_claim_note}",
        ],
        artifact_paths=artifact_paths,
        digest={**base_digest, "summary": "方法确认协议骨架 / reviewer-only / readiness mapping only"},
        body={**common_body, "route_specific_protocols": route_profiles, "protocol_overview": protocol_overview},
    )
    legacy_matrix = _method_confirmation_bundle(
        run_id=run_id,
        artifact_type="method_confirmation_matrix",
        filename=METHOD_CONFIRMATION_MATRIX_FILENAME,
        markdown_filename=METHOD_CONFIRMATION_MATRIX_MARKDOWN_FILENAME,
        artifact_role="execution_summary",
        title_text="Method Confirmation Matrix",
        summary_text="方法确认矩阵仍为 placeholder/example evidence rows，不得解释为真实方法确认结论。",
        summary_lines=[
            f"protocol overview: {protocol_overview}",
            f"matrix completeness: {matrix_completeness_summary}",
            f"current evidence coverage: {current_coverage_summary}",
            f"top gaps: {top_gaps_summary}",
        ],
        detail_lines=[
            f"reviewer actions: {reviewer_action_summary}",
            f"linked uncertainty report: {path_map['uncertainty_report_pack']}",
            f"linked pre-run gate: {path_map['pre_run_readiness_gate']}",
            f"non-claim: {non_claim_note}",
        ],
        artifact_paths=artifact_paths,
        digest={**base_digest, "summary": "方法确认矩阵骨架 / reviewer-only / example evidence rows"},
        body=matrix_body,
    )
    route_matrix = _method_confirmation_bundle(
        run_id=run_id,
        artifact_type="route_specific_validation_matrix",
        filename=ROUTE_SPECIFIC_VALIDATION_MATRIX_FILENAME,
        markdown_filename=ROUTE_SPECIFIC_VALIDATION_MATRIX_MARKDOWN_FILENAME,
        artifact_role="execution_summary",
        title_text="Route Specific Validation Matrix",
        summary_text="CO2 气路、H2O 水路与 ambient/diagnostic mode 仅生成 reviewer-facing skeleton。",
        summary_lines=[
            f"protocol overview: {protocol_overview}",
            f"matrix completeness: {matrix_completeness_summary}",
            f"current evidence coverage: {current_coverage_summary}",
            f"readiness status: {readiness_status_summary}",
        ],
        detail_lines=[
            f"top gaps: {top_gaps_summary}",
            f"reviewer actions: {reviewer_action_summary}",
            f"golden linkage source: {path_map['uncertainty_golden_cases']}",
            f"non-claim: {non_claim_note}",
        ],
        artifact_paths=artifact_paths,
        digest={**base_digest, "summary": "route specific validation matrix / reviewer-only / simulated"},
        body={**matrix_body, "route_specific_validation_matrix": validation_rows},
    )
    validation_run_set = _method_confirmation_bundle(
        run_id=run_id,
        artifact_type="validation_run_set",
        filename=VALIDATION_RUN_SET_FILENAME,
        markdown_filename=VALIDATION_RUN_SET_MARKDOWN_FILENAME,
        artifact_role="execution_summary",
        title_text="Validation Run Set",
        summary_text="validation_run_set 与 golden linkage 仅保留 reviewer-only / simulated skeleton。",
        summary_lines=[
            f"protocol overview: {protocol_overview}",
            f"linked run ids: {run_id}",
            f"matrix completeness: {matrix_completeness_summary}",
            f"current evidence coverage: {current_coverage_summary}",
        ],
        detail_lines=[
            f"reference assets: {str(base_digest.get('scope_reference_assets_summary') or '--')}",
            f"certificate lifecycle: {path_map['certificate_lifecycle_summary']}",
            f"uncertainty refs: {path_map['uncertainty_report_pack']} | {path_map['uncertainty_rollup']}",
            f"non-claim: {non_claim_note}",
        ],
        artifact_paths=artifact_paths,
        digest={**base_digest, "summary": "validation run set / reviewer linkage only"},
        body={
            **common_body,
            "route_specific_protocols": route_profiles,
            "validation_run_set": validation_runs,
            "linked_run_ids": [run_id],
            "golden_dataset_id": str(route_profiles[0].get("golden_dataset_id") or "") if route_profiles else "",
        },
    )
    verification_digest = _method_confirmation_bundle(
        run_id=run_id,
        artifact_type="verification_digest",
        filename=VERIFICATION_DIGEST_FILENAME,
        markdown_filename=VERIFICATION_DIGEST_MARKDOWN_FILENAME,
        artifact_role="diagnostic_analysis",
        title_text="Verification Digest",
        summary_text="verification_digest 仅汇总 reviewer-facing 覆盖、缺口和动作，不能形成 formal claim。",
        summary_lines=[
            f"protocol overview: {protocol_overview}",
            f"matrix completeness: {matrix_completeness_summary}",
            f"current evidence coverage: {current_coverage_summary}",
            f"top gaps: {top_gaps_summary}",
            f"reviewer actions: {reviewer_action_summary}",
        ],
        detail_lines=[
            f"readiness status: {readiness_status_summary}",
            f"linked uncertainty digest: {str(digest_raw.get('summary') or '--')}",
            f"linked uncertainty rollup: {str(rollup_raw.get('rollup_summary_display') or '--')}",
            f"non-claim: {non_claim_note}",
        ],
        artifact_paths=artifact_paths,
        digest={**base_digest, "summary": "verification digest / reviewer-only / not formal claim"},
        body=verification_body,
    )
    verification_rollup = _method_confirmation_bundle(
        run_id=run_id,
        artifact_type="verification_rollup",
        filename=VERIFICATION_ROLLUP_FILENAME,
        markdown_filename=VERIFICATION_ROLLUP_MARKDOWN_FILENAME,
        artifact_role="diagnostic_analysis",
        title_text="Verification Rollup",
        summary_text="verification_rollup 汇总 method confirmation / validation matrix / golden linkage 状态，但仍保持 reviewer-only、simulation-only、non-claim。",
        summary_lines=[
            f"protocol overview: {protocol_overview}",
            f"matrix completeness: {matrix_completeness_summary}",
            f"current evidence coverage: {current_coverage_summary}",
            f"top gaps: {top_gaps_summary}",
            f"reviewer actions: {reviewer_action_summary}",
        ],
        detail_lines=[
            f"readiness status: {readiness_status_summary}",
            f"validation run set: {path_map['validation_run_set']}",
            f"linked uncertainty rollup: {path_map['uncertainty_rollup']}",
            f"non-claim: {non_claim_note}",
        ],
        artifact_paths=artifact_paths,
        digest={
            **base_digest,
            "summary": "verification rollup / reviewer-only / simulated",
            "rollup_summary_display": matrix_completeness_summary,
        },
        body={**verification_body, "rollup_summary_display": matrix_completeness_summary},
    )
    return {
        "method_confirmation_protocol": protocol,
        "method_confirmation_matrix": legacy_matrix,
        "route_specific_validation_matrix": route_matrix,
        "validation_run_set": validation_run_set,
        "verification_digest": verification_digest,
        "verification_rollup": verification_rollup,
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


def _method_confirmation_route_profiles(
    *,
    run_id: str,
    protocol_id: str,
    protocol_version: str,
    scope_id: str,
    decision_rule_id: str,
    route_families: list[str],
    budget_cases: list[dict[str, Any]],
    golden_cases: list[dict[str, Any]],
    reference_assets: list[dict[str, Any]],
    certificate_rows: list[dict[str, Any]],
    payload_backed_phases: list[str],
    trace_only_phases: list[str],
    gap_phases: list[str],
    validation_matrix_version: str,
) -> list[dict[str, Any]]:
    route_meta = {
        "gas": ("CO2 气路", "CO2", "gas", "simulation_only", "GC-CO2-step2-reviewer"),
        "water": ("H2O 水路", "H2O", "water", "simulation_only", "GC-H2O-step2-reviewer"),
        "ambient": ("ambient/diagnostic mode", "ambient_diagnostic", "ambient", "diagnostic_mode", "GC-ambient-step2-reviewer"),
    }
    rows: list[dict[str, Any]] = []
    for family in _dedupe([*list(route_families or []), "gas", "water", "ambient"]):
        if family not in route_meta:
            continue
        route_label, measurand, route_type, environment_mode, analyzer_model = route_meta[family]
        matched_case = next(
            (
                dict(item)
                for item in budget_cases
                if str(item.get("route_type") or "") == route_type
                and str(item.get("measurand") or "") == measurand
            ),
            {},
        )
        uncertainty_case_id = str(
            matched_case.get("uncertainty_case_id") or f"{run_id}-{family}-method-confirmation-placeholder"
        )
        matched_golden = next(
            (
                dict(item)
                for item in golden_cases
                if str(item.get("uncertainty_case_id") or "") == uncertainty_case_id
            ),
            {},
        )
        rows.append(
            {
                "route_family": family,
                "route_label": route_label,
                "protocol_id": protocol_id,
                "protocol_version": protocol_version,
                "scope_id": scope_id,
                "decision_rule_id": decision_rule_id,
                "uncertainty_case_id": uncertainty_case_id,
                "golden_dataset_id": str(
                    matched_golden.get("golden_dataset_id") or f"{uncertainty_case_id}-golden-dataset"
                ),
                "measurand": measurand,
                "route_type": route_type,
                "environment_mode": environment_mode,
                "analyzer_model": analyzer_model,
                "validation_matrix_version": validation_matrix_version,
                "reference_asset_ids": _method_confirmation_asset_refs(family, reference_assets),
                "certificate_lifecycle_refs": _method_confirmation_certificate_refs(family, certificate_rows),
                "payload_backed_phases": _method_confirmation_route_phases(family, payload_backed_phases),
                "trace_only_phases": _method_confirmation_route_phases(family, trace_only_phases),
                "gap_phases": _method_confirmation_route_phases(family, gap_phases),
            }
        )
    return rows


def _method_confirmation_route_phases(route_family: str, phases: list[str]) -> list[str]:
    prefix = f"{route_family}/"
    return [
        str(item).split("/", 1)[1]
        for item in list(phases or [])
        if str(item).strip().startswith(prefix) and "/" in str(item)
    ]


def _method_confirmation_asset_refs(route_family: str, assets: list[dict[str, Any]]) -> list[str]:
    allowed_types = {
        "gas": {"standard_gas", "pressure_controller", "pressure_gauge", "digital_pressure_gauge", "analyzer_under_test"},
        "water": {"humidity_generator", "dewpoint_meter", "temp_chamber", "temperature_chamber", "analyzer_under_test"},
        "ambient": {
            "thermometer",
            "digital_thermometer",
            "temp_chamber",
            "temperature_chamber",
            "pressure_gauge",
            "digital_pressure_gauge",
            "analyzer_under_test",
        },
    }.get(route_family, set())
    return _dedupe(
        str(item.get("asset_id") or "").strip()
        for item in assets
        if str(item.get("asset_type") or "").strip() in allowed_types and str(item.get("asset_id") or "").strip()
    )


def _method_confirmation_certificate_refs(route_family: str, certificate_rows: list[dict[str, Any]]) -> list[str]:
    allowed_types = {
        "gas": {"standard_gas", "pressure_controller", "pressure_gauge", "digital_pressure_gauge", "analyzer_under_test"},
        "water": {"humidity_generator", "dewpoint_meter", "temp_chamber", "temperature_chamber", "analyzer_under_test"},
        "ambient": {
            "thermometer",
            "digital_thermometer",
            "temp_chamber",
            "temperature_chamber",
            "pressure_gauge",
            "digital_pressure_gauge",
            "analyzer_under_test",
        },
    }.get(route_family, set())
    return _dedupe(
        str(item.get("certificate_id") or "").strip()
        for item in certificate_rows
        if str(item.get("asset_type") or "").strip() in allowed_types and str(item.get("certificate_id") or "").strip()
    )


def _method_confirmation_validation_rows(*, profile: dict[str, Any]) -> list[dict[str, Any]]:
    route_label = str(profile.get("route_label") or "--")
    payload_backed = list(profile.get("payload_backed_phases") or [])
    trace_only = list(profile.get("trace_only_phases") or [])
    gap_phases = list(profile.get("gap_phases") or [])
    rows: list[dict[str, Any]] = []
    for dimension in METHOD_CONFIRMATION_VALIDATION_DIMENSIONS:
        rows.append(
            {
                "dimension_key": dimension,
                "dimension_label": dimension,
                "protocol_id": str(profile.get("protocol_id") or f"{route_label}-reviewer-protocol"),
                "protocol_version": str(
                    profile.get("protocol_version") or profile.get("validation_matrix_version") or ""
                ),
                "scope_id": str(profile.get("scope_id") or ""),
                "decision_rule_id": str(profile.get("decision_rule_id") or ""),
                "uncertainty_case_id": str(profile.get("uncertainty_case_id") or ""),
                "measurand": str(profile.get("measurand") or ""),
                "route_type": str(profile.get("route_type") or ""),
                "environment_mode": str(profile.get("environment_mode") or ""),
                "analyzer_model": str(profile.get("analyzer_model") or ""),
                "validation_matrix_version": str(profile.get("validation_matrix_version") or ""),
                "validation_dimensions": list(METHOD_CONFIRMATION_VALIDATION_DIMENSIONS),
                "validation_status": "reviewer_placeholder_only",
                "reviewer_only": True,
                "readiness_mapping_only": True,
                "ready_for_readiness_mapping": True,
                "not_real_acceptance_evidence": True,
                "not_ready_for_formal_claim": True,
                "primary_evidence_rewritten": False,
                "route_label": route_label,
                "current_evidence_coverage": (
                    f"{route_label} | payload-backed {len(payload_backed)} | trace-only {len(trace_only)} | "
                    f"gap {len(gap_phases)} | placeholder/example evidence rows only"
                ),
                "gap_note": _method_confirmation_gap_note(route_label, dimension),
                "missing_evidence": _method_confirmation_gap_note(route_label, dimension),
                "reviewer_action": _method_confirmation_reviewer_action(route_label, dimension),
                "current_coverage": (
                    f"{route_label} | payload-backed {len(payload_backed)} | trace-only {len(trace_only)} | "
                    f"gap {len(gap_phases)} | placeholder/example evidence rows only"
                ),
                "limitation_note": "当前仅保留 reviewer-facing placeholder / skeleton / example evidence rows。",
                "non_claim_note": "不构成真实方法确认、formal claim 或 real acceptance evidence。",
                "reviewer_note": "仅用于 Step 2 reviewer-facing validation matrix 展示。",
            }
        )
    return rows


def _method_confirmation_route_rollup(*, profile: dict[str, Any], rows: list[dict[str, Any]]) -> dict[str, Any]:
    route_label = str(profile.get("route_label") or "--")
    return {
        "route_label": route_label,
        "route_type": str(profile.get("route_type") or ""),
        "measurand": str(profile.get("measurand") or ""),
        "matrix_completeness_summary": (
            f"{route_label} {len(rows)}/{len(METHOD_CONFIRMATION_VALIDATION_DIMENSIONS)} 个维度已生成 skeleton，占位证据全部保持 reviewer-only"
        ),
        "current_evidence_coverage": (
            f"{route_label} | payload-backed {len(list(profile.get('payload_backed_phases') or []))} | "
            f"trace-only {len(list(profile.get('trace_only_phases') or []))} | "
            f"gap {len(list(profile.get('gap_phases') or []))} | not real method confirmation"
        ),
    }


def _method_confirmation_gap_note(route_label: str, dimension: str) -> str:
    return {
        "linearity": f"{route_label} 缺少真实线性确认数据与 released reference closure。",
        "repeatability": f"{route_label} 缺少真实重复测量批次；当前仅保留 simulation example rows。",
        "reproducibility": f"{route_label} 尚未引入跨批次/跨环境真实 reproducibility 证据。",
        "drift": f"{route_label} 尚无时间分离的真实 drift closure；当前只有 reviewer digest skeleton。",
        "temperature_effect": f"{route_label} 温度影响仅做占位映射，未形成真实方法确认结论。",
        "pressure_effect": f"{route_label} 压力影响仍停留在 simulation-only mapping。",
        "route_switch_effect": f"{route_label} 路由切换影响仅留 placeholder，不代表已完成真实切换验证。",
        "seal_ingress_sensitivity": f"{route_label} 密封/渗入敏感度仍为 skeleton，不可视作真实 ingress confirmation。",
        "freshness_check": f"{route_label} freshness 仅映射 reviewer 检查项，未形成 released freshness evidence。",
        "writeback_verification": f"{route_label} writeback verification 仅保留 file-artifact-first 占位，不代表真实写回验证通过。",
    }[dimension]


def _method_confirmation_reviewer_action(route_label: str, dimension: str) -> str:
    return {
        "linearity": f"复核 {route_label} 的 scope / decision rule / uncertainty linkage，并继续保持 real method confirmation 关闭。",
        "repeatability": f"为 {route_label} 保留 reviewer 占位行，等待未来真实重复测量 acceptance 方案。",
        "reproducibility": f"补 reviewer-facing reproducibility skeleton，禁止写成 formal compliance claim。",
        "drift": f"把 {route_label} drift 与 historical trend / report pack 的 reviewer linkage 对齐。",
        "temperature_effect": f"把温度影响占位项与 certificate / pre-run gate / uncertainty refs 对齐。",
        "pressure_effect": f"把压力影响占位项与 pressure-related reference assets / uncertainty case 对齐。",
        "route_switch_effect": f"复核 {route_label} 路由切换 placeholder 行，仅输出 reviewer action，不给真实通过结论。",
        "seal_ingress_sensitivity": f"复核 {route_label} seal / ingress skeleton 与 preseal/readiness mapping 的边界提示。",
        "freshness_check": f"保留 freshness reviewer 检查项，继续禁止 real acceptance 解释。",
        "writeback_verification": f"把 writeback reviewer 占位项与 golden / report pack linkage 对齐，不改 primary evidence。",
    }[dimension]


def _method_confirmation_validation_run(
    *,
    run_id: str,
    protocol_id: str,
    protocol_version: str,
    path_map: dict[str, str],
    gate_raw: dict[str, Any],
    profile: dict[str, Any],
) -> dict[str, Any]:
    return {
        "validation_case_id": f"{protocol_id}-{str(profile.get('route_family') or 'route')}",
        "protocol_id": protocol_id,
        "protocol_version": protocol_version,
        "scope_id": str(profile.get("scope_id") or ""),
        "decision_rule_id": str(profile.get("decision_rule_id") or ""),
        "uncertainty_case_id": str(profile.get("uncertainty_case_id") or ""),
        "measurand": str(profile.get("measurand") or ""),
        "route_type": str(profile.get("route_type") or ""),
        "environment_mode": str(profile.get("environment_mode") or "simulation_only"),
        "analyzer_model": str(profile.get("analyzer_model") or "step2-reviewer-placeholder"),
        "validation_matrix_version": str(profile.get("validation_matrix_version") or ""),
        "validation_dimensions": list(METHOD_CONFIRMATION_VALIDATION_DIMENSIONS),
        "validation_status": "reviewer_placeholder_only",
        "reviewer_only": True,
        "readiness_mapping_only": True,
        "ready_for_readiness_mapping": True,
        "not_real_acceptance_evidence": True,
        "not_ready_for_formal_claim": True,
        "primary_evidence_rewritten": False,
        "golden_dataset_id": str(profile.get("golden_dataset_id") or ""),
        "linked_run_ids": [run_id],
        "linked_artifacts": {
            "method_confirmation_protocol": path_map["method_confirmation_protocol"],
            "route_specific_validation_matrix": path_map["route_specific_validation_matrix"],
            "validation_run_set": path_map["validation_run_set"],
            "verification_digest": path_map["verification_digest"],
            "verification_rollup": path_map["verification_rollup"],
        },
        "reference_assets": list(profile.get("reference_asset_ids") or []),
        "certificate_lifecycle_refs": list(profile.get("certificate_lifecycle_refs") or []),
        "pre_run_gate_refs": [
            str(gate_raw.get("gate_id") or f"{run_id}-pre-run-readiness-gate"),
            path_map["pre_run_readiness_gate"],
        ],
        "uncertainty_refs": {
            "uncertainty_budget_stub": path_map["uncertainty_budget_stub"],
            "budget_case": path_map["budget_case"],
            "uncertainty_golden_cases": path_map["uncertainty_golden_cases"],
            "uncertainty_report_pack": path_map["uncertainty_report_pack"],
            "uncertainty_digest": path_map["uncertainty_digest"],
            "uncertainty_rollup": path_map["uncertainty_rollup"],
            "uncertainty_case_id": str(profile.get("uncertainty_case_id") or ""),
        },
        "limitation_note": "当前 validation_run_set 仅保留 reviewer-only skeleton。",
        "non_claim_note": "当前仅用于 readiness mapping，不构成 formal claim，也不是 real acceptance evidence。",
        "reviewer_note": "当前 validation linkage 不会重写 primary evidence。",
    }
def _build_uncertainty_method_readiness_summary(
    *,
    run_id: str,
    uncertainty_budget_stub: dict[str, Any],
    method_confirmation_protocol: dict[str, Any],
    method_confirmation_matrix: dict[str, Any],
    route_specific_validation_matrix: dict[str, Any] | None = None,
    verification_digest: dict[str, Any] | None = None,
    path_map: dict[str, str],
) -> dict[str, Any]:
    route_matrix_raw = dict((route_specific_validation_matrix or {}).get("raw") or {})
    verification_digest_payload = dict((verification_digest or {}).get("digest") or {})
    matrix_rows = [
        dict(item)
        for item in list(
            route_matrix_raw.get("matrix_rows")
            or route_matrix_raw.get("rows")
            or dict(method_confirmation_matrix.get("raw") or {}).get("rows")
            or []
        )
    ]
    missing_evidence = _dedupe(str(item.get("missing_evidence") or "").strip() for item in matrix_rows)
    digest = {
        "summary": (
            "uncertainty / method confirmation readiness | "
            f"matrix rows {len(matrix_rows)} | missing evidence {len(missing_evidence)}"
        ),
        "budget_summary": str(dict(uncertainty_budget_stub.get("digest") or {}).get("summary") or "--"),
        "protocol_summary": str(dict(method_confirmation_protocol.get("digest") or {}).get("summary") or "--"),
        "matrix_summary": str(
            route_matrix_raw.get("matrix_completeness_summary")
            or dict((route_specific_validation_matrix or {}).get("digest") or {}).get("summary")
            or dict(method_confirmation_matrix.get("digest") or {}).get("summary")
            or "--"
        ),
        "missing_evidence_summary": " | ".join(missing_evidence),
        "verification_summary": str(verification_digest_payload.get("summary") or "--"),
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
            f"verification digest: {digest['verification_summary']}",
        ],
        detail_lines=[
            f"missing evidence: {digest['missing_evidence_summary']}",
            f"uncertainty_budget_stub: {path_map['uncertainty_budget_stub']}",
            f"method_confirmation_protocol: {path_map['method_confirmation_protocol']}",
            f"method_confirmation_matrix: {path_map['method_confirmation_matrix']}",
            f"route_specific_validation_matrix: {path_map['route_specific_validation_matrix']}",
            f"verification_digest: {path_map['verification_digest']}",
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
            "route_specific_validation_matrix": path_map["route_specific_validation_matrix"],
            "route_specific_validation_matrix_markdown": path_map["route_specific_validation_matrix_markdown"],
            "verification_digest": path_map["verification_digest"],
            "verification_digest_markdown": path_map["verification_digest_markdown"],
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
            "boundary_filter_rows": [],
            "boundary_filters": [],
            "non_claim_filter_rows": [],
            "non_claim_filters": [],
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
    linked_measurement_phase_artifacts = [
        normalize_phase_taxonomy_row(item, display_locale="en_US")
        for item in _measurement_phase_refs_for_artifact(
            phase_coverage_payload=phase_coverage_payload,
            artifact_type=artifact_type,
        )
    ]
    linked_measurement_phases = _dedupe(
        str(item.get("route_phase") or "").strip() for item in linked_measurement_phase_artifacts if str(item.get("route_phase") or "").strip()
    )
    linked_measurement_gaps = _linked_measurement_gap_rows(linked_measurement_phase_artifacts)
    linked_method_confirmation_items = _linked_value_summary(
        linked_measurement_phase_artifacts,
        "linked_method_confirmation_items",
    )
    linked_uncertainty_inputs = _linked_value_summary(
        linked_measurement_phase_artifacts,
        "linked_uncertainty_inputs",
    )
    linked_traceability_nodes = _linked_value_summary(
        linked_measurement_phase_artifacts,
        "linked_traceability_nodes",
    )
    linked_gap_classification_keys = normalize_taxonomy_keys(
        GAP_CLASSIFICATION_FAMILY,
        [item.get("gap_classification") for item in linked_measurement_gaps],
    )
    linked_gap_severity_keys = normalize_taxonomy_keys(
        GAP_SEVERITY_FAMILY,
        [item.get("gap_severity") for item in linked_measurement_gaps],
    )
    missing_evidence = _normalize_text_list(
        raw.get("missing_evidence")
        or _RECOGNITION_MISSING_EVIDENCE_DEFAULTS.get(artifact_type)
        or []
    )
    artifact_blocker_fragments = normalize_fragment_rows(
        BLOCKER_FRAGMENT_FAMILY,
        raw.get("blocker_fragments")
        or raw.get("blockers")
        or _RECOGNITION_BLOCKER_DEFAULTS.get(artifact_type)
        or [],
        display_locale="en_US",
    )
    artifact_blockers = fragment_rows_to_texts(artifact_blocker_fragments)
    next_required_artifacts = _normalize_text_list(
        raw.get("next_required_artifacts")
        or _RECOGNITION_NEXT_ARTIFACT_DEFAULTS.get(artifact_type)
        or []
    )
    measurement_blockers = _normalize_text_list(
        [
            f"{str(item.get('route_phase') or '--')}: {fragment_summary(item.get('blocker_fragments') or [], default=' | '.join(list(item.get('blockers') or [])) or '--')}"
            for item in linked_measurement_gaps
            if list(item.get("blocker_fragments") or []) or list(item.get("blockers") or [])
        ]
    )
    blockers = _normalize_text_list([*artifact_blockers, *measurement_blockers])
    next_required_artifacts = _normalize_text_list(
        [
            *next_required_artifacts,
            *(
                value
                for item in linked_measurement_gaps
                for value in list(item.get("next_required_artifacts") or [])
                if str(value).strip()
            ),
        ]
    )
    readiness_status = str(raw.get("readiness_status") or "").strip() or f"{artifact_type}_readiness_stub"
    linked_measurement_phase_summary = _phase_route_summary(linked_measurement_phase_artifacts)
    linked_measurement_gap_summary = _linked_measurement_gap_summary(linked_measurement_phase_artifacts)
    linked_readiness_impact_summary = _fragment_summary_by_route(
        linked_measurement_phase_artifacts,
        family=READINESS_IMPACT_FRAGMENT_FAMILY,
        fragment_field_name="readiness_impact_fragments",
        text_field_name="readiness_impact_digest",
        include_complete=False,
    )
    linked_method_confirmation_summary = _field_summary_from_phase_refs(
        linked_measurement_phase_artifacts,
        "linked_method_confirmation_items",
    )
    linked_uncertainty_input_summary = _field_summary_from_phase_refs(
        linked_measurement_phase_artifacts,
        "linked_uncertainty_inputs",
    )
    linked_traceability_node_summary = _field_summary_from_phase_refs(
        linked_measurement_phase_artifacts,
        "linked_traceability_nodes",
    )
    linked_gap_classification_summary = " | ".join(
        _dedupe(
            f"{str(item.get('route_phase') or '').strip()}: {str(item.get('gap_classification') or '').strip()}"
            for item in linked_measurement_gaps
            if str(item.get("route_phase") or "").strip() and str(item.get("gap_classification") or "").strip()
        )
    )
    linked_gap_severity_summary = " | ".join(
        _dedupe(
            f"{str(item.get('route_phase') or '').strip()}: {str(item.get('gap_severity') or '').strip()}"
            for item in linked_measurement_gaps
            if str(item.get("route_phase") or "").strip() and str(item.get("gap_severity") or "").strip()
        )
    )
    linked_gap_reason_fragments = [
        dict(fragment)
        for item in linked_measurement_gaps
        for fragment in list(item.get("gap_reason_fragments") or [])
        if isinstance(fragment, dict)
    ]
    linked_gap_reason_fragment_keys = fragment_rows_to_keys(linked_gap_reason_fragments)
    linked_blocker_fragments = [
        dict(fragment)
        for item in linked_measurement_gaps
        for fragment in list(item.get("blocker_fragments") or [])
        if isinstance(fragment, dict)
    ]
    linked_blocker_fragment_keys = fragment_rows_to_keys(linked_blocker_fragments)
    linked_reviewer_next_step_fragments = [
        dict(fragment)
        for item in linked_measurement_gaps
        for fragment in list(item.get("reviewer_next_step_fragments") or [])
        if isinstance(fragment, dict)
    ]
    linked_reviewer_next_step_fragment_keys = fragment_rows_to_keys(linked_reviewer_next_step_fragments)
    preseal_partial_gap_summary = _preseal_partial_gap_summary(
        artifact_type=artifact_type,
        linked_measurement_phase_artifacts=linked_measurement_phase_artifacts,
    )
    gap_reason_fragments = normalize_fragment_rows(
        GAP_REASON_FRAGMENT_FAMILY,
        list(raw.get("gap_reason_fragments") or [])
        or linked_gap_reason_fragments
        or raw.get("gap_reason")
        or [],
        display_locale="en_US",
    )
    reviewer_next_step_fragments = normalize_fragment_rows(
        REVIEWER_NEXT_STEP_FRAGMENT_FAMILY,
        list(raw.get("reviewer_next_step_fragments") or [])
        or linked_reviewer_next_step_fragments
        or raw.get("reviewer_next_step_digest")
        or [],
        display_locale="en_US",
    )
    gap_reason = linked_measurement_gap_summary or fragment_summary(
        gap_reason_fragments,
        default=str(raw.get("gap_reason") or "").strip(),
    )
    reviewer_next_step_digest = _linked_reviewer_next_step_summary(linked_measurement_gaps) or fragment_summary(
        reviewer_next_step_fragments,
        default=str(raw.get("reviewer_next_step_digest") or "").strip(),
    )
    linked_artifact_summary = " | ".join(
        _dedupe(str(item.get("artifact_type") or item.get("anchor_label") or "").strip() for item in linked_artifact_refs)
    )
    boundary_fragments = _normalize_boundary_fragments(
        list(raw.get("boundary_fragments") or []) or raw.get("boundary_statements") or []
    )
    boundary_digest = fragment_summary(
        boundary_fragments,
        default=" | ".join(str(item).strip() for item in list(raw.get("boundary_statements") or []) if str(item).strip()),
    )
    non_claim_fragments = _normalize_non_claim_fragments(
        list(raw.get("non_claim_fragments") or []) or raw.get("non_claim") or []
    )
    non_claim_digest = fragment_summary(
        non_claim_fragments,
        default=" | ".join(str(item).strip() for item in list(raw.get("non_claim") or []) if str(item).strip()),
    )
    boundary_filter_rows = normalize_fragment_filter_rows(
        BOUNDARY_FRAGMENT_FAMILY,
        boundary_fragments,
        display_locale="en_US",
    )
    non_claim_filter_rows = normalize_fragment_filter_rows(
        NON_CLAIM_FRAGMENT_FAMILY,
        non_claim_fragments,
        display_locale="en_US",
    )
    combined_boundary_filter_rows = _combined_fragment_filter_rows(
        boundary_filter_rows,
        non_claim_filter_rows,
    )
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
    if linked_readiness_impact_summary:
        digest["linked_readiness_impact_summary"] = linked_readiness_impact_summary
    if linked_method_confirmation_summary:
        digest["linked_method_confirmation_items_summary"] = linked_method_confirmation_summary
    if linked_uncertainty_input_summary:
        digest["linked_uncertainty_inputs_summary"] = linked_uncertainty_input_summary
    if linked_traceability_node_summary:
        digest["linked_traceability_nodes_summary"] = linked_traceability_node_summary
    if linked_gap_classification_summary:
        digest["linked_gap_classification_summary"] = linked_gap_classification_summary
    if linked_gap_severity_summary:
        digest["linked_gap_severity_summary"] = linked_gap_severity_summary
    if preseal_partial_gap_summary:
        digest["preseal_partial_gap_summary"] = preseal_partial_gap_summary
    if gap_reason:
        digest["gap_reason"] = gap_reason
    if reviewer_next_step_digest:
        digest["reviewer_next_step_digest"] = reviewer_next_step_digest
    if linked_artifact_summary:
        digest["linked_artifact_summary"] = linked_artifact_summary
    if boundary_digest:
        digest["boundary_digest"] = boundary_digest
    if non_claim_digest:
        digest["non_claim_digest"] = non_claim_digest
    raw["anchor_id"] = anchor_id
    raw["anchor_label"] = anchor_label
    raw["taxonomy_contract_version"] = TAXONOMY_CONTRACT_VERSION
    raw["reviewer_fragments_contract_version"] = REVIEWER_FRAGMENTS_CONTRACT_VERSION
    raw["linked_artifact_refs"] = linked_artifact_refs
    raw["linked_measurement_phase_artifacts"] = linked_measurement_phase_artifacts
    raw["linked_measurement_phases"] = linked_measurement_phases
    raw["linked_measurement_gaps"] = linked_measurement_gaps
    raw["linked_method_confirmation_items"] = linked_method_confirmation_items
    raw["linked_uncertainty_inputs"] = linked_uncertainty_inputs
    raw["linked_traceability_nodes"] = linked_traceability_nodes
    raw["linked_gap_classification_keys"] = linked_gap_classification_keys
    raw["linked_gap_severity_keys"] = linked_gap_severity_keys
    raw["linked_gap_reason_fragment_keys"] = linked_gap_reason_fragment_keys
    raw["linked_blocker_fragment_keys"] = linked_blocker_fragment_keys
    raw["linked_reviewer_next_step_fragment_keys"] = linked_reviewer_next_step_fragment_keys
    raw["linked_measurement_gap_summary"] = linked_measurement_gap_summary
    raw["linked_readiness_impact_summary"] = linked_readiness_impact_summary
    raw["preseal_partial_gap_summary"] = preseal_partial_gap_summary
    raw["gap_reason"] = gap_reason
    raw["gap_reason_fragments"] = gap_reason_fragments
    raw["gap_reason_fragment_keys"] = fragment_rows_to_keys(gap_reason_fragments) or linked_gap_reason_fragment_keys
    raw["missing_evidence"] = missing_evidence
    raw["blockers"] = blockers
    raw["blocker_fragments"] = artifact_blocker_fragments
    raw["blocker_fragment_keys"] = _dedupe([*fragment_rows_to_keys(artifact_blocker_fragments), *linked_blocker_fragment_keys])
    raw["next_required_artifacts"] = next_required_artifacts
    raw["readiness_status"] = readiness_status
    raw["boundary_fragments"] = boundary_fragments
    raw["boundary_fragment_keys"] = fragment_rows_to_keys(boundary_fragments)
    raw["boundary_digest"] = boundary_digest
    raw["boundary_filter_rows"] = combined_boundary_filter_rows
    raw["boundary_filters"] = fragment_filter_rows_to_ids(combined_boundary_filter_rows)
    raw["non_claim_fragments"] = non_claim_fragments
    raw["non_claim_fragment_keys"] = fragment_rows_to_keys(non_claim_fragments)
    raw["non_claim_digest"] = non_claim_digest
    raw["non_claim_filter_rows"] = non_claim_filter_rows
    raw["non_claim_filters"] = fragment_filter_rows_to_ids(non_claim_filter_rows)
    raw["reviewer_next_step_digest"] = reviewer_next_step_digest
    raw["reviewer_next_step_fragments"] = reviewer_next_step_fragments
    raw["reviewer_next_step_fragment_keys"] = fragment_rows_to_keys(reviewer_next_step_fragments) or linked_reviewer_next_step_fragment_keys
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
        review_surface["boundary_filter_rows"] = combined_boundary_filter_rows
        review_surface["boundary_filters"] = fragment_filter_rows_to_ids(combined_boundary_filter_rows)
        review_surface["non_claim_filter_rows"] = non_claim_filter_rows
        review_surface["non_claim_filters"] = fragment_filter_rows_to_ids(non_claim_filter_rows)
        review_surface["summary_lines"] = _merge_unique_lines(
            list(review_surface.get("summary_lines") or []),
            [
                f"readiness status: {readiness_status}",
                f"linked measurement phases: {linked_measurement_phase_summary}" if linked_measurement_phase_summary else "",
                f"linked measurement gaps: {linked_measurement_gap_summary}" if linked_measurement_gap_summary else "",
                f"readiness impact: {linked_readiness_impact_summary}" if linked_readiness_impact_summary else "",
                f"reviewer next step: {reviewer_next_step_digest}" if reviewer_next_step_digest else "",
                f"next required artifacts: {' | '.join(next_required_artifacts)}" if next_required_artifacts else "",
                f"boundary: {boundary_digest}" if boundary_digest else "",
            ],
        )
        review_surface["detail_lines"] = _merge_unique_lines(
            list(review_surface.get("detail_lines") or []),
            [
                f"linked artifacts: {linked_artifact_summary}" if linked_artifact_summary else "",
                f"linked measurement phases: {linked_measurement_phase_summary}" if linked_measurement_phase_summary else "",
                f"linked measurement gaps: {linked_measurement_gap_summary}" if linked_measurement_gap_summary else "",
                f"readiness impact: {linked_readiness_impact_summary}" if linked_readiness_impact_summary else "",
                f"linked method confirmation items: {linked_method_confirmation_summary}" if linked_method_confirmation_summary else "",
                f"linked uncertainty inputs: {linked_uncertainty_input_summary}" if linked_uncertainty_input_summary else "",
                f"linked traceability nodes: {linked_traceability_node_summary}" if linked_traceability_node_summary else "",
                f"preseal partial gap: {preseal_partial_gap_summary}" if preseal_partial_gap_summary else "",
                f"gap reason: {gap_reason}" if gap_reason else "",
                f"missing evidence: {' | '.join(missing_evidence)}" if missing_evidence else "",
                f"blockers: {' | '.join(blockers)}" if blockers else "",
                f"next required artifacts: {' | '.join(next_required_artifacts)}" if next_required_artifacts else "",
                f"reviewer next step: {reviewer_next_step_digest}" if reviewer_next_step_digest else "",
                f"boundary: {boundary_digest}" if boundary_digest else "",
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
            normalize_phase_taxonomy_row(
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
                    "gap_reason_fragments": [dict(item) for item in list(row.get("gap_reason_fragments") or []) if isinstance(item, dict)],
                    "gap_reason_fragment_keys": list(row.get("gap_reason_fragment_keys") or []),
                    "evidence_provenance": str(row.get("evidence_provenance") or "").strip(),
                    "readiness_impact_digest": str(row.get("readiness_impact_digest") or "").strip(),
                    "readiness_impact_fragments": [dict(item) for item in list(row.get("readiness_impact_fragments") or []) if isinstance(item, dict)],
                    "readiness_impact_fragment_keys": list(row.get("readiness_impact_fragment_keys") or []),
                    "gap_classification": str(row.get("gap_classification") or "").strip(),
                    "gap_severity": str(row.get("gap_severity") or "").strip(),
                    "linked_method_confirmation_items": list(row.get("linked_method_confirmation_items") or []),
                    "linked_method_confirmation_item_keys": list(row.get("linked_method_confirmation_item_keys") or []),
                    "linked_uncertainty_inputs": list(row.get("linked_uncertainty_inputs") or []),
                    "linked_uncertainty_input_keys": list(row.get("linked_uncertainty_input_keys") or []),
                    "linked_traceability_nodes": list(row.get("linked_traceability_nodes") or []),
                    "linked_traceability_node_keys": list(row.get("linked_traceability_node_keys") or []),
                    "linked_traceability_stub_nodes": list(row.get("linked_traceability_stub_nodes") or []),
                    "linked_readiness_summary": str(row.get("linked_readiness_summary") or "").strip(),
                    "blockers": list(row.get("blockers") or []),
                    "blocker_fragments": [dict(item) for item in list(row.get("blocker_fragments") or []) if isinstance(item, dict)],
                    "blocker_fragment_keys": list(row.get("blocker_fragment_keys") or []),
                    "next_required_artifacts": list(row.get("next_required_artifacts") or []),
                    "gap_reason": str(row.get("missing_reason_digest") or row.get("readiness_impact_digest") or "").strip(),
                    "reviewer_next_step_digest": str(row.get("reviewer_next_step_digest") or "").strip(),
                    "reviewer_next_step_fragments": [dict(item) for item in list(row.get("reviewer_next_step_fragments") or []) if isinstance(item, dict)],
                    "reviewer_next_step_fragment_keys": list(row.get("reviewer_next_step_fragment_keys") or []),
                    "reviewer_next_step_template_key": str(row.get("reviewer_next_step_template_key") or "").strip(),
                    "reviewer_guidance_digest": str(row.get("reviewer_guidance_digest") or "").strip(),
                    "comparison_digest": str(row.get("comparison_digest") or "").strip(),
                },
                display_locale="en_US",
            )
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
    gap_reason_summary = _fragment_summary_by_route(
        rows,
        family=GAP_REASON_FRAGMENT_FAMILY,
        fragment_field_name="gap_reason_fragments",
        text_field_name="missing_reason_digest",
        include_complete=False,
    )
    if gap_reason_summary:
        return gap_reason_summary
    return _fragment_summary_by_route(
        rows,
        family=READINESS_IMPACT_FRAGMENT_FAMILY,
        fragment_field_name="readiness_impact_fragments",
        text_field_name="readiness_impact_digest",
        include_complete=False,
    )


def _linked_measurement_gap_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    gap_rows: list[dict[str, Any]] = []
    for item in rows:
        payload = normalize_phase_taxonomy_row(dict(item or {}), display_locale="en_US")
        if str(payload.get("coverage_bucket") or "").strip() == "actual_simulated_run_with_payload_complete":
            continue
        route_phase = str(payload.get("route_phase") or "").strip()
        if not route_phase:
            continue
        gap_rows.append(
            {
                "route_phase": route_phase,
                "coverage_bucket": str(payload.get("coverage_bucket") or "").strip(),
                "coverage_bucket_display": str(
                    payload.get("coverage_bucket_display") or payload.get("coverage_bucket") or ""
                ).strip(),
                "gap_classification": str(payload.get("gap_classification") or "").strip(),
                "gap_classification_label": str(payload.get("gap_classification_label") or "").strip(),
                "gap_severity": str(payload.get("gap_severity") or "").strip(),
                "gap_severity_label": str(payload.get("gap_severity_label") or "").strip(),
                "missing_signal_layers": list(payload.get("missing_signal_layers") or []),
                "gap_reason": str(
                    payload.get("gap_reason")
                    or payload.get("missing_reason_digest")
                    or payload.get("readiness_impact_digest")
                    or ""
                ).strip(),
                "gap_reason_fragments": [dict(item) for item in list(payload.get("gap_reason_fragments") or []) if isinstance(item, dict)],
                "gap_reason_fragment_keys": list(payload.get("gap_reason_fragment_keys") or []),
                "readiness_impact_digest": str(payload.get("readiness_impact_digest") or "").strip(),
                "readiness_impact_fragments": [dict(item) for item in list(payload.get("readiness_impact_fragments") or []) if isinstance(item, dict)],
                "readiness_impact_fragment_keys": list(payload.get("readiness_impact_fragment_keys") or []),
                "linked_method_confirmation_item_keys": list(payload.get("linked_method_confirmation_item_keys") or []),
                "linked_method_confirmation_items": list(payload.get("linked_method_confirmation_items") or []),
                "linked_uncertainty_input_keys": list(payload.get("linked_uncertainty_input_keys") or []),
                "linked_uncertainty_inputs": list(payload.get("linked_uncertainty_inputs") or []),
                "linked_traceability_node_keys": list(payload.get("linked_traceability_node_keys") or []),
                "linked_traceability_nodes": list(payload.get("linked_traceability_nodes") or []),
                "blockers": list(payload.get("blockers") or []),
                "blocker_fragments": [dict(item) for item in list(payload.get("blocker_fragments") or []) if isinstance(item, dict)],
                "blocker_fragment_keys": list(payload.get("blocker_fragment_keys") or []),
                "next_required_artifacts": list(payload.get("next_required_artifacts") or []),
                "reviewer_next_step_digest": str(payload.get("reviewer_next_step_digest") or "").strip(),
                "reviewer_next_step_fragments": [dict(item) for item in list(payload.get("reviewer_next_step_fragments") or []) if isinstance(item, dict)],
                "reviewer_next_step_fragment_keys": list(payload.get("reviewer_next_step_fragment_keys") or []),
                "reviewer_next_step_template_key": str(payload.get("reviewer_next_step_template_key") or "").strip(),
            }
        )
    return gap_rows


def _field_summary_from_phase_refs(rows: list[dict[str, Any]], field_name: str) -> str:
    return " | ".join(
        _dedupe(
            (
                f"{str(item.get('route_phase') or '').strip()}: "
                f"{', '.join(list(item.get(field_name) or []))}"
            )
            for item in rows
            if str(item.get("route_phase") or "").strip() and list(item.get(field_name) or [])
        )
    )


def _linked_value_summary(rows: list[dict[str, Any]], field_name: str) -> list[str]:
    return _dedupe(
        str(value).strip()
        for item in rows
        for value in list(dict(item or {}).get(field_name) or [])
        if str(value).strip()
    )


def _fragment_summary_by_route(
    rows: list[dict[str, Any]],
    *,
    family: str,
    fragment_field_name: str,
    text_field_name: str,
    include_complete: bool = True,
) -> str:
    return " | ".join(
        _dedupe(
            (
                f"{str(item.get('route_phase') or '').strip()}: "
                f"{fragment_summary(item.get(fragment_field_name) or [], default=str(item.get(text_field_name) or '').strip() or '--')}"
            )
            for item in rows
            if str(item.get("route_phase") or "").strip()
            and (include_complete or str(item.get("coverage_bucket") or "").strip() != "actual_simulated_run_with_payload_complete")
            and (
                list(item.get(fragment_field_name) or [])
                or str(item.get(text_field_name) or "").strip()
            )
        )
    )


def _fragment_text_list_from_rows(
    rows: list[dict[str, Any]],
    *,
    family: str,
    fragment_field_name: str,
    text_field_name: str,
) -> list[str]:
    values: list[str] = []
    for item in rows:
        fragment_rows = normalize_fragment_rows(
            family,
            list(item.get(fragment_field_name) or []) or _normalize_text_list(item.get(text_field_name)),
            display_locale="en_US",
        )
        for text in fragment_rows_to_texts(fragment_rows):
            if text and text not in values:
                values.append(text)
    return values


def _linked_reviewer_next_step_summary(rows: list[dict[str, Any]]) -> str:
    return " | ".join(
        _dedupe(
            fragment_summary(
                item.get("reviewer_next_step_fragments") or [],
                default=str(item.get("reviewer_next_step_digest") or "").strip(),
            )
            for item in rows
            if list(item.get("reviewer_next_step_fragments") or []) or str(item.get("reviewer_next_step_digest") or "").strip()
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
    "metrology_traceability_stub": "traceability nodes stay stub-only while preseal conditioning evidence is not tied to released output criteria",
    "uncertainty_budget_stub": "uncertainty inputs stay stub-only while preseal output terms remain open",
    "method_confirmation_protocol": "method protocol steps stay reviewer-only while preseal remains setup evidence",
    "method_confirmation_matrix": "method rows stay open because preseal evidence does not yet close released output terms",
    "route_specific_validation_matrix": "route-specific validation rows stay reviewer-only while preseal remains a partial conditioning window",
    "validation_run_set": "validation linkage stays skeleton-only while preseal evidence does not close released output terms",
    "verification_digest": "verification digest remains reviewer-facing while preseal still carries open readiness gaps",
    "verification_rollup": "verification rollup stays non-claim while preseal remains a partial conditioning window",
    "uncertainty_method_readiness_summary": "uncertainty + method readiness stays open while preseal remains a partial conditioning window",
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


def _normalize_boundary_fragments(values: Any) -> list[dict[str, Any]]:
    return normalize_fragment_rows(
        BOUNDARY_FRAGMENT_FAMILY,
        values,
        display_locale="en_US",
    )


def _normalize_non_claim_fragments(values: Any) -> list[dict[str, Any]]:
    return normalize_fragment_rows(
        NON_CLAIM_FRAGMENT_FAMILY,
        values,
        display_locale="en_US",
    )


def _combined_fragment_filter_rows(*row_groups: Iterable[dict[str, Any]] | None) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row_group in row_groups:
        for item in list(row_group or []):
            payload = dict(item or {})
            canonical_fragment_id = str(
                payload.get("canonical_fragment_id")
                or payload.get("id")
                or ""
            ).strip()
            if not canonical_fragment_id or canonical_fragment_id in seen:
                continue
            seen.add(canonical_fragment_id)
            rows.append(payload)
    return rows


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
        f"- linked_method_confirmation_items: {' | '.join(list(raw.get('linked_method_confirmation_items') or [])) or '--'}",
        f"- linked_uncertainty_inputs: {' | '.join(list(raw.get('linked_uncertainty_inputs') or [])) or '--'}",
        f"- linked_traceability_nodes: {' | '.join(list(raw.get('linked_traceability_nodes') or [])) or '--'}",
        f"- preseal_partial_gap: {str(raw.get('preseal_partial_gap_summary') or '--')}",
        f"- gap_reason: {str(raw.get('gap_reason') or '--')}",
        f"- missing_evidence: {' | '.join(list(raw.get('missing_evidence') or [])) or '--'}",
        f"- blockers: {' | '.join(list(raw.get('blockers') or [])) or '--'}",
        f"- next_required_artifacts: {' | '.join(list(raw.get('next_required_artifacts') or [])) or '--'}",
        f"- reviewer_next_step: {str(raw.get('reviewer_next_step_digest') or '--')}",
        f"- boundary_digest: {str(raw.get('boundary_digest') or '--')}",
        f"- non_claim_digest: {str(raw.get('non_claim_digest') or '--')}",
    ]
    return (base + "\n" + "\n".join(lines).rstrip() + "\n").lstrip()


def _asset_row(
    *,
    asset_id: str,
    asset_name: str,
    asset_type: str,
    role_in_reference_chain: str,
    manufacturer: str = "",
    model: str = "",
    serial_or_lot: str = "",
    measurand_scope: list[str] | None = None,
    route_scope: list[str] | None = None,
    environment_scope: list[str] | None = None,
    owner_state: str = "reviewer_managed_stub",
    active_state: str = "active_for_reviewer_mapping",
    quarantine_state: str = "not_quarantined",
    certificate_status: str = "missing_certificate",
    certificate_id: str = "",
    certificate_version: str = "stub-v1",
    valid_from: str = "",
    valid_to: str = "",
    expiry_status: str = "unknown_expiry",
    intermediate_check_status: str = "missing_intermediate_check",
    intermediate_check_due: str = "",
    last_check_at: str = "",
    released_for_formal_claim: bool = False,
    ready_for_readiness_mapping: bool = True,
    not_real_acceptance_evidence: bool = True,
    evidence_source: str = "simulated",
    limitation_note: str = "",
    non_claim_note: str = "",
    reviewer_note: str = "",
    current_stage_usage: str = "registry_stub_only",
    linked_run_artifacts: list[str] | None = None,
    linked_scope_ids: list[str] | None = None,
    linked_scope_names: list[str] | None = None,
    linked_decision_rule_ids: list[str] | None = None,
    lot_binding_required: bool = False,
    lot_binding_status: str = "not_required",
    lot_binding_ids: list[str] | None = None,
    substitute_standard_chain_required: bool = False,
    substitute_standard_chain_approval_status: str = "not_required",
    substitute_standard_chain_approval_ref: str = "",
    readiness_status: str = "",
    asset_type_aliases: list[str] | None = None,
) -> dict[str, Any]:
    final_readiness_status = readiness_status or (
        "blocked_for_formal_claim"
        if certificate_status in {"missing_certificate", "expired_certificate"}
        or intermediate_check_status in {"missing_intermediate_check", "overdue"}
        else "ready_for_readiness_mapping"
    )
    return {
        "asset_id": asset_id,
        "asset_name": asset_name,
        "asset_type": asset_type,
        "asset_type_aliases": list(asset_type_aliases or []),
        "manufacturer": manufacturer,
        "model": model,
        "serial_or_lot": serial_or_lot,
        "role_in_reference_chain": role_in_reference_chain,
        "measurand_scope": list(measurand_scope or []),
        "route_scope": list(route_scope or []),
        "environment_scope": list(environment_scope or []),
        "owner_state": owner_state,
        "active_state": active_state,
        "quarantine_state": quarantine_state,
        "certificate_status": certificate_status,
        "certificate_id": certificate_id,
        "certificate_version": certificate_version,
        "valid_from": valid_from,
        "valid_to": valid_to,
        "expiry_status": expiry_status,
        "intermediate_check_status": intermediate_check_status,
        "intermediate_check_due": intermediate_check_due,
        "last_check_at": last_check_at,
        "released_for_formal_claim": released_for_formal_claim,
        "ready_for_readiness_mapping": ready_for_readiness_mapping,
        "not_real_acceptance_evidence": not_real_acceptance_evidence,
        "evidence_source": evidence_source,
        "limitation_note": limitation_note,
        "non_claim_note": non_claim_note,
        "reviewer_note": reviewer_note,
        "display_name": asset_name,
        "role": role_in_reference_chain,
        "vendor": manufacturer,
        "serial": serial_or_lot,
        "current_stage_usage": current_stage_usage,
        "linked_run_artifacts": list(linked_run_artifacts or []),
        "linked_scope_ids": list(linked_scope_ids or []),
        "linked_scope_names": list(linked_scope_names or []),
        "linked_decision_rule_ids": list(linked_decision_rule_ids or []),
        "lot_binding_required": lot_binding_required,
        "lot_binding_status": lot_binding_status,
        "lot_binding_ids": list(lot_binding_ids or []),
        "substitute_standard_chain_required": substitute_standard_chain_required,
        "substitute_standard_chain_approval_status": substitute_standard_chain_approval_status,
        "substitute_standard_chain_approval_ref": substitute_standard_chain_approval_ref,
        "readiness_status": final_readiness_status,
        "non_claim": non_claim_note or "readiness registry only",
    }


def _find_asset_by_types(assets: list[dict[str, Any]], *asset_types: str) -> dict[str, Any]:
    wanted = {str(item).strip() for item in asset_types if str(item).strip()}
    for row in assets:
        asset_type = str(row.get("asset_type") or "").strip()
        aliases = {str(item).strip() for item in list(row.get("asset_type_aliases") or []) if str(item).strip()}
        if asset_type in wanted or wanted & aliases:
            return dict(row)
    return {}


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
        "certificate_lifecycle_summary": CERTIFICATE_LIFECYCLE_SUMMARY_FILENAME,
        "certificate_lifecycle_summary_markdown": CERTIFICATE_LIFECYCLE_SUMMARY_MARKDOWN_FILENAME,
        "certificate_readiness_summary": CERTIFICATE_READINESS_SUMMARY_FILENAME,
        "certificate_readiness_summary_markdown": CERTIFICATE_READINESS_SUMMARY_MARKDOWN_FILENAME,
        "pre_run_readiness_gate": PRE_RUN_READINESS_GATE_FILENAME,
        "pre_run_readiness_gate_markdown": PRE_RUN_READINESS_GATE_MARKDOWN_FILENAME,
        "metrology_traceability_stub": METROLOGY_TRACEABILITY_STUB_FILENAME,
        "metrology_traceability_stub_markdown": METROLOGY_TRACEABILITY_STUB_MARKDOWN_FILENAME,
        "uncertainty_model": UNCERTAINTY_MODEL_FILENAME,
        "uncertainty_model_markdown": UNCERTAINTY_MODEL_MARKDOWN_FILENAME,
        "uncertainty_input_set": UNCERTAINTY_INPUT_SET_FILENAME,
        "uncertainty_input_set_markdown": UNCERTAINTY_INPUT_SET_MARKDOWN_FILENAME,
        "sensitivity_coefficient_set": SENSITIVITY_COEFFICIENT_SET_FILENAME,
        "sensitivity_coefficient_set_markdown": SENSITIVITY_COEFFICIENT_SET_MARKDOWN_FILENAME,
        "budget_case": BUDGET_CASE_FILENAME,
        "budget_case_markdown": BUDGET_CASE_MARKDOWN_FILENAME,
        "uncertainty_golden_cases": UNCERTAINTY_GOLDEN_CASES_FILENAME,
        "uncertainty_golden_cases_markdown": UNCERTAINTY_GOLDEN_CASES_MARKDOWN_FILENAME,
        "uncertainty_report_pack": UNCERTAINTY_REPORT_PACK_FILENAME,
        "uncertainty_report_pack_markdown": UNCERTAINTY_REPORT_PACK_MARKDOWN_FILENAME,
        "uncertainty_digest": UNCERTAINTY_DIGEST_FILENAME,
        "uncertainty_digest_markdown": UNCERTAINTY_DIGEST_MARKDOWN_FILENAME,
        "uncertainty_rollup": UNCERTAINTY_ROLLUP_FILENAME,
        "uncertainty_rollup_markdown": UNCERTAINTY_ROLLUP_MARKDOWN_FILENAME,
        "uncertainty_budget_stub": UNCERTAINTY_BUDGET_STUB_FILENAME,
        "uncertainty_budget_stub_markdown": UNCERTAINTY_BUDGET_STUB_MARKDOWN_FILENAME,
        "method_confirmation_protocol": METHOD_CONFIRMATION_PROTOCOL_FILENAME,
        "method_confirmation_protocol_markdown": METHOD_CONFIRMATION_PROTOCOL_MARKDOWN_FILENAME,
        "method_confirmation_matrix": METHOD_CONFIRMATION_MATRIX_FILENAME,
        "method_confirmation_matrix_markdown": METHOD_CONFIRMATION_MATRIX_MARKDOWN_FILENAME,
        "route_specific_validation_matrix": ROUTE_SPECIFIC_VALIDATION_MATRIX_FILENAME,
        "route_specific_validation_matrix_markdown": ROUTE_SPECIFIC_VALIDATION_MATRIX_MARKDOWN_FILENAME,
        "validation_run_set": VALIDATION_RUN_SET_FILENAME,
        "validation_run_set_markdown": VALIDATION_RUN_SET_MARKDOWN_FILENAME,
        "verification_digest": VERIFICATION_DIGEST_FILENAME,
        "verification_digest_markdown": VERIFICATION_DIGEST_MARKDOWN_FILENAME,
        "verification_rollup": VERIFICATION_ROLLUP_FILENAME,
        "verification_rollup_markdown": VERIFICATION_ROLLUP_MARKDOWN_FILENAME,
        "uncertainty_method_readiness_summary": UNCERTAINTY_METHOD_READINESS_SUMMARY_FILENAME,
        "uncertainty_method_readiness_summary_markdown": UNCERTAINTY_METHOD_READINESS_SUMMARY_MARKDOWN_FILENAME,
        "software_validation_traceability_matrix": SOFTWARE_VALIDATION_TRACEABILITY_MATRIX_FILENAME,
        "software_validation_traceability_matrix_markdown": SOFTWARE_VALIDATION_TRACEABILITY_MATRIX_MARKDOWN_FILENAME,
        "requirement_design_code_test_links": REQUIREMENT_DESIGN_CODE_TEST_LINKS_FILENAME,
        "requirement_design_code_test_links_markdown": REQUIREMENT_DESIGN_CODE_TEST_LINKS_MARKDOWN_FILENAME,
        "validation_evidence_index": VALIDATION_EVIDENCE_INDEX_FILENAME,
        "validation_evidence_index_markdown": VALIDATION_EVIDENCE_INDEX_MARKDOWN_FILENAME,
        "change_impact_summary": CHANGE_IMPACT_SUMMARY_FILENAME,
        "change_impact_summary_markdown": CHANGE_IMPACT_SUMMARY_MARKDOWN_FILENAME,
        "rollback_readiness_summary": ROLLBACK_READINESS_SUMMARY_FILENAME,
        "rollback_readiness_summary_markdown": ROLLBACK_READINESS_SUMMARY_MARKDOWN_FILENAME,
        "artifact_hash_registry": ARTIFACT_HASH_REGISTRY_FILENAME,
        "artifact_hash_registry_markdown": ARTIFACT_HASH_REGISTRY_MARKDOWN_FILENAME,
        "audit_event_store": AUDIT_EVENT_STORE_FILENAME,
        "audit_event_store_markdown": AUDIT_EVENT_STORE_MARKDOWN_FILENAME,
        "environment_fingerprint": ENVIRONMENT_FINGERPRINT_FILENAME,
        "environment_fingerprint_markdown": ENVIRONMENT_FINGERPRINT_MARKDOWN_FILENAME,
        "config_fingerprint": CONFIG_FINGERPRINT_FILENAME,
        "config_fingerprint_markdown": CONFIG_FINGERPRINT_MARKDOWN_FILENAME,
        "release_input_digest": RELEASE_INPUT_DIGEST_FILENAME,
        "release_input_digest_markdown": RELEASE_INPUT_DIGEST_MARKDOWN_FILENAME,
        "release_manifest": RELEASE_MANIFEST_FILENAME,
        "release_manifest_markdown": RELEASE_MANIFEST_MARKDOWN_FILENAME,
        "release_scope_summary": RELEASE_SCOPE_SUMMARY_FILENAME,
        "release_scope_summary_markdown": RELEASE_SCOPE_SUMMARY_MARKDOWN_FILENAME,
        "release_boundary_digest": RELEASE_BOUNDARY_DIGEST_FILENAME,
        "release_boundary_digest_markdown": RELEASE_BOUNDARY_DIGEST_MARKDOWN_FILENAME,
        "release_evidence_pack_index": RELEASE_EVIDENCE_PACK_INDEX_FILENAME,
        "release_evidence_pack_index_markdown": RELEASE_EVIDENCE_PACK_INDEX_MARKDOWN_FILENAME,
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
