from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable, Iterable, Optional

from ..config import build_step2_config_governance_handoff
from ..core.acceptance_model import normalize_evidence_source
from ..core.artifact_catalog import KNOWN_REPORT_ARTIFACTS
from ..core.engineering_isolation_admission_checklist import (
    ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_FILENAME,
    ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_REVIEWER_FILENAME,
)
from ..core.engineering_isolation_admission_checklist_artifact_entry import (
    ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_ARTIFACT_KEY,
    ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_REVIEWER_ARTIFACT_KEY,
    build_engineering_isolation_admission_checklist_artifact_entry,
)
from ..core.controlled_state_machine_profile import (
    STATE_TRANSITION_EVIDENCE_FILENAME,
    STATE_TRANSITION_EVIDENCE_MARKDOWN_FILENAME,
)
from ..core.multi_source_stability import (
    MULTI_SOURCE_STABILITY_EVIDENCE_FILENAME,
    MULTI_SOURCE_STABILITY_EVIDENCE_MARKDOWN_FILENAME,
    SIMULATION_EVIDENCE_SIDECAR_BUNDLE_FILENAME,
)
from ..core.measurement_phase_coverage import (
    MEASUREMENT_PHASE_COVERAGE_REPORT_FILENAME,
    MEASUREMENT_PHASE_COVERAGE_REPORT_MARKDOWN_FILENAME,
)
from ..core.artifact_compatibility import (
    ARTIFACT_CONTRACT_CATALOG_FILENAME,
    ARTIFACT_CONTRACT_CATALOG_MARKDOWN_FILENAME,
    COMPATIBILITY_SCAN_SUMMARY_FILENAME,
    COMPATIBILITY_SCAN_SUMMARY_MARKDOWN_FILENAME,
    REINDEX_MANIFEST_FILENAME,
    REINDEX_MANIFEST_MARKDOWN_FILENAME,
    RUN_ARTIFACT_INDEX_FILENAME,
    RUN_ARTIFACT_INDEX_MARKDOWN_FILENAME,
    load_or_build_artifact_compatibility_payloads,
)
from ..core import recognition_readiness_artifacts as recognition_readiness
from ..core.offline_artifacts import build_point_taxonomy_handoff, summarize_offline_diagnostic_adapters
from ..core.phase_transition_bridge_reviewer_artifact_entry import (
    PHASE_TRANSITION_BRIDGE_REVIEWER_ARTIFACT_KEY,
    build_phase_transition_bridge_reviewer_artifact_entry,
)
from ..core.phase_transition_bridge_reviewer_artifact import PHASE_TRANSITION_BRIDGE_REVIEWER_FILENAME
from ..core.stage_admission_review_pack import (
    STAGE_ADMISSION_REVIEW_PACK_FILENAME,
    STAGE_ADMISSION_REVIEW_PACK_REVIEWER_FILENAME,
)
from ..core.stage_admission_review_pack_artifact_entry import (
    STAGE_ADMISSION_REVIEW_PACK_ARTIFACT_KEY,
    STAGE_ADMISSION_REVIEW_PACK_REVIEWER_ARTIFACT_KEY,
    build_stage_admission_review_pack_artifact_entry,
)
from ..core.stage3_real_validation_plan import (
    STAGE3_REAL_VALIDATION_PLAN_FILENAME,
    STAGE3_REAL_VALIDATION_PLAN_REVIEWER_FILENAME,
)
from ..core.stage3_real_validation_plan_artifact_entry import (
    STAGE3_REAL_VALIDATION_PLAN_ARTIFACT_KEY,
    STAGE3_REAL_VALIDATION_PLAN_REVIEWER_ARTIFACT_KEY,
    build_stage3_real_validation_plan_artifact_entry,
)
from ..core.stage3_standards_alignment_matrix import (
    STAGE3_STANDARDS_ALIGNMENT_MATRIX_FILENAME,
    STAGE3_STANDARDS_ALIGNMENT_MATRIX_REVIEWER_FILENAME,
)
from ..core.stage3_standards_alignment_matrix_artifact_entry import (
    STAGE3_STANDARDS_ALIGNMENT_MATRIX_ARTIFACT_KEY,
    STAGE3_STANDARDS_ALIGNMENT_MATRIX_REVIEWER_ARTIFACT_KEY,
    build_stage3_standards_alignment_matrix_artifact_entry,
)
from .method_confirmation_gateway import MethodConfirmationGateway
from .recognition_scope_gateway import RecognitionScopeGateway
from .software_validation_gateway import SoftwareValidationGateway
from .uncertainty_gateway import UncertaintyGateway
from ..review_surface_formatter import (
    collect_boundary_digest_lines,
    build_measurement_review_digest_lines,
    build_readiness_review_digest_lines,
    build_offline_diagnostic_detail_item_line,
    build_offline_diagnostic_scope_line,
    collect_offline_diagnostic_detail_lines,
    humanize_review_surface_text,
    humanize_offline_diagnostic_summary_value,
    normalize_offline_diagnostic_line,
    offline_diagnostic_scope_label,
)
from ..ui_v2.artifact_registry_governance import build_current_run_governance
from ..ui_v2.i18n import t


class ResultsGateway:
    """Read-only access layer for run artifacts and derived result payloads."""

    def __init__(
        self,
        run_dir: Path,
        *,
        output_files_provider: Optional[Callable[[], Iterable[str]]] = None,
    ) -> None:
        self.run_dir = Path(run_dir)
        self.output_files_provider = output_files_provider

    def read_results_payload(self) -> dict[str, Any]:
        summary = self.load_json("summary.json")
        manifest = self.load_json("manifest.json")
        results = self.load_json("results.json")
        analytics_summary = self.load_json("analytics_summary.json")
        evidence_registry = self.load_json("evidence_registry.json")
        workbench_action_report = self.load_json("workbench_action_report.json")
        workbench_action_snapshot = self.load_json("workbench_action_snapshot.json")
        config_safety = self._read_summary_section(
            "config_safety",
            summary,
            evidence_registry,
            analytics_summary,
            workbench_action_report,
            workbench_action_snapshot,
        )
        config_safety_review = self._read_summary_section(
            "config_safety_review",
            summary,
            evidence_registry,
            analytics_summary,
            workbench_action_report,
            workbench_action_snapshot,
        )
        offline_diagnostic_adapter_summary = self._read_summary_section(
            "offline_diagnostic_adapter_summary",
            summary,
            evidence_registry,
            analytics_summary,
            workbench_action_report,
            workbench_action_snapshot,
        )
        if not offline_diagnostic_adapter_summary:
            offline_diagnostic_adapter_summary = summarize_offline_diagnostic_adapters(self.run_dir)
        point_taxonomy_summary = self._read_summary_section(
            "point_taxonomy_summary",
            summary,
            evidence_registry,
            analytics_summary,
            workbench_action_report,
            workbench_action_snapshot,
        )
        if not point_taxonomy_summary:
            point_taxonomy_summary = (
                build_point_taxonomy_handoff(list(summary.get("stats", {}).get("point_summaries", []) or []))
                if isinstance(summary, dict)
                else {}
            )
        artifact_role_summary = self._read_summary_section(
            "artifact_role_summary",
            summary,
            evidence_registry,
            analytics_summary,
            workbench_action_report,
            workbench_action_snapshot,
        )
        workbench_evidence_summary = self._read_summary_section(
            "workbench_evidence_summary",
            summary,
            evidence_registry,
            analytics_summary,
            workbench_action_report,
            workbench_action_snapshot,
        )
        multi_source_stability_evidence = self.load_json(MULTI_SOURCE_STABILITY_EVIDENCE_FILENAME)
        if not multi_source_stability_evidence:
            multi_source_stability_evidence = self._read_summary_section(
                "multi_source_stability_evidence",
                summary,
                evidence_registry,
                analytics_summary,
                workbench_action_report,
                workbench_action_snapshot,
            )
        state_transition_evidence = self.load_json(STATE_TRANSITION_EVIDENCE_FILENAME)
        if not state_transition_evidence:
            state_transition_evidence = self._read_summary_section(
                "state_transition_evidence",
                summary,
                evidence_registry,
                analytics_summary,
                workbench_action_report,
                workbench_action_snapshot,
            )
        simulation_evidence_sidecar_bundle = self.load_json(SIMULATION_EVIDENCE_SIDECAR_BUNDLE_FILENAME)
        if not simulation_evidence_sidecar_bundle:
            simulation_evidence_sidecar_bundle = self._read_summary_section(
                "simulation_evidence_sidecar_bundle",
                summary,
                evidence_registry,
                analytics_summary,
                workbench_action_report,
                workbench_action_snapshot,
            )
        measurement_phase_coverage_report = self.load_json(MEASUREMENT_PHASE_COVERAGE_REPORT_FILENAME)
        if not measurement_phase_coverage_report:
            measurement_phase_coverage_report = self._read_summary_section(
                "measurement_phase_coverage_report",
                summary,
                evidence_registry,
                analytics_summary,
                workbench_action_report,
                workbench_action_snapshot,
            )
        scope_readiness_summary = self.load_json(recognition_readiness.SCOPE_READINESS_SUMMARY_FILENAME)
        if not scope_readiness_summary:
            scope_readiness_summary = self._read_summary_section(
                "scope_readiness_summary",
                summary,
                evidence_registry,
                analytics_summary,
                workbench_action_report,
                workbench_action_snapshot,
            )
        certificate_readiness_summary = self.load_json(recognition_readiness.CERTIFICATE_READINESS_SUMMARY_FILENAME)
        if not certificate_readiness_summary:
            certificate_readiness_summary = self._read_summary_section(
                "certificate_readiness_summary",
                summary,
                evidence_registry,
                analytics_summary,
                workbench_action_report,
                workbench_action_snapshot,
            )
        uncertainty_method_readiness_summary = self.load_json(
            recognition_readiness.UNCERTAINTY_METHOD_READINESS_SUMMARY_FILENAME
        )
        if not uncertainty_method_readiness_summary:
            uncertainty_method_readiness_summary = self._read_summary_section(
                "uncertainty_method_readiness_summary",
                summary,
                evidence_registry,
                analytics_summary,
                workbench_action_report,
                workbench_action_snapshot,
            )
        audit_readiness_digest = self.load_json(recognition_readiness.AUDIT_READINESS_DIGEST_FILENAME)
        if not audit_readiness_digest:
            audit_readiness_digest = self._read_summary_section(
                "audit_readiness_digest",
                summary,
                evidence_registry,
                analytics_summary,
                workbench_action_report,
                workbench_action_snapshot,
            )
        role_catalog = dict(dict(manifest or {}).get("artifacts", {}) or {}).get("role_catalog", {})
        compatibility_payloads = load_or_build_artifact_compatibility_payloads(
            self.run_dir,
            summary=summary if isinstance(summary, dict) else None,
            manifest=manifest if isinstance(manifest, dict) else None,
            results=results if isinstance(results, dict) else None,
            output_files=self.list_output_files(),
            role_catalog=role_catalog if isinstance(role_catalog, dict) else None,
        )
        run_artifact_index = dict(compatibility_payloads.get("run_artifact_index") or {})
        artifact_contract_catalog = dict(compatibility_payloads.get("artifact_contract_catalog") or {})
        compatibility_scan_summary = dict(compatibility_payloads.get("compatibility_scan_summary") or {})
        reindex_manifest = dict(compatibility_payloads.get("reindex_manifest") or {})
        compatibility_overview = dict(compatibility_scan_summary.get("compatibility_overview") or {})
        compatibility_rollup = dict(
            compatibility_scan_summary.get("compatibility_rollup")
            or compatibility_overview.get("compatibility_rollup")
            or {}
        )
        recognition_scope_payload = RecognitionScopeGateway(
            self.run_dir,
            summary=summary if isinstance(summary, dict) else None,
            analytics_summary=analytics_summary if isinstance(analytics_summary, dict) else None,
            evidence_registry=evidence_registry if isinstance(evidence_registry, dict) else None,
            workbench_action_report=workbench_action_report if isinstance(workbench_action_report, dict) else None,
            workbench_action_snapshot=workbench_action_snapshot if isinstance(workbench_action_snapshot, dict) else None,
            scope_readiness_summary=scope_readiness_summary,
            compatibility_scan_summary=compatibility_scan_summary,
        ).read_payload()
        scope_definition_pack = dict(recognition_scope_payload.get("scope_definition_pack") or {})
        decision_rule_profile = dict(recognition_scope_payload.get("decision_rule_profile") or {})
        reference_asset_registry = dict(recognition_scope_payload.get("reference_asset_registry") or {})
        certificate_lifecycle_summary = dict(recognition_scope_payload.get("certificate_lifecycle_summary") or {})
        pre_run_readiness_gate = dict(recognition_scope_payload.get("pre_run_readiness_gate") or {})
        recognition_scope_rollup = dict(recognition_scope_payload.get("recognition_scope_rollup") or {})
        uncertainty_payload = UncertaintyGateway(
            self.run_dir,
            summary=summary if isinstance(summary, dict) else None,
            analytics_summary=analytics_summary if isinstance(analytics_summary, dict) else None,
            evidence_registry=evidence_registry if isinstance(evidence_registry, dict) else None,
            workbench_action_report=workbench_action_report if isinstance(workbench_action_report, dict) else None,
            workbench_action_snapshot=workbench_action_snapshot if isinstance(workbench_action_snapshot, dict) else None,
            scope_readiness_summary=scope_readiness_summary,
            compatibility_scan_summary=compatibility_scan_summary,
        ).read_payload()
        uncertainty_model = dict(uncertainty_payload.get("uncertainty_model") or {})
        uncertainty_input_set = dict(uncertainty_payload.get("uncertainty_input_set") or {})
        sensitivity_coefficient_set = dict(uncertainty_payload.get("sensitivity_coefficient_set") or {})
        budget_case = dict(uncertainty_payload.get("budget_case") or {})
        uncertainty_golden_cases = dict(uncertainty_payload.get("uncertainty_golden_cases") or {})
        uncertainty_report_pack = dict(uncertainty_payload.get("uncertainty_report_pack") or {})
        uncertainty_digest = dict(uncertainty_payload.get("uncertainty_digest") or {})
        uncertainty_rollup = dict(uncertainty_payload.get("uncertainty_rollup") or {})
        method_confirmation_payload = MethodConfirmationGateway(
            self.run_dir,
            summary=summary if isinstance(summary, dict) else None,
            analytics_summary=analytics_summary if isinstance(analytics_summary, dict) else None,
            evidence_registry=evidence_registry if isinstance(evidence_registry, dict) else None,
            workbench_action_report=workbench_action_report if isinstance(workbench_action_report, dict) else None,
            workbench_action_snapshot=workbench_action_snapshot if isinstance(workbench_action_snapshot, dict) else None,
            scope_readiness_summary=scope_readiness_summary,
            compatibility_scan_summary=compatibility_scan_summary,
        ).read_payload()
        method_confirmation_protocol = dict(method_confirmation_payload.get("method_confirmation_protocol") or {})
        method_confirmation_matrix = dict(method_confirmation_payload.get("method_confirmation_matrix") or {})
        route_specific_validation_matrix = dict(
            method_confirmation_payload.get("route_specific_validation_matrix") or {}
        )
        validation_run_set = dict(method_confirmation_payload.get("validation_run_set") or {})
        verification_digest = dict(method_confirmation_payload.get("verification_digest") or {})
        verification_rollup = dict(method_confirmation_payload.get("verification_rollup") or {})
        software_validation_payload = SoftwareValidationGateway(
            self.run_dir,
            summary=summary if isinstance(summary, dict) else None,
            analytics_summary=analytics_summary if isinstance(analytics_summary, dict) else None,
            evidence_registry=evidence_registry if isinstance(evidence_registry, dict) else None,
            workbench_action_report=workbench_action_report if isinstance(workbench_action_report, dict) else None,
            workbench_action_snapshot=workbench_action_snapshot if isinstance(workbench_action_snapshot, dict) else None,
            scope_readiness_summary=scope_readiness_summary,
            compatibility_scan_summary=compatibility_scan_summary,
        ).read_payload()
        software_validation_traceability_matrix = dict(
            software_validation_payload.get("software_validation_traceability_matrix") or {}
        )
        requirement_design_code_test_links = dict(
            software_validation_payload.get("requirement_design_code_test_links") or {}
        )
        validation_evidence_index = dict(
            software_validation_payload.get("validation_evidence_index") or {}
        )
        change_impact_summary = dict(software_validation_payload.get("change_impact_summary") or {})
        rollback_readiness_summary = dict(
            software_validation_payload.get("rollback_readiness_summary") or {}
        )
        artifact_hash_registry = dict(software_validation_payload.get("artifact_hash_registry") or {})
        audit_event_store = dict(software_validation_payload.get("audit_event_store") or {})
        environment_fingerprint = dict(software_validation_payload.get("environment_fingerprint") or {})
        config_fingerprint = dict(software_validation_payload.get("config_fingerprint") or {})
        release_input_digest = dict(software_validation_payload.get("release_input_digest") or {})
        release_manifest = dict(software_validation_payload.get("release_manifest") or {})
        release_scope_summary = dict(software_validation_payload.get("release_scope_summary") or {})
        release_boundary_digest = dict(software_validation_payload.get("release_boundary_digest") or {})
        release_evidence_pack_index = dict(
            software_validation_payload.get("release_evidence_pack_index") or {}
        )
        release_validation_manifest = dict(
            software_validation_payload.get("release_validation_manifest") or {}
        )
        software_validation_rollup = dict(software_validation_payload.get("software_validation_rollup") or {})
        evidence_source = self._resolve_current_run_evidence_source(workbench_evidence_summary, workbench_action_report)
        evidence_state = str(
            workbench_evidence_summary.get("evidence_state")
            or dict(workbench_action_report or {}).get("evidence_state")
            or "collected"
        )
        not_real_acceptance_evidence = bool(
            workbench_evidence_summary.get(
                "not_real_acceptance_evidence",
                dict(workbench_action_report or {}).get("not_real_acceptance_evidence", True),
            )
        )
        acceptance_level = str(
            workbench_evidence_summary.get("acceptance_level")
            or dict(workbench_action_report or {}).get("acceptance_level")
            or "offline_regression"
        )
        promotion_state = str(
            workbench_evidence_summary.get("promotion_state")
            or dict(workbench_action_report or {}).get("promotion_state")
            or "dry_run_only"
        )
        result_summary_text = self._build_result_summary_text(
            summary=summary,
            artifact_role_summary=artifact_role_summary,
            config_safety=config_safety,
            config_safety_review=config_safety_review,
            offline_diagnostic_adapter_summary=offline_diagnostic_adapter_summary,
            point_taxonomy_summary=point_taxonomy_summary,
            workbench_evidence_summary=workbench_evidence_summary,
            evidence_source=evidence_source,
            multi_source_stability_evidence=multi_source_stability_evidence,
            state_transition_evidence=state_transition_evidence,
            simulation_evidence_sidecar_bundle=simulation_evidence_sidecar_bundle,
            measurement_phase_coverage_report=measurement_phase_coverage_report,
            scope_definition_pack=scope_definition_pack,
            decision_rule_profile=decision_rule_profile,
            reference_asset_registry=reference_asset_registry,
            certificate_lifecycle_summary=certificate_lifecycle_summary,
            scope_readiness_summary=scope_readiness_summary,
            certificate_readiness_summary=certificate_readiness_summary,
            pre_run_readiness_gate=pre_run_readiness_gate,
            method_confirmation_protocol=method_confirmation_protocol,
            method_confirmation_matrix=method_confirmation_matrix,
            route_specific_validation_matrix=route_specific_validation_matrix,
            validation_run_set=validation_run_set,
            verification_digest=verification_digest,
            verification_rollup=verification_rollup,
            uncertainty_report_pack=uncertainty_report_pack,
            uncertainty_digest=uncertainty_digest,
            uncertainty_rollup=uncertainty_rollup,
            uncertainty_method_readiness_summary=uncertainty_method_readiness_summary,
            software_validation_traceability_matrix=software_validation_traceability_matrix,
            artifact_hash_registry=artifact_hash_registry,
            environment_fingerprint=environment_fingerprint,
            release_manifest=release_manifest,
            release_scope_summary=release_scope_summary,
            release_boundary_digest=release_boundary_digest,
            release_evidence_pack_index=release_evidence_pack_index,
            software_validation_rollup=software_validation_rollup,
            audit_readiness_digest=audit_readiness_digest,
            compatibility_scan_summary=compatibility_scan_summary,
            recognition_scope_rollup=recognition_scope_rollup,
        )
        return {
            "summary": summary,
            "manifest": manifest,
            "results": results,
            "acceptance_plan": self.load_json("acceptance_plan.json"),
            "analytics_summary": analytics_summary,
            "spectral_quality_summary": self.load_json("spectral_quality_summary.json"),
            "trend_registry": self.load_json("trend_registry.json"),
            "lineage_summary": self.load_json("lineage_summary.json"),
            "evidence_registry": evidence_registry,
            "coefficient_registry": self.load_json("coefficient_registry.json"),
            "suite_summary": self.load_json("suite_summary.json"),
            "suite_analytics_summary": self.load_json("suite_analytics_summary.json"),
            "suite_acceptance_plan": self.load_json("suite_acceptance_plan.json"),
            "suite_evidence_registry": self.load_json("suite_evidence_registry.json"),
            "workbench_action_report": workbench_action_report,
            "workbench_action_snapshot": workbench_action_snapshot,
            "ai_summary_text": self.load_text("ai_run_summary.md") or self.load_text("run_summary.txt"),
            "output_files": self.list_output_files(),
            "reporting": dict(summary.get("reporting", {}) or {}) if isinstance(summary, dict) else {},
            "config_safety": config_safety,
            "config_safety_review": config_safety_review,
            "config_governance_handoff": self._read_config_governance_handoff(
                config_safety,
                config_safety_review,
                summary,
                evidence_registry,
                analytics_summary,
                workbench_action_report,
                workbench_action_snapshot,
            ),
            "artifact_exports": dict(summary.get("stats", {}).get("artifact_exports", {}) or {}) if isinstance(summary, dict) else {},
            "artifact_role_summary": artifact_role_summary,
            "workbench_evidence_summary": workbench_evidence_summary,
            "offline_diagnostic_adapter_summary": offline_diagnostic_adapter_summary,
            "point_taxonomy_summary": point_taxonomy_summary,
            "multi_source_stability_evidence": multi_source_stability_evidence,
            "state_transition_evidence": state_transition_evidence,
            "simulation_evidence_sidecar_bundle": simulation_evidence_sidecar_bundle,
            "measurement_phase_coverage_report": measurement_phase_coverage_report,
            "scope_definition_pack": scope_definition_pack,
            "decision_rule_profile": decision_rule_profile,
            "reference_asset_registry": reference_asset_registry,
            "certificate_lifecycle_summary": certificate_lifecycle_summary,
            "scope_readiness_summary": scope_readiness_summary,
            "certificate_readiness_summary": certificate_readiness_summary,
            "pre_run_readiness_gate": pre_run_readiness_gate,
            "method_confirmation_protocol": method_confirmation_protocol,
            "method_confirmation_matrix": method_confirmation_matrix,
            "route_specific_validation_matrix": route_specific_validation_matrix,
            "validation_run_set": validation_run_set,
            "verification_digest": verification_digest,
            "verification_rollup": verification_rollup,
            "software_validation_traceability_matrix": software_validation_traceability_matrix,
            "requirement_design_code_test_links": requirement_design_code_test_links,
            "validation_evidence_index": validation_evidence_index,
            "change_impact_summary": change_impact_summary,
            "rollback_readiness_summary": rollback_readiness_summary,
            "artifact_hash_registry": artifact_hash_registry,
            "audit_event_store": audit_event_store,
            "environment_fingerprint": environment_fingerprint,
            "config_fingerprint": config_fingerprint,
            "release_input_digest": release_input_digest,
            "release_manifest": release_manifest,
            "release_scope_summary": release_scope_summary,
            "release_boundary_digest": release_boundary_digest,
            "release_evidence_pack_index": release_evidence_pack_index,
            "release_validation_manifest": release_validation_manifest,
            "software_validation_rollup": software_validation_rollup,
            "uncertainty_model": uncertainty_model,
            "uncertainty_input_set": uncertainty_input_set,
            "sensitivity_coefficient_set": sensitivity_coefficient_set,
            "budget_case": budget_case,
            "uncertainty_golden_cases": uncertainty_golden_cases,
            "uncertainty_report_pack": uncertainty_report_pack,
            "uncertainty_digest": uncertainty_digest,
            "uncertainty_rollup": uncertainty_rollup,
            "uncertainty_method_readiness_summary": uncertainty_method_readiness_summary,
            "audit_readiness_digest": audit_readiness_digest,
            "run_artifact_index": run_artifact_index,
            "artifact_contract_catalog": artifact_contract_catalog,
            "compatibility_scan_summary": compatibility_scan_summary,
            "compatibility_overview": compatibility_overview,
            "compatibility_rollup": compatibility_rollup,
            "recognition_scope_rollup": recognition_scope_rollup,
            "reindex_manifest": reindex_manifest,
            "result_summary_text": result_summary_text,
            "evidence_source": evidence_source,
            "evidence_state": evidence_state,
            "not_real_acceptance_evidence": not_real_acceptance_evidence,
            "acceptance_level": acceptance_level,
            "promotion_state": promotion_state,
        }

    def read_reports_payload(self) -> dict[str, Any]:
        payload = self.read_results_payload()
        manifest = dict(payload.get("manifest", {}) or {})
        role_catalog = dict(manifest.get("artifacts", {}) or {}).get("role_catalog", {})
        artifact_exports = dict(payload.get("artifact_exports", {}) or {})
        offline_diagnostic_adapter_summary = dict(payload.get("offline_diagnostic_adapter_summary", {}) or {})
        analytics_summary = dict(payload.get("analytics_summary", {}) or {})
        summary_stats = dict(dict(payload.get("summary", {}) or {}).get("stats", {}) or {})
        multi_source_stability_evidence = dict(payload.get("multi_source_stability_evidence", {}) or {})
        state_transition_evidence = dict(payload.get("state_transition_evidence", {}) or {})
        simulation_evidence_sidecar_bundle = dict(payload.get("simulation_evidence_sidecar_bundle", {}) or {})
        measurement_phase_coverage_report = dict(payload.get("measurement_phase_coverage_report", {}) or {})
        scope_definition_pack = dict(payload.get("scope_definition_pack", {}) or {})
        decision_rule_profile = dict(payload.get("decision_rule_profile", {}) or {})
        reference_asset_registry = dict(payload.get("reference_asset_registry", {}) or {})
        certificate_lifecycle_summary = dict(payload.get("certificate_lifecycle_summary", {}) or {})
        scope_readiness_summary = dict(payload.get("scope_readiness_summary", {}) or {})
        certificate_readiness_summary = dict(payload.get("certificate_readiness_summary", {}) or {})
        pre_run_readiness_gate = dict(payload.get("pre_run_readiness_gate", {}) or {})
        method_confirmation_protocol = dict(payload.get("method_confirmation_protocol", {}) or {})
        method_confirmation_matrix = dict(payload.get("method_confirmation_matrix", {}) or {})
        route_specific_validation_matrix = dict(payload.get("route_specific_validation_matrix", {}) or {})
        validation_run_set = dict(payload.get("validation_run_set", {}) or {})
        verification_digest = dict(payload.get("verification_digest", {}) or {})
        verification_rollup = dict(payload.get("verification_rollup", {}) or {})
        uncertainty_model = dict(payload.get("uncertainty_model", {}) or {})
        uncertainty_input_set = dict(payload.get("uncertainty_input_set", {}) or {})
        sensitivity_coefficient_set = dict(payload.get("sensitivity_coefficient_set", {}) or {})
        budget_case = dict(payload.get("budget_case", {}) or {})
        uncertainty_golden_cases = dict(payload.get("uncertainty_golden_cases", {}) or {})
        uncertainty_report_pack = dict(payload.get("uncertainty_report_pack", {}) or {})
        uncertainty_digest = dict(payload.get("uncertainty_digest", {}) or {})
        uncertainty_rollup = dict(payload.get("uncertainty_rollup", {}) or {})
        uncertainty_method_readiness_summary = dict(payload.get("uncertainty_method_readiness_summary", {}) or {})
        software_validation_traceability_matrix = dict(
            payload.get("software_validation_traceability_matrix", {}) or {}
        )
        requirement_design_code_test_links = dict(
            payload.get("requirement_design_code_test_links", {}) or {}
        )
        validation_evidence_index = dict(payload.get("validation_evidence_index", {}) or {})
        change_impact_summary = dict(payload.get("change_impact_summary", {}) or {})
        rollback_readiness_summary = dict(payload.get("rollback_readiness_summary", {}) or {})
        artifact_hash_registry = dict(payload.get("artifact_hash_registry", {}) or {})
        audit_event_store = dict(payload.get("audit_event_store", {}) or {})
        environment_fingerprint = dict(payload.get("environment_fingerprint", {}) or {})
        config_fingerprint = dict(payload.get("config_fingerprint", {}) or {})
        release_input_digest = dict(payload.get("release_input_digest", {}) or {})
        release_manifest = dict(payload.get("release_manifest", {}) or {})
        release_scope_summary = dict(payload.get("release_scope_summary", {}) or {})
        release_boundary_digest = dict(payload.get("release_boundary_digest", {}) or {})
        release_evidence_pack_index = dict(payload.get("release_evidence_pack_index", {}) or {})
        release_validation_manifest = dict(payload.get("release_validation_manifest", {}) or {})
        software_validation_rollup = dict(payload.get("software_validation_rollup", {}) or {})
        audit_readiness_digest = dict(payload.get("audit_readiness_digest", {}) or {})
        run_artifact_index = dict(payload.get("run_artifact_index", {}) or {})
        artifact_contract_catalog = dict(payload.get("artifact_contract_catalog", {}) or {})
        compatibility_scan_summary = dict(payload.get("compatibility_scan_summary", {}) or {})
        compatibility_overview = dict(payload.get("compatibility_overview", {}) or {})
        compatibility_rollup = dict(payload.get("compatibility_rollup", {}) or {})
        recognition_scope_rollup = dict(payload.get("recognition_scope_rollup", {}) or {})
        reindex_manifest = dict(payload.get("reindex_manifest", {}) or {})
        compatibility_lookup = self._build_artifact_compatibility_lookup(run_artifact_index)

        def _artifact_path(value: Any) -> Path:
            candidate = Path(str(value or "").strip())
            if candidate.is_absolute():
                return candidate
            return self.run_dir / candidate

        reviewer_artifact_section = dict(manifest.get(PHASE_TRANSITION_BRIDGE_REVIEWER_ARTIFACT_KEY) or {})
        reviewer_surface_section = (
            dict(manifest.get("phase_transition_bridge_reviewer_section") or {})
            or dict(analytics_summary.get("phase_transition_bridge_reviewer_section") or {})
        )
        reviewer_artifact_path = str(reviewer_artifact_section.get("path") or "").strip()
        if not reviewer_artifact_path:
            fallback_path = self.run_dir / PHASE_TRANSITION_BRIDGE_REVIEWER_FILENAME
            if fallback_path.exists():
                reviewer_artifact_path = str(fallback_path)
        reviewer_artifact_entry = build_phase_transition_bridge_reviewer_artifact_entry(
            artifact_path=reviewer_artifact_path,
            manifest_section=reviewer_artifact_section,
            reviewer_section=reviewer_surface_section,
        )
        if not bool(reviewer_artifact_entry.get("available", False)):
            reviewer_artifact_entry = {}
        stage_admission_review_pack_section = dict(manifest.get(STAGE_ADMISSION_REVIEW_PACK_ARTIFACT_KEY) or {})
        stage_admission_review_pack_reviewer_section = dict(
            manifest.get(STAGE_ADMISSION_REVIEW_PACK_REVIEWER_ARTIFACT_KEY) or {}
        )
        stage_admission_review_pack_path = str(stage_admission_review_pack_section.get("path") or "").strip()
        if not stage_admission_review_pack_path:
            fallback_path = self.run_dir / STAGE_ADMISSION_REVIEW_PACK_FILENAME
            if fallback_path.exists():
                stage_admission_review_pack_path = str(fallback_path)
        stage_admission_review_pack_reviewer_path = str(
            stage_admission_review_pack_reviewer_section.get("path") or ""
        ).strip()
        if not stage_admission_review_pack_reviewer_path:
            fallback_path = self.run_dir / STAGE_ADMISSION_REVIEW_PACK_REVIEWER_FILENAME
            if fallback_path.exists():
                stage_admission_review_pack_reviewer_path = str(fallback_path)
        stage_admission_review_pack_entry = build_stage_admission_review_pack_artifact_entry(
            artifact_path=stage_admission_review_pack_path,
            reviewer_artifact_path=stage_admission_review_pack_reviewer_path,
            manifest_section=stage_admission_review_pack_section,
            reviewer_manifest_section=stage_admission_review_pack_reviewer_section,
        )
        if not bool(stage_admission_review_pack_entry.get("available", False)):
            stage_admission_review_pack_entry = {}
        engineering_isolation_admission_checklist_section = dict(
            manifest.get(ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_ARTIFACT_KEY) or {}
        )
        engineering_isolation_admission_checklist_reviewer_section = dict(
            manifest.get(ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_REVIEWER_ARTIFACT_KEY) or {}
        )
        engineering_isolation_admission_checklist_path = str(
            engineering_isolation_admission_checklist_section.get("path") or ""
        ).strip()
        if not engineering_isolation_admission_checklist_path:
            fallback_path = self.run_dir / ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_FILENAME
            if fallback_path.exists():
                engineering_isolation_admission_checklist_path = str(fallback_path)
        engineering_isolation_admission_checklist_reviewer_path = str(
            engineering_isolation_admission_checklist_reviewer_section.get("path") or ""
        ).strip()
        if not engineering_isolation_admission_checklist_reviewer_path:
            fallback_path = self.run_dir / ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_REVIEWER_FILENAME
            if fallback_path.exists():
                engineering_isolation_admission_checklist_reviewer_path = str(fallback_path)
        engineering_isolation_admission_checklist_entry = (
            build_engineering_isolation_admission_checklist_artifact_entry(
                artifact_path=engineering_isolation_admission_checklist_path,
                reviewer_artifact_path=engineering_isolation_admission_checklist_reviewer_path,
                manifest_section=engineering_isolation_admission_checklist_section,
                reviewer_manifest_section=engineering_isolation_admission_checklist_reviewer_section,
            )
        )
        if not bool(engineering_isolation_admission_checklist_entry.get("available", False)):
            engineering_isolation_admission_checklist_entry = {}
        stage3_real_validation_plan_section = dict(manifest.get(STAGE3_REAL_VALIDATION_PLAN_ARTIFACT_KEY) or {})
        stage3_real_validation_plan_reviewer_section = dict(
            manifest.get(STAGE3_REAL_VALIDATION_PLAN_REVIEWER_ARTIFACT_KEY) or {}
        )
        stage3_real_validation_plan_path = str(stage3_real_validation_plan_section.get("path") or "").strip()
        if not stage3_real_validation_plan_path:
            fallback_path = self.run_dir / STAGE3_REAL_VALIDATION_PLAN_FILENAME
            if fallback_path.exists():
                stage3_real_validation_plan_path = str(fallback_path)
        stage3_real_validation_plan_reviewer_path = str(
            stage3_real_validation_plan_reviewer_section.get("path") or ""
        ).strip()
        if not stage3_real_validation_plan_reviewer_path:
            fallback_path = self.run_dir / STAGE3_REAL_VALIDATION_PLAN_REVIEWER_FILENAME
            if fallback_path.exists():
                stage3_real_validation_plan_reviewer_path = str(fallback_path)
        stage3_reviewer_markdown_text = ""
        if stage3_real_validation_plan_reviewer_path:
            try:
                stage3_reviewer_markdown_text = _artifact_path(
                    stage3_real_validation_plan_reviewer_path
                ).read_text(encoding="utf-8")
            except Exception:
                stage3_reviewer_markdown_text = ""
        stage3_real_validation_plan_entry = build_stage3_real_validation_plan_artifact_entry(
            artifact_path=stage3_real_validation_plan_path,
            reviewer_artifact_path=stage3_real_validation_plan_reviewer_path,
            manifest_section=stage3_real_validation_plan_section,
            reviewer_manifest_section=stage3_real_validation_plan_reviewer_section,
            digest_section=dict(summary_stats.get("stage3_real_validation_plan_digest") or {}),
            reviewer_markdown_text=stage3_reviewer_markdown_text,
        )
        if not bool(stage3_real_validation_plan_entry.get("available", False)):
            stage3_real_validation_plan_entry = {}
        stage3_standards_alignment_matrix_section = dict(
            manifest.get(STAGE3_STANDARDS_ALIGNMENT_MATRIX_ARTIFACT_KEY) or {}
        )
        stage3_standards_alignment_matrix_reviewer_section = dict(
            manifest.get(STAGE3_STANDARDS_ALIGNMENT_MATRIX_REVIEWER_ARTIFACT_KEY) or {}
        )
        stage3_standards_alignment_matrix_path = str(
            stage3_standards_alignment_matrix_section.get("path") or ""
        ).strip()
        if not stage3_standards_alignment_matrix_path:
            fallback_path = self.run_dir / STAGE3_STANDARDS_ALIGNMENT_MATRIX_FILENAME
            if fallback_path.exists():
                stage3_standards_alignment_matrix_path = str(fallback_path)
        stage3_standards_alignment_matrix_reviewer_path = str(
            stage3_standards_alignment_matrix_reviewer_section.get("path") or ""
        ).strip()
        if not stage3_standards_alignment_matrix_reviewer_path:
            fallback_path = self.run_dir / STAGE3_STANDARDS_ALIGNMENT_MATRIX_REVIEWER_FILENAME
            if fallback_path.exists():
                stage3_standards_alignment_matrix_reviewer_path = str(fallback_path)
        stage3_standards_alignment_matrix_markdown_text = ""
        if stage3_standards_alignment_matrix_reviewer_path:
            try:
                stage3_standards_alignment_matrix_markdown_text = _artifact_path(
                    stage3_standards_alignment_matrix_reviewer_path
                ).read_text(encoding="utf-8")
            except Exception:
                stage3_standards_alignment_matrix_markdown_text = ""
        stage3_standards_alignment_matrix_entry = build_stage3_standards_alignment_matrix_artifact_entry(
            artifact_path=stage3_standards_alignment_matrix_path,
            reviewer_artifact_path=stage3_standards_alignment_matrix_reviewer_path,
            manifest_section=stage3_standards_alignment_matrix_section,
            reviewer_manifest_section=stage3_standards_alignment_matrix_reviewer_section,
            digest_section=dict(summary_stats.get("stage3_standards_alignment_matrix_digest") or {}),
            reviewer_markdown_text=stage3_standards_alignment_matrix_markdown_text,
        )
        if not bool(stage3_standards_alignment_matrix_entry.get("available", False)):
            stage3_standards_alignment_matrix_entry = {}
        files = []
        seen: set[str] = set()

        candidate_paths = [self.run_dir / item for item in KNOWN_REPORT_ARTIFACTS]
        candidate_paths.extend(_artifact_path(item) for item in payload["output_files"] if str(item or "").strip())
        candidate_paths.extend(
            _artifact_path(item)
            for item in list(offline_diagnostic_adapter_summary.get("artifact_paths") or [])
            if str(item or "").strip()
        )
        for path in candidate_paths:
            key = str(path)
            if key in seen:
                continue
            seen.add(key)
            present_on_disk = path.exists()
            governance = build_current_run_governance(
                path,
                artifact_exports=artifact_exports,
                role_catalog=role_catalog if isinstance(role_catalog, dict) else None,
                present_on_disk=present_on_disk,
            )
            row = {
                "name": path.name,
                "path": str(path),
                "present": present_on_disk,
                "present_on_disk": present_on_disk,
                "listed_in_current_run": True,
                "artifact_origin": "current_run",
                "scope_match": "all",
                **governance,
            }
            row = self._decorate_phase_transition_bridge_reviewer_artifact_row(
                row,
                reviewer_artifact_entry=reviewer_artifact_entry,
            )
            row = self._decorate_stage_admission_review_pack_row(
                row,
                stage_admission_review_pack_entry=stage_admission_review_pack_entry,
            )
            row = self._decorate_engineering_isolation_admission_checklist_row(
                row,
                engineering_isolation_admission_checklist_entry=engineering_isolation_admission_checklist_entry,
            )
            row = self._decorate_stage3_real_validation_plan_row(
                row,
                stage3_real_validation_plan_entry=stage3_real_validation_plan_entry,
            )
            row = self._decorate_stage3_standards_alignment_matrix_row(
                row,
                stage3_standards_alignment_matrix_entry=stage3_standards_alignment_matrix_entry,
            )
            row = self._decorate_multi_source_stability_row(
                row,
                multi_source_stability_evidence=multi_source_stability_evidence,
            )
            row = self._decorate_state_transition_evidence_row(
                row,
                state_transition_evidence=state_transition_evidence,
            )
            row = self._decorate_simulation_sidecar_bundle_row(
                row,
                simulation_evidence_sidecar_bundle=simulation_evidence_sidecar_bundle,
            )
            row = self._decorate_artifact_compatibility_row(
                row,
                compatibility_lookup=compatibility_lookup,
                compatibility_scan_summary=compatibility_scan_summary,
            )
            row = self._decorate_measurement_phase_coverage_row(
                row,
                measurement_phase_coverage_report=measurement_phase_coverage_report,
            )
            row = self._decorate_scope_definition_pack_row(
                row,
                scope_definition_pack=scope_definition_pack,
            )
            row = self._decorate_decision_rule_profile_row(
                row,
                decision_rule_profile=decision_rule_profile,
            )
            row = self._decorate_reference_asset_registry_row(
                row,
                reference_asset_registry=reference_asset_registry,
            )
            row = self._decorate_certificate_lifecycle_summary_row(
                row,
                certificate_lifecycle_summary=certificate_lifecycle_summary,
            )
            row = self._decorate_scope_readiness_summary_row(
                row,
                scope_readiness_summary=scope_readiness_summary,
            )
            row = self._decorate_certificate_readiness_summary_row(
                row,
                certificate_readiness_summary=certificate_readiness_summary,
            )
            row = self._decorate_pre_run_readiness_gate_row(
                row,
                pre_run_readiness_gate=pre_run_readiness_gate,
            )
            row = self._decorate_method_confirmation_protocol_row(
                row,
                method_confirmation_protocol=method_confirmation_protocol,
            )
            row = self._decorate_method_confirmation_matrix_row(
                row,
                method_confirmation_matrix=method_confirmation_matrix,
            )
            row = self._decorate_route_specific_validation_matrix_row(
                row,
                route_specific_validation_matrix=route_specific_validation_matrix,
            )
            row = self._decorate_validation_run_set_row(
                row,
                validation_run_set=validation_run_set,
            )
            row = self._decorate_verification_digest_row(
                row,
                verification_digest=verification_digest,
            )
            row = self._decorate_verification_rollup_row(
                row,
                verification_rollup=verification_rollup,
            )
            row = self._decorate_uncertainty_model_row(
                row,
                uncertainty_model=uncertainty_model,
            )
            row = self._decorate_uncertainty_input_set_row(
                row,
                uncertainty_input_set=uncertainty_input_set,
            )
            row = self._decorate_sensitivity_coefficient_set_row(
                row,
                sensitivity_coefficient_set=sensitivity_coefficient_set,
            )
            row = self._decorate_budget_case_row(
                row,
                budget_case=budget_case,
            )
            row = self._decorate_uncertainty_golden_cases_row(
                row,
                uncertainty_golden_cases=uncertainty_golden_cases,
            )
            row = self._decorate_uncertainty_report_pack_row(
                row,
                uncertainty_report_pack=uncertainty_report_pack,
            )
            row = self._decorate_uncertainty_digest_row(
                row,
                uncertainty_digest=uncertainty_digest,
            )
            row = self._decorate_uncertainty_rollup_row(
                row,
                uncertainty_rollup=uncertainty_rollup,
            )
            row = self._decorate_uncertainty_method_readiness_summary_row(
                row,
                uncertainty_method_readiness_summary=uncertainty_method_readiness_summary,
            )
            row = self._decorate_audit_readiness_digest_row(
                row,
                audit_readiness_digest=audit_readiness_digest,
            )
            for software_validation_payload, json_filename, markdown_filename, entry_key in (
                (
                    software_validation_traceability_matrix,
                    recognition_readiness.SOFTWARE_VALIDATION_TRACEABILITY_MATRIX_FILENAME,
                    recognition_readiness.SOFTWARE_VALIDATION_TRACEABILITY_MATRIX_MARKDOWN_FILENAME,
                    "software_validation_traceability_matrix_entry",
                ),
                (
                    requirement_design_code_test_links,
                    recognition_readiness.REQUIREMENT_DESIGN_CODE_TEST_LINKS_FILENAME,
                    recognition_readiness.REQUIREMENT_DESIGN_CODE_TEST_LINKS_MARKDOWN_FILENAME,
                    "requirement_design_code_test_links_entry",
                ),
                (
                    validation_evidence_index,
                    recognition_readiness.VALIDATION_EVIDENCE_INDEX_FILENAME,
                    recognition_readiness.VALIDATION_EVIDENCE_INDEX_MARKDOWN_FILENAME,
                    "validation_evidence_index_entry",
                ),
                (
                    change_impact_summary,
                    recognition_readiness.CHANGE_IMPACT_SUMMARY_FILENAME,
                    recognition_readiness.CHANGE_IMPACT_SUMMARY_MARKDOWN_FILENAME,
                    "change_impact_summary_entry",
                ),
                (
                    rollback_readiness_summary,
                    recognition_readiness.ROLLBACK_READINESS_SUMMARY_FILENAME,
                    recognition_readiness.ROLLBACK_READINESS_SUMMARY_MARKDOWN_FILENAME,
                    "rollback_readiness_summary_entry",
                ),
                (
                    artifact_hash_registry,
                    recognition_readiness.ARTIFACT_HASH_REGISTRY_FILENAME,
                    recognition_readiness.ARTIFACT_HASH_REGISTRY_MARKDOWN_FILENAME,
                    "artifact_hash_registry_entry",
                ),
                (
                    audit_event_store,
                    recognition_readiness.AUDIT_EVENT_STORE_FILENAME,
                    recognition_readiness.AUDIT_EVENT_STORE_MARKDOWN_FILENAME,
                    "audit_event_store_entry",
                ),
                (
                    environment_fingerprint,
                    recognition_readiness.ENVIRONMENT_FINGERPRINT_FILENAME,
                    recognition_readiness.ENVIRONMENT_FINGERPRINT_MARKDOWN_FILENAME,
                    "environment_fingerprint_entry",
                ),
                (
                    config_fingerprint,
                    recognition_readiness.CONFIG_FINGERPRINT_FILENAME,
                    recognition_readiness.CONFIG_FINGERPRINT_MARKDOWN_FILENAME,
                    "config_fingerprint_entry",
                ),
                (
                    release_input_digest,
                    recognition_readiness.RELEASE_INPUT_DIGEST_FILENAME,
                    recognition_readiness.RELEASE_INPUT_DIGEST_MARKDOWN_FILENAME,
                    "release_input_digest_entry",
                ),
                (
                    release_manifest,
                    recognition_readiness.RELEASE_MANIFEST_FILENAME,
                    recognition_readiness.RELEASE_MANIFEST_MARKDOWN_FILENAME,
                    "release_manifest_entry",
                ),
                (
                    release_scope_summary,
                    recognition_readiness.RELEASE_SCOPE_SUMMARY_FILENAME,
                    recognition_readiness.RELEASE_SCOPE_SUMMARY_MARKDOWN_FILENAME,
                    "release_scope_summary_entry",
                ),
                (
                    release_boundary_digest,
                    recognition_readiness.RELEASE_BOUNDARY_DIGEST_FILENAME,
                    recognition_readiness.RELEASE_BOUNDARY_DIGEST_MARKDOWN_FILENAME,
                    "release_boundary_digest_entry",
                ),
                (
                    release_evidence_pack_index,
                    recognition_readiness.RELEASE_EVIDENCE_PACK_INDEX_FILENAME,
                    recognition_readiness.RELEASE_EVIDENCE_PACK_INDEX_MARKDOWN_FILENAME,
                    "release_evidence_pack_index_entry",
                ),
                (
                    release_validation_manifest,
                    recognition_readiness.RELEASE_VALIDATION_MANIFEST_FILENAME,
                    recognition_readiness.RELEASE_VALIDATION_MANIFEST_MARKDOWN_FILENAME,
                    "release_validation_manifest_entry",
                ),
            ):
                row = self._decorate_measurement_core_row(
                    row,
                    payload=software_validation_payload,
                    json_filename=json_filename,
                    markdown_filename=markdown_filename,
                    entry_key=entry_key,
                )
            files.append(row)
        return {
            "run_dir": str(self.run_dir),
            "files": files,
            "ai_summary_text": str(payload.get("ai_summary_text", "") or ""),
            "result_summary_text": str(payload.get("result_summary_text", "") or ""),
            "output_files": list(payload["output_files"]),
            "reporting": dict(payload.get("reporting", {}) or {}),
            "config_safety": dict(payload.get("config_safety", {}) or {}),
            "config_safety_review": dict(payload.get("config_safety_review", {}) or {}),
            "config_governance_handoff": dict(payload.get("config_governance_handoff", {}) or {}),
            "artifact_exports": dict(payload.get("artifact_exports", {}) or {}),
            "artifact_role_summary": dict(payload.get("artifact_role_summary", {}) or {}),
            "workbench_evidence_summary": dict(payload.get("workbench_evidence_summary", {}) or {}),
            "offline_diagnostic_adapter_summary": offline_diagnostic_adapter_summary,
            "point_taxonomy_summary": dict(payload.get("point_taxonomy_summary", {}) or {}),
            "multi_source_stability_evidence": multi_source_stability_evidence,
            "state_transition_evidence": state_transition_evidence,
            "simulation_evidence_sidecar_bundle": simulation_evidence_sidecar_bundle,
            "measurement_phase_coverage_report": measurement_phase_coverage_report,
            "scope_definition_pack": scope_definition_pack,
            "decision_rule_profile": decision_rule_profile,
            "reference_asset_registry": reference_asset_registry,
            "certificate_lifecycle_summary": certificate_lifecycle_summary,
            "scope_readiness_summary": scope_readiness_summary,
            "certificate_readiness_summary": certificate_readiness_summary,
            "pre_run_readiness_gate": pre_run_readiness_gate,
            "method_confirmation_protocol": method_confirmation_protocol,
            "method_confirmation_matrix": method_confirmation_matrix,
            "route_specific_validation_matrix": route_specific_validation_matrix,
            "validation_run_set": validation_run_set,
            "verification_digest": verification_digest,
            "verification_rollup": verification_rollup,
            "software_validation_traceability_matrix": software_validation_traceability_matrix,
            "requirement_design_code_test_links": requirement_design_code_test_links,
            "validation_evidence_index": validation_evidence_index,
            "change_impact_summary": change_impact_summary,
            "rollback_readiness_summary": rollback_readiness_summary,
            "artifact_hash_registry": artifact_hash_registry,
            "audit_event_store": audit_event_store,
            "environment_fingerprint": environment_fingerprint,
            "config_fingerprint": config_fingerprint,
            "release_input_digest": release_input_digest,
            "release_manifest": release_manifest,
            "release_scope_summary": release_scope_summary,
            "release_boundary_digest": release_boundary_digest,
            "release_evidence_pack_index": release_evidence_pack_index,
            "release_validation_manifest": release_validation_manifest,
            "software_validation_rollup": software_validation_rollup,
            "uncertainty_model": uncertainty_model,
            "uncertainty_input_set": uncertainty_input_set,
            "sensitivity_coefficient_set": sensitivity_coefficient_set,
            "budget_case": budget_case,
            "uncertainty_golden_cases": uncertainty_golden_cases,
            "uncertainty_report_pack": uncertainty_report_pack,
            "uncertainty_digest": uncertainty_digest,
            "uncertainty_rollup": uncertainty_rollup,
            "uncertainty_method_readiness_summary": uncertainty_method_readiness_summary,
            "audit_readiness_digest": audit_readiness_digest,
            "run_artifact_index": run_artifact_index,
            "artifact_contract_catalog": artifact_contract_catalog,
            "compatibility_scan_summary": compatibility_scan_summary,
            "compatibility_overview": compatibility_overview,
            "compatibility_rollup": compatibility_rollup,
            "recognition_scope_rollup": recognition_scope_rollup,
            "reindex_manifest": reindex_manifest,
            "phase_transition_bridge_reviewer_artifact_entry": dict(reviewer_artifact_entry),
            "stage_admission_review_pack_artifact_entry": dict(stage_admission_review_pack_entry),
            "engineering_isolation_admission_checklist_artifact_entry": dict(
                engineering_isolation_admission_checklist_entry
            ),
            "stage3_real_validation_plan_artifact_entry": dict(stage3_real_validation_plan_entry),
            "stage3_standards_alignment_matrix_artifact_entry": dict(stage3_standards_alignment_matrix_entry),
            "evidence_source": str(payload.get("evidence_source", "") or "simulated_protocol"),
            "evidence_state": str(payload.get("evidence_state", "") or "collected"),
            "not_real_acceptance_evidence": bool(payload.get("not_real_acceptance_evidence", True)),
            "acceptance_level": str(payload.get("acceptance_level", "") or "offline_regression"),
            "promotion_state": str(payload.get("promotion_state", "") or "dry_run_only"),
        }

    def list_output_files(self) -> list[str]:
        files = set()
        if self.output_files_provider is not None:
            try:
                files.update(str(item) for item in self.output_files_provider() or [])
            except Exception:
                pass
        if self.run_dir.exists():
            for path in self.run_dir.iterdir():
                files.add(str(path))
        return sorted(files)

    def load_json(self, relative_name: str) -> dict[str, Any] | None:
        path = self.run_dir / relative_name
        if not path.exists():
            return None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return None
        return payload if isinstance(payload, dict) else None

    def load_text(self, relative_name: str) -> str:
        path = self.run_dir / relative_name
        if not path.exists():
            return ""
        try:
            return path.read_text(encoding="utf-8")
        except Exception:
            return ""

    @staticmethod
    def _read_section_from_payload(payload: dict[str, Any] | None, key: str) -> dict[str, Any]:
        if not isinstance(payload, dict):
            return {}
        stats = payload.get("stats")
        if isinstance(stats, dict):
            legacy = stats.get(key)
            if isinstance(legacy, dict):
                return dict(legacy)
        direct = payload.get(key)
        return dict(direct) if isinstance(direct, dict) else {}

    @classmethod
    def _read_summary_section(cls, key: str, *payloads: dict[str, Any] | None) -> dict[str, Any]:
        for payload in payloads:
            section = cls._read_section_from_payload(payload, key)
            if section:
                return section
        return {}

    @classmethod
    def _read_config_governance_handoff(
        cls,
        config_safety: dict[str, Any] | None,
        config_safety_review: dict[str, Any] | None,
        *payloads: dict[str, Any] | None,
    ) -> dict[str, Any]:
        for payload in payloads:
            section = cls._read_section_from_payload(payload, "config_governance_handoff")
            if section:
                return section
        if config_safety_review:
            return build_step2_config_governance_handoff(config_safety_review)
        if config_safety:
            return build_step2_config_governance_handoff(config_safety)
        return {}

    @staticmethod
    def _build_result_summary_text(
        *,
        summary: dict[str, Any] | None,
        artifact_role_summary: dict[str, Any] | None,
        config_safety: dict[str, Any] | None,
        config_safety_review: dict[str, Any] | None,
        offline_diagnostic_adapter_summary: dict[str, Any] | None,
        point_taxonomy_summary: dict[str, Any] | None,
        workbench_evidence_summary: dict[str, Any] | None,
        evidence_source: str,
        multi_source_stability_evidence: dict[str, Any] | None,
        state_transition_evidence: dict[str, Any] | None,
        simulation_evidence_sidecar_bundle: dict[str, Any] | None,
        measurement_phase_coverage_report: dict[str, Any] | None,
        scope_definition_pack: dict[str, Any] | None,
        decision_rule_profile: dict[str, Any] | None,
        reference_asset_registry: dict[str, Any] | None,
        certificate_lifecycle_summary: dict[str, Any] | None,
        scope_readiness_summary: dict[str, Any] | None,
        certificate_readiness_summary: dict[str, Any] | None,
        pre_run_readiness_gate: dict[str, Any] | None,
        method_confirmation_protocol: dict[str, Any] | None,
        method_confirmation_matrix: dict[str, Any] | None,
        route_specific_validation_matrix: dict[str, Any] | None,
        validation_run_set: dict[str, Any] | None,
        verification_digest: dict[str, Any] | None,
        verification_rollup: dict[str, Any] | None,
        uncertainty_report_pack: dict[str, Any] | None,
        uncertainty_digest: dict[str, Any] | None,
        uncertainty_rollup: dict[str, Any] | None,
        uncertainty_method_readiness_summary: dict[str, Any] | None,
        software_validation_traceability_matrix: dict[str, Any] | None,
        artifact_hash_registry: dict[str, Any] | None,
        environment_fingerprint: dict[str, Any] | None,
        release_manifest: dict[str, Any] | None,
        release_scope_summary: dict[str, Any] | None,
        release_boundary_digest: dict[str, Any] | None,
        release_evidence_pack_index: dict[str, Any] | None,
        software_validation_rollup: dict[str, Any] | None,
        audit_readiness_digest: dict[str, Any] | None,
        compatibility_scan_summary: dict[str, Any] | None,
        recognition_scope_rollup: dict[str, Any] | None,
    ) -> str:
        summary_payload = dict(summary or {})
        stats = dict(summary_payload.get("stats", {}) or {})
        role_summary = dict(artifact_role_summary or {})
        safety = dict(config_safety or {})
        safety_review = dict(config_safety_review or {})
        offline_summary = dict(offline_diagnostic_adapter_summary or {})
        taxonomy_summary = dict(point_taxonomy_summary or {})
        workbench_summary = dict(workbench_evidence_summary or {})
        stability_summary = dict(multi_source_stability_evidence or {})
        transition_summary = dict(state_transition_evidence or {})
        sidecar_summary = dict(simulation_evidence_sidecar_bundle or {})
        phase_coverage_summary = dict(measurement_phase_coverage_report or {})
        scope_definition_payload = dict(scope_definition_pack or {})
        decision_rule_payload = dict(decision_rule_profile or {})
        reference_asset_payload = dict(reference_asset_registry or {})
        certificate_lifecycle_payload = dict(certificate_lifecycle_summary or {})
        scope_readiness_payload = dict(scope_readiness_summary or {})
        certificate_readiness_payload = dict(certificate_readiness_summary or {})
        pre_run_gate_payload = dict(pre_run_readiness_gate or {})
        method_confirmation_protocol_payload = dict(method_confirmation_protocol or {})
        method_confirmation_matrix_payload = dict(method_confirmation_matrix or {})
        route_specific_validation_matrix_payload = dict(route_specific_validation_matrix or {})
        validation_run_set_payload = dict(validation_run_set or {})
        verification_digest_payload = dict(verification_digest or {})
        verification_rollup_payload = dict(verification_rollup or {})
        uncertainty_report_payload = dict(uncertainty_report_pack or {})
        uncertainty_digest_payload = dict(uncertainty_digest or {})
        uncertainty_rollup_payload = dict(uncertainty_rollup or {})
        uncertainty_method_payload = dict(uncertainty_method_readiness_summary or {})
        software_validation_traceability_payload = dict(software_validation_traceability_matrix or {})
        artifact_hash_registry_payload = dict(artifact_hash_registry or {})
        environment_fingerprint_payload = dict(environment_fingerprint or {})
        release_manifest_payload = dict(release_manifest or {})
        release_scope_payload = dict(release_scope_summary or {})
        release_boundary_payload = dict(release_boundary_digest or {})
        release_evidence_payload = dict(release_evidence_pack_index or {})
        software_validation_rollup_payload = dict(software_validation_rollup or {})
        audit_readiness_payload = dict(audit_readiness_digest or {})
        compatibility_summary = dict(compatibility_scan_summary or {})
        compatibility_overview = dict(compatibility_summary.get("compatibility_overview") or {})
        compatibility_rollup = dict(
            compatibility_summary.get("compatibility_rollup")
            or compatibility_overview.get("compatibility_rollup")
            or {}
        )
        scope_rollup = dict(recognition_scope_rollup or {})

        role_parts: list[str] = []
        for role in ("execution_summary", "execution_rows", "diagnostic_analysis", "formal_analysis"):
            payload = dict(role_summary.get(role) or {})
            count = int(payload.get("count", 0) or 0)
            if count > 0:
                role_parts.append(f"{role} {count}")
        artifact_role_text = " | ".join(role_parts) if role_parts else "--"

        sample_count = int(stats.get("sample_count", 0) or 0)
        point_summary_count = len(list(stats.get("point_summaries", []) or []))
        lines = [
            f"结果文件: {'已生成' if isinstance(summary, dict) else '缺失'}",
            f"样本数: {sample_count}",
            f"点摘要数: {point_summary_count}",
            f"工件角色: {artifact_role_text}",
            f"配置安全: {str(safety_review.get('summary') or safety.get('summary') or '--')}",
        ]

        lines.insert(4, f"证据来源: {evidence_source}")
        if offline_summary:
            lines.append(
                "离线诊断: "
                + str(
                    offline_summary.get("summary")
                    or (
                        f"room-temp {int(offline_summary.get('room_temp_count', 0) or 0)} | "
                        f"analyzer-chain {int(offline_summary.get('analyzer_chain_count', 0) or 0)}"
                    )
                )
            )
        if str(offline_summary.get("coverage_summary") or "").strip():
            coverage_summary = humanize_offline_diagnostic_summary_value(str(offline_summary.get("coverage_summary") or ""))
            lines.append(
                t(
                    "facade.results.result_summary.offline_diagnostic_coverage",
                    value=coverage_summary,
                    default=f"离线诊断覆盖：{coverage_summary}",
                )
            )
        if str(offline_summary.get("review_scope_summary") or "").strip():
            review_scope_summary = humanize_offline_diagnostic_summary_value(
                str(offline_summary.get("review_scope_summary") or "")
            )
            lines.append(
                t(
                    "facade.results.result_summary.offline_diagnostic_scope",
                    value=review_scope_summary,
                    default="离线诊断工件范围：" + review_scope_summary,
                )
            )
        if str(offline_summary.get("next_check_summary") or "").strip():
            lines.append(
                t(
                    "facade.results.result_summary.offline_diagnostic_next_checks",
                    value=str(offline_summary.get("next_check_summary") or ""),
                    default=f"离线诊断下一步：{str(offline_summary.get('next_check_summary') or '')}",
                )
            )

        for detail_line in ResultsGateway._offline_diagnostic_detail_lines(offline_summary):
            lines.append(
                t(
                    "facade.results.result_summary.offline_diagnostic_detail",
                    value=detail_line,
                    default=f"离线诊断补充: {detail_line}",
                )
            )
        point_taxonomy_summary = taxonomy_summary
        pressure_summary = str(taxonomy_summary.get("pressure_summary") or "").strip()
        pressure_mode_summary = str(taxonomy_summary.get("pressure_mode_summary") or "").strip()
        pressure_target_label_summary = str(taxonomy_summary.get("pressure_target_label_summary") or "").strip()
        if pressure_summary:
            lines.append(
                t(
                    "facade.results.result_summary.taxonomy_pressure",
                    value=pressure_summary,
                    default=f"压力语义：{pressure_summary}",
                )
            )
        if pressure_mode_summary and pressure_mode_summary != pressure_summary:
            lines.append(
                t(
                    "facade.results.result_summary.taxonomy_pressure_mode",
                    value=pressure_mode_summary,
                    default=f"压力模式：{pressure_mode_summary}",
                )
            )
        if pressure_target_label_summary and pressure_target_label_summary != pressure_summary:
            lines.append(
                t(
                    "facade.results.result_summary.taxonomy_pressure_target_label",
                    value=pressure_target_label_summary,
                    default=f"压力目标标签：{pressure_target_label_summary}",
                )
            )
        if str(point_taxonomy_summary.get("flush_gate_summary") or "").strip():
            lines.append(
                t(
                    "facade.results.result_summary.taxonomy_flush",
                    value=str(point_taxonomy_summary.get("flush_gate_summary") or ""),
                    default=f"冲洗门禁：{str(point_taxonomy_summary.get('flush_gate_summary') or '')}",
                )
            )
        if str(point_taxonomy_summary.get("preseal_summary") or "").strip():
            lines.append(
                t(
                    "facade.results.result_summary.taxonomy_preseal",
                    value=str(point_taxonomy_summary.get("preseal_summary") or ""),
                    default=f"前封气：{str(point_taxonomy_summary.get('preseal_summary') or '')}",
                )
            )
        if str(point_taxonomy_summary.get("postseal_summary") or "").strip():
            lines.append(
                t(
                    "facade.results.result_summary.taxonomy_postseal",
                    value=str(point_taxonomy_summary.get("postseal_summary") or ""),
                    default=f"后封气：{str(point_taxonomy_summary.get('postseal_summary') or '')}",
                )
            )
        if str(point_taxonomy_summary.get("stale_gauge_summary") or "").strip():
            lines.append(
                t(
                    "facade.results.result_summary.taxonomy_stale_gauge",
                    value=str(point_taxonomy_summary.get("stale_gauge_summary") or ""),
                    default=f"压力参考陈旧：{str(point_taxonomy_summary.get('stale_gauge_summary') or '')}",
                )
            )

        workbench_text = str(
            workbench_summary.get("summary_line")
            or workbench_summary.get("summary")
            or workbench_summary.get("review_summary")
            or "--"
        )
        lines.append(f"工作台诊断证据: {workbench_text}")

        if compatibility_summary:
            compatibility_reader_mode = str(
                compatibility_overview.get("current_reader_mode_display")
                or compatibility_summary.get("current_reader_mode_display")
                or compatibility_overview.get("current_reader_mode")
                or compatibility_summary.get("current_reader_mode")
                or "--"
            )
            compatibility_status_text = str(
                compatibility_overview.get("compatibility_status_display")
                or compatibility_summary.get("compatibility_status_display")
                or compatibility_overview.get("compatibility_status")
                or compatibility_summary.get("compatibility_status")
                or "--"
            )
            lines.append(
                t(
                    "facade.results.result_summary.artifact_compatibility",
                    value=f"{compatibility_reader_mode} | {compatibility_status_text}",
                    default=f"工件兼容: {compatibility_reader_mode} | {compatibility_status_text}",
                )
            )
            schema_contract_summary = str(
                compatibility_overview.get("schema_contract_summary_display")
                or compatibility_summary.get("schema_or_contract_version_summary")
                or ""
            ).strip()
            if schema_contract_summary:
                lines.append(
                    t(
                        "facade.results.result_summary.artifact_compatibility_contracts",
                        value=schema_contract_summary,
                        default=f"工件合同/Schema: {schema_contract_summary}",
                    )
                )
            if str(compatibility_summary.get("summary") or "").strip():
                lines.append(
                    t(
                        "facade.results.result_summary.artifact_compatibility_summary",
                        value=str(compatibility_summary.get("summary") or ""),
                        default=f"兼容摘要: {str(compatibility_summary.get('summary') or '')}",
                    )
                )
            rollup_summary = str(
                compatibility_rollup.get("rollup_summary_display")
                or compatibility_overview.get("rollup_summary_display")
                or ""
            ).strip()
            if rollup_summary:
                lines.append(
                    t(
                        "facade.results.result_summary.artifact_compatibility_rollup",
                        value=rollup_summary,
                        default=f"兼容性 rollup：{rollup_summary}",
                    )
                )
            recommendation_text = str(
                compatibility_overview.get("regenerate_recommendation_display")
                or ""
            ).strip()
            if recommendation_text:
                lines.append(
                    t(
                        "facade.results.result_summary.artifact_compatibility_recommendation",
                        value=recommendation_text,
                        default=f"兼容建议: {recommendation_text}",
                    )
                )
            boundary_text = str(
                compatibility_overview.get("non_primary_boundary_display")
                or compatibility_overview.get("non_primary_chain_display")
                or ""
            ).strip()
            if boundary_text:
                lines.append(
                    t(
                        "facade.results.result_summary.artifact_compatibility_boundary",
                        value=boundary_text,
                        default=f"兼容边界: {boundary_text}",
                    )
                )
            non_claim_text = str(
                compatibility_overview.get("non_claim_digest")
                or compatibility_summary.get("non_claim_digest")
                or ""
            ).strip()
            if non_claim_text:
                lines.append(
                    t(
                        "facade.results.result_summary.artifact_compatibility_non_claim",
                        value=non_claim_text,
                        default=f"兼容 non-claim: {non_claim_text}",
                    )
                )
            if bool(compatibility_summary.get("regenerate_recommended", False)):
                lines.append(
                    t(
                        "facade.results.result_summary.artifact_compatibility_regenerate",
                        default="建议运行轻量 reindex/regenerate，仅重建 reviewer/index sidecar，不改写原始主证据",
                    )
                )

        if scope_definition_payload or decision_rule_payload or scope_rollup:
            scope_overview_text = str(
                scope_rollup.get("scope_overview_display")
                or dict(scope_definition_payload.get("digest") or {}).get("scope_overview_summary")
                or dict(scope_definition_payload.get("scope_overview") or {}).get("summary")
                or ""
            ).strip()
            if scope_overview_text:
                lines.append(
                    t(
                        "facade.results.result_summary.scope_package",
                        value=scope_overview_text,
                        default=f"认可范围包：{scope_overview_text}",
                    )
                )
            decision_rule_text = str(
                scope_rollup.get("decision_rule_display")
                or dict(decision_rule_payload.get("digest") or {}).get("decision_rule_summary")
                or decision_rule_payload.get("decision_rule_id")
                or ""
            ).strip()
            if decision_rule_text:
                lines.append(
                    t(
                        "facade.results.result_summary.decision_rule_profile",
                        value=decision_rule_text,
                        default=f"决策规则：{decision_rule_text}",
                    )
                )
            conformity_boundary_text = str(
                scope_rollup.get("conformity_boundary_display")
                or dict(decision_rule_payload.get("digest") or {}).get("conformity_boundary_summary")
                or decision_rule_payload.get("non_claim_note")
                or scope_definition_payload.get("non_claim_note")
                or ""
            ).strip()
            if conformity_boundary_text:
                lines.append(
                    t(
                        "facade.results.result_summary.conformity_boundary",
                        value=conformity_boundary_text,
                        default=f"符合性边界：{conformity_boundary_text}",
                    )
                )
            repository_text = " / ".join(
                part
                for part in (
                    str(scope_rollup.get("repository_mode") or "").strip(),
                    str(scope_rollup.get("gateway_mode") or "").strip(),
                )
                if part
            ).strip()
            if repository_text:
                lines.append(
                    t(
                        "facade.results.result_summary.recognition_scope_repository",
                        value=repository_text,
                        default=f"范围仓储：{repository_text}",
                    )
                )
            if str(scope_rollup.get("rollup_summary_display") or "").strip():
                lines.append(
                    t(
                        "facade.results.result_summary.recognition_scope_rollup",
                        value=str(scope_rollup.get("rollup_summary_display") or "").strip(),
                        default="范围/规则 rollup：{value}",
                    )
                )
            scope_non_claim_text = str(
                scope_rollup.get("non_claim_note")
                or decision_rule_payload.get("non_claim_note")
                or scope_definition_payload.get("non_claim_note")
                or ""
            ).strip()
            if scope_non_claim_text:
                lines.append(
                    t(
                        "facade.results.result_summary.scope_non_claim",
                        value=scope_non_claim_text,
                        default=f"非声明边界：{scope_non_claim_text}",
                    )
                )

            reference_asset_text = str(
                dict(reference_asset_payload.get("digest") or {}).get("asset_readiness_overview")
                or dict(reference_asset_payload.get("digest") or {}).get("summary")
                or ""
            ).strip()
            if reference_asset_text:
                lines.append(
                    t(
                        "facade.results.result_summary.asset_readiness_overview",
                        value=reference_asset_text,
                        default=f"asset readiness overview: {reference_asset_text}",
                    )
                )
            certificate_lifecycle_text = str(
                dict(certificate_lifecycle_payload.get("digest") or {}).get("certificate_lifecycle_overview")
                or dict(certificate_lifecycle_payload.get("digest") or {}).get("summary")
                or ""
            ).strip()
            if certificate_lifecycle_text:
                lines.append(
                    t(
                        "facade.results.result_summary.certificate_lifecycle_overview",
                        value=certificate_lifecycle_text,
                        default=f"certificate lifecycle overview: {certificate_lifecycle_text}",
                    )
                )
            pre_run_gate_text = str(
                dict(pre_run_gate_payload.get("digest") or {}).get("pre_run_gate_status")
                or pre_run_gate_payload.get("gate_status")
                or ""
            ).strip()
            if pre_run_gate_text:
                lines.append(
                    t(
                        "facade.results.result_summary.pre_run_readiness_gate",
                        value=pre_run_gate_text,
                        default=f"pre-run readiness gate: {pre_run_gate_text}",
                    )
                )
            blocking_text = str(
                scope_rollup.get("blocking_digest")
                or dict(pre_run_gate_payload.get("digest") or {}).get("blocker_summary")
                or ""
            ).strip()
            if blocking_text:
                lines.append(
                    t(
                        "facade.results.result_summary.pre_run_blocking_digest",
                        value=blocking_text,
                        default=f"blocking digest: {blocking_text}",
                    )
                )
            warning_text = str(
                scope_rollup.get("warning_digest")
                or dict(pre_run_gate_payload.get("digest") or {}).get("warning_summary")
                or ""
            ).strip()
            if warning_text:
                lines.append(
                    t(
                        "facade.results.result_summary.pre_run_warning_digest",
                        value=warning_text,
                        default=f"warning digest: {warning_text}",
                    )
                )

        uncertainty_digest_text = dict(
            uncertainty_rollup_payload.get("digest")
            or uncertainty_digest_payload.get("digest")
            or uncertainty_report_payload.get("digest")
            or {}
        )
        if uncertainty_report_payload or uncertainty_digest_payload or uncertainty_rollup_payload:
            uncertainty_overview_text = str(
                uncertainty_rollup_payload.get("overview_display")
                or uncertainty_rollup_payload.get("rollup_summary_display")
                or uncertainty_digest_text.get("uncertainty_overview_summary")
                or uncertainty_digest_text.get("summary")
                or ""
            ).strip()
            if uncertainty_overview_text:
                lines.append(
                    t(
                        "facade.results.result_summary.uncertainty_overview",
                        value=uncertainty_overview_text,
                        default=f"不确定度概览：{uncertainty_overview_text}",
                    )
                )
            budget_completeness_text = str(
                uncertainty_rollup_payload.get("budget_completeness_summary")
                or uncertainty_digest_text.get("budget_component_summary")
                or ""
            ).strip()
            if budget_completeness_text:
                lines.append(
                    t(
                        "facade.results.result_summary.uncertainty_budget_completeness",
                        value=budget_completeness_text,
                        default=f"预算完整度：{budget_completeness_text}",
                    )
                )
            top_contributors_text = str(
                uncertainty_rollup_payload.get("top_contributors_summary")
                or uncertainty_digest_text.get("top_contributors_summary")
                or ""
            ).strip()
            if top_contributors_text:
                lines.append(
                    t(
                        "facade.results.result_summary.uncertainty_top_contributors",
                        value=top_contributors_text,
                        default=f"主要不确定度贡献：{top_contributors_text}",
                    )
                )
            data_completeness_text = str(
                uncertainty_rollup_payload.get("data_completeness_summary")
                or uncertainty_digest_text.get("data_completeness_summary")
                or ""
            ).strip()
            if data_completeness_text:
                lines.append(
                    t(
                        "facade.results.result_summary.uncertainty_data_completeness",
                        value=data_completeness_text,
                        default=f"数据完整度：{data_completeness_text}",
                    )
                )
            rollup_status_text = str(
                uncertainty_rollup_payload.get("rollup_summary_display")
                or uncertainty_digest_text.get("summary")
                or ""
            ).strip()
            if rollup_status_text:
                lines.append(
                    t(
                        "facade.results.result_summary.uncertainty_rollup",
                        value=rollup_status_text,
                        default=f"不确定度 rollup：{rollup_status_text}",
                    )
                )
            non_claim_text = str(
                uncertainty_rollup_payload.get("non_claim_note")
                or uncertainty_report_payload.get("non_claim_note")
                or uncertainty_digest_text.get("non_claim_digest")
                or ""
            ).strip()
            if non_claim_text:
                lines.append(
                    t(
                        "facade.results.result_summary.uncertainty_non_claim",
                        value=non_claim_text,
                        default=f"不确定度 non-claim：{non_claim_text}",
                    )
                )

        verification_digest_text = dict(
            verification_rollup_payload.get("digest")
            or verification_digest_payload.get("digest")
            or route_specific_validation_matrix_payload.get("digest")
            or {}
        )
        if (
            method_confirmation_protocol_payload
            or method_confirmation_matrix_payload
            or route_specific_validation_matrix_payload
            or validation_run_set_payload
            or verification_digest_payload
            or verification_rollup_payload
        ):
            protocol_overview_text = str(
                verification_digest_text.get("protocol_overview_summary")
                or dict(method_confirmation_protocol_payload.get("digest") or {}).get("summary")
                or method_confirmation_protocol_payload.get("protocol_id")
                or ""
            ).strip()
            if protocol_overview_text:
                lines.append(
                    t(
                        "facade.results.result_summary.method_confirmation_overview",
                        value=protocol_overview_text,
                        default=f"方法确认概览：{protocol_overview_text}",
                    )
                )
            matrix_completeness_text = str(
                verification_rollup_payload.get("rollup_summary_display")
                or verification_digest_text.get("matrix_completeness_summary")
                or ""
            ).strip()
            if matrix_completeness_text:
                lines.append(
                    t(
                        "facade.results.result_summary.validation_matrix_completeness",
                        value=matrix_completeness_text,
                        default=f"验证矩阵完整度：{matrix_completeness_text}",
                    )
                )
            current_evidence_coverage_text = str(
                verification_digest_text.get("current_evidence_coverage_summary")
                or verification_digest_text.get("current_coverage_summary")
                or ""
            ).strip()
            if current_evidence_coverage_text:
                lines.append(
                    t(
                        "facade.results.result_summary.validation_current_evidence_coverage",
                        value=current_evidence_coverage_text,
                        default=f"当前证据覆盖：{current_evidence_coverage_text}",
                    )
                )
            top_gaps_text = str(
                verification_digest_text.get("top_gaps_summary")
                or verification_digest_text.get("missing_evidence_summary")
                or ""
            ).strip()
            if top_gaps_text:
                lines.append(
                    t(
                        "facade.results.result_summary.validation_top_gaps",
                        value=top_gaps_text,
                        default=f"主要缺口：{top_gaps_text}",
                    )
                )
            reviewer_actions_text = str(
                verification_digest_text.get("reviewer_action_summary")
                or ""
            ).strip()
            if reviewer_actions_text:
                lines.append(
                    t(
                        "facade.results.result_summary.validation_reviewer_actions",
                        value=reviewer_actions_text,
                        default=f"审阅动作：{reviewer_actions_text}",
                    )
                )
            method_non_claim_text = str(
                verification_rollup_payload.get("non_claim_note")
                or verification_digest_payload.get("non_claim_note")
                or verification_digest_text.get("non_claim_digest")
                or ""
            ).strip()
            if method_non_claim_text:
                lines.append(
                    t(
                        "facade.results.result_summary.method_confirmation_non_claim",
                        value=method_non_claim_text,
                        default=f"方法确认 non-claim：{method_non_claim_text}",
                    )
                )
            readiness_status_text = str(
                verification_rollup_payload.get("readiness_status_summary")
                or verification_digest_text.get("readiness_status_summary")
                or route_specific_validation_matrix_payload.get("validation_status")
                or ""
            ).strip()
            if readiness_status_text:
                lines.append(
                    t(
                        "facade.results.result_summary.verification_readiness_status",
                        value=readiness_status_text,
                        default=f"验证就绪状态：{readiness_status_text}",
                    )
                )

        stability_digest = dict(stability_summary.get("digest") or {})
        transition_digest = dict(transition_summary.get("digest") or {})
        phase_coverage_digest = dict(phase_coverage_summary.get("digest") or {})
        measurement_core_stability_text = (
            str(
                stability_digest.get("summary")
                or stability_summary.get("summary")
                or stability_summary.get("coverage_status")
                or "--"
            )
            if stability_summary
            else ""
        )
        measurement_core_transition_text = (
            str(
                transition_digest.get("summary")
                or transition_summary.get("summary")
                or transition_summary.get("overall_status")
                or "--"
            )
            if transition_summary
            else ""
        )
        measurement_core_phase_coverage_text = (
            str(
                phase_coverage_digest.get("summary")
                or phase_coverage_summary.get("summary")
                or phase_coverage_summary.get("overall_status")
                or "--"
            )
            if phase_coverage_summary
            else ""
        )
        measurement_core_payload_phase_text = (
            str(phase_coverage_digest.get("payload_phase_summary") or "").strip()
            if phase_coverage_summary
            else ""
        )
        measurement_core_payload_complete_text = (
            str(phase_coverage_digest.get("payload_complete_phase_summary") or "").strip()
            if phase_coverage_summary
            else ""
        )
        measurement_core_payload_partial_text = (
            str(phase_coverage_digest.get("payload_partial_phase_summary") or "").strip()
            if phase_coverage_summary
            else ""
        )
        measurement_core_trace_only_text = (
            str(phase_coverage_digest.get("trace_only_phase_summary") or "").strip()
            if phase_coverage_summary
            else ""
        )
        measurement_core_payload_completeness_text = (
            str(phase_coverage_digest.get("payload_completeness_summary") or "").strip()
            if phase_coverage_summary
            else ""
        )
        measurement_core_next_artifacts_text = (
            str(phase_coverage_digest.get("next_required_artifacts_summary") or "").strip()
            if phase_coverage_summary
            else ""
        )
        measurement_core_sidecar_text = (
            " | ".join(
                f"{key} {len(list(value or []))}"
                for key, value in dict(sidecar_summary.get("stores") or {}).items()
            )
            if sidecar_summary
            else ""
        )
        measurement_review_lines = build_measurement_review_digest_lines(phase_coverage_summary)

        if measurement_core_stability_text:
            lines.append(
                humanize_review_surface_text(
                    t(
                        "facade.results.result_summary.measurement_core_stability",
                        value=measurement_core_stability_text,
                        default=f"multi-source stability shadow: {measurement_core_stability_text}",
                    )
                )
            )
        if measurement_core_transition_text:
            lines.append(
                humanize_review_surface_text(
                    t(
                        "facade.results.result_summary.measurement_core_transition",
                        value=measurement_core_transition_text,
                        default=f"controlled state trace: {measurement_core_transition_text}",
                    )
                )
            )
        if measurement_core_phase_coverage_text:
            lines.extend(measurement_review_lines.get("summary_lines") or [])
            lines.extend((measurement_review_lines.get("detail_lines") or [])[:4])
        if measurement_core_sidecar_text or dict(simulation_evidence_sidecar_bundle or {}):
            sidecar_contract_text = str(sidecar_summary.get("reviewer_note") or "").strip()
            lines.append(
                humanize_review_surface_text(
                    "sidecar-ready contract: "
                    + (
                        measurement_core_sidecar_text
                        or sidecar_contract_text
                        or "future database intake / sidecar-ready"
                    )
                )
            )
        software_validation_overview_text = str(
            software_validation_rollup_payload.get("rollup_summary_display")
            or dict(release_manifest_payload.get("digest") or {}).get("summary")
            or dict(software_validation_traceability_payload.get("digest") or {}).get("summary")
            or ""
        ).strip()
        if software_validation_overview_text:
            lines.append(
                t(
                    "facade.results.result_summary.software_validation_overview",
                    value=software_validation_overview_text,
                    default=f"软件验证总览：{software_validation_overview_text}",
                )
            )
        traceability_completeness_text = str(
            software_validation_rollup_payload.get("traceability_completeness_summary")
            or dict(software_validation_traceability_payload.get("digest") or {}).get("current_coverage_summary")
            or ""
        ).strip()
        if traceability_completeness_text:
            lines.append(
                t(
                    "facade.results.result_summary.traceability_completeness",
                    value=traceability_completeness_text,
                    default=f"追溯完整度：{traceability_completeness_text}",
                )
            )
        audit_hash_summary_text = str(
            software_validation_rollup_payload.get("hash_registry_summary")
            or dict(artifact_hash_registry_payload.get("digest") or {}).get("summary")
            or ""
        ).strip()
        if audit_hash_summary_text:
            lines.append(
                t(
                    "facade.results.result_summary.audit_hash_summary",
                    value=audit_hash_summary_text,
                    default=f"审计哈希：{audit_hash_summary_text}",
                )
            )
        environment_fingerprint_text = str(
            software_validation_rollup_payload.get("environment_summary")
            or environment_fingerprint_payload.get("environment_summary")
            or dict(environment_fingerprint_payload.get("digest") or {}).get("summary")
            or ""
        ).strip()
        if environment_fingerprint_text:
            lines.append(
                t(
                    "facade.results.result_summary.environment_fingerprint_summary",
                    value=environment_fingerprint_text,
                    default=f"环境指纹：{environment_fingerprint_text}",
                )
            )
        release_manifest_overview_text = str(
            software_validation_rollup_payload.get("release_manifest_summary")
            or dict(release_manifest_payload.get("digest") or {}).get("summary")
            or ""
        ).strip()
        if release_manifest_overview_text:
            lines.append(
                t(
                    "facade.results.result_summary.release_manifest_overview",
                    value=release_manifest_overview_text,
                    default=f"Release manifest：{release_manifest_overview_text}",
                )
            )
        release_linkage_text = " | ".join(
            part
            for part in (
                f"parity {str(software_validation_rollup_payload.get('parity_status') or release_manifest_payload.get('parity_status') or '--').strip()}",
                f"resilience {str(software_validation_rollup_payload.get('resilience_status') or release_manifest_payload.get('resilience_status') or '--').strip()}",
                f"smoke {str(software_validation_rollup_payload.get('smoke_status') or release_manifest_payload.get('smoke_status') or '--').strip()}",
            )
            if str(part).strip()
        ).strip()
        if release_linkage_text:
            lines.append(
                t(
                    "facade.results.result_summary.release_test_linkage",
                    value=release_linkage_text,
                    default=f"验证联动：{release_linkage_text}",
                )
            )
        release_scope_text = str(
            dict(release_scope_payload.get("digest") or {}).get("summary")
            or release_scope_payload.get("scope_id")
            or ""
        ).strip()
        if release_scope_text:
            lines.append(
                t(
                    "facade.results.result_summary.release_scope_summary",
                    value=release_scope_text,
                    default=f"Release scope：{release_scope_text}",
                )
            )
        release_boundary_text = str(
            dict(release_boundary_payload.get("digest") or {}).get("summary")
            or release_boundary_payload.get("non_claim_note")
            or ""
        ).strip()
        if release_boundary_text:
            lines.append(
                t(
                    "facade.results.result_summary.release_boundary_summary",
                    value=release_boundary_text,
                    default=f"非 claim 边界：{release_boundary_text}",
                )
            )
        release_pack_index_text = str(
            dict(release_evidence_payload.get("digest") or {}).get("summary")
            or ""
        ).strip()
        if release_pack_index_text:
            lines.append(
                t(
                    "facade.results.result_summary.release_evidence_pack_index",
                    value=release_pack_index_text,
                    default=f"Evidence pack：{release_pack_index_text}",
                )
            )
        for readiness_payload in (
            scope_definition_payload,
            decision_rule_payload,
            reference_asset_payload,
            certificate_lifecycle_payload,
            scope_readiness_payload,
            certificate_readiness_payload,
            pre_run_gate_payload,
            uncertainty_method_payload,
            software_validation_traceability_payload,
            artifact_hash_registry_payload,
            environment_fingerprint_payload,
            release_manifest_payload,
            release_boundary_payload,
            audit_readiness_payload,
        ):
            localized_lines = build_readiness_review_digest_lines(readiness_payload)
            lines.extend(localized_lines.get("summary_lines") or [])
            lines.extend((localized_lines.get("detail_lines") or [])[:5])

        return "\n".join(line for line in lines if str(line).strip())

    @staticmethod
    def _resolve_current_run_evidence_source(
        workbench_evidence_summary: dict[str, Any] | None,
        workbench_action_report: dict[str, Any] | None,
    ) -> str:
        source = (
            dict(workbench_evidence_summary or {}).get("evidence_source")
            or dict(workbench_action_report or {}).get("evidence_source")
            or "simulated_protocol"
        )
        return normalize_evidence_source(source)

    @staticmethod
    def _offline_diagnostic_detail_lines(
        offline_diagnostic_adapter_summary: dict[str, Any] | None,
        *,
        limit: int = 3,
    ) -> list[str]:
        return collect_offline_diagnostic_detail_lines(offline_diagnostic_adapter_summary, limit=limit)

    @staticmethod
    def _offline_diagnostic_detail_item_line(item: Any) -> str:
        return build_offline_diagnostic_detail_item_line(item)

    @staticmethod
    def _offline_diagnostic_scope_line(scope_summary: str) -> str:
        return build_offline_diagnostic_scope_line(scope_summary)

    @staticmethod
    def _offline_diagnostic_scope_label() -> str:
        return offline_diagnostic_scope_label()

    @staticmethod
    def _normalize_offline_diagnostic_line(line: str) -> str:
        return normalize_offline_diagnostic_line(line)

    @staticmethod
    def _decorate_phase_transition_bridge_reviewer_artifact_row(
        row: dict[str, Any],
        *,
        reviewer_artifact_entry: dict[str, Any],
    ) -> dict[str, Any]:
        payload = dict(row or {})
        if str(payload.get("artifact_key") or "") != PHASE_TRANSITION_BRIDGE_REVIEWER_ARTIFACT_KEY:
            return payload
        entry = dict(reviewer_artifact_entry or {})
        if not entry:
            return payload
        return {
            **payload,
            "name": str(entry.get("name_text") or payload.get("name") or ""),
            "note": str(entry.get("note_text") or payload.get("note") or ""),
            "role_status_display": str(entry.get("role_status_display") or payload.get("role_status_display") or ""),
            "phase_transition_bridge_reviewer_artifact_entry": entry,
        }

    @staticmethod
    def _decorate_stage_admission_review_pack_row(
        row: dict[str, Any],
        *,
        stage_admission_review_pack_entry: dict[str, Any],
    ) -> dict[str, Any]:
        payload = dict(row or {})
        artifact_key = str(payload.get("artifact_key") or "")
        if artifact_key not in {
            STAGE_ADMISSION_REVIEW_PACK_ARTIFACT_KEY,
            STAGE_ADMISSION_REVIEW_PACK_REVIEWER_ARTIFACT_KEY,
        }:
            return payload
        entry = dict(stage_admission_review_pack_entry or {})
        if not entry:
            return payload
        is_reviewer_artifact = artifact_key == STAGE_ADMISSION_REVIEW_PACK_REVIEWER_ARTIFACT_KEY
        existing_role_status = str(payload.get("role_status_display") or "").strip()
        entry_role_status = str(entry.get("role_status_display") or "").strip()
        role_status_display = " | ".join(
            part
            for part in (existing_role_status, entry_role_status)
            if str(part).strip()
        )
        return {
            **payload,
            "name": str(
                entry.get("name_text")
                or payload.get("name")
                or ""
            )
            + (" (Markdown)" if is_reviewer_artifact else " (JSON)"),
            "note": str(entry.get("note_text") or payload.get("note") or ""),
            "role_status_display": role_status_display or existing_role_status,
            "stage_admission_review_pack_artifact_entry": entry,
        }

    @staticmethod
    def _decorate_engineering_isolation_admission_checklist_row(
        row: dict[str, Any],
        *,
        engineering_isolation_admission_checklist_entry: dict[str, Any],
    ) -> dict[str, Any]:
        payload = dict(row or {})
        artifact_key = str(payload.get("artifact_key") or "")
        if artifact_key not in {
            ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_ARTIFACT_KEY,
            ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_REVIEWER_ARTIFACT_KEY,
        }:
            return payload
        entry = dict(engineering_isolation_admission_checklist_entry or {})
        if not entry:
            return payload
        is_reviewer_artifact = artifact_key == ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_REVIEWER_ARTIFACT_KEY
        existing_role_status = str(payload.get("role_status_display") or "").strip()
        entry_role_status = str(entry.get("role_status_display") or "").strip()
        role_status_display = " | ".join(
            part
            for part in (existing_role_status, entry_role_status)
            if str(part).strip()
        )
        return {
            **payload,
            "name": str(
                entry.get("name_text")
                or payload.get("name")
                or ""
            )
            + (" (Markdown)" if is_reviewer_artifact else " (JSON)"),
            "note": str(entry.get("note_text") or payload.get("note") or ""),
            "role_status_display": role_status_display or existing_role_status,
            "engineering_isolation_admission_checklist_artifact_entry": entry,
        }

    @staticmethod
    def _decorate_stage3_real_validation_plan_row(
        row: dict[str, Any],
        *,
        stage3_real_validation_plan_entry: dict[str, Any],
    ) -> dict[str, Any]:
        payload = dict(row or {})
        artifact_key = str(payload.get("artifact_key") or "")
        if artifact_key not in {
            STAGE3_REAL_VALIDATION_PLAN_ARTIFACT_KEY,
            STAGE3_REAL_VALIDATION_PLAN_REVIEWER_ARTIFACT_KEY,
        }:
            return payload
        entry = dict(stage3_real_validation_plan_entry or {})
        if not entry:
            return payload
        is_reviewer_artifact = artifact_key == STAGE3_REAL_VALIDATION_PLAN_REVIEWER_ARTIFACT_KEY
        existing_role_status = str(payload.get("role_status_display") or "").strip()
        entry_role_status = str(entry.get("role_status_display") or "").strip()
        role_status_display = " | ".join(
            part
            for part in (existing_role_status, entry_role_status)
            if str(part).strip()
        )
        return {
            **payload,
            "name": str(
                entry.get("name_text")
                or payload.get("name")
                or ""
            )
            + (" (Markdown)" if is_reviewer_artifact else " (JSON)"),
            "note": str(entry.get("summary_text") or payload.get("note") or ""),
            "role_status_display": role_status_display or existing_role_status,
            "stage3_real_validation_plan_artifact_entry": entry,
        }

    @staticmethod
    def _decorate_stage3_standards_alignment_matrix_row(
        row: dict[str, Any],
        *,
        stage3_standards_alignment_matrix_entry: dict[str, Any],
    ) -> dict[str, Any]:
        payload = dict(row or {})
        artifact_key = str(payload.get("artifact_key") or "")
        if artifact_key not in {
            STAGE3_STANDARDS_ALIGNMENT_MATRIX_ARTIFACT_KEY,
            STAGE3_STANDARDS_ALIGNMENT_MATRIX_REVIEWER_ARTIFACT_KEY,
        }:
            return payload
        entry = dict(stage3_standards_alignment_matrix_entry or {})
        if not entry:
            return payload
        is_reviewer_artifact = artifact_key == STAGE3_STANDARDS_ALIGNMENT_MATRIX_REVIEWER_ARTIFACT_KEY
        existing_role_status = str(payload.get("role_status_display") or "").strip()
        entry_role_status = str(entry.get("role_status_display") or "").strip()
        role_status_display = " | ".join(
            part
            for part in (existing_role_status, entry_role_status)
            if str(part).strip()
        )
        return {
            **payload,
            "name": str(entry.get("name_text") or payload.get("name") or "")
            + (" (Markdown)" if is_reviewer_artifact else " (JSON)"),
            "note": str(entry.get("summary_text") or payload.get("note") or ""),
            "role_status_display": role_status_display or existing_role_status,
            "stage3_standards_alignment_matrix_artifact_entry": entry,
        }

    @classmethod
    def _decorate_multi_source_stability_row(
        cls,
        row: dict[str, Any],
        *,
        multi_source_stability_evidence: dict[str, Any],
    ) -> dict[str, Any]:
        return cls._decorate_measurement_core_row(
            row,
            payload=multi_source_stability_evidence,
            json_filename=MULTI_SOURCE_STABILITY_EVIDENCE_FILENAME,
            markdown_filename=MULTI_SOURCE_STABILITY_EVIDENCE_MARKDOWN_FILENAME,
            entry_key="multi_source_stability_evidence_entry",
        )

    @classmethod
    def _decorate_state_transition_evidence_row(
        cls,
        row: dict[str, Any],
        *,
        state_transition_evidence: dict[str, Any],
    ) -> dict[str, Any]:
        return cls._decorate_measurement_core_row(
            row,
            payload=state_transition_evidence,
            json_filename=STATE_TRANSITION_EVIDENCE_FILENAME,
            markdown_filename=STATE_TRANSITION_EVIDENCE_MARKDOWN_FILENAME,
            entry_key="state_transition_evidence_entry",
        )

    @classmethod
    def _decorate_measurement_phase_coverage_row(
        cls,
        row: dict[str, Any],
        *,
        measurement_phase_coverage_report: dict[str, Any],
    ) -> dict[str, Any]:
        return cls._decorate_measurement_core_row(
            row,
            payload=measurement_phase_coverage_report,
            json_filename=MEASUREMENT_PHASE_COVERAGE_REPORT_FILENAME,
            markdown_filename=MEASUREMENT_PHASE_COVERAGE_REPORT_MARKDOWN_FILENAME,
            entry_key="measurement_phase_coverage_report_entry",
        )

    @classmethod
    def _decorate_scope_readiness_summary_row(
        cls,
        row: dict[str, Any],
        *,
        scope_readiness_summary: dict[str, Any],
    ) -> dict[str, Any]:
        return cls._decorate_measurement_core_row(
            row,
            payload=scope_readiness_summary,
            json_filename=recognition_readiness.SCOPE_READINESS_SUMMARY_FILENAME,
            markdown_filename=recognition_readiness.SCOPE_READINESS_SUMMARY_MARKDOWN_FILENAME,
            entry_key="scope_readiness_summary_entry",
        )

    @classmethod
    def _decorate_scope_definition_pack_row(
        cls,
        row: dict[str, Any],
        *,
        scope_definition_pack: dict[str, Any],
    ) -> dict[str, Any]:
        return cls._decorate_measurement_core_row(
            row,
            payload=scope_definition_pack,
            json_filename=recognition_readiness.SCOPE_DEFINITION_PACK_FILENAME,
            markdown_filename=recognition_readiness.SCOPE_DEFINITION_PACK_MARKDOWN_FILENAME,
            entry_key="scope_definition_pack_entry",
        )

    @classmethod
    def _decorate_decision_rule_profile_row(
        cls,
        row: dict[str, Any],
        *,
        decision_rule_profile: dict[str, Any],
    ) -> dict[str, Any]:
        return cls._decorate_measurement_core_row(
            row,
            payload=decision_rule_profile,
            json_filename=recognition_readiness.DECISION_RULE_PROFILE_FILENAME,
            markdown_filename=recognition_readiness.DECISION_RULE_PROFILE_MARKDOWN_FILENAME,
            entry_key="decision_rule_profile_entry",
        )

    @classmethod
    def _decorate_reference_asset_registry_row(
        cls,
        row: dict[str, Any],
        *,
        reference_asset_registry: dict[str, Any],
    ) -> dict[str, Any]:
        return cls._decorate_measurement_core_row(
            row,
            payload=reference_asset_registry,
            json_filename=recognition_readiness.REFERENCE_ASSET_REGISTRY_FILENAME,
            markdown_filename=recognition_readiness.REFERENCE_ASSET_REGISTRY_MARKDOWN_FILENAME,
            entry_key="reference_asset_registry_entry",
        )

    @classmethod
    def _decorate_certificate_lifecycle_summary_row(
        cls,
        row: dict[str, Any],
        *,
        certificate_lifecycle_summary: dict[str, Any],
    ) -> dict[str, Any]:
        return cls._decorate_measurement_core_row(
            row,
            payload=certificate_lifecycle_summary,
            json_filename=recognition_readiness.CERTIFICATE_LIFECYCLE_SUMMARY_FILENAME,
            markdown_filename=recognition_readiness.CERTIFICATE_LIFECYCLE_SUMMARY_MARKDOWN_FILENAME,
            entry_key="certificate_lifecycle_summary_entry",
        )

    @classmethod
    def _decorate_certificate_readiness_summary_row(
        cls,
        row: dict[str, Any],
        *,
        certificate_readiness_summary: dict[str, Any],
    ) -> dict[str, Any]:
        return cls._decorate_measurement_core_row(
            row,
            payload=certificate_readiness_summary,
            json_filename=recognition_readiness.CERTIFICATE_READINESS_SUMMARY_FILENAME,
            markdown_filename=recognition_readiness.CERTIFICATE_READINESS_SUMMARY_MARKDOWN_FILENAME,
            entry_key="certificate_readiness_summary_entry",
        )

    @classmethod
    def _decorate_pre_run_readiness_gate_row(
        cls,
        row: dict[str, Any],
        *,
        pre_run_readiness_gate: dict[str, Any],
    ) -> dict[str, Any]:
        return cls._decorate_measurement_core_row(
            row,
            payload=pre_run_readiness_gate,
            json_filename=recognition_readiness.PRE_RUN_READINESS_GATE_FILENAME,
            markdown_filename=recognition_readiness.PRE_RUN_READINESS_GATE_MARKDOWN_FILENAME,
            entry_key="pre_run_readiness_gate_entry",
        )

    @classmethod
    def _decorate_method_confirmation_protocol_row(
        cls,
        row: dict[str, Any],
        *,
        method_confirmation_protocol: dict[str, Any],
    ) -> dict[str, Any]:
        return cls._decorate_measurement_core_row(
            row,
            payload=method_confirmation_protocol,
            json_filename=recognition_readiness.METHOD_CONFIRMATION_PROTOCOL_FILENAME,
            markdown_filename=recognition_readiness.METHOD_CONFIRMATION_PROTOCOL_MARKDOWN_FILENAME,
            entry_key="method_confirmation_protocol_entry",
        )

    @classmethod
    def _decorate_method_confirmation_matrix_row(
        cls,
        row: dict[str, Any],
        *,
        method_confirmation_matrix: dict[str, Any],
    ) -> dict[str, Any]:
        return cls._decorate_measurement_core_row(
            row,
            payload=method_confirmation_matrix,
            json_filename=recognition_readiness.METHOD_CONFIRMATION_MATRIX_FILENAME,
            markdown_filename=recognition_readiness.METHOD_CONFIRMATION_MATRIX_MARKDOWN_FILENAME,
            entry_key="method_confirmation_matrix_entry",
        )

    @classmethod
    def _decorate_route_specific_validation_matrix_row(
        cls,
        row: dict[str, Any],
        *,
        route_specific_validation_matrix: dict[str, Any],
    ) -> dict[str, Any]:
        return cls._decorate_measurement_core_row(
            row,
            payload=route_specific_validation_matrix,
            json_filename=recognition_readiness.ROUTE_SPECIFIC_VALIDATION_MATRIX_FILENAME,
            markdown_filename=recognition_readiness.ROUTE_SPECIFIC_VALIDATION_MATRIX_MARKDOWN_FILENAME,
            entry_key="route_specific_validation_matrix_entry",
        )

    @classmethod
    def _decorate_validation_run_set_row(
        cls,
        row: dict[str, Any],
        *,
        validation_run_set: dict[str, Any],
    ) -> dict[str, Any]:
        return cls._decorate_measurement_core_row(
            row,
            payload=validation_run_set,
            json_filename=recognition_readiness.VALIDATION_RUN_SET_FILENAME,
            markdown_filename=recognition_readiness.VALIDATION_RUN_SET_MARKDOWN_FILENAME,
            entry_key="validation_run_set_entry",
        )

    @classmethod
    def _decorate_verification_digest_row(
        cls,
        row: dict[str, Any],
        *,
        verification_digest: dict[str, Any],
    ) -> dict[str, Any]:
        return cls._decorate_measurement_core_row(
            row,
            payload=verification_digest,
            json_filename=recognition_readiness.VERIFICATION_DIGEST_FILENAME,
            markdown_filename=recognition_readiness.VERIFICATION_DIGEST_MARKDOWN_FILENAME,
            entry_key="verification_digest_entry",
        )

    @classmethod
    def _decorate_verification_rollup_row(
        cls,
        row: dict[str, Any],
        *,
        verification_rollup: dict[str, Any],
    ) -> dict[str, Any]:
        return cls._decorate_measurement_core_row(
            row,
            payload=verification_rollup,
            json_filename=recognition_readiness.VERIFICATION_ROLLUP_FILENAME,
            markdown_filename=recognition_readiness.VERIFICATION_ROLLUP_MARKDOWN_FILENAME,
            entry_key="verification_rollup_entry",
        )

    @classmethod
    def _decorate_uncertainty_model_row(
        cls,
        row: dict[str, Any],
        *,
        uncertainty_model: dict[str, Any],
    ) -> dict[str, Any]:
        return cls._decorate_measurement_core_row(
            row,
            payload=uncertainty_model,
            json_filename=recognition_readiness.UNCERTAINTY_MODEL_FILENAME,
            markdown_filename=recognition_readiness.UNCERTAINTY_MODEL_MARKDOWN_FILENAME,
            entry_key="uncertainty_model_entry",
        )

    @classmethod
    def _decorate_uncertainty_input_set_row(
        cls,
        row: dict[str, Any],
        *,
        uncertainty_input_set: dict[str, Any],
    ) -> dict[str, Any]:
        return cls._decorate_measurement_core_row(
            row,
            payload=uncertainty_input_set,
            json_filename=recognition_readiness.UNCERTAINTY_INPUT_SET_FILENAME,
            markdown_filename=recognition_readiness.UNCERTAINTY_INPUT_SET_MARKDOWN_FILENAME,
            entry_key="uncertainty_input_set_entry",
        )

    @classmethod
    def _decorate_sensitivity_coefficient_set_row(
        cls,
        row: dict[str, Any],
        *,
        sensitivity_coefficient_set: dict[str, Any],
    ) -> dict[str, Any]:
        return cls._decorate_measurement_core_row(
            row,
            payload=sensitivity_coefficient_set,
            json_filename=recognition_readiness.SENSITIVITY_COEFFICIENT_SET_FILENAME,
            markdown_filename=recognition_readiness.SENSITIVITY_COEFFICIENT_SET_MARKDOWN_FILENAME,
            entry_key="sensitivity_coefficient_set_entry",
        )

    @classmethod
    def _decorate_budget_case_row(
        cls,
        row: dict[str, Any],
        *,
        budget_case: dict[str, Any],
    ) -> dict[str, Any]:
        return cls._decorate_measurement_core_row(
            row,
            payload=budget_case,
            json_filename=recognition_readiness.BUDGET_CASE_FILENAME,
            markdown_filename=recognition_readiness.BUDGET_CASE_MARKDOWN_FILENAME,
            entry_key="budget_case_entry",
        )

    @classmethod
    def _decorate_uncertainty_golden_cases_row(
        cls,
        row: dict[str, Any],
        *,
        uncertainty_golden_cases: dict[str, Any],
    ) -> dict[str, Any]:
        return cls._decorate_measurement_core_row(
            row,
            payload=uncertainty_golden_cases,
            json_filename=recognition_readiness.UNCERTAINTY_GOLDEN_CASES_FILENAME,
            markdown_filename=recognition_readiness.UNCERTAINTY_GOLDEN_CASES_MARKDOWN_FILENAME,
            entry_key="uncertainty_golden_cases_entry",
        )

    @classmethod
    def _decorate_uncertainty_report_pack_row(
        cls,
        row: dict[str, Any],
        *,
        uncertainty_report_pack: dict[str, Any],
    ) -> dict[str, Any]:
        return cls._decorate_measurement_core_row(
            row,
            payload=uncertainty_report_pack,
            json_filename=recognition_readiness.UNCERTAINTY_REPORT_PACK_FILENAME,
            markdown_filename=recognition_readiness.UNCERTAINTY_REPORT_PACK_MARKDOWN_FILENAME,
            entry_key="uncertainty_report_pack_entry",
        )

    @classmethod
    def _decorate_uncertainty_digest_row(
        cls,
        row: dict[str, Any],
        *,
        uncertainty_digest: dict[str, Any],
    ) -> dict[str, Any]:
        return cls._decorate_measurement_core_row(
            row,
            payload=uncertainty_digest,
            json_filename=recognition_readiness.UNCERTAINTY_DIGEST_FILENAME,
            markdown_filename=recognition_readiness.UNCERTAINTY_DIGEST_MARKDOWN_FILENAME,
            entry_key="uncertainty_digest_entry",
        )

    @classmethod
    def _decorate_uncertainty_rollup_row(
        cls,
        row: dict[str, Any],
        *,
        uncertainty_rollup: dict[str, Any],
    ) -> dict[str, Any]:
        return cls._decorate_measurement_core_row(
            row,
            payload=uncertainty_rollup,
            json_filename=recognition_readiness.UNCERTAINTY_ROLLUP_FILENAME,
            markdown_filename=recognition_readiness.UNCERTAINTY_ROLLUP_MARKDOWN_FILENAME,
            entry_key="uncertainty_rollup_entry",
        )

    @classmethod
    def _decorate_uncertainty_method_readiness_summary_row(
        cls,
        row: dict[str, Any],
        *,
        uncertainty_method_readiness_summary: dict[str, Any],
    ) -> dict[str, Any]:
        return cls._decorate_measurement_core_row(
            row,
            payload=uncertainty_method_readiness_summary,
            json_filename=recognition_readiness.UNCERTAINTY_METHOD_READINESS_SUMMARY_FILENAME,
            markdown_filename=recognition_readiness.UNCERTAINTY_METHOD_READINESS_SUMMARY_MARKDOWN_FILENAME,
            entry_key="uncertainty_method_readiness_summary_entry",
        )

    @classmethod
    def _decorate_audit_readiness_digest_row(
        cls,
        row: dict[str, Any],
        *,
        audit_readiness_digest: dict[str, Any],
    ) -> dict[str, Any]:
        return cls._decorate_measurement_core_row(
            row,
            payload=audit_readiness_digest,
            json_filename=recognition_readiness.AUDIT_READINESS_DIGEST_FILENAME,
            markdown_filename=recognition_readiness.AUDIT_READINESS_DIGEST_MARKDOWN_FILENAME,
            entry_key="audit_readiness_digest_entry",
        )

    @staticmethod
    def _decorate_simulation_sidecar_bundle_row(
        row: dict[str, Any],
        *,
        simulation_evidence_sidecar_bundle: dict[str, Any],
    ) -> dict[str, Any]:
        payload = dict(row or {})
        if Path(str(payload.get("path") or "")).name != SIMULATION_EVIDENCE_SIDECAR_BUNDLE_FILENAME:
            return payload
        sidecar_payload = dict(simulation_evidence_sidecar_bundle or {})
        if not sidecar_payload:
            return payload
        note = str(sidecar_payload.get("reviewer_note") or payload.get("note") or "").strip()
        boundary_summary = " | ".join(str(item).strip() for item in list(sidecar_payload.get("boundary_statements") or []) if str(item).strip())
        role_status_display = " | ".join(
            part
            for part in (
                str(payload.get("role_status_display") or "").strip(),
                boundary_summary,
            )
            if str(part).strip()
        )
        return {
            **payload,
            "name": str(sidecar_payload.get("title_text") or payload.get("name") or ""),
            "note": note,
            "role_status_display": role_status_display or str(payload.get("role_status_display") or ""),
            "simulation_evidence_sidecar_bundle_entry": sidecar_payload,
        }

    @staticmethod
    def _build_artifact_compatibility_lookup(run_artifact_index: dict[str, Any]) -> dict[str, dict[str, Any]]:
        lookup: dict[str, dict[str, Any]] = {}
        for entry in list(dict(run_artifact_index or {}).get("entries") or []):
            if not isinstance(entry, dict):
                continue
            artifact_path = str(entry.get("artifact_path") or "").strip()
            artifact_name = str(entry.get("artifact_name") or "").strip()
            if artifact_path:
                lookup[artifact_path] = dict(entry)
            if artifact_name and artifact_name not in lookup:
                lookup[artifact_name] = dict(entry)
        return lookup

    @staticmethod
    def _decorate_artifact_compatibility_row(
        row: dict[str, Any],
        *,
        compatibility_lookup: dict[str, dict[str, Any]],
        compatibility_scan_summary: dict[str, Any],
    ) -> dict[str, Any]:
        payload = dict(row or {})
        key_by_path = str(payload.get("path") or "").strip()
        key_by_name = Path(key_by_path).name if key_by_path else str(payload.get("name") or "").strip()
        compatibility_entry = dict(compatibility_lookup.get(key_by_path) or compatibility_lookup.get(key_by_name) or {})
        if not compatibility_entry:
            return payload
        summary_payload = dict(compatibility_scan_summary or {})
        compatibility_overview = dict(summary_payload.get("compatibility_overview") or {})
        compatibility_status = str(
            compatibility_entry.get("compatibility_status_display")
            or compatibility_entry.get("compatibility_status")
            or "--"
        )
        reader_mode = str(
            compatibility_entry.get("reader_mode_display")
            or compatibility_entry.get("reader_mode")
            or "--"
        )
        version_text = str(compatibility_entry.get("schema_or_contract_version") or "--")
        schema_contract_summary = str(
            compatibility_overview.get("schema_contract_summary_display")
            or summary_payload.get("schema_or_contract_version_summary")
            or version_text
        ).strip()
        recommendation_text = str(
            compatibility_overview.get("regenerate_recommendation_display")
            or ""
        ).strip()
        boundary_text = str(
            compatibility_overview.get("non_primary_boundary_display")
            or compatibility_overview.get("non_primary_chain_display")
            or ""
        ).strip()
        entry_lines = [
            f"版本 {version_text}",
            f"状态 {compatibility_status}",
            f"读取 {reader_mode}",
        ]
        if schema_contract_summary:
            entry_lines.append(f"合同/Schema {schema_contract_summary}")
        if bool(compatibility_entry.get("regenerate_recommended", False)):
            entry_lines.append("建议再生成 reviewer/index sidecar")
        if recommendation_text:
            entry_lines.append(f"建议 {recommendation_text}")
        note_parts = [
            str(payload.get("note") or "").strip(),
            " | ".join(entry_lines),
        ]
        if bool(summary_payload.get("regenerate_recommended", False)):
            note_parts.append("不改写原始主证据")
        role_status_display = " | ".join(
            part
            for part in (
                str(payload.get("role_status_display") or "").strip(),
                f"Schema {version_text}",
                compatibility_status,
                reader_mode,
            )
            if str(part).strip()
        )
        return {
            **payload,
            "note": " | ".join(part for part in note_parts if str(part).strip()),
            "role_status_display": role_status_display or str(payload.get("role_status_display") or ""),
            "schema_or_contract_version": version_text,
            "compatibility_status": str(compatibility_entry.get("compatibility_status") or ""),
            "compatibility_status_display": compatibility_status,
            "reader_mode": str(compatibility_entry.get("reader_mode") or ""),
            "reader_mode_display": reader_mode,
            "canonical_reader_available": bool(compatibility_entry.get("canonical_reader_available", False)),
            "regenerate_recommended": bool(compatibility_entry.get("regenerate_recommended", False)),
            "compatibility_boundary_digest": str(compatibility_entry.get("boundary_digest") or ""),
            "compatibility_non_claim_digest": str(compatibility_entry.get("non_claim_digest") or ""),
            "compatibility_overview": compatibility_overview,
            "artifact_compatibility_entry": compatibility_entry,
        }

    @staticmethod
    def _decorate_measurement_core_row(
        row: dict[str, Any],
        *,
        payload: dict[str, Any],
        json_filename: str,
        markdown_filename: str,
        entry_key: str,
    ) -> dict[str, Any]:
        artifact_row = dict(row or {})
        path_name = Path(str(artifact_row.get("path") or "")).name
        if path_name not in {json_filename, markdown_filename}:
            return artifact_row
        evidence_payload = dict(payload or {})
        review_surface = dict(evidence_payload.get("review_surface") or {})
        digest = dict(evidence_payload.get("digest") or {})
        if not review_surface and not digest:
            return artifact_row
        is_markdown = path_name == markdown_filename
        boundary_summary = " | ".join(collect_boundary_digest_lines(evidence_payload))
        role_status_display = " | ".join(
            part
            for part in (
                str(artifact_row.get("role_status_display") or "").strip(),
                str(evidence_payload.get("overall_status") or "").strip(),
                boundary_summary,
            )
            if str(part).strip()
        )
        return {
            **artifact_row,
            "name": str(review_surface.get("title_text") or artifact_row.get("name") or "")
            + (" (Markdown)" if is_markdown else " (JSON)"),
            "note": str(
                review_surface.get("reviewer_note")
                or digest.get("summary")
                or artifact_row.get("note")
                or ""
            ),
            "role_status_display": role_status_display or str(artifact_row.get("role_status_display") or ""),
            entry_key: {
                "review_surface": review_surface,
                "digest": digest,
                "artifact_paths": dict(evidence_payload.get("artifact_paths") or {}),
                "overall_status": str(evidence_payload.get("overall_status") or ""),
                "reviewer_fragments_contract_version": str(
                    evidence_payload.get("reviewer_fragments_contract_version") or ""
                ),
                "anchor_id": str(evidence_payload.get("anchor_id") or review_surface.get("anchor_id") or ""),
                "anchor_label": str(evidence_payload.get("anchor_label") or review_surface.get("anchor_label") or ""),
                "readiness_status": str(evidence_payload.get("readiness_status") or digest.get("readiness_status") or ""),
                "linked_artifact_refs": [dict(item) for item in list(evidence_payload.get("linked_artifact_refs") or []) if isinstance(item, dict)],
                "linked_measurement_phase_artifacts": [
                    dict(item) for item in list(evidence_payload.get("linked_measurement_phase_artifacts") or []) if isinstance(item, dict)
                ],
                "linked_measurement_phases": list(evidence_payload.get("linked_measurement_phases") or []),
                "linked_measurement_gaps": [
                    dict(item) for item in list(evidence_payload.get("linked_measurement_gaps") or []) if isinstance(item, dict)
                ],
                "linked_method_confirmation_item_keys": list(
                    evidence_payload.get("linked_method_confirmation_item_keys") or []
                ),
                "linked_method_confirmation_items": list(evidence_payload.get("linked_method_confirmation_items") or []),
                "linked_uncertainty_input_keys": list(evidence_payload.get("linked_uncertainty_input_keys") or []),
                "linked_uncertainty_inputs": list(evidence_payload.get("linked_uncertainty_inputs") or []),
                "linked_traceability_node_keys": list(evidence_payload.get("linked_traceability_node_keys") or []),
                "linked_traceability_nodes": list(evidence_payload.get("linked_traceability_nodes") or []),
                "linked_gap_classification_keys": list(evidence_payload.get("linked_gap_classification_keys") or []),
                "linked_gap_severity_keys": list(evidence_payload.get("linked_gap_severity_keys") or []),
                "next_required_artifacts": list(evidence_payload.get("next_required_artifacts") or []),
                "blockers": list(evidence_payload.get("blockers") or []),
                "blocker_fragments": [dict(item) for item in list(evidence_payload.get("blocker_fragments") or []) if isinstance(item, dict)],
                "blocker_fragment_keys": list(evidence_payload.get("blocker_fragment_keys") or []),
                "gap_reason": str(evidence_payload.get("gap_reason") or digest.get("gap_reason") or ""),
                "gap_reason_fragments": [dict(item) for item in list(evidence_payload.get("gap_reason_fragments") or []) if isinstance(item, dict)],
                "gap_reason_fragment_keys": list(evidence_payload.get("gap_reason_fragment_keys") or []),
                "boundary_fragments": [dict(item) for item in list(evidence_payload.get("boundary_fragments") or []) if isinstance(item, dict)],
                "boundary_fragment_keys": list(evidence_payload.get("boundary_fragment_keys") or []),
                "boundary_filter_rows": [dict(item) for item in list(evidence_payload.get("boundary_filter_rows") or review_surface.get("boundary_filter_rows") or []) if isinstance(item, dict)],
                "boundary_filters": list(evidence_payload.get("boundary_filters") or review_surface.get("boundary_filters") or []),
                "non_claim_fragments": [dict(item) for item in list(evidence_payload.get("non_claim_fragments") or []) if isinstance(item, dict)],
                "non_claim_fragment_keys": list(evidence_payload.get("non_claim_fragment_keys") or []),
                "non_claim_filter_rows": [dict(item) for item in list(evidence_payload.get("non_claim_filter_rows") or review_surface.get("non_claim_filter_rows") or []) if isinstance(item, dict)],
                "non_claim_filters": list(evidence_payload.get("non_claim_filters") or review_surface.get("non_claim_filters") or []),
                "linked_readiness_impact_summary": str(
                    evidence_payload.get("linked_readiness_impact_summary")
                    or digest.get("linked_readiness_impact_summary")
                    or ""
                ),
                "gap_classification_label": str(
                    evidence_payload.get("gap_classification_label") or digest.get("gap_classification_label") or ""
                ),
                "gap_severity_label": str(
                    evidence_payload.get("gap_severity_label") or digest.get("gap_severity_label") or ""
                ),
                "reviewer_next_step_digest": str(
                    evidence_payload.get("reviewer_next_step_digest") or digest.get("reviewer_next_step_digest") or ""
                ),
                "reviewer_next_step_fragments": [
                    dict(item) for item in list(evidence_payload.get("reviewer_next_step_fragments") or []) if isinstance(item, dict)
                ],
                "reviewer_next_step_fragment_keys": list(evidence_payload.get("reviewer_next_step_fragment_keys") or []),
                "reviewer_next_step_template_key": str(
                    evidence_payload.get("reviewer_next_step_template_key")
                    or digest.get("reviewer_next_step_template_key")
                    or ""
                ),
                "boundary_digest": str(evidence_payload.get("boundary_digest") or digest.get("boundary_digest") or ""),
                "non_claim_digest": str(evidence_payload.get("non_claim_digest") or digest.get("non_claim_digest") or ""),
                "phase_contrast_fragments": [dict(item) for item in list(evidence_payload.get("phase_contrast_fragments") or []) if isinstance(item, dict)],
                "phase_contrast_fragment_keys": list(evidence_payload.get("phase_contrast_fragment_keys") or []),
                "phase_contrast_filter_rows": [dict(item) for item in list(evidence_payload.get("phase_contrast_filter_rows") or review_surface.get("phase_contrast_filter_rows") or []) if isinstance(item, dict)],
                "phase_contrast_filters": list(evidence_payload.get("phase_contrast_filters") or review_surface.get("phase_contrast_filters") or []),
                "phase_contrast_summary": str(
                    evidence_payload.get("phase_contrast_summary")
                    or dict(digest).get("phase_contrast_summary")
                    or ""
                ),
            },
        }


def _results_gateway_decorate_artifact_compatibility_row(
    row: dict[str, Any],
    *,
    compatibility_lookup: dict[str, dict[str, Any]],
    compatibility_scan_summary: dict[str, Any],
) -> dict[str, Any]:
    payload = dict(row or {})
    key_by_path = str(payload.get("path") or "").strip()
    key_by_name = Path(key_by_path).name if key_by_path else str(payload.get("name") or "").strip()
    compatibility_entry = dict(compatibility_lookup.get(key_by_path) or compatibility_lookup.get(key_by_name) or {})
    if not compatibility_entry:
        return payload
    summary_payload = dict(compatibility_scan_summary or {})
    compatibility_overview = dict(summary_payload.get("compatibility_overview") or {})
    compatibility_rollup = dict(
        summary_payload.get("compatibility_rollup")
        or compatibility_overview.get("compatibility_rollup")
        or {}
    )
    compatibility_status = str(
        compatibility_entry.get("compatibility_status_display")
        or compatibility_entry.get("compatibility_status")
        or "--"
    )
    reader_mode = str(
        compatibility_entry.get("reader_mode_display")
        or compatibility_entry.get("reader_mode")
        or "--"
    )
    version_text = str(compatibility_entry.get("schema_or_contract_version") or "--")
    schema_contract_summary = str(
        compatibility_overview.get("schema_contract_summary_display")
        or summary_payload.get("schema_or_contract_version_summary")
        or version_text
    ).strip()
    recommendation_text = str(
        compatibility_overview.get("regenerate_recommendation_display")
        or ""
    ).strip()
    rollup_summary = str(
        compatibility_rollup.get("rollup_summary_display")
        or compatibility_overview.get("rollup_summary_display")
        or ""
    ).strip()
    entry_lines = [
        f"版本 {version_text}",
        f"状态 {compatibility_status}",
        f"读取 {reader_mode}",
    ]
    if schema_contract_summary:
        entry_lines.append(f"合同/Schema {schema_contract_summary}")
    if rollup_summary:
        entry_lines.append(
            t(
                "facade.results.result_summary.artifact_compatibility_rollup",
                value=rollup_summary,
                default=f"兼容性 rollup：{rollup_summary}",
            )
        )
    if bool(compatibility_entry.get("regenerate_recommended", False)):
        entry_lines.append("建议再生成 reviewer/index sidecar")
    if recommendation_text:
        entry_lines.append(f"建议 {recommendation_text}")
    note_parts = [
        str(payload.get("note") or "").strip(),
        " | ".join(entry_lines),
    ]
    if bool(summary_payload.get("regenerate_recommended", False)):
        note_parts.append("不改写原始主证据")
    role_status_display = " | ".join(
        part
        for part in (
            str(payload.get("role_status_display") or "").strip(),
            f"Schema {version_text}",
            compatibility_status,
            reader_mode,
        )
        if str(part).strip()
    )
    return {
        **payload,
        "note": " | ".join(part for part in note_parts if str(part).strip()),
        "role_status_display": role_status_display or str(payload.get("role_status_display") or ""),
        "schema_or_contract_version": version_text,
        "compatibility_status": str(compatibility_entry.get("compatibility_status") or ""),
        "compatibility_status_display": compatibility_status,
        "reader_mode": str(compatibility_entry.get("reader_mode") or ""),
        "reader_mode_display": reader_mode,
        "canonical_reader_available": bool(compatibility_entry.get("canonical_reader_available", False)),
        "regenerate_recommended": bool(compatibility_entry.get("regenerate_recommended", False)),
        "compatibility_boundary_digest": str(compatibility_entry.get("boundary_digest") or ""),
        "compatibility_non_claim_digest": str(compatibility_entry.get("non_claim_digest") or ""),
        "compatibility_overview": compatibility_overview,
        "compatibility_rollup": compatibility_rollup,
        "artifact_compatibility_entry": compatibility_entry,
    }


ResultsGateway._decorate_artifact_compatibility_row = staticmethod(
    _results_gateway_decorate_artifact_compatibility_row
)
