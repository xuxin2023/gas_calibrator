from __future__ import annotations

from pathlib import Path
import sys

from gas_calibrator.v2.adapters import SoftwareValidationGateway
from gas_calibrator.v2.core import recognition_readiness_artifacts as recognition_readiness
from gas_calibrator.v2.core.software_validation_repository import (
    DatabaseReadySoftwareValidationRepositoryStub,
)
from gas_calibrator.v2.scripts.build_offline_governance_artifacts import rebuild_run

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import build_fake_service


def _build_gateway(tmp_path: Path) -> tuple[Path, SoftwareValidationGateway]:
    service = build_fake_service(tmp_path)
    run_dir = Path(service.result_store.run_dir)
    rebuild_run(run_dir)
    gateway = SoftwareValidationGateway(run_dir)
    return run_dir, gateway


def test_software_validation_wp5_repository_and_narrow_gateway_contract(
    tmp_path: Path,
) -> None:
    run_dir, gateway = _build_gateway(tmp_path)
    results_payload = gateway.read_payload()

    traceability = dict(results_payload["software_validation_traceability_matrix"])
    links = dict(results_payload["requirement_design_code_test_links"])
    evidence_index = dict(results_payload["validation_evidence_index"])
    change_impact = dict(results_payload["change_impact_summary"])
    rollback = dict(results_payload["rollback_readiness_summary"])
    hash_registry = dict(results_payload["artifact_hash_registry"])
    audit_event_store = dict(results_payload["audit_event_store"])
    environment_fingerprint = dict(results_payload["environment_fingerprint"])
    config_fingerprint = dict(results_payload["config_fingerprint"])
    release_input_digest = dict(results_payload["release_input_digest"])
    release_manifest = dict(results_payload["release_manifest"])
    release_scope_summary = dict(results_payload["release_scope_summary"])
    release_boundary_digest = dict(results_payload["release_boundary_digest"])
    release_evidence_pack_index = dict(results_payload["release_evidence_pack_index"])
    release_validation_manifest = dict(results_payload["release_validation_manifest"])
    audit_digest = dict(results_payload["audit_readiness_digest"])
    rollup = dict(results_payload["software_validation_rollup"])

    assert traceability["artifact_type"] == "software_validation_traceability_matrix"
    assert links["artifact_type"] == "requirement_design_code_test_links"
    assert evidence_index["artifact_type"] == "validation_evidence_index"
    assert change_impact["artifact_type"] == "change_impact_summary"
    assert rollback["artifact_type"] == "rollback_readiness_summary"
    assert hash_registry["artifact_type"] == "artifact_hash_registry"
    assert audit_event_store["artifact_type"] == "audit_event_store"
    assert environment_fingerprint["artifact_type"] == "environment_fingerprint"
    assert config_fingerprint["artifact_type"] == "config_fingerprint"
    assert release_input_digest["artifact_type"] == "release_input_digest"
    assert release_manifest["artifact_type"] == "release_manifest"
    assert release_scope_summary["artifact_type"] == "release_scope_summary"
    assert release_boundary_digest["artifact_type"] == "release_boundary_digest"
    assert release_evidence_pack_index["artifact_type"] == "release_evidence_pack_index"
    assert release_validation_manifest["artifact_type"] == "release_validation_manifest"
    assert audit_digest["artifact_type"] == "audit_readiness_digest"

    wp5_payloads = {
        "software_validation_traceability_matrix": traceability,
        "requirement_design_code_test_links": links,
        "validation_evidence_index": evidence_index,
        "change_impact_summary": change_impact,
        "rollback_readiness_summary": rollback,
        "artifact_hash_registry": hash_registry,
        "audit_event_store": audit_event_store,
        "environment_fingerprint": environment_fingerprint,
        "config_fingerprint": config_fingerprint,
        "release_input_digest": release_input_digest,
        "release_manifest": release_manifest,
        "release_scope_summary": release_scope_summary,
        "release_boundary_digest": release_boundary_digest,
        "release_evidence_pack_index": release_evidence_pack_index,
        "release_validation_manifest": release_validation_manifest,
        "audit_readiness_digest": audit_digest,
    }
    for artifact_key, artifact_payload in wp5_payloads.items():
        assert artifact_payload["artifact_type"] == artifact_key
        assert artifact_payload["not_real_acceptance_evidence"] is True
        assert artifact_payload["not_ready_for_formal_claim"] is True
        assert artifact_payload["primary_evidence_rewritten"] is False

    required_traceability_fields = {
        "traceability_id",
        "traceability_version",
        "scope_id",
        "decision_rule_id",
        "uncertainty_case_id",
        "method_confirmation_protocol_id",
        "requirement_refs",
        "design_refs",
        "code_refs",
        "test_refs",
        "artifact_refs",
        "change_set_refs",
        "impact_scope",
        "reviewer_only",
        "readiness_mapping_only",
        "not_real_acceptance_evidence",
        "not_ready_for_formal_claim",
        "limitation_note",
        "non_claim_note",
        "reviewer_note",
    }
    assert required_traceability_fields <= set(traceability)
    assert traceability["reviewer_only"] is True
    assert traceability["readiness_mapping_only"] is True
    assert traceability["not_real_acceptance_evidence"] is True
    assert traceability["not_ready_for_formal_claim"] is True
    assert traceability["primary_evidence_rewritten"] is False
    assert traceability["traceability_completeness"] == "4/4 linked"
    assert traceability["traceability_rows"]
    assert traceability["artifact_paths"]["software_validation_traceability_matrix"].endswith(
        recognition_readiness.SOFTWARE_VALIDATION_TRACEABILITY_MATRIX_FILENAME
    )
    validation_runs = list(evidence_index["method_confirmation_validation_runs"])
    assert validation_runs
    assert {item["route_type"] for item in validation_runs} >= {"gas", "water"}
    assert all(item["reviewer_only"] is True for item in validation_runs)
    assert all(item["not_real_acceptance_evidence"] is True for item in validation_runs)

    assert change_impact["changed_modules"]
    assert change_impact["changed_module_paths"]
    assert change_impact["changed_modules_summary"]
    assert change_impact["impacts_main_execution_chain"] is False
    assert "unchanged" in change_impact["main_execution_chain_impact_summary"].lower()
    assert change_impact["impacts_artifact_schema"] is True
    assert "reviewer-sidecar" in change_impact["artifact_schema_impact_summary"].lower()
    assert change_impact["impacts_results_surface"] is False
    assert change_impact["impacts_review_center_surface"] is False
    assert change_impact["impacts_workbench_surface"] is False
    assert "retired active surface" in change_impact["review_center_surface_impact_summary"].lower()
    assert "retired active surface" in change_impact["workbench_surface_impact_summary"].lower()
    assert change_impact["impacts_reports_surface"] is False
    assert change_impact["db_ready_stub_only"] is True
    assert set(change_impact["linked_surface_visibility"]) >= {
        "persisted_artifacts",
        "historical_artifacts",
    }

    assert rollback["rollback_mode"] == "file_artifact_first"
    assert rollback["file_artifact_first"] is True
    assert rollback["sidecar_revocable"] is True
    assert rollback["primary_evidence_preserved"] is True
    assert rollback["touches_primary_evidence"] is False
    assert rollback["rollback_steps"]
    assert rollback["db_ready_stub_only"] is True

    required_hash_registry_fields = {
        "hash_registry_id",
        "entries",
        "hash_algorithm",
        "linked_release_manifest_id",
        "environment_summary",
        "python_version",
        "platform",
        "repo_ref",
        "workspace_mode",
        "generated_by_tool",
        "reviewer_only",
        "not_real_acceptance_evidence",
        "primary_evidence_rewritten",
    }
    assert required_hash_registry_fields <= set(hash_registry)
    assert hash_registry["reviewer_only"] is True
    assert hash_registry["not_real_acceptance_evidence"] is True
    assert hash_registry["primary_evidence_rewritten"] is False
    assert hash_registry["reviewer_trace_only"] is True
    assert hash_registry["file_backed_only"] is True
    assert hash_registry["formal_anti_tamper_claim"] is False
    assert hash_registry["tamper_evidence_claimed"] is False
    assert hash_registry["trace_purpose"] == "file_backed_reviewer_trace"
    assert hash_registry["entries"]
    first_hash_entry = dict(hash_registry["entries"][0])
    assert {
        "artifact_type",
        "artifact_path",
        "content_hash",
        "hash_algorithm",
        "linked_run_id",
        "linked_scope_id",
        "linked_release_manifest_id",
        "generated_at",
        "generated_by_tool",
        "environment_summary",
        "python_version",
        "platform",
        "repo_ref",
        "workspace_mode",
        "primary_evidence_rewritten",
        "reviewer_only",
        "not_real_acceptance_evidence",
    } <= set(first_hash_entry)
    assert first_hash_entry["primary_evidence_rewritten"] is False
    assert first_hash_entry["reviewer_only"] is True
    assert first_hash_entry["not_real_acceptance_evidence"] is True
    assert first_hash_entry["reviewer_trace_only"] is True
    assert first_hash_entry["formal_anti_tamper_claim"] is False
    assert first_hash_entry["tamper_evidence_claimed"] is False

    assert environment_fingerprint["reviewer_trace_only"] is True
    assert environment_fingerprint["formal_anti_tamper_claim"] is False
    assert environment_fingerprint["tamper_evidence_claimed"] is False
    assert environment_fingerprint["fingerprint_scope"] == "file_backed_reviewer_trace"
    assert config_fingerprint["reviewer_trace_only"] is True
    assert config_fingerprint["formal_anti_tamper_claim"] is False
    assert config_fingerprint["tamper_evidence_claimed"] is False
    assert config_fingerprint["fingerprint_scope"] == "file_backed_reviewer_trace"
    assert release_input_digest["reviewer_trace_only"] is True
    assert release_input_digest["formal_anti_tamper_claim"] is False
    assert release_input_digest["tamper_evidence_claimed"] is False
    assert release_input_digest["digest_scope"] == "file_backed_reviewer_trace"
    assert audit_event_store["event_store_mode"] == "file_backed_reviewer_trace"
    assert audit_event_store["reviewer_trace_only"] is True

    required_release_fields = {
        "release_id",
        "release_version",
        "created_at",
        "repo_ref",
        "branch_or_head",
        "workspace_mode",
        "linked_scope_ids",
        "linked_decision_rules",
        "linked_assets_certificates_summary",
        "linked_uncertainty_cases",
        "linked_method_confirmation_protocols",
        "linked_traceability_matrix",
        "linked_hash_registry",
        "linked_test_suites",
        "parity_status",
        "resilience_status",
        "smoke_status",
        "simulation_only",
        "not_real_acceptance_evidence",
        "not_ready_for_formal_claim",
        "non_claim_note",
        "limitation_note",
        "reviewer_actions",
    }
    assert required_release_fields <= set(release_manifest)
    assert release_manifest["simulation_only"] is True
    assert release_manifest["not_real_acceptance_evidence"] is True
    assert release_manifest["not_ready_for_formal_claim"] is True
    assert release_manifest["linked_scope_ids"]
    assert release_manifest["linked_decision_rules"]
    assert release_manifest["linked_uncertainty_cases"]
    assert release_manifest["linked_method_confirmation_protocols"]
    assert release_manifest["linked_test_suites"]
    assert release_manifest["linked_traceability_matrix"]["artifact_type"] == (
        "software_validation_traceability_matrix"
    )
    assert release_manifest["linked_hash_registry"]["artifact_type"] == "artifact_hash_registry"

    assert rollup["repository_mode"] == "file_artifact_first"
    assert rollup["gateway_mode"] == "file_backed_default"
    assert rollup["db_ready_stub"]["not_in_default_chain"] is True
    assert rollup["reviewer_only"] is True
    assert rollup["readiness_mapping_only"] is True
    assert rollup["not_real_acceptance_evidence"] is True
    assert rollup["not_ready_for_formal_claim"] is True
    assert rollup["primary_evidence_rewritten"] is False
    assert rollup["traceability_summary"]
    assert rollup["hash_registry_summary"]
    assert rollup["release_manifest_summary"]
    assert rollup["change_impact_summary"]
    assert rollup["changed_modules_summary"]
    assert rollup["main_execution_chain_impacted"] is False
    assert rollup["artifact_schema_impacted"] is True
    assert rollup["results_surface_impacted"] is False
    assert rollup["review_center_surface_impacted"] is False
    assert rollup["workbench_surface_impacted"] is False
    assert rollup["rollback_summary"]
    assert rollup["rollback_mode"] == "file_artifact_first"
    assert rollup["file_artifact_first"] is True
    assert rollup["sidecar_revocable"] is True
    assert rollup["primary_evidence_preserved"] is True
    assert rollup["audit_event_summary"]
    assert rollup["config_fingerprint_summary"]
    assert rollup["release_input_summary"]
    assert set(rollup["linked_surface_visibility"]) >= {
        "persisted_artifacts",
        "historical_artifacts",
    }
    assert rollup["parity_status"]
    assert rollup["resilience_status"]
    assert rollup["smoke_status"]

    stub_snapshot = DatabaseReadySoftwareValidationRepositoryStub(run_dir).load_snapshot()
    stub_rollup = dict(stub_snapshot["software_validation_rollup"])
    assert stub_rollup["repository_mode"] == "db_ready_stub"
    assert stub_rollup["db_ready_stub"]["not_in_default_chain"] is True
    assert stub_rollup["primary_evidence_rewritten"] is False
    assert stub_rollup["not_real_acceptance_evidence"] is True
