from __future__ import annotations

import json
from pathlib import Path
import sys

from gas_calibrator.v2.adapters.results_gateway import ResultsGateway
from gas_calibrator.v2.core import recognition_readiness_artifacts as recognition_readiness
from gas_calibrator.v2.core.software_validation_repository import (
    DatabaseReadySoftwareValidationRepositoryStub,
)
from gas_calibrator.v2.scripts import historical_artifacts
from gas_calibrator.v2.scripts.build_offline_governance_artifacts import rebuild_run

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import build_fake_facade


def _build_gateway(tmp_path: Path) -> tuple[Path, ResultsGateway]:
    facade = build_fake_facade(tmp_path)
    run_dir = Path(facade.result_store.run_dir)
    rebuild_run(run_dir)
    gateway = ResultsGateway(
        run_dir,
        output_files_provider=facade.service.get_output_files,
    )
    return run_dir, gateway


def test_software_validation_wp5_repository_and_gateway_contract(tmp_path: Path) -> None:
    run_dir, gateway = _build_gateway(tmp_path)
    results_payload = gateway.read_results_payload()

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

    assert change_impact["changed_modules"]
    assert change_impact["changed_module_paths"]
    assert change_impact["changed_modules_summary"]
    assert change_impact["impacts_main_execution_chain"] is False
    assert "unchanged" in change_impact["main_execution_chain_impact_summary"].lower()
    assert change_impact["impacts_artifact_schema"] is True
    assert "reviewer-sidecar" in change_impact["artifact_schema_impact_summary"].lower()
    assert change_impact["impacts_results_surface"] is True
    assert change_impact["impacts_review_center_surface"] is True
    assert change_impact["impacts_workbench_surface"] is True
    assert change_impact["impacts_reports_surface"] is True
    assert change_impact["db_ready_stub_only"] is True
    assert set(change_impact["linked_surface_visibility"]) >= {
        "results_payload",
        "reports",
        "review_center",
        "workbench_recognition_readiness",
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
    assert rollup["results_surface_impacted"] is True
    assert rollup["review_center_surface_impacted"] is True
    assert rollup["workbench_surface_impacted"] is True
    assert rollup["rollback_summary"]
    assert rollup["rollback_mode"] == "file_artifact_first"
    assert rollup["file_artifact_first"] is True
    assert rollup["sidecar_revocable"] is True
    assert rollup["primary_evidence_preserved"] is True
    assert rollup["audit_event_summary"]
    assert rollup["config_fingerprint_summary"]
    assert rollup["release_input_summary"]
    assert set(rollup["linked_surface_visibility"]) >= {
        "results_payload",
        "reports",
        "review_center",
        "workbench_recognition_readiness",
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


def test_software_validation_wp5_results_and_review_center_visibility(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)
    run_dir = Path(facade.result_store.run_dir)
    rebuild_run(run_dir)

    results_payload = facade.results_gateway.read_results_payload()
    reports_payload = facade.results_gateway.read_reports_payload()
    results_snapshot = facade.build_results_snapshot()
    review_center = dict(results_snapshot["review_center"])
    rows_by_path = {
        str(Path(str(row.get("path") or "")).resolve()): dict(row)
        for row in list(reports_payload["files"] or [])
    }

    assert "软件验证总览" in results_payload["result_summary_text"] or "Software validation overview" in results_payload["result_summary_text"]
    assert "追溯完整度" in results_payload["result_summary_text"] or "Traceability completeness" in results_payload["result_summary_text"]
    assert "审计哈希" in results_payload["result_summary_text"] or "Audit hash" in results_payload["result_summary_text"]
    assert "Release manifest" in results_payload["result_summary_text"]
    assert "验证联动" in results_payload["result_summary_text"] or "Verification linkage" in results_payload["result_summary_text"]

    traceability_row = rows_by_path[
        str((run_dir / recognition_readiness.SOFTWARE_VALIDATION_TRACEABILITY_MATRIX_FILENAME).resolve())
    ]
    hash_registry_row = rows_by_path[
        str((run_dir / recognition_readiness.ARTIFACT_HASH_REGISTRY_FILENAME).resolve())
    ]
    release_manifest_row = rows_by_path[
        str((run_dir / recognition_readiness.RELEASE_MANIFEST_FILENAME).resolve())
    ]

    assert traceability_row["artifact_key"] == "software_validation_traceability_matrix"
    assert hash_registry_row["artifact_key"] == "artifact_hash_registry"
    assert release_manifest_row["artifact_key"] == "release_manifest"
    assert traceability_row["software_validation_traceability_matrix_entry"]["review_surface"]["title_text"] == (
        "Software Validation Traceability Matrix"
    )
    assert hash_registry_row["artifact_hash_registry_entry"]["review_surface"]["title_text"] == (
        "Artifact Hash Registry"
    )
    assert release_manifest_row["release_manifest_entry"]["review_surface"]["title_text"] == "Release Manifest"
    assert "formal release approval" in str(release_manifest_row["note"] or "").lower()
    assert "anti-tamper" in str(hash_registry_row["note"] or "").lower()
    assert "requirement -> design -> code -> test -> artifact" in str(traceability_row["note"] or "")

    index_summary = dict(review_center["index_summary"])
    assert index_summary["software_validation_rollup"]["repository_mode"] == "file_artifact_first"
    assert index_summary["software_validation_summary"]
    assert index_summary["traceability_summary"]
    assert index_summary["audit_hash_summary"]
    assert index_summary["change_impact_summary"]
    assert index_summary["rollback_summary"]
    assert index_summary["audit_event_summary"]
    assert index_summary["config_fingerprint_summary"]
    assert index_summary["release_input_summary"]
    assert index_summary["release_manifest_summary"]
    assert "software" in str(index_summary["summary"] or "").lower() or "软件验证" in str(index_summary["summary"] or "")

    readiness_items = [
        dict(item)
        for item in list(review_center["evidence_items"] or [])
        if str(item.get("type") or "") == "readiness_governance"
    ]
    readiness_filenames = {Path(str(item.get("path") or "")).name for item in readiness_items}
    assert {
        recognition_readiness.REQUIREMENT_DESIGN_CODE_TEST_LINKS_FILENAME,
        recognition_readiness.VALIDATION_EVIDENCE_INDEX_FILENAME,
        recognition_readiness.CHANGE_IMPACT_SUMMARY_FILENAME,
        recognition_readiness.ROLLBACK_READINESS_SUMMARY_FILENAME,
        recognition_readiness.SOFTWARE_VALIDATION_TRACEABILITY_MATRIX_FILENAME,
        recognition_readiness.ARTIFACT_HASH_REGISTRY_FILENAME,
        recognition_readiness.AUDIT_EVENT_STORE_FILENAME,
        recognition_readiness.ENVIRONMENT_FINGERPRINT_FILENAME,
        recognition_readiness.CONFIG_FINGERPRINT_FILENAME,
        recognition_readiness.RELEASE_INPUT_DIGEST_FILENAME,
        recognition_readiness.RELEASE_MANIFEST_FILENAME,
        recognition_readiness.RELEASE_SCOPE_SUMMARY_FILENAME,
        recognition_readiness.RELEASE_BOUNDARY_DIGEST_FILENAME,
        recognition_readiness.RELEASE_EVIDENCE_PACK_INDEX_FILENAME,
        recognition_readiness.RELEASE_VALIDATION_MANIFEST_FILENAME,
        recognition_readiness.AUDIT_READINESS_DIGEST_FILENAME,
    } <= readiness_filenames
    assert any(
        "Release Manifest" in str(item.get("detail_text") or "")
        or "software validation" in str(item.get("detail_text") or "").lower()
        for item in readiness_items
    )
    assert any("Changed modules:" in str(item.get("detail_text") or "") for item in readiness_items)
    assert any("Rollback mode:" in str(item.get("detail_text") or "") for item in readiness_items)


def test_software_validation_wp5_workbench_and_historical_visibility(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)
    run_dir = Path(facade.result_store.run_dir)
    rebuild_run(run_dir)

    facade.execute_device_workbench_action("thermometer", "set_mode", mode="stale")
    facade.execute_device_workbench_action(
        "workbench",
        "generate_diagnostic_evidence",
        current_device="thermometer",
        current_action="set_mode",
    )

    report_payload = json.loads((run_dir / "workbench_action_report.json").read_text(encoding="utf-8"))
    readiness_evidence = dict(report_payload["recognition_readiness_evidence"])
    artifact_paths = dict(readiness_evidence["artifact_paths"])

    expected_workbench_keys = {
        "requirement_design_code_test_links": recognition_readiness.REQUIREMENT_DESIGN_CODE_TEST_LINKS_FILENAME,
        "validation_evidence_index": recognition_readiness.VALIDATION_EVIDENCE_INDEX_FILENAME,
        "change_impact_summary": recognition_readiness.CHANGE_IMPACT_SUMMARY_FILENAME,
        "rollback_readiness_summary": recognition_readiness.ROLLBACK_READINESS_SUMMARY_FILENAME,
        "software_validation_traceability_matrix": recognition_readiness.SOFTWARE_VALIDATION_TRACEABILITY_MATRIX_FILENAME,
        "artifact_hash_registry": recognition_readiness.ARTIFACT_HASH_REGISTRY_FILENAME,
        "audit_event_store": recognition_readiness.AUDIT_EVENT_STORE_FILENAME,
        "environment_fingerprint": recognition_readiness.ENVIRONMENT_FINGERPRINT_FILENAME,
        "config_fingerprint": recognition_readiness.CONFIG_FINGERPRINT_FILENAME,
        "release_input_digest": recognition_readiness.RELEASE_INPUT_DIGEST_FILENAME,
        "release_manifest": recognition_readiness.RELEASE_MANIFEST_FILENAME,
        "release_scope_summary": recognition_readiness.RELEASE_SCOPE_SUMMARY_FILENAME,
        "release_boundary_digest": recognition_readiness.RELEASE_BOUNDARY_DIGEST_FILENAME,
        "release_evidence_pack_index": recognition_readiness.RELEASE_EVIDENCE_PACK_INDEX_FILENAME,
        "release_validation_manifest": recognition_readiness.RELEASE_VALIDATION_MANIFEST_FILENAME,
        "audit_readiness_digest": recognition_readiness.AUDIT_READINESS_DIGEST_FILENAME,
    }
    for artifact_key, filename in expected_workbench_keys.items():
        assert readiness_evidence[artifact_key]["artifact_type"] == artifact_key
        assert readiness_evidence[artifact_key]["not_real_acceptance_evidence"] is True
        assert readiness_evidence[artifact_key]["not_ready_for_formal_claim"] is True
        assert readiness_evidence[artifact_key]["primary_evidence_rewritten"] is False
        assert artifact_paths[artifact_key].endswith(filename)
    assert readiness_evidence["software_validation_rollup"]["repository_mode"] == "file_artifact_first"
    assert readiness_evidence["change_impact_summary"]["changed_modules_summary"]
    assert readiness_evidence["change_impact_summary"]["impacts_main_execution_chain"] is False
    assert readiness_evidence["rollback_readiness_summary"]["rollback_mode"] == "file_artifact_first"
    assert readiness_evidence["rollback_readiness_summary"]["touches_primary_evidence"] is False
    assert any(
        "软件验证总览" in str(line) or "Software validation overview" in str(line)
        for line in list(readiness_evidence["summary_lines"] or [])
    )
    assert any(
        "验证联动" in str(line) or "Verification linkage" in str(line)
        for line in list(readiness_evidence["detail_lines"] or [])
    )
    assert any(
        "not real acceptance" in str(line).lower() or "非 claim" in str(line)
        for line in list(readiness_evidence["boundary_lines"] or [])
    )

    historical_report = historical_artifacts._build_run_report(  # noqa: SLF001
        run_dir,
        operation="scan",
        dry_run=True,
    )
    assert historical_report["software_validation_overview"]
    assert historical_report["traceability_completeness"]
    assert historical_report["audit_hash_summary"]
    assert historical_report["environment_fingerprint_summary"]
    assert historical_report["release_manifest_overview"]
    assert historical_report["release_scope_overview"]
    assert historical_report["release_boundary_overview"]
    assert historical_report["release_evidence_pack_overview"]
    assert historical_report["linked_scope_ids"]
    assert historical_report["linked_decision_rules"]
    assert historical_report["linked_test_suites"]
    assert historical_report["software_validation_not_real_acceptance_evidence"] is True
    assert historical_report["software_validation_not_ready_for_formal_claim"] is True
    assert historical_report["software_validation_primary_evidence_rewritten"] is False
