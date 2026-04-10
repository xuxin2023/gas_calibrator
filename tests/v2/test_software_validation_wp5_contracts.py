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
    assert traceability["artifact_paths"]["software_validation_traceability_matrix"].endswith(
        recognition_readiness.SOFTWARE_VALIDATION_TRACEABILITY_MATRIX_FILENAME
    )

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
    assert index_summary["release_manifest_summary"]
    assert "software" in str(index_summary["summary"] or "").lower() or "软件验证" in str(index_summary["summary"] or "")

    readiness_items = [
        dict(item)
        for item in list(review_center["evidence_items"] or [])
        if str(item.get("type") or "") == "readiness_governance"
    ]
    assert any(
        Path(str(item.get("path") or "")).name == recognition_readiness.RELEASE_MANIFEST_FILENAME
        for item in readiness_items
    )
    assert any(
        Path(str(item.get("path") or "")).name == recognition_readiness.ARTIFACT_HASH_REGISTRY_FILENAME
        for item in readiness_items
    )
    assert any(
        "Release Manifest" in str(item.get("detail_text") or "")
        or "software validation" in str(item.get("detail_text") or "").lower()
        for item in readiness_items
    )


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

    assert readiness_evidence["software_validation_traceability_matrix"]["artifact_type"] == (
        "software_validation_traceability_matrix"
    )
    assert readiness_evidence["artifact_hash_registry"]["artifact_type"] == "artifact_hash_registry"
    assert readiness_evidence["environment_fingerprint"]["artifact_type"] == "environment_fingerprint"
    assert readiness_evidence["release_manifest"]["artifact_type"] == "release_manifest"
    assert readiness_evidence["software_validation_rollup"]["repository_mode"] == "file_artifact_first"
    assert artifact_paths["software_validation_traceability_matrix"].endswith(
        recognition_readiness.SOFTWARE_VALIDATION_TRACEABILITY_MATRIX_FILENAME
    )
    assert artifact_paths["artifact_hash_registry"].endswith(
        recognition_readiness.ARTIFACT_HASH_REGISTRY_FILENAME
    )
    assert artifact_paths["release_manifest"].endswith(recognition_readiness.RELEASE_MANIFEST_FILENAME)
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
