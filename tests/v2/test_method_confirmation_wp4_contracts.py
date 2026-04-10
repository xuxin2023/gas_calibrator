from pathlib import Path
import json
import sys

from gas_calibrator.v2.adapters.method_confirmation_gateway import MethodConfirmationGateway
from gas_calibrator.v2.adapters.results_gateway import ResultsGateway
from gas_calibrator.v2.core.method_confirmation_repository import (
    DatabaseReadyMethodConfirmationRepositoryStub,
    FileBackedMethodConfirmationRepository,
)
from gas_calibrator.v2.scripts.build_offline_governance_artifacts import rebuild_run

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import build_fake_facade


REQUIRED_DIMENSIONS = {
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
}

REQUIRED_TOP_LEVEL_FIELDS = {
    "protocol_id",
    "protocol_version",
    "scope_id",
    "decision_rule_id",
    "uncertainty_case_id",
    "measurand",
    "route_type",
    "environment_mode",
    "analyzer_model",
    "validation_matrix_version",
    "validation_dimensions",
    "validation_status",
    "reviewer_only",
    "readiness_mapping_only",
    "not_real_acceptance_evidence",
    "not_ready_for_formal_claim",
    "limitation_note",
    "non_claim_note",
    "reviewer_note",
}


def _write_legacy_run(run_dir: Path) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)
    for name in ("summary.json", "manifest.json", "results.json"):
        (run_dir / name).write_text(
            json.dumps({"run_id": run_dir.name, "stats": {"point_summaries": []}}, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )


def test_method_confirmation_wp4_object_model_and_results_contract(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)
    run_dir = Path(facade.result_store.run_dir)
    rebuild_run(run_dir)

    repository = FileBackedMethodConfirmationRepository(run_dir)
    snapshot = repository.load_snapshot()
    payload = MethodConfirmationGateway(run_dir).read_payload()
    results_payload = ResultsGateway(
        run_dir,
        output_files_provider=facade.service.get_output_files,
    ).read_results_payload()

    protocol = payload["method_confirmation_protocol"]
    matrix = payload["method_confirmation_matrix"]
    route_matrix = payload["route_specific_validation_matrix"]
    validation_run_set = payload["validation_run_set"]
    verification_digest = payload["verification_digest"]
    verification_rollup = payload["verification_rollup"]

    assert protocol["artifact_type"] == "method_confirmation_protocol"
    assert matrix["artifact_type"] == "method_confirmation_matrix"
    assert route_matrix["artifact_type"] == "route_specific_validation_matrix"
    assert validation_run_set["artifact_type"] == "validation_run_set"
    assert verification_digest["artifact_type"] == "verification_digest"
    assert verification_rollup["artifact_type"] == "verification_rollup"
    assert all(REQUIRED_TOP_LEVEL_FIELDS <= set(dict(item)) for item in (protocol, matrix, route_matrix, validation_run_set, verification_digest, verification_rollup))

    validation_rows = list(
        route_matrix.get("route_specific_validation_matrix")
        or route_matrix.get("matrix_rows")
        or route_matrix.get("rows")
        or []
    )
    assert len(validation_rows) >= 30
    assert all({"current_evidence_coverage", "gap_note", "reviewer_action"} <= set(dict(item)) for item in validation_rows)

    dimensions_by_route: dict[tuple[str, str], set[str]] = {}
    for row in validation_rows:
        key = (str(row.get("route_type") or ""), str(row.get("measurand") or ""))
        dimensions_by_route.setdefault(key, set()).add(str(row.get("dimension_key") or ""))
        assert bool(dict(row).get("reviewer_only")) is True
        assert bool(dict(row).get("readiness_mapping_only")) is True
        assert bool(dict(row).get("not_real_acceptance_evidence")) is True
        assert bool(dict(row).get("not_ready_for_formal_claim")) is True
        assert bool(dict(row).get("primary_evidence_rewritten")) is False

    assert dimensions_by_route[("gas", "CO2")] == REQUIRED_DIMENSIONS
    assert dimensions_by_route[("water", "H2O")] == REQUIRED_DIMENSIONS
    assert dimensions_by_route[("ambient", "ambient_diagnostic")] == REQUIRED_DIMENSIONS

    validation_runs = list(validation_run_set.get("validation_run_set") or [])
    assert len(validation_runs) >= 3
    for item in validation_runs:
        assert {"golden_dataset_id", "linked_run_ids", "linked_artifacts", "reference_assets", "certificate_lifecycle_refs", "pre_run_gate_refs", "uncertainty_refs"} <= set(dict(item))
        assert list(item.get("linked_run_ids") or []) == [run_dir.name]
        assert bool(list(item.get("reference_assets") or []))
        assert bool(list(item.get("certificate_lifecycle_refs") or []))
        assert bool(list(item.get("pre_run_gate_refs") or []))
        assert bool(dict(item.get("uncertainty_refs") or {}))
        assert item["scope_id"] == protocol["scope_id"]
        assert item["decision_rule_id"] == protocol["decision_rule_id"]

    verification_digest_fields = dict(verification_digest.get("digest") or {})
    assert verification_digest_fields["protocol_overview_summary"]
    assert verification_digest_fields["matrix_completeness_summary"]
    assert verification_digest_fields["current_evidence_coverage_summary"]
    assert verification_digest_fields["top_gaps_summary"]
    assert verification_digest_fields["reviewer_action_summary"]
    assert verification_digest_fields["readiness_status_summary"]
    assert verification_rollup["repository_mode"] == "file_artifact_first"
    assert verification_rollup["gateway_mode"] == "file_backed_default"
    assert verification_rollup["db_ready_stub"]["not_in_default_chain"] is True
    assert verification_rollup["ready_for_readiness_mapping"] is True
    assert verification_rollup["not_ready_for_formal_claim"] is True
    assert verification_rollup["not_real_acceptance_evidence"] is True
    assert verification_rollup["primary_evidence_rewritten"] is False
    assert verification_rollup["rollup_summary_display"]

    assert snapshot["verification_rollup"]["rollup_summary_display"]
    assert results_payload["method_confirmation_protocol"]["artifact_type"] == "method_confirmation_protocol"
    assert results_payload["route_specific_validation_matrix"]["artifact_type"] == "route_specific_validation_matrix"
    assert results_payload["validation_run_set"]["artifact_type"] == "validation_run_set"
    assert results_payload["verification_digest"]["artifact_type"] == "verification_digest"
    assert results_payload["verification_rollup"]["artifact_type"] == "verification_rollup"
    assert "方法确认概览" in results_payload["result_summary_text"] or "Method confirmation overview" in results_payload["result_summary_text"]
    assert "验证矩阵完整度" in results_payload["result_summary_text"] or "Validation matrix completeness" in results_payload["result_summary_text"]
    assert "当前证据覆盖" in results_payload["result_summary_text"] or "Current evidence coverage" in results_payload["result_summary_text"]
    assert "主要缺口" in results_payload["result_summary_text"] or "Top gaps" in results_payload["result_summary_text"]
    assert "审阅动作" in results_payload["result_summary_text"] or "Reviewer actions" in results_payload["result_summary_text"]
    assert "验证就绪状态" in results_payload["result_summary_text"] or "Verification readiness status" in results_payload["result_summary_text"]


def test_method_confirmation_wp4_db_stub_and_placeholder_fallback(tmp_path: Path) -> None:
    run_dir = tmp_path / "legacy_run"
    _write_legacy_run(run_dir)

    stub_payload = DatabaseReadyMethodConfirmationRepositoryStub(run_dir).load_snapshot()
    snapshot = FileBackedMethodConfirmationRepository(run_dir).load_snapshot()
    payload = MethodConfirmationGateway(run_dir).read_payload()
    results_payload = ResultsGateway(run_dir).read_results_payload()

    assert stub_payload["verification_rollup"]["repository_mode"] == "db_ready_stub"
    assert stub_payload["verification_rollup"]["db_ready_stub"]["not_in_default_chain"] is True
    assert stub_payload["verification_rollup"]["primary_evidence_rewritten"] is False
    assert payload["method_confirmation_protocol"]["reviewer_placeholder"] is True
    assert payload["route_specific_validation_matrix"]["reviewer_placeholder"] is True
    assert payload["validation_run_set"]["reviewer_placeholder"] is True
    assert payload["verification_digest"]["reviewer_placeholder"] is True
    assert payload["verification_rollup"]["legacy_placeholder_used"] is True
    assert payload["verification_rollup"]["missing_artifact_types"]
    assert payload["verification_rollup"]["ready_for_readiness_mapping"] is True
    assert payload["verification_rollup"]["not_ready_for_formal_claim"] is True
    assert payload["verification_rollup"]["not_real_acceptance_evidence"] is True
    assert payload["verification_rollup"]["primary_evidence_rewritten"] is False
    assert payload["verification_rollup"]["db_ready_stub"]["not_in_default_chain"] is True
    assert payload["verification_rollup"]["non_claim_note"]
    assert snapshot["verification_digest"]["reviewer_placeholder"] is True
    assert results_payload["verification_rollup"]["legacy_placeholder_used"] is True
    assert results_payload["verification_rollup"]["primary_evidence_rewritten"] is False
    assert results_payload["route_specific_validation_matrix"]["reviewer_placeholder"] is True
    assert "方法确认概览" in results_payload["result_summary_text"] or "Method confirmation overview" in results_payload["result_summary_text"]
    assert "non-claim" in results_payload["result_summary_text"].lower()
