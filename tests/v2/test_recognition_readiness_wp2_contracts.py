import json
from pathlib import Path
import sys

from gas_calibrator.v2.core import recognition_readiness_artifacts as recognition_readiness
from gas_calibrator.v2.scripts.build_offline_governance_artifacts import rebuild_run

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import build_fake_facade


def test_wp2_reference_lifecycle_and_gate_artifacts_keep_step2_boundaries(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)
    run_dir = Path(facade.result_store.run_dir)
    rebuild_run(run_dir)

    reference_registry = json.loads(
        (run_dir / recognition_readiness.REFERENCE_ASSET_REGISTRY_FILENAME).read_text(encoding="utf-8")
    )
    certificate_lifecycle = json.loads(
        (run_dir / recognition_readiness.CERTIFICATE_LIFECYCLE_SUMMARY_FILENAME).read_text(encoding="utf-8")
    )
    pre_run_gate = json.loads(
        (run_dir / recognition_readiness.PRE_RUN_READINESS_GATE_FILENAME).read_text(encoding="utf-8")
    )

    assert reference_registry["artifact_type"] == "reference_asset_registry"
    assert reference_registry["evidence_source"] == "simulated"
    assert reference_registry["not_real_acceptance_evidence"] is True
    assert reference_registry["reviewer_stub_only"] is True
    assert reference_registry["readiness_mapping_only"] is True
    assert reference_registry["not_released_for_formal_claim"] is True
    assert reference_registry["ready_for_readiness_mapping"] is True
    assert reference_registry["primary_evidence_rewritten"] is False
    assert {str(item.get("asset_type") or "") for item in list(reference_registry.get("assets") or [])} >= {
        "standard_gas",
        "humidity_generator",
        "dewpoint_meter",
        "digital_pressure_gauge",
        "temperature_chamber",
        "digital_thermometer",
        "pressure_controller",
        "analyzer_under_test",
    }
    required_asset_fields = {
        "asset_id",
        "asset_name",
        "asset_type",
        "manufacturer",
        "model",
        "serial_or_lot",
        "role_in_reference_chain",
        "measurand_scope",
        "route_scope",
        "environment_scope",
        "owner_state",
        "active_state",
        "quarantine_state",
        "certificate_status",
        "certificate_id",
        "certificate_version",
        "valid_from",
        "valid_to",
        "intermediate_check_status",
        "intermediate_check_due",
        "last_check_at",
        "released_for_formal_claim",
        "ready_for_readiness_mapping",
        "not_real_acceptance_evidence",
        "evidence_source",
        "limitation_note",
        "non_claim_note",
        "reviewer_note",
    }
    assert all(required_asset_fields <= set(item) for item in list(reference_registry.get("assets") or []))
    assert all(item["evidence_source"] == "simulated" for item in list(reference_registry.get("assets") or []))
    assert all(item["not_real_acceptance_evidence"] is True for item in list(reference_registry.get("assets") or []))

    assert certificate_lifecycle["artifact_type"] == "certificate_lifecycle_summary"
    assert certificate_lifecycle["evidence_source"] == "simulated"
    assert certificate_lifecycle["not_real_acceptance_evidence"] is True
    assert certificate_lifecycle["reviewer_stub_only"] is True
    assert certificate_lifecycle["readiness_mapping_only"] is True
    assert certificate_lifecycle["not_released_for_formal_claim"] is True
    assert certificate_lifecycle["not_ready_for_formal_claim"] is True
    assert certificate_lifecycle["ready_for_readiness_mapping"] is True
    assert certificate_lifecycle["primary_evidence_rewritten"] is False
    assert certificate_lifecycle["certificate_rows"]
    assert certificate_lifecycle["lot_bindings"]
    assert certificate_lifecycle["intermediate_check_plans"]
    assert certificate_lifecycle["intermediate_check_records"]
    assert certificate_lifecycle["out_of_tolerance_events"]
    for collection_name in (
        "certificate_rows",
        "lot_bindings",
        "intermediate_check_plans",
        "intermediate_check_records",
        "out_of_tolerance_events",
    ):
        rows = list(certificate_lifecycle.get(collection_name) or [])
        assert all(row["evidence_source"] == "simulated" for row in rows)
        assert all(row["not_real_acceptance_evidence"] is True for row in rows)
        assert all(row["reviewer_stub_only"] is True for row in rows)
        assert all(row["readiness_mapping_only"] is True for row in rows)
        assert all(row["not_released_for_formal_claim"] is True for row in rows)

    assert pre_run_gate["artifact_type"] == "pre_run_readiness_gate"
    assert pre_run_gate["evidence_source"] == "simulated"
    assert pre_run_gate["not_real_acceptance_evidence"] is True
    assert pre_run_gate["reviewer_stub_only"] is True
    assert pre_run_gate["readiness_mapping_only"] is True
    assert pre_run_gate["not_released_for_formal_claim"] is True
    assert pre_run_gate["not_ready_for_formal_claim"] is True
    assert pre_run_gate["ready_for_readiness_mapping"] is True
    assert pre_run_gate["primary_evidence_rewritten"] is False
    assert pre_run_gate["gate_status"] in {
        "ok_for_reviewer_mapping",
        "warning_reviewer_attention",
        "blocked_for_formal_claim",
    }
    assert pre_run_gate["gate_status"] == "blocked_for_formal_claim"
    assert pre_run_gate["blocking_items"]
    assert pre_run_gate["warning_items"]
    assert pre_run_gate["reviewer_actions"]
    assert pre_run_gate["checks"]
    assert pre_run_gate["scope_reference_assets"]
    assert pre_run_gate["decision_rule_dependencies"]
    check_ids = {str(item.get("check_id") or "") for item in list(pre_run_gate.get("checks") or [])}
    assert check_ids >= {
        "certificate_missing",
        "certificate_validity",
        "intermediate_check",
        "out_of_tolerance",
        "lot_binding",
        "substitute_standard_chain_approval",
        "scope_decision_asset_integrity",
    }
