import json
from pathlib import Path
import sys

from gas_calibrator.v2.config import summarize_step2_config_safety

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import build_fake_facade


def test_workbench_evidence_generation_updates_artifacts_and_results_snapshot(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)

    facade.execute_device_workbench_action("thermometer", "set_mode", mode="stale")
    result = facade.execute_device_workbench_action(
        "workbench",
        "generate_diagnostic_evidence",
        current_device="thermometer",
        current_action="set_mode",
    )

    run_dir = Path(facade.result_store.run_dir)
    report_json_path = run_dir / "workbench_action_report.json"
    report_md_path = run_dir / "workbench_action_report.md"
    snapshot_json_path = run_dir / "workbench_action_snapshot.json"

    assert result["ok"] is True
    assert report_json_path.exists()
    assert report_md_path.exists()
    assert snapshot_json_path.exists()
    assert not any(path.name.endswith("_latest.json") for path in run_dir.glob("*_latest.json"))

    report_payload = json.loads(report_json_path.read_text(encoding="utf-8"))
    snapshot_payload = json.loads(snapshot_json_path.read_text(encoding="utf-8"))
    summary_payload = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
    manifest_payload = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))

    assert report_payload["evidence_source"] == "simulated_protocol"
    assert report_payload["evidence_state"] == "simulated_workbench"
    assert report_payload["not_real_acceptance_evidence"] is True
    assert report_payload["acceptance_level"] == "offline_regression"
    assert report_payload["promotion_state"] == "dry_run_only"
    assert report_payload["qc_review_summary"]["lines"]
    assert report_payload["qc_reviewer_card"]["lines"]
    assert report_payload["qc_evidence_section"]["lines"]
    assert report_payload["qc_evidence_section"]["cards"]
    assert report_payload["qc_review_cards"]
    assert report_payload["qc_review_summary"]["evidence_source"] == "simulated_protocol"
    assert report_payload["qc_review_summary"]["run_gate"]["status"] == "warn"
    assert report_payload["config_safety"]["classification"] == "simulation_real_port_inventory_risk"
    assert report_payload["config_safety_review"]["status"] == "blocked"
    assert report_payload["config_safety_review"]["execution_gate"]["status"] == "blocked"
    assert report_payload["config_governance_handoff"]["execution_gate"]["status"] == "blocked"
    assert report_payload["config_safety_review"]["warnings"]
    assert "real_com_risk" in report_payload["config_safety"]["badge_ids"]
    assert report_payload["publish_primary_latest_allowed"] is False
    assert report_payload["artifact_role"] == "diagnostic_analysis"
    assert report_payload["risk_level"] in {"medium", "high"}
    assert report_payload["device_category"] == "thermometer"
    assert report_payload["has_fault_injection"] is True
    assert report_payload["reference_quality_summary"]
    assert report_payload["route_relay_summary"]
    assert "summary" in report_payload["snapshot_compare"]
    assert snapshot_payload["evidence_state"] == "simulated_workbench"
    assert snapshot_payload["evidence_source"] == "simulated_protocol"
    assert snapshot_payload["acceptance_level"] == "offline_regression"
    assert snapshot_payload["promotion_state"] == "dry_run_only"
    assert snapshot_payload["config_safety"]["classification"] == "simulation_real_port_inventory_risk"
    assert snapshot_payload["config_safety_review"]["execution_gate"]["status"] == "blocked"
    assert snapshot_payload["config_governance_handoff"]["blocked_reason_details"]
    assert snapshot_payload["qc_evidence_section"]["cards"]
    assert snapshot_payload["qc_review_cards"]
    assert report_payload["reference_quality"]["thermometer_reference_status"] == "stale"
    assert report_payload["simulation_context"]["workbench_reports"]

    exports = summary_payload["stats"]["artifact_exports"]
    assert exports["workbench_action_report_json"]["role"] == "diagnostic_analysis"
    assert exports["workbench_action_report_markdown"]["role"] == "diagnostic_analysis"
    assert exports["workbench_action_snapshot"]["role"] == "diagnostic_analysis"
    assert summary_payload["stats"]["workbench_evidence_summary"]["evidence_state"] == "simulated_workbench"
    assert summary_payload["stats"]["workbench_evidence_summary"]["evidence_source"] == "simulated_protocol"
    assert summary_payload["stats"]["workbench_evidence_summary"]["acceptance_level"] == "offline_regression"
    assert summary_payload["stats"]["workbench_evidence_summary"]["promotion_state"] == "dry_run_only"
    assert summary_payload["stats"]["workbench_evidence_summary"]["config_safety"]["classification"] == (
        "simulation_real_port_inventory_risk"
    )
    assert summary_payload["stats"]["workbench_evidence_summary"]["config_safety_review"]["status"] == "blocked"
    assert summary_payload["stats"]["workbench_evidence_summary"]["config_safety_review"]["execution_gate"]["status"] == "blocked"
    assert summary_payload["stats"]["workbench_evidence_summary"]["config_governance_handoff"]["execution_gate"]["status"] == "blocked"
    assert "workbench_action_report_json" in manifest_payload["artifacts"]["role_catalog"]["diagnostic_analysis"]
    assert manifest_payload["workbench_evidence"]["config_safety"]["classification"] == "simulation_real_port_inventory_risk"
    assert manifest_payload["workbench_evidence"]["config_safety_review"]["execution_gate"]["status"] == "blocked"
    assert manifest_payload["workbench_evidence"]["qc_evidence_section"]["lines"]

    results_snapshot = facade.build_results_snapshot()

    assert results_snapshot["workbench_action_report"]["evidence_state"] == "simulated_workbench"
    assert results_snapshot["workbench_action_snapshot"]["evidence_state"] == "simulated_workbench"
    assert results_snapshot["workbench_action_report"]["evidence_source"] == "simulated_protocol"
    assert results_snapshot["workbench_evidence_summary"]["not_real_acceptance_evidence"] is True
    assert results_snapshot["workbench_action_snapshot"]["acceptance_level"] == "offline_regression"
    assert results_snapshot["workbench_action_snapshot"]["promotion_state"] == "dry_run_only"
    assert results_snapshot["workbench_evidence_summary"]["acceptance_level"] == "offline_regression"
    assert results_snapshot["workbench_evidence_summary"]["promotion_state"] == "dry_run_only"
    assert results_snapshot["config_safety"]["classification"] == "simulation_real_port_inventory_risk"
    assert results_snapshot["config_safety_review"]["status"] == "blocked"
    assert results_snapshot["config_safety_review"]["execution_gate"]["status"] == "blocked"
    assert results_snapshot["config_governance_handoff"]["execution_gate"]["status"] == "blocked"
    assert results_snapshot["workbench_action_report"]["config_safety"]["classification"] == "simulation_real_port_inventory_risk"
    assert results_snapshot["workbench_action_snapshot"]["config_safety_review"]["status"] == "blocked"
    assert results_snapshot["workbench_action_snapshot"]["config_safety_review"]["execution_gate"]["status"] == "blocked"
    assert results_snapshot["workbench_action_report"]["qc_evidence_section"]["lines"]
    assert results_snapshot["workbench_action_report"]["qc_evidence_section"]["cards"]
    assert results_snapshot["workbench_action_report"]["qc_review_cards"]
    assert results_snapshot["workbench_action_snapshot"]["qc_review_cards"]
    assert results_snapshot["workbench_action_snapshot"]["config_governance_handoff"]["blocked_reason_details"]
    assert results_snapshot["review_digest"]["items"]["workbench"]["available"] is True
    assert "diagnostic_analysis" in results_snapshot["artifact_role_summary"]


def test_workbench_evidence_preserves_runtime_config_unlock_state(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)
    runtime_config_safety = summarize_step2_config_safety(
        facade.config,
        allow_unsafe_step2_config=True,
        unsafe_config_env_enabled=True,
    )
    for config_obj in (facade.config, facade.service.config):
        setattr(config_obj, "_config_safety", dict(runtime_config_safety))
        setattr(config_obj, "_step2_execution_gate", dict(runtime_config_safety.get("execution_gate") or {}))

    facade.execute_device_workbench_action("thermometer", "set_mode", mode="stale")
    facade.execute_device_workbench_action(
        "workbench",
        "generate_diagnostic_evidence",
        current_device="thermometer",
        current_action="set_mode",
    )

    run_dir = Path(facade.result_store.run_dir)
    report_payload = json.loads((run_dir / "workbench_action_report.json").read_text(encoding="utf-8"))
    snapshot_payload = json.loads((run_dir / "workbench_action_snapshot.json").read_text(encoding="utf-8"))

    assert report_payload["config_safety_review"]["status"] == "unlocked_override"
    assert report_payload["config_safety_review"]["execution_gate"]["status"] == "unlocked_override"
    assert report_payload["config_governance_handoff"]["execution_gate"]["status"] == "unlocked_override"
    assert snapshot_payload["config_safety_review"]["execution_gate"]["allow_unsafe_step2_config_flag"] is True
    assert snapshot_payload["config_safety_review"]["execution_gate"]["allow_unsafe_step2_config_env"] is True
