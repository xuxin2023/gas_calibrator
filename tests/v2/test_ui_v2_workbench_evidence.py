import json
from pathlib import Path
import sys

from gas_calibrator.v2.config import summarize_step2_config_safety
from gas_calibrator.v2.core.controlled_state_machine_profile import STATE_TRANSITION_EVIDENCE_FILENAME
from gas_calibrator.v2.core.measurement_phase_coverage import MEASUREMENT_PHASE_COVERAGE_REPORT_FILENAME
from gas_calibrator.v2.core.multi_source_stability import (
    MULTI_SOURCE_STABILITY_EVIDENCE_FILENAME,
    SIMULATION_EVIDENCE_SIDECAR_BUNDLE_FILENAME,
)
from gas_calibrator.v2.core.phase_taxonomy_contract import (
    METHOD_CONFIRMATION_FAMILY,
    UNCERTAINTY_INPUT_FAMILY,
)
from gas_calibrator.v2.core import recognition_readiness_artifacts as recognition_readiness
from gas_calibrator.v2.core.reviewer_fragments_contract import (
    BOUNDARY_FRAGMENT_FAMILY,
    NON_CLAIM_FRAGMENT_FAMILY,
)
from gas_calibrator.v2.ui_v2.i18n import display_fragment_value, display_taxonomy_value

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import build_fake_facade


def _inject_point_taxonomy_summary(run_dir: Path) -> None:
    summary_path = run_dir / "summary.json"
    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    stats = dict(payload.get("stats", {}) or {})
    stats["point_summaries"] = [
        {
            "point": {
                "index": 1,
                "pressure_target_label": "ambient",
                "pressure_mode": "ambient",
            },
            "stats": {
                "flush_gate_status": "pass",
                "preseal_dewpoint_c": 6.1,
                "preseal_trigger_overshoot_hpa": 4.2,
                "preseal_vent_off_begin_to_route_sealed_ms": 1200,
                "pressure_gauge_stale_ratio": 0.25,
                "pressure_gauge_stale_count": 1,
                "pressure_gauge_total_count": 4,
            },
        },
        {
            "point": {
                "index": 2,
                "pressure_target_label": "ambient_open",
                "pressure_mode": "ambient_open",
            },
            "stats": {
                "flush_gate_status": "veto",
                "postseal_timeout_blocked": True,
                "dewpoint_rebound_detected": True,
            },
        },
    ]
    stats["point_taxonomy_summary"] = {
        "pressure_summary": "ambient 1 | ambient_open 1",
        "flush_gate_summary": "pass 1 | veto 1 | rebound 1",
        "preseal_summary": "points 1 | max overshoot 4.2 hPa | max sealed wait 1200 ms",
        "postseal_summary": "timeout blocked 1 | late rebound 1",
        "stale_gauge_summary": "points 1 | worst 25%",
    }
    payload["stats"] = stats
    summary_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def test_workbench_evidence_generation_updates_artifacts_and_results_snapshot(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)
    _inject_point_taxonomy_summary(Path(facade.result_store.run_dir))

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
    assert report_payload["point_taxonomy_summary"]["pressure_summary"] == "ambient 1 | ambient_open 1"
    assert report_payload["point_taxonomy_summary"]["postseal_summary"] == "timeout blocked 1 | late rebound 1"
    assert report_payload["measurement_core_evidence"]["available"] is True
    assert report_payload["measurement_core_evidence"]["multi_source_stability_evidence"]["artifact_type"] == (
        "multi_source_stability_evidence"
    )
    assert report_payload["measurement_core_evidence"]["stability_policy_profile"]["artifact_type"] == (
        "stability_policy_profile"
    )
    assert report_payload["measurement_core_evidence"]["stability_decision_rollup"]["artifact_type"] == (
        "stability_decision_rollup"
    )
    assert report_payload["measurement_core_evidence"]["shadow_stability_diff"]["artifact_type"] == (
        "shadow_stability_diff"
    )
    assert report_payload["measurement_core_evidence"]["state_transition_evidence"]["artifact_type"] == (
        "state_transition_evidence"
    )
    assert report_payload["measurement_core_evidence"]["transition_policy_profile"]["artifact_type"] == (
        "transition_policy_profile"
    )
    assert report_payload["measurement_core_evidence"]["simulation_evidence_sidecar_bundle"]["artifact_type"] == (
        "simulation_evidence_sidecar_bundle"
    )
    assert report_payload["measurement_core_evidence"]["measurement_phase_coverage_report"]["artifact_type"] == (
        "measurement_phase_coverage_report"
    )
    assert report_payload["recognition_readiness_evidence"]["available"] is True
    assert (
        report_payload["recognition_readiness_evidence"]["scope_readiness_summary"]["artifact_type"]
        == "scope_readiness_summary"
    )
    assert (
        report_payload["recognition_readiness_evidence"]["reference_asset_registry"]["artifact_type"]
        == "reference_asset_registry"
    )
    assert (
        report_payload["recognition_readiness_evidence"]["certificate_lifecycle_summary"]["artifact_type"]
        == "certificate_lifecycle_summary"
    )
    assert (
        report_payload["recognition_readiness_evidence"]["certificate_readiness_summary"]["artifact_type"]
        == "certificate_readiness_summary"
    )
    assert (
        report_payload["recognition_readiness_evidence"]["pre_run_readiness_gate"]["artifact_type"]
        == "pre_run_readiness_gate"
    )
    assert (
        report_payload["recognition_readiness_evidence"]["method_confirmation_protocol"]["artifact_type"]
        == "method_confirmation_protocol"
    )
    assert (
        report_payload["recognition_readiness_evidence"]["route_specific_validation_matrix"]["artifact_type"]
        == "route_specific_validation_matrix"
    )
    assert (
        report_payload["recognition_readiness_evidence"]["validation_run_set"]["artifact_type"]
        == "validation_run_set"
    )
    assert (
        report_payload["recognition_readiness_evidence"]["verification_digest"]["artifact_type"]
        == "verification_digest"
    )
    assert (
        report_payload["recognition_readiness_evidence"]["verification_rollup"]["artifact_type"]
        == "verification_rollup"
    )
    assert (
        report_payload["recognition_readiness_evidence"]["uncertainty_report_pack"]["artifact_type"]
        == "uncertainty_report_pack"
    )
    assert (
        report_payload["recognition_readiness_evidence"]["uncertainty_digest"]["artifact_type"]
        == "uncertainty_digest"
    )
    assert (
        report_payload["recognition_readiness_evidence"]["uncertainty_rollup"]["artifact_type"]
        == "uncertainty_rollup"
    )
    assert (
        report_payload["recognition_readiness_evidence"]["uncertainty_method_readiness_summary"]["artifact_type"]
        == "uncertainty_method_readiness_summary"
    )
    assert (
        report_payload["recognition_readiness_evidence"]["audit_readiness_digest"]["artifact_type"]
        == "audit_readiness_digest"
    )
    expected_software_validation_keys = {
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
    for artifact_key, filename in expected_software_validation_keys.items():
        artifact_payload = dict(report_payload["recognition_readiness_evidence"][artifact_key])
        assert artifact_payload["artifact_type"] == artifact_key
        assert artifact_payload["not_real_acceptance_evidence"] is True
        assert artifact_payload["not_ready_for_formal_claim"] is True
        assert artifact_payload["primary_evidence_rewritten"] is False
        assert report_payload["recognition_readiness_evidence"]["artifact_paths"][artifact_key].endswith(filename)
    assert any(
        "Asset count:" in str(line)
        for line in list(report_payload["recognition_readiness_evidence"]["summary_lines"] or [])
    )
    assert any(
        "Certificate validity:" in str(line)
        for line in list(report_payload["recognition_readiness_evidence"]["summary_lines"] or [])
    )
    assert any(
        "Lot binding:" in str(line)
        for line in list(report_payload["recognition_readiness_evidence"]["summary_lines"] or [])
    )
    assert any(
        "Intermediate checks:" in str(line)
        for line in list(report_payload["recognition_readiness_evidence"]["summary_lines"] or [])
    )
    assert display_fragment_value(BOUNDARY_FRAGMENT_FAMILY, "shadow_evaluation_only") in report_payload["measurement_core_evidence"]["boundary_lines"]
    assert display_fragment_value(NON_CLAIM_FRAGMENT_FAMILY, "not_accreditation_claim") in report_payload["recognition_readiness_evidence"]["boundary_lines"]
    assert any(
        "payload" in str(line).lower()
        for line in list(report_payload["measurement_core_evidence"]["summary_lines"] or [])
    )
    assert any(
        "candidate" in str(line).lower() or "routes" in str(line).lower()
        for line in list(report_payload["measurement_core_evidence"]["summary_lines"] or [])
    )
    assert any(
        "工件范围" in str(line) or "Scope Readiness Summary" in str(line)
        for line in list(report_payload["recognition_readiness_evidence"]["summary_lines"] or [])
    )
    assert any(
        "方法确认概览" in str(line) or "Method confirmation overview" in str(line)
        for line in list(report_payload["recognition_readiness_evidence"]["summary_lines"] or [])
    )
    assert any(
        "验证矩阵完整度" in str(line) or "Validation matrix completeness" in str(line)
        for line in list(report_payload["recognition_readiness_evidence"]["summary_lines"] or [])
    )
    assert any(
        "关联测量阶段" in str(line) or "关联测量缺口" in str(line)
        for line in list(report_payload["recognition_readiness_evidence"]["detail_lines"] or [])
    )
    assert any(
        "当前证据覆盖" in str(line) or "Current evidence coverage" in str(line)
        for line in list(report_payload["recognition_readiness_evidence"]["detail_lines"] or [])
    )
    assert any(
        "下一步补证工件" in str(line)
        for line in list(report_payload["recognition_readiness_evidence"]["detail_lines"] or [])
    )
    assert (
        report_payload["recognition_readiness_evidence"]["change_impact_summary"]["changed_modules_summary"]
    )
    assert (
        report_payload["recognition_readiness_evidence"]["change_impact_summary"]["impacts_main_execution_chain"] is False
    )
    assert (
        report_payload["recognition_readiness_evidence"]["rollback_readiness_summary"]["rollback_mode"]
        == "file_artifact_first"
    )
    assert (
        report_payload["recognition_readiness_evidence"]["rollback_readiness_summary"]["touches_primary_evidence"]
        is False
    )
    assert any(
        "关联方法确认条目" in str(line)
        and display_taxonomy_value(METHOD_CONFIRMATION_FAMILY, "water_preseal_window_definition") in str(line)
        for line in list(report_payload["recognition_readiness_evidence"]["detail_lines"] or [])
    )
    assert any(
        "关联不确定度输入" in str(line)
        and display_taxonomy_value(UNCERTAINTY_INPUT_FAMILY, "preseal_pressure_term") in str(line)
        for line in list(report_payload["measurement_core_evidence"]["summary_lines"] or [])
    )
    assert any(
        "scope_id:" in str(line).lower() or "simulation_offline_headless" in str(line)
        for line in list(report_payload["recognition_readiness_evidence"]["summary_lines"] or [])
    )
    assert any(
        "limitation" in str(line).lower() or "formal compliance" in str(line).lower()
        for line in list(report_payload["recognition_readiness_evidence"]["detail_lines"] or [])
    )
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
    assert snapshot_payload["point_taxonomy_summary"]["stale_gauge_summary"] == "points 1 | worst 25%"
    assert snapshot_payload["measurement_core_evidence"]["artifact_paths"]["multi_source_stability_evidence"].endswith(
        MULTI_SOURCE_STABILITY_EVIDENCE_FILENAME
    )
    assert snapshot_payload["measurement_core_evidence"]["artifact_paths"]["state_transition_evidence"].endswith(
        STATE_TRANSITION_EVIDENCE_FILENAME
    )
    assert snapshot_payload["measurement_core_evidence"]["artifact_paths"][
        "simulation_evidence_sidecar_bundle"
    ].endswith(SIMULATION_EVIDENCE_SIDECAR_BUNDLE_FILENAME)
    assert snapshot_payload["measurement_core_evidence"]["artifact_paths"][
        "measurement_phase_coverage_report"
    ].endswith(MEASUREMENT_PHASE_COVERAGE_REPORT_FILENAME)
    assert snapshot_payload["measurement_core_evidence"]["artifact_paths"]["compatibility_scan_summary"].endswith(
        "compatibility_scan_summary.json"
    )
    assert snapshot_payload["measurement_core_evidence"]["artifact_paths"]["run_artifact_index"].endswith(
        "run_artifact_index.json"
    )
    assert snapshot_payload["recognition_readiness_evidence"]["available"] is True
    assert any(
        "Asset count:" in str(line)
        for line in list(snapshot_payload["recognition_readiness_evidence"]["summary_lines"] or [])
    )
    for artifact_key, filename in expected_software_validation_keys.items():
        assert snapshot_payload["recognition_readiness_evidence"][artifact_key]["artifact_type"] == artifact_key
        assert snapshot_payload["recognition_readiness_evidence"]["artifact_paths"][artifact_key].endswith(filename)
    assert (
        "scope_readiness_summary"
        in snapshot_payload["recognition_readiness_evidence"]["artifact_paths"]
    )
    assert (
        "reference_asset_registry"
        in snapshot_payload["recognition_readiness_evidence"]["artifact_paths"]
    )
    assert (
        "certificate_lifecycle_summary"
        in snapshot_payload["recognition_readiness_evidence"]["artifact_paths"]
    )
    assert (
        "pre_run_readiness_gate"
        in snapshot_payload["recognition_readiness_evidence"]["artifact_paths"]
    )
    assert (
        "uncertainty_report_pack"
        in snapshot_payload["recognition_readiness_evidence"]["artifact_paths"]
    )
    assert (
        "uncertainty_digest"
        in snapshot_payload["recognition_readiness_evidence"]["artifact_paths"]
    )
    assert (
        "uncertainty_rollup"
        in snapshot_payload["recognition_readiness_evidence"]["artifact_paths"]
    )
    assert any(
        "不确定度" in str(line) or "Uncertainty overview" in str(line)
        for line in list(snapshot_payload["recognition_readiness_evidence"]["summary_lines"] or [])
    )
    assert any(
        "主要不确定度贡献" in str(line) or "Top uncertainty contributors" in str(line)
        for line in list(snapshot_payload["recognition_readiness_evidence"]["detail_lines"] or [])
    )
    assert any(
        "工件兼容" in str(line)
        for line in list(snapshot_payload["measurement_core_evidence"]["summary_lines"] or [])
    )
    assert any(
        "工件兼容" in str(line)
        for line in list(snapshot_payload["recognition_readiness_evidence"]["summary_lines"] or [])
    )
    assert any(
        "scope_id:" in str(line).lower() or "simulation_offline_headless" in str(line)
        for line in list(snapshot_payload["recognition_readiness_evidence"]["summary_lines"] or [])
    )
    assert report_payload["reference_quality"]["thermometer_reference_status"] == "stale"
    assert report_payload["simulation_context"]["workbench_reports"]
    assert "压力语义" in report_md_path.read_text(encoding="utf-8")
    assert "冲洗门禁" in report_md_path.read_text(encoding="utf-8")

    assert "measurement-core readiness" in report_md_path.read_text(encoding="utf-8")
    assert "认可就绪治理骨架" in report_md_path.read_text(encoding="utf-8")
    assert MULTI_SOURCE_STABILITY_EVIDENCE_FILENAME in report_md_path.read_text(encoding="utf-8")
    assert STATE_TRANSITION_EVIDENCE_FILENAME in report_md_path.read_text(encoding="utf-8")
    assert MEASUREMENT_PHASE_COVERAGE_REPORT_FILENAME in report_md_path.read_text(encoding="utf-8")
    assert "scope_readiness_summary" in report_md_path.read_text(encoding="utf-8")

    exports = summary_payload["stats"]["artifact_exports"]
    assert exports["workbench_action_report_json"]["role"] == "diagnostic_analysis"
    assert exports["workbench_action_report_markdown"]["role"] == "diagnostic_analysis"
    assert exports["workbench_action_snapshot"]["role"] == "diagnostic_analysis"
    assert summary_payload["config_safety"]["classification"] == "simulation_real_port_inventory_risk"
    assert summary_payload["config_safety_review"]["status"] == "blocked"
    assert summary_payload["config_safety_review"]["execution_gate"]["status"] == "blocked"
    assert summary_payload["config_governance_handoff"]["execution_gate"]["status"] == "blocked"
    assert summary_payload["point_taxonomy_summary"]["flush_gate_summary"] == "pass 1 | veto 1 | rebound 1"
    assert summary_payload["artifact_role_summary"] == summary_payload["stats"]["artifact_role_summary"]
    assert summary_payload["workbench_evidence_summary"]["evidence_state"] == "simulated_workbench"
    assert summary_payload["workbench_evidence_summary"]["evidence_source"] == "simulated_protocol"
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
    assert summary_payload["stats"]["workbench_evidence_summary"]["point_taxonomy_summary"]["flush_gate_summary"] == (
        "pass 1 | veto 1 | rebound 1"
    )
    assert "workbench_action_report_json" in manifest_payload["artifacts"]["role_catalog"]["diagnostic_analysis"]
    assert manifest_payload["workbench_evidence"]["config_safety"]["classification"] == "simulation_real_port_inventory_risk"
    assert manifest_payload["workbench_evidence"]["config_safety_review"]["execution_gate"]["status"] == "blocked"
    assert manifest_payload["workbench_evidence"]["qc_evidence_section"]["lines"]
    assert manifest_payload["workbench_evidence"]["point_taxonomy_summary"]["preseal_summary"] == (
        "points 1 | max overshoot 4.2 hPa | max sealed wait 1200 ms"
    )

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
    assert results_snapshot["workbench_action_report"]["point_taxonomy_summary"]["pressure_summary"] == (
        "ambient 1 | ambient_open 1"
    )
    assert results_snapshot["workbench_evidence_summary"]["point_taxonomy_summary"]["stale_gauge_summary"] == (
        "points 1 | worst 25%"
    )
    assert results_snapshot["multi_source_stability_evidence"]["artifact_type"] == "multi_source_stability_evidence"
    assert results_snapshot["state_transition_evidence"]["artifact_type"] == "state_transition_evidence"
    assert results_snapshot["simulation_evidence_sidecar_bundle"]["artifact_type"] == "simulation_evidence_sidecar_bundle"
    assert results_snapshot["measurement_phase_coverage_report"]["artifact_type"] == "measurement_phase_coverage_report"
    assert results_snapshot["scope_readiness_summary"]["artifact_type"] == "scope_readiness_summary"
    assert results_snapshot["reference_asset_registry"]["artifact_type"] == "reference_asset_registry"
    assert results_snapshot["certificate_lifecycle_summary"]["artifact_type"] == "certificate_lifecycle_summary"
    assert results_snapshot["certificate_readiness_summary"]["artifact_type"] == "certificate_readiness_summary"
    assert results_snapshot["pre_run_readiness_gate"]["artifact_type"] == "pre_run_readiness_gate"
    assert (
        results_snapshot["uncertainty_method_readiness_summary"]["artifact_type"]
        == "uncertainty_method_readiness_summary"
    )
    assert results_snapshot["audit_readiness_digest"]["artifact_type"] == "audit_readiness_digest"
    assert "payload" in str(results_snapshot["measurement_core_summary_text"]).lower()
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
