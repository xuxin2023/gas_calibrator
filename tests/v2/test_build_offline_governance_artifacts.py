from __future__ import annotations

import json
from pathlib import Path

from gas_calibrator.v2.core.phase_transition_bridge_presenter import (
    build_phase_transition_bridge_panel_payload,
)
from gas_calibrator.v2.core.phase_transition_bridge_reviewer_artifact import (
    build_phase_transition_bridge_reviewer_artifact,
)
from gas_calibrator.v2.scripts.build_offline_governance_artifacts import main, rebuild_run, rebuild_suite


def _write_offline_diagnostic_bundles(run_dir: Path) -> None:
    room_temp_dir = run_dir / "room_temp_diagnostic"
    room_temp_dir.mkdir(parents=True, exist_ok=True)
    (room_temp_dir / "diagnostic_plot.png").write_text("png", encoding="utf-8")
    (room_temp_dir / "readable_report.md").write_text("# room temp\n", encoding="utf-8")
    (room_temp_dir / "diagnostic_workbook.xlsx").write_text("", encoding="utf-8")
    (room_temp_dir / "diagnostic_summary.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-04-04T10:00:00",
                "classification": "warn",
                "recommended_variant": "ambient_open",
                "dominant_error": "pressure_bias",
                "next_check": "verify ambient chain",
                "summary": "Room-temp diagnostic summary",
                "plot_files": ["diagnostic_plot.png"],
                "evidence_source": "diagnostic",
                "not_real_acceptance_evidence": True,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    analyzer_dir = run_dir / "analyzer_chain_isolation"
    analyzer_dir.mkdir(parents=True, exist_ok=True)
    (analyzer_dir / "isolation_plot.png").write_text("png", encoding="utf-8")
    (analyzer_dir / "summary.json").write_text("{}", encoding="utf-8")
    (analyzer_dir / "readable_report.md").write_text("# analyzer chain\n", encoding="utf-8")
    (analyzer_dir / "diagnostic_workbook.xlsx").write_text("", encoding="utf-8")
    (analyzer_dir / "operator_checklist.md").write_text("checklist\n", encoding="utf-8")
    (analyzer_dir / "compare_vs_8ch.md").write_text("8ch\n", encoding="utf-8")
    (analyzer_dir / "compare_vs_baseline.md").write_text("baseline\n", encoding="utf-8")
    (analyzer_dir / "isolation_comparison_summary.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-04-04T11:00:00",
                "should_continue_s1": False,
                "dominant_conclusion": "chain mismatch",
                "recommendation": "inspect analyzer chain",
                "summary": "Analyzer-chain isolation summary",
                "plot_files": ["isolation_plot.png"],
                "evidence_source": "diagnostic",
                "not_real_acceptance_evidence": True,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def test_rebuild_run_generates_governance_artifacts(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_001"
    run_dir.mkdir()
    (run_dir / "summary.json").write_text(
        json.dumps(
            {
                "run_id": "run_001",
                "software_build_id": "build-1",
                "config_safety": {
                    "classification": "simulation_real_port_inventory_risk",
                    "summary": "top-level config safety",
                    "execution_gate": {"status": "blocked", "summary": "blocked by top-level safety"},
                },
                "config_safety_review": {
                    "status": "blocked",
                    "summary": "top-level review",
                    "inventory_summary": "inventory summary",
                    "warnings": ["top-level warning"],
                    "execution_gate": {"status": "blocked", "summary": "blocked by top-level review"},
                },
                "stats": {
                    "output_files": [str(run_dir / "summary.json")],
                    "artifact_exports": {
                        "run_summary": {"status": "ok", "role": "execution_summary", "path": str(run_dir / "summary.json")},
                        "coefficient_report": {
                            "status": "ok",
                            "role": "formal_analysis",
                            "path": str(run_dir / "calibration_coefficients.xlsx"),
                        },
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    (run_dir / "manifest.json").write_text(
        json.dumps(
            {
                "run_id": "run_001",
                "software_build_id": "build-1",
                "source_points_file": str(run_dir / "points.xlsx"),
                "config_snapshot": {
                    "features": {"simulation_mode": True},
                    "workflow": {"profile_name": "bench_profile", "profile_version": "1.2"},
                    "devices": {"gas_analyzers": [{"id": "GA01", "enabled": True}]},
                },
            }
        ),
        encoding="utf-8",
    )
    (run_dir / "results.json").write_text(
        json.dumps(
            {
                "samples": [
                    {
                        "analyzer_id": "GA01",
                        "frame_has_data": True,
                        "frame_usable": True,
                        "frame_status": "ok",
                        "pressure_gauge_hpa": 998.0,
                        "thermometer_temp_c": 25.1,
                        "point": {"route": "co2", "temperature_c": 25.0, "co2_ppm": 400.0},
                    }
                ],
                "point_summaries": [{"stats": {"reason": "passed"}}],
            }
        ),
        encoding="utf-8",
    )
    (run_dir / "calibration_coefficients.xlsx").write_text("", encoding="utf-8")
    _write_offline_diagnostic_bundles(run_dir)

    payload = rebuild_run(run_dir)

    assert payload["summary_stats"]["acceptance_plan"]["promotion_state"] == "dry_run_only"
    assert (run_dir / "analytics_summary.json").exists()
    assert (run_dir / "step2_readiness_summary.json").exists()
    assert (run_dir / "metrology_calibration_contract.json").exists()
    assert (run_dir / "phase_transition_bridge.json").exists()
    assert (run_dir / "phase_transition_bridge_reviewer.md").exists()
    assert (run_dir / "lineage_summary.json").exists()
    assert (run_dir / "evidence_registry.json").exists()
    analytics_summary = json.loads((run_dir / "analytics_summary.json").read_text(encoding="utf-8"))
    readiness_summary = json.loads((run_dir / "step2_readiness_summary.json").read_text(encoding="utf-8"))
    metrology_contract = json.loads((run_dir / "metrology_calibration_contract.json").read_text(encoding="utf-8"))
    phase_transition_bridge = json.loads((run_dir / "phase_transition_bridge.json").read_text(encoding="utf-8"))
    phase_transition_bridge_reviewer_markdown = (run_dir / "phase_transition_bridge_reviewer.md").read_text(
        encoding="utf-8"
    )
    evidence_registry = json.loads((run_dir / "evidence_registry.json").read_text(encoding="utf-8"))
    assert analytics_summary["evidence_source"] == "simulated_protocol"
    assert analytics_summary["not_real_acceptance_evidence"] is True
    assert analytics_summary["config_safety"]["classification"] == "simulation_real_port_inventory_risk"
    assert analytics_summary["config_safety_review"]["status"] == "blocked"
    assert analytics_summary["config_safety_review"]["warnings"] == ["top-level warning"]
    assert analytics_summary["config_governance_handoff"]["execution_gate"]["status"] == "blocked"
    assert analytics_summary["offline_diagnostic_adapter_summary"]["found"] is True
    assert analytics_summary["offline_diagnostic_adapter_summary"]["bundle_count"] == 2
    assert analytics_summary["offline_diagnostic_adapter_summary"]["coverage_summary"] == (
        "room-temp 1 | analyzer-chain 1 | artifacts 12 | plots 2"
    )
    assert analytics_summary["offline_diagnostic_adapter_summary"]["review_scope_summary"] == (
        "primary 2 | supporting 8 | plots 2"
    )
    assert analytics_summary["offline_diagnostic_adapter_summary"]["next_check_summary"] == (
        "verify ambient chain | inspect analyzer chain"
    )
    assert analytics_summary["qc_evidence_section"]["cards"]
    assert analytics_summary["qc_review_cards"]
    assert analytics_summary["run_kpis"]["point_count"] == 1
    assert analytics_summary["point_kpis"]["point_count"] == 1
    assert analytics_summary["qc_overview"]["run_gate"]["status"] == "pass"
    assert analytics_summary["qc_overview"]["decision_counts"]["pass"] == 1
    assert analytics_summary["drift_summary"]["overall_trend"] in {"stable", "increasing", "decreasing"}
    assert analytics_summary["control_chart_summary"]["status"] in {"insufficient_history", "in_control", "out_of_control"}
    assert analytics_summary["analyzer_health_digest"]["overall_status"] in {"healthy", "attention", "failed", "missing"}
    assert analytics_summary["fault_attribution_summary"]["primary_fault"] in {"none", "passed"}
    assert "离线分析摘要" in analytics_summary["unified_review_summary"]["summary"]
    assert analytics_summary["unified_review_summary"]["qc_summary"]["summary"]
    assert analytics_summary["unified_review_summary"]["analytics_summary"]["summary"]
    assert analytics_summary["unified_review_summary"]["boundary_note"].startswith("证据边界:")
    assert any("质控" in item for item in analytics_summary["unified_review_summary"]["reviewer_notes"])
    assert analytics_summary["step2_readiness_summary"]["artifact_type"] == "step2_readiness_summary"
    assert analytics_summary["step2_readiness_summary"]["overall_status"] == "not_ready"
    assert analytics_summary["step2_readiness_summary"]["ready_for_engineering_isolation"] is False
    assert analytics_summary["step2_readiness_summary"]["real_acceptance_ready"] is False
    assert analytics_summary["step2_readiness_summary"]["evidence_mode"] == "simulation_offline_headless"
    assert "不是 real acceptance" in analytics_summary["step2_readiness_summary"]["reviewer_display"]["summary_text"]
    assert analytics_summary["metrology_calibration_contract"]["artifact_type"] == "metrology_calibration_contract"
    assert analytics_summary["metrology_calibration_contract"]["overall_status"] == "contract_ready_for_stage3_bridge"
    assert analytics_summary["metrology_calibration_contract"]["real_acceptance_ready"] is False
    assert "不是 real acceptance" in analytics_summary["metrology_calibration_contract"]["reviewer_display"]["summary_text"]
    assert analytics_summary["phase_transition_bridge"]["artifact_type"] == "phase_transition_bridge"
    assert analytics_summary["phase_transition_bridge"]["overall_status"] == "step2_tail_in_progress"
    assert analytics_summary["phase_transition_bridge"]["recommended_next_stage"] == "close_step2_tail_gaps"
    assert analytics_summary["phase_transition_bridge"]["real_acceptance_ready"] is False
    assert "阶段桥工件" in analytics_summary["phase_transition_bridge"]["reviewer_display"]["summary_text"]
    expected_bridge_section = build_phase_transition_bridge_panel_payload(phase_transition_bridge)
    expected_bridge_reviewer_artifact = build_phase_transition_bridge_reviewer_artifact(phase_transition_bridge)
    assert analytics_summary["phase_transition_bridge_reviewer_section"]["available"] is True
    assert (
        analytics_summary["phase_transition_bridge_reviewer_section"]["display"]
        == expected_bridge_section["display"]
    )
    assert phase_transition_bridge_reviewer_markdown == expected_bridge_reviewer_artifact["markdown"]
    assert readiness_summary["phase"] == "step2_readiness_bridge"
    assert readiness_summary["overall_status"] == "not_ready"
    assert readiness_summary["ready_for_engineering_isolation"] is False
    assert readiness_summary["real_acceptance_ready"] is False
    assert readiness_summary["not_real_acceptance_evidence"] is True
    assert any(
        item["gate_id"] == "readiness_evidence_complete" and item["status"] == "pass"
        for item in readiness_summary["gates"]
    )
    assert readiness_summary["gates"][-1]["gate_id"] == "step2_gate_status"
    assert readiness_summary["gates"][-1]["status"] == "not_ready"
    assert "real acceptance passed" not in readiness_summary["reviewer_display"]["summary_text"].lower()
    assert metrology_contract["phase"] == "step2_tail_step3_bridge"
    assert metrology_contract["overall_status"] == "contract_ready_for_stage3_bridge"
    assert metrology_contract["not_real_acceptance_evidence"] is True
    assert metrology_contract["reference_traceability_contract"]["required_reference_chain_declaration"] is True
    assert metrology_contract["uncertainty_budget_template"]["template_only"] is True
    assert "coefficient_writeback_real_acceptance" in metrology_contract["stage3_execution_items"]
    assert "reference_traceability_contract_schema" in metrology_contract["stage_assignment"]["execute_now_in_step2_tail"]
    assert "real_reference_instrument_enforcement" in metrology_contract["stage_assignment"]["defer_to_stage3_real_validation"]
    assert "real acceptance passed" not in metrology_contract["reviewer_display"]["summary_text"].lower()
    assert phase_transition_bridge["phase"] == "step2_tail_stage3_bridge"
    assert phase_transition_bridge["overall_status"] == "step2_tail_in_progress"
    assert phase_transition_bridge["ready_for_engineering_isolation"] is False
    assert phase_transition_bridge["real_acceptance_ready"] is False
    assert phase_transition_bridge["step2_readiness_ref"]["overall_status"] == "not_ready"
    assert phase_transition_bridge["metrology_contract_ref"]["overall_status"] == "contract_ready_for_stage3_bridge"
    assert "real_reference_evidence" in phase_transition_bridge["missing_real_world_evidence"]
    assert "resolve_step2_gate_status" in phase_transition_bridge["execute_now_in_step2_tail"]
    assert "real_reference_instrument_enforcement" in phase_transition_bridge["defer_to_stage3_real_validation"]
    assert "real acceptance passed" not in phase_transition_bridge["reviewer_display"]["summary_text"].lower()
    assert evidence_registry["evidence_source"] == "simulated_protocol"
    assert evidence_registry["not_real_acceptance_evidence"] is True
    assert evidence_registry["acceptance_level"] == "offline_regression"
    assert evidence_registry["promotion_state"] == "dry_run_only"
    assert evidence_registry["config_safety"]["classification"] == "simulation_real_port_inventory_risk"
    assert evidence_registry["config_safety_review"]["status"] == "blocked"
    assert evidence_registry["config_safety_review"]["warnings"] == ["top-level warning"]
    assert payload["summary_stats"]["offline_diagnostic_adapter_summary"]["found"] is True
    assert payload["summary_stats"]["step2_readiness_summary"]["overall_status"] == "not_ready"
    assert payload["summary_stats"]["step2_readiness_digest"]["overall_status"] == "not_ready"
    assert payload["summary_stats"]["step2_readiness_digest"]["ready_for_engineering_isolation"] is False
    assert payload["summary_stats"]["step2_readiness_digest"]["real_acceptance_ready"] is False
    assert payload["summary_stats"]["step2_readiness_digest"]["gate_status_counts"]["not_ready"] == 1
    assert payload["summary_stats"]["metrology_calibration_contract"]["overall_status"] == "contract_ready_for_stage3_bridge"
    assert payload["summary_stats"]["metrology_calibration_contract_digest"]["overall_status"] == "contract_ready_for_stage3_bridge"
    assert payload["summary_stats"]["metrology_calibration_contract_digest"]["real_acceptance_ready"] is False
    assert "real_run_uncertainty_result" in payload["summary_stats"]["metrology_calibration_contract_digest"]["stage3_execution_items"]
    assert payload["summary_stats"]["phase_transition_bridge"]["overall_status"] == "step2_tail_in_progress"
    assert payload["summary_stats"]["phase_transition_bridge_digest"]["overall_status"] == "step2_tail_in_progress"
    assert payload["summary_stats"]["phase_transition_bridge_digest"]["recommended_next_stage"] == "close_step2_tail_gaps"
    assert payload["summary_stats"]["phase_transition_bridge_digest"]["ready_for_engineering_isolation"] is False
    assert payload["summary_stats"]["phase_transition_bridge_reviewer_section"]["available"] is True
    assert payload["summary_stats"]["phase_transition_bridge_reviewer_section"]["raw"]["ready_for_engineering_isolation"] is False
    assert payload["summary_stats"]["phase_transition_bridge_reviewer_section"]["raw"]["real_acceptance_ready"] is False
    assert (
        payload["summary_stats"]["phase_transition_bridge_reviewer_section"]["display"]
        == expected_bridge_section["display"]
    )
    assert payload["manifest_sections"]["step2_readiness"]["overall_status"] == "not_ready"
    assert payload["manifest_sections"]["step2_readiness"]["ready_for_engineering_isolation"] is False
    assert payload["manifest_sections"]["step2_readiness"]["real_acceptance_ready"] is False
    assert payload["manifest_sections"]["metrology_calibration_contract"]["overall_status"] == "contract_ready_for_stage3_bridge"
    assert payload["manifest_sections"]["metrology_calibration_contract"]["real_acceptance_ready"] is False
    assert payload["manifest_sections"]["phase_transition_bridge"]["overall_status"] == "step2_tail_in_progress"
    assert payload["manifest_sections"]["phase_transition_bridge"]["recommended_next_stage"] == "close_step2_tail_gaps"
    assert payload["manifest_sections"]["phase_transition_bridge"]["real_acceptance_ready"] is False
    assert payload["manifest_sections"]["phase_transition_bridge_reviewer_section"]["available"] is True
    assert (
        payload["manifest_sections"]["phase_transition_bridge_reviewer_section"]["display"]
        == expected_bridge_section["display"]
    )
    assert payload["manifest_sections"]["phase_transition_bridge_reviewer_artifact"]["artifact_type"] == (
        "phase_transition_bridge_reviewer_artifact"
    )
    assert payload["manifest_sections"]["phase_transition_bridge_reviewer_artifact"]["path"] == str(
        run_dir / "phase_transition_bridge_reviewer.md"
    )
    assert payload["manifest_sections"]["phase_transition_bridge_reviewer_artifact"]["summary_text"] == (
        expected_bridge_reviewer_artifact["display"]["summary_text"]
    )
    assert payload["manifest_sections"]["phase_transition_bridge_reviewer_artifact"]["engineering_isolation_text"] == (
        expected_bridge_reviewer_artifact["display"]["engineering_isolation_text"]
    )
    assert payload["manifest_sections"]["phase_transition_bridge_reviewer_artifact"]["real_acceptance_text"] == (
        expected_bridge_reviewer_artifact["display"]["real_acceptance_text"]
    )
    assert payload["manifest_sections"]["phase_transition_bridge_reviewer_artifact"]["blocking_text"] == (
        expected_bridge_reviewer_artifact["display"]["blocking_text"]
    )
    section_text = payload["manifest_sections"]["phase_transition_bridge_reviewer_section"]["display"]["section_text"]
    assert "Step 2 tail / Stage 3 bridge" in phase_transition_bridge_reviewer_markdown
    assert "engineering-isolation" in phase_transition_bridge_reviewer_markdown
    assert "engineering-isolation 准备：尚未具备。" in phase_transition_bridge_reviewer_markdown
    assert "real acceptance 准备：尚未具备。" in phase_transition_bridge_reviewer_markdown
    assert "当前执行" in phase_transition_bridge_reviewer_markdown
    assert "第三阶段执行" in phase_transition_bridge_reviewer_markdown
    assert "不是 real acceptance" in phase_transition_bridge_reviewer_markdown
    assert "不能替代真实计量验证" in phase_transition_bridge_reviewer_markdown
    assert "ready_for_engineering_isolation" not in phase_transition_bridge_reviewer_markdown
    assert "real_acceptance_ready" not in phase_transition_bridge_reviewer_markdown
    assert "Step 2 tail / Stage 3 bridge" in section_text
    assert "engineering-isolation" in section_text
    assert "engineering-isolation 准备：尚未具备。" in section_text
    assert "real acceptance 准备：尚未具备。" in section_text
    assert "当前执行" in section_text
    assert "第三阶段执行" in section_text
    assert "不是 real acceptance" in section_text
    assert "不能替代真实计量验证" in section_text
    assert payload["artifact_statuses"]["step2_readiness_summary"]["role"] == "execution_summary"
    assert payload["artifact_statuses"]["step2_readiness_summary"]["path"] == str(run_dir / "step2_readiness_summary.json")
    assert payload["artifact_statuses"]["metrology_calibration_contract"]["role"] == "formal_analysis"
    assert payload["artifact_statuses"]["metrology_calibration_contract"]["path"] == str(
        run_dir / "metrology_calibration_contract.json"
    )
    assert payload["artifact_statuses"]["phase_transition_bridge"]["role"] == "execution_summary"
    assert payload["artifact_statuses"]["phase_transition_bridge"]["path"] == str(run_dir / "phase_transition_bridge.json")
    assert payload["artifact_statuses"]["phase_transition_bridge_reviewer_artifact"]["role"] == "formal_analysis"
    assert payload["artifact_statuses"]["phase_transition_bridge_reviewer_artifact"]["path"] == str(
        run_dir / "phase_transition_bridge_reviewer.md"
    )
    assert str(run_dir / "step2_readiness_summary.json") in payload["remembered_files"]
    assert str(run_dir / "metrology_calibration_contract.json") in payload["remembered_files"]
    assert str(run_dir / "phase_transition_bridge.json") in payload["remembered_files"]
    assert str(run_dir / "phase_transition_bridge_reviewer.md") in payload["remembered_files"]


def test_rebuild_suite_generates_governance_artifacts(tmp_path: Path) -> None:
    suite_dir = tmp_path / "suite"
    suite_dir.mkdir()
    (suite_dir / "suite_summary.json").write_text(
        json.dumps(
            {
                "suite": "smoke",
                "all_passed": True,
                "cases": [
                    {
                        "name": "summary_parity",
                        "kind": "scenario",
                        "status": "MATCH",
                        "ok": True,
                        "evidence_source": "simulated",
                        "evidence_state": "collected",
                        "risk_level": "low",
                        "failure_type": "summary_parity",
                        "failure_phase": "summary_parity",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    payload = rebuild_suite(suite_dir)

    assert payload["suite_analytics_summary"]["artifact_type"] == "suite_analytics_summary"
    assert payload["suite_analytics_summary"]["evidence_sources_present"] == ["simulated_protocol"]
    assert payload["suite_acceptance_plan"]["evidence_source"] == "simulated_protocol"
    assert payload["suite_acceptance_plan"]["evidence_sources_present"] == ["simulated_protocol"]
    assert payload["suite_evidence_registry"]["entries"][0]["evidence_source"] == "simulated_protocol"
    assert payload["suite_evidence_registry"]["indexes"]["by_evidence_source"]["simulated_protocol"] == ["smoke:summary_parity"]
    assert (suite_dir / "suite_acceptance_plan.json").exists()
    assert (suite_dir / "suite_evidence_registry.json").exists()


def test_main_reports_clear_error_for_non_run_directory(tmp_path: Path, capsys) -> None:
    run_dir = tmp_path / "compare_like_dir"
    run_dir.mkdir()

    code = main(["--run-dir", str(run_dir)])

    captured = capsys.readouterr()
    assert code == 2
    assert "not a formal V2 run directory" in captured.err
