from __future__ import annotations

import json
from pathlib import Path
import sys

from gas_calibrator.v2.core.phase_transition_bridge_presenter import (
    build_phase_transition_bridge_panel_payload,
)
from gas_calibrator.v2.core.phase_transition_bridge_reviewer_artifact import (
    build_phase_transition_bridge_reviewer_artifact,
)
from gas_calibrator.v2.core.phase_transition_bridge_reviewer_artifact_entry import (
    build_phase_transition_bridge_reviewer_artifact_entry,
)
from gas_calibrator.v2.core.stage_admission_review_pack import (
    STAGE_ADMISSION_REVIEW_PACK_FILENAME,
    STAGE_ADMISSION_REVIEW_PACK_REVIEWER_FILENAME,
    build_stage_admission_review_pack,
)
from gas_calibrator.v2.core.engineering_isolation_admission_checklist import (
    ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_FILENAME,
    ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_REVIEWER_FILENAME,
    build_engineering_isolation_admission_checklist,
)
from gas_calibrator.v2.core.stage3_real_validation_plan import (
    STAGE3_REAL_VALIDATION_PLAN_FILENAME,
    STAGE3_REAL_VALIDATION_PLAN_REVIEWER_FILENAME,
    build_stage3_real_validation_plan,
)
from gas_calibrator.v2.core.stage3_standards_alignment_matrix import (
    STAGE3_STANDARDS_ALIGNMENT_MATRIX_FILENAME,
    STAGE3_STANDARDS_ALIGNMENT_MATRIX_REVIEWER_FILENAME,
)
from gas_calibrator.v2.core import recognition_readiness_artifacts as recognition_readiness
from gas_calibrator.v2.scripts.build_offline_governance_artifacts import main, rebuild_run, rebuild_suite

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import build_fake_facade


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
    assert (run_dir / STAGE_ADMISSION_REVIEW_PACK_FILENAME).exists()
    assert (run_dir / STAGE_ADMISSION_REVIEW_PACK_REVIEWER_FILENAME).exists()
    assert (run_dir / ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_FILENAME).exists()
    assert (run_dir / ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_REVIEWER_FILENAME).exists()
    assert (run_dir / STAGE3_REAL_VALIDATION_PLAN_FILENAME).exists()
    assert (run_dir / STAGE3_REAL_VALIDATION_PLAN_REVIEWER_FILENAME).exists()
    assert (run_dir / "lineage_summary.json").exists()
    assert (run_dir / "evidence_registry.json").exists()
    summary_after_rebuild = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
    manifest_after_rebuild = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    analytics_summary = json.loads((run_dir / "analytics_summary.json").read_text(encoding="utf-8"))
    readiness_summary = json.loads((run_dir / "step2_readiness_summary.json").read_text(encoding="utf-8"))
    metrology_contract = json.loads((run_dir / "metrology_calibration_contract.json").read_text(encoding="utf-8"))
    phase_transition_bridge = json.loads((run_dir / "phase_transition_bridge.json").read_text(encoding="utf-8"))
    phase_transition_bridge_reviewer_markdown = (run_dir / "phase_transition_bridge_reviewer.md").read_text(
        encoding="utf-8"
    )
    stage_admission_review_pack = json.loads((run_dir / STAGE_ADMISSION_REVIEW_PACK_FILENAME).read_text(encoding="utf-8"))
    stage_admission_review_pack_markdown = (run_dir / STAGE_ADMISSION_REVIEW_PACK_REVIEWER_FILENAME).read_text(
        encoding="utf-8"
    )
    engineering_isolation_admission_checklist = json.loads(
        (run_dir / ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_FILENAME).read_text(encoding="utf-8")
    )
    engineering_isolation_admission_checklist_markdown = (
        run_dir / ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_REVIEWER_FILENAME
    ).read_text(encoding="utf-8")
    stage3_real_validation_plan = json.loads(
        (run_dir / STAGE3_REAL_VALIDATION_PLAN_FILENAME).read_text(encoding="utf-8")
    )
    stage3_real_validation_plan_markdown = (
        run_dir / STAGE3_REAL_VALIDATION_PLAN_REVIEWER_FILENAME
    ).read_text(encoding="utf-8")
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
    assert analytics_summary["stage3_real_validation_plan"]["artifact_type"] == "stage3_real_validation_plan"
    assert analytics_summary["stage3_real_validation_plan"]["overall_status"] == "step2_tail_in_progress"
    assert analytics_summary["stage3_real_validation_plan"]["real_acceptance_ready"] is False
    expected_bridge_section = build_phase_transition_bridge_panel_payload(phase_transition_bridge)
    expected_bridge_reviewer_artifact = build_phase_transition_bridge_reviewer_artifact(phase_transition_bridge)
    expected_bridge_reviewer_entry = build_phase_transition_bridge_reviewer_artifact_entry(
        artifact_path=run_dir / "phase_transition_bridge_reviewer.md",
        manifest_section=payload["manifest_sections"].get("phase_transition_bridge_reviewer_artifact"),
        reviewer_section=payload["manifest_sections"].get("phase_transition_bridge_reviewer_section"),
    )
    expected_stage_admission_review_pack = build_stage_admission_review_pack(
        run_id="run_001",
        step2_readiness_summary=readiness_summary,
        metrology_calibration_contract=metrology_contract,
        phase_transition_bridge=phase_transition_bridge,
        phase_transition_bridge_reviewer_artifact=expected_bridge_reviewer_artifact,
        artifact_paths={
            "step2_readiness_summary": run_dir / "step2_readiness_summary.json",
            "metrology_calibration_contract": run_dir / "metrology_calibration_contract.json",
            "phase_transition_bridge": run_dir / "phase_transition_bridge.json",
            "phase_transition_bridge_reviewer_artifact": run_dir / "phase_transition_bridge_reviewer.md",
        },
    )
    expected_engineering_isolation_admission_checklist = build_engineering_isolation_admission_checklist(
        run_id="run_001",
        step2_readiness_summary=readiness_summary,
        metrology_calibration_contract=metrology_contract,
        phase_transition_bridge=phase_transition_bridge,
        stage_admission_review_pack=expected_stage_admission_review_pack,
        artifact_paths={
            "step2_readiness_summary": run_dir / "step2_readiness_summary.json",
            "metrology_calibration_contract": run_dir / "metrology_calibration_contract.json",
            "phase_transition_bridge": run_dir / "phase_transition_bridge.json",
            "phase_transition_bridge_reviewer_artifact": run_dir / "phase_transition_bridge_reviewer.md",
            "stage_admission_review_pack": run_dir / STAGE_ADMISSION_REVIEW_PACK_FILENAME,
            "stage_admission_review_pack_reviewer_artifact": run_dir / STAGE_ADMISSION_REVIEW_PACK_REVIEWER_FILENAME,
        },
    )
    expected_stage3_real_validation_plan = build_stage3_real_validation_plan(
        run_id="run_001",
        step2_readiness_summary=readiness_summary,
        metrology_calibration_contract=metrology_contract,
        phase_transition_bridge=phase_transition_bridge,
        stage_admission_review_pack=expected_stage_admission_review_pack,
        engineering_isolation_admission_checklist=expected_engineering_isolation_admission_checklist,
        artifact_paths={
            "step2_readiness_summary": run_dir / "step2_readiness_summary.json",
            "metrology_calibration_contract": run_dir / "metrology_calibration_contract.json",
            "phase_transition_bridge": run_dir / "phase_transition_bridge.json",
            "phase_transition_bridge_reviewer_artifact": run_dir / "phase_transition_bridge_reviewer.md",
            "stage_admission_review_pack": run_dir / STAGE_ADMISSION_REVIEW_PACK_FILENAME,
            "stage_admission_review_pack_reviewer_artifact": run_dir / STAGE_ADMISSION_REVIEW_PACK_REVIEWER_FILENAME,
            "engineering_isolation_admission_checklist": run_dir / ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_FILENAME,
            "engineering_isolation_admission_checklist_reviewer_artifact": (
                run_dir / ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_REVIEWER_FILENAME
            ),
        },
    )
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
    assert payload["summary_stats"]["stage_admission_review_pack"]["artifact_type"] == "stage_admission_review_pack"
    assert payload["summary_stats"]["stage_admission_review_pack"]["overall_status"] == "step2_tail_in_progress"
    assert payload["summary_stats"]["stage_admission_review_pack"]["ready_for_engineering_isolation"] is False
    assert payload["summary_stats"]["stage_admission_review_pack"]["real_acceptance_ready"] is False
    assert payload["summary_stats"]["stage_admission_review_pack_digest"]["overall_status"] == "step2_tail_in_progress"
    assert payload["summary_stats"]["stage_admission_review_pack_digest"]["recommended_next_stage"] == "close_step2_tail_gaps"
    assert payload["summary_stats"]["stage_admission_review_pack_digest"]["artifact_paths"][
        "phase_transition_bridge_reviewer_artifact"
    ] == str(run_dir / "phase_transition_bridge_reviewer.md")
    assert payload["summary_stats"]["engineering_isolation_admission_checklist"]["artifact_type"] == (
        "engineering_isolation_admission_checklist"
    )
    assert payload["summary_stats"]["engineering_isolation_admission_checklist"]["overall_status"] == (
        "step2_tail_in_progress"
    )
    assert payload["summary_stats"]["engineering_isolation_admission_checklist"]["ready_for_engineering_isolation"] is False
    assert payload["summary_stats"]["engineering_isolation_admission_checklist"]["real_acceptance_ready"] is False
    assert payload["summary_stats"]["engineering_isolation_admission_checklist_digest"]["overall_status"] == (
        "step2_tail_in_progress"
    )
    assert payload["summary_stats"]["engineering_isolation_admission_checklist_digest"]["recommended_next_stage"] == (
        "close_step2_tail_gaps"
    )
    assert payload["summary_stats"]["engineering_isolation_admission_checklist_digest"]["artifact_paths"][
        "stage_admission_review_pack"
    ] == str(run_dir / STAGE_ADMISSION_REVIEW_PACK_FILENAME)
    assert payload["summary_stats"]["stage3_real_validation_plan"]["artifact_type"] == "stage3_real_validation_plan"
    assert payload["summary_stats"]["stage3_real_validation_plan"]["overall_status"] == "step2_tail_in_progress"
    assert payload["summary_stats"]["stage3_real_validation_plan"]["ready_for_engineering_isolation"] is False
    assert payload["summary_stats"]["stage3_real_validation_plan"]["real_acceptance_ready"] is False
    assert payload["summary_stats"]["stage3_real_validation_plan_digest"]["overall_status"] == (
        "step2_tail_in_progress"
    )
    assert payload["summary_stats"]["stage3_real_validation_plan_digest"]["recommended_next_stage"] == (
        "close_step2_tail_gaps"
    )
    assert payload["summary_stats"]["stage3_real_validation_plan_digest"]["artifact_paths"][
        "engineering_isolation_admission_checklist"
    ] == str(run_dir / ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_FILENAME)
    assert payload["summary_stats"]["stage3_real_validation_plan_digest"]["validation_status_counts"][
        "blocked_until_stage3"
    ] >= 4
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
    assert payload["manifest_sections"]["stage_admission_review_pack"]["artifact_type"] == "stage_admission_review_pack"
    assert payload["manifest_sections"]["stage_admission_review_pack"]["path"] == str(
        run_dir / STAGE_ADMISSION_REVIEW_PACK_FILENAME
    )
    assert payload["manifest_sections"]["stage_admission_review_pack"]["reviewer_path"] == str(
        run_dir / STAGE_ADMISSION_REVIEW_PACK_REVIEWER_FILENAME
    )
    assert payload["manifest_sections"]["stage_admission_review_pack"]["artifact_paths"] == (
        expected_stage_admission_review_pack["raw"]["artifact_paths"]
    )
    assert payload["manifest_sections"]["stage_admission_review_pack"]["ready_for_engineering_isolation"] is False
    assert payload["manifest_sections"]["stage_admission_review_pack"]["real_acceptance_ready"] is False
    assert payload["manifest_sections"]["stage_admission_review_pack_reviewer_artifact"]["artifact_type"] == (
        "stage_admission_review_pack_reviewer_artifact"
    )
    assert payload["manifest_sections"]["stage_admission_review_pack_reviewer_artifact"]["path"] == str(
        run_dir / STAGE_ADMISSION_REVIEW_PACK_REVIEWER_FILENAME
    )
    assert payload["manifest_sections"]["stage_admission_review_pack_reviewer_artifact"]["summary_text"] == (
        expected_stage_admission_review_pack["display"]["summary_text"]
    )
    assert payload["manifest_sections"]["stage_admission_review_pack_reviewer_artifact"]["execute_now_text"] == (
        expected_stage_admission_review_pack["display"]["execute_now_text"]
    )
    assert payload["manifest_sections"]["stage_admission_review_pack_reviewer_artifact"]["defer_to_stage3_text"] == (
        expected_stage_admission_review_pack["display"]["defer_to_stage3_text"]
    )
    assert payload["manifest_sections"]["engineering_isolation_admission_checklist"]["artifact_type"] == (
        "engineering_isolation_admission_checklist"
    )
    assert payload["manifest_sections"]["engineering_isolation_admission_checklist"]["path"] == str(
        run_dir / ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_FILENAME
    )
    assert payload["manifest_sections"]["engineering_isolation_admission_checklist"]["reviewer_path"] == str(
        run_dir / ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_REVIEWER_FILENAME
    )
    assert payload["manifest_sections"]["engineering_isolation_admission_checklist"]["artifact_paths"] == (
        expected_engineering_isolation_admission_checklist["raw"]["artifact_paths"]
    )
    assert payload["manifest_sections"]["engineering_isolation_admission_checklist"]["checklist_status_counts"] == (
        expected_engineering_isolation_admission_checklist["raw"]["checklist_status_counts"]
    )
    assert payload["manifest_sections"]["engineering_isolation_admission_checklist"]["real_acceptance_ready"] is False
    assert payload["manifest_sections"]["engineering_isolation_admission_checklist_reviewer_artifact"]["artifact_type"] == (
        "engineering_isolation_admission_checklist_reviewer_artifact"
    )
    assert payload["manifest_sections"]["engineering_isolation_admission_checklist_reviewer_artifact"]["path"] == str(
        run_dir / ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_REVIEWER_FILENAME
    )
    assert payload["manifest_sections"]["engineering_isolation_admission_checklist_reviewer_artifact"]["summary_text"] == (
        expected_engineering_isolation_admission_checklist["display"]["summary_text"]
    )
    assert payload["manifest_sections"]["engineering_isolation_admission_checklist_reviewer_artifact"][
        "execute_now_text"
    ] == expected_engineering_isolation_admission_checklist["display"]["execute_now_text"]
    assert payload["manifest_sections"]["engineering_isolation_admission_checklist_reviewer_artifact"][
        "defer_to_stage3_text"
    ] == expected_engineering_isolation_admission_checklist["display"]["defer_to_stage3_text"]
    assert payload["manifest_sections"]["stage3_real_validation_plan"]["artifact_type"] == (
        "stage3_real_validation_plan"
    )
    assert payload["manifest_sections"]["stage3_real_validation_plan"]["path"] == str(
        run_dir / STAGE3_REAL_VALIDATION_PLAN_FILENAME
    )
    assert payload["manifest_sections"]["stage3_real_validation_plan"]["reviewer_path"] == str(
        run_dir / STAGE3_REAL_VALIDATION_PLAN_REVIEWER_FILENAME
    )
    assert payload["manifest_sections"]["stage3_real_validation_plan"]["artifact_paths"] == (
        expected_stage3_real_validation_plan["raw"]["artifact_paths"]
    )
    assert payload["manifest_sections"]["stage3_real_validation_plan"]["validation_status_counts"] == (
        expected_stage3_real_validation_plan["raw"]["validation_status_counts"]
    )
    assert payload["manifest_sections"]["stage3_real_validation_plan"]["required_real_world_evidence"] == (
        expected_stage3_real_validation_plan["raw"]["required_real_world_evidence"]
    )
    assert payload["manifest_sections"]["stage3_real_validation_plan_reviewer_artifact"]["artifact_type"] == (
        "stage3_real_validation_plan_reviewer_artifact"
    )
    assert payload["manifest_sections"]["stage3_real_validation_plan_reviewer_artifact"]["path"] == str(
        run_dir / STAGE3_REAL_VALIDATION_PLAN_REVIEWER_FILENAME
    )
    assert payload["manifest_sections"]["stage3_real_validation_plan_reviewer_artifact"]["summary_text"] == (
        expected_stage3_real_validation_plan["display"]["summary_text"]
    )
    assert payload["manifest_sections"]["stage3_real_validation_plan_reviewer_artifact"]["execute_now_text"] == (
        expected_stage3_real_validation_plan["display"]["execute_now_text"]
    )
    assert payload["manifest_sections"]["stage3_real_validation_plan_reviewer_artifact"]["defer_to_stage3_text"] == (
        expected_stage3_real_validation_plan["display"]["defer_to_stage3_text"]
    )
    assert payload["manifest_sections"]["stage3_real_validation_plan_reviewer_artifact"]["plan_boundary_text"] == (
        expected_stage3_real_validation_plan["display"]["plan_boundary_text"]
    )
    assert expected_bridge_reviewer_entry["summary_text"] == expected_bridge_reviewer_artifact["display"]["summary_text"]
    assert expected_bridge_reviewer_entry["status_line"] == expected_bridge_reviewer_artifact["display"]["status_line"]
    assert expected_bridge_reviewer_entry["stage_marker_text"] == expected_bridge_reviewer_artifact["display"]["current_stage_text"]
    assert "Step 2 tail / Stage 3 bridge" in expected_bridge_reviewer_entry["entry_text"]
    assert "engineering-isolation" in expected_bridge_reviewer_entry["entry_text"]
    assert "不是 real acceptance" in expected_bridge_reviewer_entry["entry_text"]
    assert "不能替代真实计量验证" in expected_bridge_reviewer_entry["entry_text"]
    assert "ready_for_engineering_isolation" not in expected_bridge_reviewer_entry["entry_text"]
    assert "real_acceptance_ready" not in expected_bridge_reviewer_entry["entry_text"]
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
    assert stage_admission_review_pack["artifact_type"] == "stage_admission_review_pack"
    assert stage_admission_review_pack["artifact_refs"] == expected_stage_admission_review_pack["raw"]["artifact_refs"]
    assert stage_admission_review_pack["artifact_paths"] == expected_stage_admission_review_pack["raw"]["artifact_paths"]
    assert stage_admission_review_pack["execute_now_in_step2_tail"] == phase_transition_bridge["execute_now_in_step2_tail"]
    assert stage_admission_review_pack["defer_to_stage3_real_validation"] == (
        phase_transition_bridge["defer_to_stage3_real_validation"]
    )
    assert stage_admission_review_pack["missing_real_world_evidence"] == phase_transition_bridge["missing_real_world_evidence"]
    assert stage_admission_review_pack["handoff_checklist"]["stage3_prerequisites"] == (
        phase_transition_bridge["missing_real_world_evidence"]
    )
    assert stage_admission_review_pack_markdown == expected_stage_admission_review_pack["markdown"]
    assert "Step 2 tail / Stage 3 bridge" in stage_admission_review_pack_markdown
    assert "engineering-isolation" in stage_admission_review_pack_markdown
    assert "当前执行" in stage_admission_review_pack_markdown
    assert "第三阶段执行" in stage_admission_review_pack_markdown
    assert "不是 real acceptance" in stage_admission_review_pack_markdown
    assert "不能替代真实计量验证" in stage_admission_review_pack_markdown
    assert "step2_readiness_summary.json" in stage_admission_review_pack_markdown
    assert "metrology_calibration_contract.json" in stage_admission_review_pack_markdown
    assert "phase_transition_bridge.json" in stage_admission_review_pack_markdown
    assert "phase_transition_bridge_reviewer.md" in stage_admission_review_pack_markdown
    assert "ready_for_engineering_isolation" not in stage_admission_review_pack_markdown
    assert "real_acceptance_ready" not in stage_admission_review_pack_markdown
    assert engineering_isolation_admission_checklist["artifact_type"] == "engineering_isolation_admission_checklist"
    assert engineering_isolation_admission_checklist["artifact_refs"] == (
        expected_engineering_isolation_admission_checklist["raw"]["artifact_refs"]
    )
    assert engineering_isolation_admission_checklist["artifact_paths"] == (
        expected_engineering_isolation_admission_checklist["raw"]["artifact_paths"]
    )
    assert engineering_isolation_admission_checklist["checklist_status_counts"] == (
        expected_engineering_isolation_admission_checklist["raw"]["checklist_status_counts"]
    )
    assert engineering_isolation_admission_checklist["missing_real_world_evidence"] == (
        phase_transition_bridge["missing_real_world_evidence"]
    )
    assert engineering_isolation_admission_checklist_markdown == (
        expected_engineering_isolation_admission_checklist["markdown"]
    )
    assert "Step 2 tail / Stage 3 bridge" in engineering_isolation_admission_checklist_markdown
    assert "engineering-isolation" in engineering_isolation_admission_checklist_markdown
    assert "当前执行" in engineering_isolation_admission_checklist_markdown
    assert "第三阶段执行" in engineering_isolation_admission_checklist_markdown
    assert "不是 real acceptance" in engineering_isolation_admission_checklist_markdown
    assert "不能替代真实计量验证" in engineering_isolation_admission_checklist_markdown
    assert "stage_admission_review_pack.json" in engineering_isolation_admission_checklist_markdown
    assert "stage_admission_review_pack.md" in engineering_isolation_admission_checklist_markdown
    assert "ready_for_engineering_isolation" not in engineering_isolation_admission_checklist_markdown
    assert "real_acceptance_ready" not in engineering_isolation_admission_checklist_markdown
    assert stage3_real_validation_plan["artifact_type"] == "stage3_real_validation_plan"
    assert stage3_real_validation_plan["artifact_refs"] == expected_stage3_real_validation_plan["raw"]["artifact_refs"]
    assert stage3_real_validation_plan["artifact_paths"] == expected_stage3_real_validation_plan["raw"]["artifact_paths"]
    assert stage3_real_validation_plan["validation_status_counts"] == (
        expected_stage3_real_validation_plan["raw"]["validation_status_counts"]
    )
    assert stage3_real_validation_plan["required_real_world_evidence"] == (
        expected_stage3_real_validation_plan["raw"]["required_real_world_evidence"]
    )
    assert stage3_real_validation_plan["pass_fail_contract"] == expected_stage3_real_validation_plan["raw"][
        "pass_fail_contract"
    ]
    assert stage3_real_validation_plan_markdown == expected_stage3_real_validation_plan["markdown"]
    assert "Step 2 tail / Stage 3 bridge" in stage3_real_validation_plan_markdown
    assert "engineering-isolation" in stage3_real_validation_plan_markdown
    assert "第三阶段真实验证" in stage3_real_validation_plan_markdown
    assert "不是 real acceptance" in stage3_real_validation_plan_markdown
    assert "不能替代真实计量验证" in stage3_real_validation_plan_markdown
    assert "本工件只定义第三阶段真实验证计划，不代表验证已完成" in stage3_real_validation_plan_markdown
    assert "step2_readiness_summary.json" in stage3_real_validation_plan_markdown
    assert "metrology_calibration_contract.json" in stage3_real_validation_plan_markdown
    assert "phase_transition_bridge.json" in stage3_real_validation_plan_markdown
    assert "stage_admission_review_pack.json" in stage3_real_validation_plan_markdown
    assert "engineering_isolation_admission_checklist.json" in stage3_real_validation_plan_markdown
    assert "ready_for_engineering_isolation" not in stage3_real_validation_plan_markdown
    assert "real_acceptance_ready" not in stage3_real_validation_plan_markdown
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
    assert payload["artifact_statuses"]["stage_admission_review_pack"]["role"] == "execution_summary"
    assert payload["artifact_statuses"]["stage_admission_review_pack"]["path"] == str(
        run_dir / STAGE_ADMISSION_REVIEW_PACK_FILENAME
    )
    assert payload["artifact_statuses"]["stage_admission_review_pack_reviewer_artifact"]["role"] == "formal_analysis"
    assert payload["artifact_statuses"]["stage_admission_review_pack_reviewer_artifact"]["path"] == str(
        run_dir / STAGE_ADMISSION_REVIEW_PACK_REVIEWER_FILENAME
    )
    assert payload["artifact_statuses"]["engineering_isolation_admission_checklist"]["role"] == "execution_summary"
    assert payload["artifact_statuses"]["engineering_isolation_admission_checklist"]["path"] == str(
        run_dir / ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_FILENAME
    )
    assert payload["artifact_statuses"]["engineering_isolation_admission_checklist_reviewer_artifact"]["role"] == (
        "formal_analysis"
    )
    assert payload["artifact_statuses"]["engineering_isolation_admission_checklist_reviewer_artifact"]["path"] == str(
        run_dir / ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_REVIEWER_FILENAME
    )
    assert payload["artifact_statuses"]["stage3_real_validation_plan"]["role"] == "execution_summary"
    assert payload["artifact_statuses"]["stage3_real_validation_plan"]["path"] == str(
        run_dir / STAGE3_REAL_VALIDATION_PLAN_FILENAME
    )
    assert payload["artifact_statuses"]["stage3_real_validation_plan_reviewer_artifact"]["role"] == (
        "formal_analysis"
    )
    assert payload["artifact_statuses"]["stage3_real_validation_plan_reviewer_artifact"]["path"] == str(
        run_dir / STAGE3_REAL_VALIDATION_PLAN_REVIEWER_FILENAME
    )
    assert str(run_dir / "step2_readiness_summary.json") in payload["remembered_files"]
    assert str(run_dir / "metrology_calibration_contract.json") in payload["remembered_files"]
    assert str(run_dir / "phase_transition_bridge.json") in payload["remembered_files"]
    assert str(run_dir / "phase_transition_bridge_reviewer.md") in payload["remembered_files"]
    assert str(run_dir / STAGE_ADMISSION_REVIEW_PACK_FILENAME) in payload["remembered_files"]
    assert str(run_dir / STAGE_ADMISSION_REVIEW_PACK_REVIEWER_FILENAME) in payload["remembered_files"]
    assert str(run_dir / ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_FILENAME) in payload["remembered_files"]
    assert str(run_dir / ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_REVIEWER_FILENAME) in payload["remembered_files"]
    assert str(run_dir / STAGE3_REAL_VALIDATION_PLAN_FILENAME) in payload["remembered_files"]
    assert str(run_dir / STAGE3_REAL_VALIDATION_PLAN_REVIEWER_FILENAME) in payload["remembered_files"]
    assert summary_after_rebuild["stats"]["artifact_exports"]["stage_admission_review_pack"]["role"] == "execution_summary"
    assert summary_after_rebuild["stats"]["artifact_exports"]["stage_admission_review_pack_reviewer_artifact"]["role"] == (
        "formal_analysis"
    )
    assert summary_after_rebuild["stats"]["artifact_exports"]["engineering_isolation_admission_checklist"]["role"] == (
        "execution_summary"
    )
    assert summary_after_rebuild["stats"]["artifact_exports"][
        "engineering_isolation_admission_checklist_reviewer_artifact"
    ]["role"] == "formal_analysis"
    assert summary_after_rebuild["stats"]["artifact_exports"]["stage3_real_validation_plan"]["role"] == (
        "execution_summary"
    )
    assert summary_after_rebuild["stats"]["artifact_exports"]["stage3_real_validation_plan_reviewer_artifact"][
        "role"
    ] == "formal_analysis"
    assert str(run_dir / STAGE_ADMISSION_REVIEW_PACK_FILENAME) in summary_after_rebuild["stats"]["output_files"]
    assert str(run_dir / STAGE_ADMISSION_REVIEW_PACK_REVIEWER_FILENAME) in summary_after_rebuild["stats"]["output_files"]
    assert str(run_dir / ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_FILENAME) in summary_after_rebuild["stats"]["output_files"]
    assert str(run_dir / ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_REVIEWER_FILENAME) in summary_after_rebuild["stats"]["output_files"]
    assert str(run_dir / STAGE3_REAL_VALIDATION_PLAN_FILENAME) in summary_after_rebuild["stats"]["output_files"]
    assert str(run_dir / STAGE3_REAL_VALIDATION_PLAN_REVIEWER_FILENAME) in summary_after_rebuild["stats"]["output_files"]
    assert manifest_after_rebuild["stage_admission_review_pack"]["artifact_paths"] == (
        expected_stage_admission_review_pack["raw"]["artifact_paths"]
    )
    assert manifest_after_rebuild["stage_admission_review_pack_reviewer_artifact"]["path"] == str(
        run_dir / STAGE_ADMISSION_REVIEW_PACK_REVIEWER_FILENAME
    )
    assert manifest_after_rebuild["engineering_isolation_admission_checklist"]["artifact_paths"] == (
        expected_engineering_isolation_admission_checklist["raw"]["artifact_paths"]
    )
    assert manifest_after_rebuild["engineering_isolation_admission_checklist_reviewer_artifact"]["path"] == str(
        run_dir / ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_REVIEWER_FILENAME
    )
    assert manifest_after_rebuild["stage3_real_validation_plan"]["artifact_paths"] == (
        expected_stage3_real_validation_plan["raw"]["artifact_paths"]
    )
    assert manifest_after_rebuild["stage3_real_validation_plan_reviewer_artifact"]["path"] == str(
        run_dir / STAGE3_REAL_VALIDATION_PLAN_REVIEWER_FILENAME
    )


def test_rebuild_run_generates_stage3_standards_alignment_matrix_artifacts(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)
    run_dir = Path(facade.result_store.run_dir)

    payload = rebuild_run(run_dir)
    summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    matrix_json = json.loads((run_dir / STAGE3_STANDARDS_ALIGNMENT_MATRIX_FILENAME).read_text(encoding="utf-8"))
    matrix_markdown = (run_dir / STAGE3_STANDARDS_ALIGNMENT_MATRIX_REVIEWER_FILENAME).read_text(
        encoding="utf-8"
    )

    assert (run_dir / STAGE3_STANDARDS_ALIGNMENT_MATRIX_FILENAME).exists()
    assert (run_dir / STAGE3_STANDARDS_ALIGNMENT_MATRIX_REVIEWER_FILENAME).exists()
    assert payload["summary_stats"]["stage3_standards_alignment_matrix"]["artifact_type"] == (
        "stage3_standards_alignment_matrix"
    )
    assert payload["summary_stats"]["stage3_standards_alignment_matrix_digest"]["mapping_scope"] == (
        "family_topic_level_only"
    )
    assert payload["summary_stats"]["stage3_standards_alignment_matrix_digest"]["mapping_row_count"] == 9
    assert payload["artifact_statuses"]["stage3_standards_alignment_matrix"]["role"] == "execution_summary"
    assert payload["artifact_statuses"]["stage3_standards_alignment_matrix_reviewer_artifact"]["role"] == (
        "formal_analysis"
    )
    assert payload["manifest_sections"]["stage3_standards_alignment_matrix"]["path"] == str(
        run_dir / STAGE3_STANDARDS_ALIGNMENT_MATRIX_FILENAME
    )
    assert payload["manifest_sections"]["stage3_standards_alignment_matrix"]["reviewer_path"] == str(
        run_dir / STAGE3_STANDARDS_ALIGNMENT_MATRIX_REVIEWER_FILENAME
    )
    assert payload["manifest_sections"]["stage3_standards_alignment_matrix_reviewer_artifact"]["path"] == str(
        run_dir / STAGE3_STANDARDS_ALIGNMENT_MATRIX_REVIEWER_FILENAME
    )
    assert str(run_dir / STAGE3_STANDARDS_ALIGNMENT_MATRIX_FILENAME) in payload["remembered_files"]
    assert str(run_dir / STAGE3_STANDARDS_ALIGNMENT_MATRIX_REVIEWER_FILENAME) in payload["remembered_files"]
    assert summary["stats"]["artifact_exports"]["stage3_standards_alignment_matrix"]["role"] == (
        "execution_summary"
    )
    assert summary["stats"]["artifact_exports"]["stage3_standards_alignment_matrix_reviewer_artifact"]["role"] == (
        "formal_analysis"
    )
    assert str(run_dir / STAGE3_STANDARDS_ALIGNMENT_MATRIX_FILENAME) in summary["stats"]["output_files"]
    assert str(run_dir / STAGE3_STANDARDS_ALIGNMENT_MATRIX_REVIEWER_FILENAME) in summary["stats"]["output_files"]
    assert manifest["stage3_standards_alignment_matrix"]["path"] == str(
        run_dir / STAGE3_STANDARDS_ALIGNMENT_MATRIX_FILENAME
    )
    assert manifest["stage3_standards_alignment_matrix"]["reviewer_path"] == str(
        run_dir / STAGE3_STANDARDS_ALIGNMENT_MATRIX_REVIEWER_FILENAME
    )
    assert manifest["stage3_standards_alignment_matrix_reviewer_artifact"]["path"] == str(
        run_dir / STAGE3_STANDARDS_ALIGNMENT_MATRIX_REVIEWER_FILENAME
    )
    assert matrix_json["mapping_scope"] == "family_topic_level_only"
    assert matrix_json["standard_families"] == [
        "中国气象局 / 气象行业观测与质量控制相关要求",
        "CNAS-CL01",
        "CNAS-CL01-G002",
        "CNAS-CL01-G003",
        "ISO/IEC 17025",
        "ISO 6142 family",
        "ISO 6143",
        "ISO 6145 family",
        "WMO / GAW QA",
    ]
    assert len(matrix_json["rows"]) == 9
    assert all(row["mapping_level"] == "family_topic_level_only" for row in matrix_json["rows"])
    assert all("clause_number" not in row and "clause_id" not in row for row in matrix_json["rows"])
    assert "Step 2 tail / Stage 3 bridge" in matrix_markdown
    assert "readiness mapping only" in matrix_markdown
    assert "not accreditation claim" in matrix_markdown
    assert "not compliance certification" in matrix_markdown
    assert "not real acceptance" in matrix_markdown
    assert "cannot replace real metrology validation" in matrix_markdown
    assert "simulation / offline / headless only" in matrix_markdown
    assert "stage3_real_validation_plan.json" in matrix_markdown
    assert "ready_for_engineering_isolation" not in matrix_markdown
    assert "real_acceptance_ready" not in matrix_markdown


def test_rebuild_run_generates_scope_package_and_decision_rule_contracts(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)
    run_dir = Path(facade.result_store.run_dir)

    rebuild_run(run_dir)

    scope_payload = json.loads(
        (run_dir / recognition_readiness.SCOPE_DEFINITION_PACK_FILENAME).read_text(encoding="utf-8")
    )
    decision_payload = json.loads(
        (run_dir / recognition_readiness.DECISION_RULE_PROFILE_FILENAME).read_text(encoding="utf-8")
    )

    assert scope_payload["scope_id"]
    assert scope_payload["scope_name"]
    assert scope_payload["scope_version"]
    assert scope_payload["scope_export_pack"]["ready_for_readiness_mapping"] is True
    assert scope_payload["scope_export_pack"]["not_ready_for_formal_claim"] is True
    assert scope_payload["standard_family"]
    assert scope_payload["required_evidence_categories"]
    assert scope_payload["evidence_source"] == "simulated_protocol"
    assert scope_payload["not_real_acceptance_evidence"] is True
    assert scope_payload["non_claim_note"]

    assert decision_payload["decision_rule_id"]
    assert decision_payload["source_standard_or_method"]
    assert decision_payload["acceptance_limit"]["mode"] == "readiness_mapping_only"
    assert decision_payload["reviewer_gate"]["mode"] == "reviewer_digest_only"
    assert "formal_compliance_claim" in decision_payload["reviewer_gate"]["deny_outputs"]
    assert decision_payload["acceptance_contract"]["repository_mode"] == "file_artifact_first"
    assert decision_payload["acceptance_contract"]["non_primary_evidence_chain"] is True
    assert decision_payload["statement_template"]
    assert decision_payload["not_real_acceptance_evidence"] is True
    assert decision_payload["non_claim_note"]


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


def test_rebuild_run_generates_recognition_readiness_artifacts(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)
    run_dir = Path(facade.result_store.run_dir)

    payload = rebuild_run(run_dir)

    expected_filenames = (
        recognition_readiness.SCOPE_DEFINITION_PACK_FILENAME,
        recognition_readiness.SCOPE_DEFINITION_PACK_MARKDOWN_FILENAME,
        recognition_readiness.DECISION_RULE_PROFILE_FILENAME,
        recognition_readiness.DECISION_RULE_PROFILE_MARKDOWN_FILENAME,
        recognition_readiness.SCOPE_READINESS_SUMMARY_FILENAME,
        recognition_readiness.SCOPE_READINESS_SUMMARY_MARKDOWN_FILENAME,
        recognition_readiness.REFERENCE_ASSET_REGISTRY_FILENAME,
        recognition_readiness.REFERENCE_ASSET_REGISTRY_MARKDOWN_FILENAME,
        recognition_readiness.CERTIFICATE_LIFECYCLE_SUMMARY_FILENAME,
        recognition_readiness.CERTIFICATE_LIFECYCLE_SUMMARY_MARKDOWN_FILENAME,
        recognition_readiness.CERTIFICATE_READINESS_SUMMARY_FILENAME,
        recognition_readiness.CERTIFICATE_READINESS_SUMMARY_MARKDOWN_FILENAME,
        recognition_readiness.PRE_RUN_READINESS_GATE_FILENAME,
        recognition_readiness.PRE_RUN_READINESS_GATE_MARKDOWN_FILENAME,
        recognition_readiness.METROLOGY_TRACEABILITY_STUB_FILENAME,
        recognition_readiness.METROLOGY_TRACEABILITY_STUB_MARKDOWN_FILENAME,
        recognition_readiness.UNCERTAINTY_BUDGET_STUB_FILENAME,
        recognition_readiness.UNCERTAINTY_BUDGET_STUB_MARKDOWN_FILENAME,
        recognition_readiness.UNCERTAINTY_MODEL_FILENAME,
        recognition_readiness.UNCERTAINTY_MODEL_MARKDOWN_FILENAME,
        recognition_readiness.UNCERTAINTY_INPUT_SET_FILENAME,
        recognition_readiness.UNCERTAINTY_INPUT_SET_MARKDOWN_FILENAME,
        recognition_readiness.SENSITIVITY_COEFFICIENT_SET_FILENAME,
        recognition_readiness.SENSITIVITY_COEFFICIENT_SET_MARKDOWN_FILENAME,
        recognition_readiness.BUDGET_CASE_FILENAME,
        recognition_readiness.BUDGET_CASE_MARKDOWN_FILENAME,
        recognition_readiness.UNCERTAINTY_GOLDEN_CASES_FILENAME,
        recognition_readiness.UNCERTAINTY_GOLDEN_CASES_MARKDOWN_FILENAME,
        recognition_readiness.UNCERTAINTY_REPORT_PACK_FILENAME,
        recognition_readiness.UNCERTAINTY_REPORT_PACK_MARKDOWN_FILENAME,
        recognition_readiness.UNCERTAINTY_DIGEST_FILENAME,
        recognition_readiness.UNCERTAINTY_DIGEST_MARKDOWN_FILENAME,
        recognition_readiness.UNCERTAINTY_ROLLUP_FILENAME,
        recognition_readiness.UNCERTAINTY_ROLLUP_MARKDOWN_FILENAME,
        recognition_readiness.METHOD_CONFIRMATION_PROTOCOL_FILENAME,
        recognition_readiness.METHOD_CONFIRMATION_PROTOCOL_MARKDOWN_FILENAME,
        recognition_readiness.METHOD_CONFIRMATION_MATRIX_FILENAME,
        recognition_readiness.METHOD_CONFIRMATION_MATRIX_MARKDOWN_FILENAME,
        recognition_readiness.ROUTE_SPECIFIC_VALIDATION_MATRIX_FILENAME,
        recognition_readiness.ROUTE_SPECIFIC_VALIDATION_MATRIX_MARKDOWN_FILENAME,
        recognition_readiness.VALIDATION_RUN_SET_FILENAME,
        recognition_readiness.VALIDATION_RUN_SET_MARKDOWN_FILENAME,
        recognition_readiness.VERIFICATION_DIGEST_FILENAME,
        recognition_readiness.VERIFICATION_DIGEST_MARKDOWN_FILENAME,
        recognition_readiness.VERIFICATION_ROLLUP_FILENAME,
        recognition_readiness.VERIFICATION_ROLLUP_MARKDOWN_FILENAME,
        recognition_readiness.UNCERTAINTY_METHOD_READINESS_SUMMARY_FILENAME,
        recognition_readiness.UNCERTAINTY_METHOD_READINESS_SUMMARY_MARKDOWN_FILENAME,
        recognition_readiness.SOFTWARE_VALIDATION_TRACEABILITY_MATRIX_FILENAME,
        recognition_readiness.SOFTWARE_VALIDATION_TRACEABILITY_MATRIX_MARKDOWN_FILENAME,
        recognition_readiness.REQUIREMENT_DESIGN_CODE_TEST_LINKS_FILENAME,
        recognition_readiness.REQUIREMENT_DESIGN_CODE_TEST_LINKS_MARKDOWN_FILENAME,
        recognition_readiness.VALIDATION_EVIDENCE_INDEX_FILENAME,
        recognition_readiness.VALIDATION_EVIDENCE_INDEX_MARKDOWN_FILENAME,
        recognition_readiness.CHANGE_IMPACT_SUMMARY_FILENAME,
        recognition_readiness.CHANGE_IMPACT_SUMMARY_MARKDOWN_FILENAME,
        recognition_readiness.ROLLBACK_READINESS_SUMMARY_FILENAME,
        recognition_readiness.ROLLBACK_READINESS_SUMMARY_MARKDOWN_FILENAME,
        recognition_readiness.ARTIFACT_HASH_REGISTRY_FILENAME,
        recognition_readiness.ARTIFACT_HASH_REGISTRY_MARKDOWN_FILENAME,
        recognition_readiness.AUDIT_EVENT_STORE_FILENAME,
        recognition_readiness.AUDIT_EVENT_STORE_MARKDOWN_FILENAME,
        recognition_readiness.ENVIRONMENT_FINGERPRINT_FILENAME,
        recognition_readiness.ENVIRONMENT_FINGERPRINT_MARKDOWN_FILENAME,
        recognition_readiness.CONFIG_FINGERPRINT_FILENAME,
        recognition_readiness.CONFIG_FINGERPRINT_MARKDOWN_FILENAME,
        recognition_readiness.RELEASE_INPUT_DIGEST_FILENAME,
        recognition_readiness.RELEASE_INPUT_DIGEST_MARKDOWN_FILENAME,
        recognition_readiness.RELEASE_MANIFEST_FILENAME,
        recognition_readiness.RELEASE_MANIFEST_MARKDOWN_FILENAME,
        recognition_readiness.RELEASE_SCOPE_SUMMARY_FILENAME,
        recognition_readiness.RELEASE_SCOPE_SUMMARY_MARKDOWN_FILENAME,
        recognition_readiness.RELEASE_BOUNDARY_DIGEST_FILENAME,
        recognition_readiness.RELEASE_BOUNDARY_DIGEST_MARKDOWN_FILENAME,
        recognition_readiness.RELEASE_EVIDENCE_PACK_INDEX_FILENAME,
        recognition_readiness.RELEASE_EVIDENCE_PACK_INDEX_MARKDOWN_FILENAME,
        recognition_readiness.RELEASE_VALIDATION_MANIFEST_FILENAME,
        recognition_readiness.RELEASE_VALIDATION_MANIFEST_MARKDOWN_FILENAME,
        recognition_readiness.AUDIT_READINESS_DIGEST_FILENAME,
        recognition_readiness.AUDIT_READINESS_DIGEST_MARKDOWN_FILENAME,
    )

    for filename in expected_filenames:
        assert (run_dir / filename).exists(), filename

    scope_pack = json.loads((run_dir / recognition_readiness.SCOPE_DEFINITION_PACK_FILENAME).read_text(encoding="utf-8"))
    decision_rule = json.loads((run_dir / recognition_readiness.DECISION_RULE_PROFILE_FILENAME).read_text(encoding="utf-8"))
    scope_summary = json.loads(
        (run_dir / recognition_readiness.SCOPE_READINESS_SUMMARY_FILENAME).read_text(encoding="utf-8")
    )
    reference_registry = json.loads(
        (run_dir / recognition_readiness.REFERENCE_ASSET_REGISTRY_FILENAME).read_text(encoding="utf-8")
    )
    certificate_lifecycle = json.loads(
        (run_dir / recognition_readiness.CERTIFICATE_LIFECYCLE_SUMMARY_FILENAME).read_text(encoding="utf-8")
    )
    certificate_summary = json.loads(
        (run_dir / recognition_readiness.CERTIFICATE_READINESS_SUMMARY_FILENAME).read_text(encoding="utf-8")
    )
    pre_run_gate = json.loads(
        (run_dir / recognition_readiness.PRE_RUN_READINESS_GATE_FILENAME).read_text(encoding="utf-8")
    )
    traceability_stub = json.loads(
        (run_dir / recognition_readiness.METROLOGY_TRACEABILITY_STUB_FILENAME).read_text(encoding="utf-8")
    )
    uncertainty_stub = json.loads(
        (run_dir / recognition_readiness.UNCERTAINTY_BUDGET_STUB_FILENAME).read_text(encoding="utf-8")
    )
    uncertainty_model = json.loads(
        (run_dir / recognition_readiness.UNCERTAINTY_MODEL_FILENAME).read_text(encoding="utf-8")
    )
    uncertainty_input_set = json.loads(
        (run_dir / recognition_readiness.UNCERTAINTY_INPUT_SET_FILENAME).read_text(encoding="utf-8")
    )
    sensitivity_coefficient_set = json.loads(
        (run_dir / recognition_readiness.SENSITIVITY_COEFFICIENT_SET_FILENAME).read_text(encoding="utf-8")
    )
    budget_case = json.loads((run_dir / recognition_readiness.BUDGET_CASE_FILENAME).read_text(encoding="utf-8"))
    uncertainty_golden_cases = json.loads(
        (run_dir / recognition_readiness.UNCERTAINTY_GOLDEN_CASES_FILENAME).read_text(encoding="utf-8")
    )
    uncertainty_report_pack = json.loads(
        (run_dir / recognition_readiness.UNCERTAINTY_REPORT_PACK_FILENAME).read_text(encoding="utf-8")
    )
    uncertainty_digest = json.loads(
        (run_dir / recognition_readiness.UNCERTAINTY_DIGEST_FILENAME).read_text(encoding="utf-8")
    )
    uncertainty_rollup = json.loads(
        (run_dir / recognition_readiness.UNCERTAINTY_ROLLUP_FILENAME).read_text(encoding="utf-8")
    )
    method_matrix = json.loads(
        (run_dir / recognition_readiness.METHOD_CONFIRMATION_MATRIX_FILENAME).read_text(encoding="utf-8")
    )
    route_validation_matrix = json.loads(
        (run_dir / recognition_readiness.ROUTE_SPECIFIC_VALIDATION_MATRIX_FILENAME).read_text(encoding="utf-8")
    )
    validation_run_set = json.loads(
        (run_dir / recognition_readiness.VALIDATION_RUN_SET_FILENAME).read_text(encoding="utf-8")
    )
    verification_digest = json.loads(
        (run_dir / recognition_readiness.VERIFICATION_DIGEST_FILENAME).read_text(encoding="utf-8")
    )
    verification_rollup = json.loads(
        (run_dir / recognition_readiness.VERIFICATION_ROLLUP_FILENAME).read_text(encoding="utf-8")
    )
    uncertainty_summary = json.loads(
        (run_dir / recognition_readiness.UNCERTAINTY_METHOD_READINESS_SUMMARY_FILENAME).read_text(encoding="utf-8")
    )
    software_matrix = json.loads(
        (run_dir / recognition_readiness.SOFTWARE_VALIDATION_TRACEABILITY_MATRIX_FILENAME).read_text(encoding="utf-8")
    )
    requirement_design_code_test_links = json.loads(
        (run_dir / recognition_readiness.REQUIREMENT_DESIGN_CODE_TEST_LINKS_FILENAME).read_text(encoding="utf-8")
    )
    validation_evidence_index = json.loads(
        (run_dir / recognition_readiness.VALIDATION_EVIDENCE_INDEX_FILENAME).read_text(encoding="utf-8")
    )
    change_impact_summary = json.loads(
        (run_dir / recognition_readiness.CHANGE_IMPACT_SUMMARY_FILENAME).read_text(encoding="utf-8")
    )
    rollback_readiness_summary = json.loads(
        (run_dir / recognition_readiness.ROLLBACK_READINESS_SUMMARY_FILENAME).read_text(encoding="utf-8")
    )
    artifact_hash_registry = json.loads(
        (run_dir / recognition_readiness.ARTIFACT_HASH_REGISTRY_FILENAME).read_text(encoding="utf-8")
    )
    audit_event_store = json.loads(
        (run_dir / recognition_readiness.AUDIT_EVENT_STORE_FILENAME).read_text(encoding="utf-8")
    )
    environment_fingerprint = json.loads(
        (run_dir / recognition_readiness.ENVIRONMENT_FINGERPRINT_FILENAME).read_text(encoding="utf-8")
    )
    config_fingerprint = json.loads(
        (run_dir / recognition_readiness.CONFIG_FINGERPRINT_FILENAME).read_text(encoding="utf-8")
    )
    release_input_digest = json.loads(
        (run_dir / recognition_readiness.RELEASE_INPUT_DIGEST_FILENAME).read_text(encoding="utf-8")
    )
    release_manifest = json.loads(
        (run_dir / recognition_readiness.RELEASE_MANIFEST_FILENAME).read_text(encoding="utf-8")
    )
    release_scope_summary = json.loads(
        (run_dir / recognition_readiness.RELEASE_SCOPE_SUMMARY_FILENAME).read_text(encoding="utf-8")
    )
    release_boundary_digest = json.loads(
        (run_dir / recognition_readiness.RELEASE_BOUNDARY_DIGEST_FILENAME).read_text(encoding="utf-8")
    )
    release_evidence_pack_index = json.loads(
        (run_dir / recognition_readiness.RELEASE_EVIDENCE_PACK_INDEX_FILENAME).read_text(encoding="utf-8")
    )
    release_validation_manifest = json.loads(
        (run_dir / recognition_readiness.RELEASE_VALIDATION_MANIFEST_FILENAME).read_text(encoding="utf-8")
    )
    audit_digest = json.loads((run_dir / recognition_readiness.AUDIT_READINESS_DIGEST_FILENAME).read_text(encoding="utf-8"))

    assert scope_pack["artifact_type"] == "scope_definition_pack"
    assert scope_pack["not_real_acceptance_evidence"] is True
    assert "not accreditation claim" in scope_pack["boundary_statements"]
    assert decision_rule["artifact_type"] == "decision_rule_profile"
    assert "reviewer decision support" in decision_rule["current_stage_applicability"].lower()
    assert scope_summary["artifact_type"] == "scope_readiness_summary"
    assert scope_summary["review_surface"]["anchor_id"] == "scope-readiness-summary"
    assert "formal scope approval chain is not closed" in scope_summary["missing_evidence"]
    assert "not compliance claim" in scope_summary["boundary_statements"]
    assert scope_pack["anchor_id"] == "scope-definition-pack"
    assert scope_pack["linked_artifact_refs"]
    assert scope_pack["next_required_artifacts"]
    assert scope_pack["boundary_digest"]
    assert any(
        str(item.get("certificate_status") or "").startswith("missing")
        for item in list(reference_registry.get("assets") or [])
    )
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
    assert reference_registry["reviewer_stub_only"] is True
    assert reference_registry["ready_for_readiness_mapping"] is True
    assert reference_registry["not_released_for_formal_claim"] is True
    assert reference_registry["evidence_source"] == "simulated"
    assert reference_registry["not_real_acceptance_evidence"] is True
    assert certificate_lifecycle["artifact_type"] == "certificate_lifecycle_summary"
    assert certificate_lifecycle["reviewer_stub_only"] is True
    assert certificate_lifecycle["readiness_mapping_only"] is True
    assert certificate_lifecycle["not_released_for_formal_claim"] is True
    assert certificate_lifecycle["not_ready_for_formal_claim"] is True
    assert certificate_lifecycle["ready_for_readiness_mapping"] is True
    assert certificate_lifecycle["evidence_source"] == "simulated"
    assert certificate_lifecycle["not_real_acceptance_evidence"] is True
    assert certificate_lifecycle["certificate_rows"]
    assert certificate_lifecycle["lot_bindings"]
    assert certificate_lifecycle["intermediate_check_plans"]
    assert certificate_lifecycle["intermediate_check_records"]
    assert certificate_lifecycle["out_of_tolerance_events"]
    assert pre_run_gate["artifact_type"] == "pre_run_readiness_gate"
    assert pre_run_gate["gate_status"] in {
        "ok_for_reviewer_mapping",
        "warning_reviewer_attention",
        "blocked_for_formal_claim",
    }
    assert pre_run_gate["blocking_items"]
    assert route_validation_matrix["artifact_type"] == "route_specific_validation_matrix"
    assert route_validation_matrix["reviewer_only"] is True
    assert route_validation_matrix["readiness_mapping_only"] is True
    assert route_validation_matrix["not_real_acceptance_evidence"] is True
    assert route_validation_matrix["not_ready_for_formal_claim"] is True
    assert route_validation_matrix["route_specific_validation_matrix"]
    assert validation_run_set["artifact_type"] == "validation_run_set"
    assert validation_run_set["readiness_mapping_only"] is True
    assert validation_run_set["validation_run_set"]
    assert verification_digest["artifact_type"] == "verification_digest"
    assert verification_digest["digest"]["protocol_overview_summary"]
    assert verification_digest["digest"]["matrix_completeness_summary"]
    assert verification_digest["digest"]["current_evidence_coverage_summary"]
    assert verification_digest["digest"]["top_gaps_summary"]
    assert verification_rollup["artifact_type"] == "verification_rollup"
    assert verification_rollup["digest"]["readiness_status_summary"]
    assert verification_rollup["not_real_acceptance_evidence"] is True
    assert verification_rollup["not_ready_for_formal_claim"] is True
    assert verification_rollup["primary_evidence_rewritten"] is False
    assert pre_run_gate["warning_items"]
    assert pre_run_gate["reviewer_actions"]
    assert pre_run_gate["checks"]
    assert pre_run_gate["not_ready_for_formal_claim"] is True
    assert pre_run_gate["ready_for_readiness_mapping"] is True
    assert pre_run_gate["readiness_mapping_only"] is True
    assert pre_run_gate["not_released_for_formal_claim"] is True
    assert pre_run_gate["evidence_source"] == "simulated"
    assert pre_run_gate["not_real_acceptance_evidence"] is True
    assert pre_run_gate["primary_evidence_rewritten"] is False
    assert certificate_summary["artifact_type"] == "certificate_readiness_summary"
    assert "certificate missing" in certificate_summary["digest"]["current_coverage_summary"]
    assert "no released certificate files attached" in certificate_summary["missing_evidence"]
    assert reference_registry["linked_artifact_refs"]
    assert reference_registry["blockers"]
    assert reference_registry["next_required_artifacts"]
    assert uncertainty_stub["artifact_type"] == "uncertainty_budget_stub"
    assert any(
        str(item.get("combined_uncertainty_status") or "") == "placeholder_closed_for_reviewer_pack"
        for item in list(uncertainty_stub.get("rows") or [])
    )
    assert uncertainty_model["artifact_type"] == "uncertainty_model"
    assert uncertainty_model["uncertainty_case_ids"]
    assert uncertainty_input_set["artifact_type"] == "uncertainty_input_set"
    assert uncertainty_input_set["input_quantity_set"]
    assert sensitivity_coefficient_set["artifact_type"] == "sensitivity_coefficient_set"
    assert sensitivity_coefficient_set["sensitivity_coefficients"]
    assert budget_case["artifact_type"] == "budget_case"
    assert len(list(budget_case.get("budget_case") or [])) >= 5
    assert uncertainty_golden_cases["artifact_type"] == "uncertainty_golden_cases"
    assert len(list(uncertainty_golden_cases.get("golden_cases") or [])) >= 5
    assert uncertainty_report_pack["artifact_type"] == "uncertainty_report_pack"
    assert uncertainty_report_pack["top_contributors"]
    assert uncertainty_digest["artifact_type"] == "uncertainty_digest"
    assert uncertainty_rollup["artifact_type"] == "uncertainty_rollup"
    assert uncertainty_rollup["linked_surface_visibility"] == [
        "results",
        "review_center",
        "workbench",
        "historical_artifacts",
    ]
    assert uncertainty_rollup["report_pack_available"] is True
    assert uncertainty_rollup["ready_for_readiness_mapping"] is True
    assert uncertainty_rollup["not_ready_for_formal_claim"] is True
    assert uncertainty_rollup["not_real_acceptance_evidence"] is True
    assert uncertainty_rollup["primary_evidence_rewritten"] is False
    assert "readiness mapping only" in uncertainty_report_pack["non_claim_note"].lower()
    assert "formal uncertainty" in uncertainty_report_pack["gap_note"].lower()
    assert "all values placeholder/simulated" in uncertainty_report_pack["digest"]["data_completeness_summary"]
    assert "scope_definition_pack" in uncertainty_report_pack["artifact_paths"]
    assert any(
        str(item.get("route_type") or "") == "gas" and str(item.get("measurand") or "") == "CO2"
        for item in list(budget_case.get("budget_case") or [])
    )
    assert any(
        str(item.get("route_type") or "") == "water" and str(item.get("measurand") or "") == "H2O"
        for item in list(budget_case.get("budget_case") or [])
    )
    assert any(
        "writeback-rounding" in str(item.get("uncertainty_case_id") or "")
        for item in list(budget_case.get("budget_case") or [])
    )
    assert any(
        "pressure-handoff-seal-ingress" in str(item.get("uncertainty_case_id") or "")
        for item in list(budget_case.get("budget_case") or [])
    )
    assert any(
        str(item.get("current_coverage") or "")
        for item in list(method_matrix.get("rows") or [])
    )
    assert uncertainty_summary["artifact_type"] == "uncertainty_method_readiness_summary"
    assert "missing evidence" in uncertainty_summary["digest"]["summary"]
    assert software_matrix["artifact_type"] == "software_validation_traceability_matrix"
    assert audit_digest["artifact_type"] == "audit_readiness_digest"
    assert "file-artifact-first reviewer digest" in audit_digest["digest"]["summary"]
    assert audit_digest["linked_measurement_phase_artifacts"]
    assert audit_digest["linked_measurement_phases"]
    assert audit_digest["linked_measurement_gaps"]
    assert "linked_measurement_phase_summary" in audit_digest["digest"]
    assert "linked_measurement_gap_summary" in scope_summary["digest"]
    assert "linked_method_confirmation_items_summary" in scope_summary["digest"]
    assert "linked_uncertainty_inputs_summary" in scope_summary["digest"]
    assert "linked_traceability_nodes_summary" in uncertainty_summary["digest"]
    assert uncertainty_summary["linked_method_confirmation_items"]
    assert uncertainty_summary["linked_uncertainty_inputs"]
    assert audit_digest["reviewer_next_step_digest"]
    assert scope_summary["gap_reason"]
    assert "ambient/ambient_diagnostic" in list(scope_summary.get("linked_measurement_phases") or [])
    assert "ambient/sample_ready" in list(scope_summary.get("linked_measurement_phases") or [])
    assert "system/recovery_retry" in list(audit_digest.get("linked_measurement_phases") or [])
    assert "Ambient baseline stabilization rule" in list(scope_summary.get("linked_method_confirmation_items") or [])
    assert "Ambient stabilization window" in list(uncertainty_summary.get("linked_uncertainty_inputs") or [])
    assert "Software event log chain" in list(audit_digest.get("linked_traceability_nodes") or [])
    if "preseal_partial_gap_summary" in scope_summary["digest"]:
        assert scope_summary["digest"]["preseal_partial_gap_summary"]
    assert "next_required_artifacts_summary" in audit_digest["digest"]
    assert traceability_stub["linked_traceability_nodes"]
    assert "traceability" in str(traceability_stub.get("gap_reason") or "").lower() or str(
        traceability_stub["digest"].get("gap_reason") or ""
    ).lower()

    for payload_item in (scope_summary, certificate_summary, uncertainty_summary, audit_digest):
        assert payload_item["review_surface"]["summary_text"]
        assert payload_item["review_surface"]["artifact_paths"]
        assert payload_item["review_surface"]["anchor_refs"]
        assert "not real acceptance" in payload_item["boundary_statements"]
        rendered = json.dumps(payload_item, ensure_ascii=False).lower()
        assert "real acceptance ready" not in rendered
        assert "\"compliant\"" not in rendered
        assert "\"accredited\"" not in rendered

    assert payload["summary_stats"]["scope_readiness_summary"]["artifact_type"] == "scope_readiness_summary"
    assert payload["summary_stats"]["certificate_readiness_summary"]["artifact_type"] == "certificate_readiness_summary"
    assert (
        payload["summary_stats"]["uncertainty_method_readiness_summary"]["artifact_type"]
        == "uncertainty_method_readiness_summary"
    )
    assert payload["summary_stats"]["audit_readiness_digest"]["artifact_type"] == "audit_readiness_digest"
    assert payload["manifest_sections"]["scope_readiness_summary"]["review_surface"]["anchor_id"] == (
        "scope-readiness-summary"
    )
    assert payload["manifest_sections"]["certificate_readiness_summary"]["review_surface"]["anchor_id"] == (
        "certificate-readiness-summary"
    )
    assert payload["manifest_sections"]["uncertainty_method_readiness_summary"]["review_surface"]["anchor_id"] == (
        "uncertainty-method-readiness-summary"
    )
    assert payload["manifest_sections"]["audit_readiness_digest"]["review_surface"]["anchor_id"] == (
        "audit-readiness-digest"
    )
    remembered = {str(item) for item in list(payload.get("remembered_files") or [])}
    assert str(run_dir / recognition_readiness.SCOPE_READINESS_SUMMARY_FILENAME) in remembered
    assert str(run_dir / recognition_readiness.AUDIT_READINESS_DIGEST_MARKDOWN_FILENAME) in remembered
