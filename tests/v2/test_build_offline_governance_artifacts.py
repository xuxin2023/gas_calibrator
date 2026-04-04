from __future__ import annotations

import json
from pathlib import Path

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
    assert (run_dir / "lineage_summary.json").exists()
    assert (run_dir / "evidence_registry.json").exists()
    analytics_summary = json.loads((run_dir / "analytics_summary.json").read_text(encoding="utf-8"))
    evidence_registry = json.loads((run_dir / "evidence_registry.json").read_text(encoding="utf-8"))
    assert analytics_summary["evidence_source"] == "simulated_protocol"
    assert analytics_summary["not_real_acceptance_evidence"] is True
    assert analytics_summary["config_safety"]["classification"] == "simulation_real_port_inventory_risk"
    assert analytics_summary["config_safety_review"]["status"] == "blocked"
    assert analytics_summary["config_safety_review"]["warnings"] == ["top-level warning"]
    assert analytics_summary["config_governance_handoff"]["execution_gate"]["status"] == "blocked"
    assert analytics_summary["offline_diagnostic_adapter_summary"]["found"] is True
    assert analytics_summary["offline_diagnostic_adapter_summary"]["bundle_count"] == 2
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
    assert evidence_registry["evidence_source"] == "simulated_protocol"
    assert evidence_registry["not_real_acceptance_evidence"] is True
    assert evidence_registry["acceptance_level"] == "offline_regression"
    assert evidence_registry["promotion_state"] == "dry_run_only"
    assert evidence_registry["config_safety"]["classification"] == "simulation_real_port_inventory_risk"
    assert evidence_registry["config_safety_review"]["status"] == "blocked"
    assert evidence_registry["config_safety_review"]["warnings"] == ["top-level warning"]
    assert payload["summary_stats"]["offline_diagnostic_adapter_summary"]["found"] is True


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
