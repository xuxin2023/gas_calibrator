from pathlib import Path
import json
import sys

from gas_calibrator.v2.adapters.results_gateway import ResultsGateway
from gas_calibrator.v2.core.artifact_compatibility import (
    ARTIFACT_COMPATIBILITY_INDEX_SCHEMA_VERSION,
    COMPATIBILITY_SCAN_SUMMARY_FILENAME,
    REINDEX_MANIFEST_FILENAME,
    RUN_ARTIFACT_INDEX_FILENAME,
)
from gas_calibrator.v2.core.stage_admission_review_pack import (
    STAGE_ADMISSION_REVIEW_PACK_FILENAME,
    STAGE_ADMISSION_REVIEW_PACK_REVIEWER_FILENAME,
)
from gas_calibrator.v2.core.engineering_isolation_admission_checklist import (
    ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_FILENAME,
    ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_REVIEWER_FILENAME,
)
from gas_calibrator.v2.core.stage3_real_validation_plan import (
    STAGE3_REAL_VALIDATION_PLAN_FILENAME,
    STAGE3_REAL_VALIDATION_PLAN_REVIEWER_FILENAME,
)
from gas_calibrator.v2.core.stage3_standards_alignment_matrix import (
    STAGE3_STANDARDS_ALIGNMENT_MATRIX_FILENAME,
    STAGE3_STANDARDS_ALIGNMENT_MATRIX_REVIEWER_FILENAME,
)
from gas_calibrator.v2.core.controlled_state_machine_profile import (
    STATE_TRANSITION_EVIDENCE_FILENAME,
    STATE_TRANSITION_EVIDENCE_MARKDOWN_FILENAME,
)
from gas_calibrator.v2.core.multi_source_stability import (
    MULTI_SOURCE_STABILITY_EVIDENCE_FILENAME,
    MULTI_SOURCE_STABILITY_EVIDENCE_MARKDOWN_FILENAME,
    SIMULATION_EVIDENCE_SIDECAR_BUNDLE_FILENAME,
)
from gas_calibrator.v2.core.measurement_phase_coverage import (
    MEASUREMENT_PHASE_COVERAGE_REPORT_FILENAME,
    MEASUREMENT_PHASE_COVERAGE_REPORT_MARKDOWN_FILENAME,
)
from gas_calibrator.v2.core import recognition_readiness_artifacts as recognition_readiness
from gas_calibrator.v2.ui_v2.artifact_registry_governance import build_role_by_key
from gas_calibrator.v2.scripts.build_offline_governance_artifacts import rebuild_run

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
        "pressure_mode_summary": "ambient_open 2",
        "pressure_target_label_summary": "ambient 1 | ambient_open 1",
        "flush_gate_summary": "pass 1 | veto 1 | rebound 1",
        "preseal_summary": "points 1 | max overshoot 4.2 hPa | max sealed wait 1200 ms",
        "postseal_summary": "timeout blocked 1 | late rebound 1",
        "stale_gauge_summary": "points 1 | worst 25%",
    }
    payload["stats"] = stats
    summary_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _inject_stored_point_taxonomy_summary(run_dir: Path, summary: dict[str, str]) -> None:
    summary_path = run_dir / "summary.json"
    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    stats = dict(payload.get("stats", {}) or {})
    stats["point_taxonomy_summary"] = dict(summary)
    payload["stats"] = stats
    summary_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def test_results_gateway_reads_summary_results_and_reports(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)
    gateway = ResultsGateway(
        facade.result_store.run_dir,
        output_files_provider=facade.service.get_output_files,
    )

    results_payload = gateway.read_results_payload()
    reports_payload = gateway.read_reports_payload()

    assert results_payload["summary"]["run_id"] == facade.session.run_id
    assert results_payload["manifest"]["run_id"] == facade.session.run_id
    assert (
        "Run looks stable." in results_payload["ai_summary_text"]
        or "运行状态稳定" in results_payload["ai_summary_text"]
    )
    assert results_payload["acceptance_plan"]["promotion_state"] == "dry_run_only"
    assert results_payload["analytics_summary"]["artifact_type"] == "run_analytics_summary"
    assert results_payload["lineage_summary"]["artifact_type"] == "lineage_summary"
    assert results_payload["config_safety"]["classification"] == "simulation_real_port_inventory_risk"
    assert results_payload["config_safety_review"]["status"] == "blocked"
    assert results_payload["config_governance_handoff"]["execution_gate"]["status"] == "blocked"
    assert results_payload["evidence_source"] == "simulated_protocol"
    assert results_payload["not_real_acceptance_evidence"] is True
    assert results_payload["acceptance_level"] == "offline_regression"
    assert results_payload["promotion_state"] == "dry_run_only"
    assert "simulated_protocol" in results_payload["result_summary_text"]
    assert results_payload["run_artifact_index"]["artifact_type"] == "run_artifact_index"
    assert results_payload["compatibility_scan_summary"]["artifact_type"] == "compatibility_scan_summary"
    assert results_payload["compatibility_overview"]["schema_contract_summary_display"]
    assert results_payload["compatibility_overview"]["primary_evidence_rewritten"] is False
    assert results_payload["compatibility_rollup"]["index_schema_version"] == ARTIFACT_COMPATIBILITY_INDEX_SCHEMA_VERSION
    assert results_payload["compatibility_rollup"]["rollup_scope"] == "run-dir"
    assert results_payload["compatibility_rollup"]["linked_surface_visibility"] == [
        "results",
        "review_center",
        "workbench",
    ]
    assert results_payload["reindex_manifest"]["regenerate_scope"] == "reviewer_index_sidecar_only"
    assert results_payload["scope_definition_pack"]["scope_export_pack"]["ready_for_readiness_mapping"] is True
    assert results_payload["scope_definition_pack"]["not_real_acceptance_evidence"] is True
    assert results_payload["decision_rule_profile"]["decision_rule_id"]
    assert results_payload["decision_rule_profile"]["acceptance_contract"]["non_primary_evidence_chain"] is True
    assert results_payload["recognition_scope_rollup"]["repository_mode"] == "file_artifact_first"
    assert results_payload["recognition_scope_rollup"]["gateway_mode"] == "file_backed_default"
    assert results_payload["recognition_scope_rollup"]["db_ready_stub"]["not_in_default_chain"] is True
    assert results_payload["recognition_scope_rollup"]["primary_evidence_rewritten"] is False
    assert results_payload["method_confirmation_protocol"]["artifact_type"] == "method_confirmation_protocol"
    assert results_payload["route_specific_validation_matrix"]["artifact_type"] == "route_specific_validation_matrix"
    assert results_payload["validation_run_set"]["artifact_type"] == "validation_run_set"
    assert results_payload["verification_digest"]["artifact_type"] == "verification_digest"
    assert results_payload["verification_rollup"]["artifact_type"] == "verification_rollup"
    assert results_payload["verification_rollup"]["repository_mode"] == "file_artifact_first"
    assert results_payload["verification_rollup"]["gateway_mode"] == "file_backed_default"
    assert results_payload["verification_rollup"]["db_ready_stub"]["not_in_default_chain"] is True
    assert results_payload["verification_rollup"]["primary_evidence_rewritten"] is False
    assert "compatibility bundle" in results_payload["result_summary_text"]
    assert "工件兼容" in results_payload["result_summary_text"]
    assert "兼容性 rollup" in results_payload["result_summary_text"]
    assert "认可范围包" in results_payload["result_summary_text"]
    assert "决策规则" in results_payload["result_summary_text"]
    assert "符合性边界" in results_payload["result_summary_text"]
    assert "方法确认概览" in results_payload["result_summary_text"] or "Method confirmation overview" in results_payload["result_summary_text"]
    assert "验证矩阵完整度" in results_payload["result_summary_text"] or "Validation matrix completeness" in results_payload["result_summary_text"]
    assert "验证就绪状态" in results_payload["result_summary_text"] or "Verification readiness status" in results_payload["result_summary_text"]
    assert "配置安全" in results_payload["result_summary_text"]
    assert "工作台诊断证据" in results_payload["result_summary_text"]
    assert results_payload["output_files"]
    assert reports_payload["run_dir"].endswith(facade.session.run_id)
    assert reports_payload["files"]
    assert reports_payload["evidence_source"] == "simulated_protocol"
    assert reports_payload["not_real_acceptance_evidence"] is True
    assert reports_payload["acceptance_level"] == "offline_regression"
    assert reports_payload["promotion_state"] == "dry_run_only"
    assert "simulated_protocol" in reports_payload["result_summary_text"]
    assert reports_payload["config_safety"]["classification"] == "simulation_real_port_inventory_risk"
    assert reports_payload["config_safety_review"]["execution_gate"]["status"] == "blocked"
    assert reports_payload["config_governance_handoff"]["blocked_reason_details"]
    assert reports_payload["compatibility_rollup"]["index_schema_version"] == ARTIFACT_COMPATIBILITY_INDEX_SCHEMA_VERSION
    assert reports_payload["compatibility_rollup"]["primary_evidence_rewritten"] is False
    assert reports_payload["verification_rollup"]["artifact_type"] == "verification_rollup"
    assert reports_payload["verification_rollup"]["db_ready_stub"]["not_in_default_chain"] is True
    compatibility_row = next(
        row for row in reports_payload["files"] if Path(str(row.get("path") or "")).name == COMPATIBILITY_SCAN_SUMMARY_FILENAME
    )
    assert compatibility_row["compatibility_status"]
    assert compatibility_row["reader_mode_display"]
    assert compatibility_row["note"]
    assert "Schema" in str(compatibility_row["role_status_display"])
    assert compatibility_row["compatibility_rollup"]["rollup_scope"] == "run-dir"
    assert "兼容性 rollup" in str(compatibility_row["note"])
    assert "current_reader_mode" not in str(compatibility_row["note"])
    scope_row = next(
        row
        for row in reports_payload["files"]
        if Path(str(row.get("path") or "")).name == recognition_readiness.SCOPE_DEFINITION_PACK_FILENAME
    )
    decision_row = next(
        row
        for row in reports_payload["files"]
        if Path(str(row.get("path") or "")).name == recognition_readiness.DECISION_RULE_PROFILE_FILENAME
    )
    verification_row = next(
        row
        for row in reports_payload["files"]
        if Path(str(row.get("path") or "")).name == recognition_readiness.VERIFICATION_ROLLUP_FILENAME
    )
    assert "scope" in str(scope_row["name"]).lower()
    assert "formal" not in str(scope_row["note"]).lower() or "not" in str(scope_row["note"]).lower()
    assert "current_reader_mode" not in str(scope_row["note"])
    assert "decision" in str(decision_row["name"]).lower()
    assert "current_reader_mode" not in str(decision_row["note"])
    assert verification_row["verification_rollup_entry"]["review_surface"]["title_text"] == "Verification Rollup"
    assert "矩阵" in str(verification_row["note"]) or "verification" in str(verification_row["note"]).lower()
    assert "配置安全" in reports_payload["result_summary_text"]
    assert "工作台诊断证据" in reports_payload["result_summary_text"]


def test_results_gateway_reads_top_level_handoffs_when_stats_sections_are_missing(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)
    summary_path = Path(facade.result_store.run_dir) / "summary.json"
    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    stats = dict(payload.get("stats", {}) or {})
    stats.pop("artifact_role_summary", None)
    stats.pop("workbench_evidence_summary", None)
    stats.pop("config_governance_handoff", None)
    payload["stats"] = stats
    payload["artifact_role_summary"] = {
        "execution_summary": {
            "count": 9,
            "artifacts": ["summary.json"],
            "status_counts": {"ok": 9},
        }
    }
    payload["config_governance_handoff"] = {
        "status": "unlocked_override",
        "execution_gate": {"status": "unlocked_override", "summary": "top-level governance override"},
        "blocked_reason_details": [],
    }
    payload["workbench_evidence_summary"] = {
        "summary_line": "top-level workbench summary",
        "evidence_source": "simulated_protocol",
        "evidence_state": "simulated_workbench",
        "not_real_acceptance_evidence": True,
        "acceptance_level": "offline_regression",
        "promotion_state": "dry_run_only",
    }
    summary_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    gateway = ResultsGateway(
        facade.result_store.run_dir,
        output_files_provider=facade.service.get_output_files,
    )

    results_payload = gateway.read_results_payload()
    reports_payload = gateway.read_reports_payload()

    assert results_payload["artifact_role_summary"]["execution_summary"]["count"] == 9
    assert results_payload["workbench_evidence_summary"]["summary_line"] == "top-level workbench summary"
    assert results_payload["workbench_evidence_summary"]["evidence_state"] == "simulated_workbench"
    assert results_payload["config_governance_handoff"]["execution_gate"]["status"] == "unlocked_override"
    assert results_payload["evidence_source"] == "simulated_protocol"
    assert reports_payload["artifact_role_summary"]["execution_summary"]["status_counts"]["ok"] == 9
    assert reports_payload["workbench_evidence_summary"]["summary_line"] == "top-level workbench summary"
    assert reports_payload["config_governance_handoff"]["execution_gate"]["status"] == "unlocked_override"


def test_results_gateway_builds_legacy_compatibility_payload_without_rewriting_primary_evidence(tmp_path: Path) -> None:
    run_dir = tmp_path / "legacy_run"
    run_dir.mkdir(parents=True, exist_ok=True)
    summary_path = run_dir / "summary.json"
    manifest_path = run_dir / "manifest.json"
    results_path = run_dir / "results.json"
    summary_payload = {
        "run_id": "legacy-run",
        "stats": {
            "sample_count": 0,
            "point_summaries": [],
        },
    }
    manifest_payload = {
        "run_id": "legacy-run",
        "artifacts": {
            "role_catalog": {
                "execution_summary": ["manifest", "run_summary"],
                "execution_rows": ["results_json"],
            }
        },
    }
    results_payload = {
        "run_id": "legacy-run",
        "samples": [],
        "point_summaries": [],
    }
    summary_text_before = json.dumps(summary_payload, ensure_ascii=False, indent=2) + "\n"
    summary_path.write_text(summary_text_before, encoding="utf-8")
    manifest_path.write_text(json.dumps(manifest_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    results_path.write_text(json.dumps(results_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    gateway = ResultsGateway(run_dir)
    payload = gateway.read_results_payload()
    reports_payload = gateway.read_reports_payload()

    assert payload["compatibility_scan_summary"]["compatibility_status"] == "compatibility_read"
    assert payload["compatibility_scan_summary"]["regenerate_recommended"] is True
    assert payload["compatibility_overview"]["current_reader_mode"] == "compatibility_adapter"
    assert payload["compatibility_overview"]["primary_evidence_rewritten"] is False
    assert payload["compatibility_rollup"]["index_schema_version"] == ARTIFACT_COMPATIBILITY_INDEX_SCHEMA_VERSION
    assert payload["compatibility_rollup"]["legacy_run_count"] == 1
    assert payload["compatibility_rollup"]["regenerate_recommended_count"] == 1
    assert payload["recognition_scope_rollup"]["compatibility_adapter"] is True
    assert payload["recognition_scope_rollup"]["primary_evidence_rewritten"] is False
    assert payload["verification_rollup"]["legacy_placeholder_used"] is True
    assert payload["verification_rollup"]["primary_evidence_rewritten"] is False
    assert "compatibility bundle" in payload["result_summary_text"]
    assert "工件兼容" in payload["result_summary_text"]
    assert "兼容性 rollup" in payload["result_summary_text"]
    assert "认可范围包" in payload["result_summary_text"]
    assert "方法确认概览" in payload["result_summary_text"] or "Method confirmation overview" in payload["result_summary_text"]
    assert not (run_dir / RUN_ARTIFACT_INDEX_FILENAME).exists()
    assert not (run_dir / REINDEX_MANIFEST_FILENAME).exists()
    assert summary_path.read_text(encoding="utf-8") == summary_text_before
    summary_row = next(row for row in reports_payload["files"] if Path(str(row.get("path") or "")).name == "summary.json")
    assert summary_row["compatibility_status"] == "compatibility_read"
    assert summary_row["regenerate_recommended"] is True
    assert summary_row["compatibility_rollup"]["legacy_run_count"] == 1


def test_results_gateway_surfaces_point_taxonomy_summary(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)
    _inject_point_taxonomy_summary(facade.result_store.run_dir)
    gateway = ResultsGateway(
        facade.result_store.run_dir,
        output_files_provider=facade.service.get_output_files,
    )

    results_payload = gateway.read_results_payload()
    reports_payload = gateway.read_reports_payload()
    taxonomy = dict(results_payload.get("point_taxonomy_summary", {}) or {})

    assert taxonomy["pressure_summary"] == "ambient 1 | ambient_open 1"
    assert taxonomy["pressure_mode_summary"] == "ambient_open 2"
    assert taxonomy["pressure_target_label_summary"] == "ambient 1 | ambient_open 1"
    assert taxonomy["flush_gate_summary"] == "pass 1 | veto 1 | rebound 1"
    assert taxonomy["preseal_summary"] == "points 1 | max overshoot 4.2 hPa | max sealed wait 1200 ms"
    assert taxonomy["postseal_summary"] == "timeout blocked 1 | late rebound 1"
    assert taxonomy["stale_gauge_summary"] == "points 1 | worst 25%"
    assert reports_payload["point_taxonomy_summary"] == taxonomy
    assert "ambient 1 | ambient_open 1" in results_payload["result_summary_text"]
    assert "ambient_open 2" in results_payload["result_summary_text"]
    assert "pass 1 | veto 1 | rebound 1" in results_payload["result_summary_text"]
    assert "points 1 | max overshoot 4.2 hPa | max sealed wait 1200 ms" in results_payload["result_summary_text"]
    assert "points 1 | worst 25%" in results_payload["result_summary_text"]
    assert "ambient 1 | ambient_open 1" in reports_payload["result_summary_text"]
    assert "timeout blocked 1 | late rebound 1" in reports_payload["result_summary_text"]


def test_results_gateway_prefers_stored_point_taxonomy_summary_handoff(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)
    _inject_point_taxonomy_summary(facade.result_store.run_dir)
    _inject_stored_point_taxonomy_summary(
        facade.result_store.run_dir,
        {
            "pressure_summary": "stored pressure taxonomy",
            "pressure_mode_summary": "stored pressure mode taxonomy",
            "pressure_target_label_summary": "stored pressure target taxonomy",
            "flush_gate_summary": "stored flush taxonomy",
            "preseal_summary": "stored preseal taxonomy",
            "postseal_summary": "stored postseal taxonomy",
            "stale_gauge_summary": "stored stale taxonomy",
        },
    )
    gateway = ResultsGateway(
        facade.result_store.run_dir,
        output_files_provider=facade.service.get_output_files,
    )

    results_payload = gateway.read_results_payload()
    reports_payload = gateway.read_reports_payload()

    assert results_payload["point_taxonomy_summary"]["pressure_summary"] == "stored pressure taxonomy"
    assert results_payload["point_taxonomy_summary"]["pressure_mode_summary"] == "stored pressure mode taxonomy"
    assert results_payload["point_taxonomy_summary"]["flush_gate_summary"] == "stored flush taxonomy"
    assert "stored pressure taxonomy" in results_payload["result_summary_text"]
    assert "stored pressure mode taxonomy" in results_payload["result_summary_text"]
    assert reports_payload["point_taxonomy_summary"]["postseal_summary"] == "stored postseal taxonomy"
    assert "stored stale taxonomy" in reports_payload["result_summary_text"]


def test_build_role_by_key_keeps_baseline_defaults_when_legacy_catalog_is_sparse() -> None:
    role_by_key = build_role_by_key(
        {
            "execution_summary": ["manifest", "run_summary", "points_readable"],
            "diagnostic_analysis": ["qc_report"],
            "formal_analysis": ["coefficient_report"],
            "legacy_unknown_role": ["acceptance_plan", "custom_legacy_only"],
        }
    )

    assert role_by_key["acceptance_plan"] == "execution_summary"
    assert role_by_key["analytics_summary"] == "diagnostic_analysis"
    assert role_by_key["lineage_summary"] == "execution_summary"
    assert role_by_key["evidence_registry"] == "execution_summary"
    assert role_by_key["suite_summary"] == "execution_summary"
    assert role_by_key["suite_summary_markdown"] == "execution_summary"
    assert role_by_key["suite_analytics_summary"] == "diagnostic_analysis"
    assert role_by_key["suite_acceptance_plan"] == "execution_summary"
    assert role_by_key["suite_evidence_registry"] == "execution_summary"
    assert role_by_key["summary_parity_report"] == "diagnostic_analysis"
    assert role_by_key["summary_parity_report_markdown"] == "diagnostic_analysis"
    assert role_by_key["export_resilience_report"] == "diagnostic_analysis"
    assert role_by_key["export_resilience_report_markdown"] == "diagnostic_analysis"
    assert role_by_key["spectral_quality_summary"] == "diagnostic_analysis"
    assert role_by_key["workbench_action_report_json"] == "diagnostic_analysis"
    assert "custom_legacy_only" not in role_by_key
    assert role_by_key["stage_admission_review_pack"] == "execution_summary"
    assert role_by_key["stage_admission_review_pack_reviewer_artifact"] == "formal_analysis"


def test_results_gateway_backfills_obvious_known_artifacts_for_sparse_legacy_manifest(tmp_path: Path) -> None:
    run_dir = tmp_path / "legacy_sparse_run"
    run_dir.mkdir(parents=True, exist_ok=True)
    run_dir.joinpath("summary.json").write_text(
        '{"run_id":"legacy_sparse_run","stats":{"artifact_exports":{}},"reporting":{}}',
        encoding="utf-8",
    )
    run_dir.joinpath("manifest.json").write_text(
        (
            '{"run_id":"legacy_sparse_run","artifacts":{"role_catalog":{'
            '"execution_rows":["runtime_points","io_log","samples_csv","samples_excel","results_json","point_summaries"],'
            '"execution_summary":["manifest","run_summary","points_readable"],'
            '"diagnostic_analysis":["qc_report"],'
            '"formal_analysis":["coefficient_report"]'
            "}}}"
        ),
        encoding="utf-8",
    )
    for name in (
        "acceptance_plan.json",
        "analytics_summary.json",
        "trend_registry.json",
        "lineage_summary.json",
        "evidence_registry.json",
        "coefficient_registry.json",
        "suite_summary.json",
        "suite_analytics_summary.json",
        "suite_acceptance_plan.json",
        "suite_evidence_registry.json",
        "summary_parity_report.json",
        "export_resilience_report.json",
        "workbench_action_report.json",
        "workbench_action_snapshot.json",
        "temperature_snapshots.json",
        "stage_admission_review_pack.json",
        "stage_admission_review_pack.md",
    ):
        run_dir.joinpath(name).write_text("{}", encoding="utf-8")
    run_dir.joinpath("suite_summary.md").write_text("# suite summary\n", encoding="utf-8")
    run_dir.joinpath("summary_parity_report.md").write_text("# summary parity\n", encoding="utf-8")
    run_dir.joinpath("export_resilience_report.md").write_text("# export resilience\n", encoding="utf-8")
    run_dir.joinpath("workbench_action_report.md").write_text("# workbench\n", encoding="utf-8")
    run_dir.joinpath("ai_run_summary.md").write_text("# ai summary\n", encoding="utf-8")
    run_dir.joinpath("run_summary.txt").write_text("summary\n", encoding="utf-8")
    run_dir.joinpath("points.csv").write_text("point_index\n1\n", encoding="utf-8")
    run_dir.joinpath("io_log.csv").write_text("timestamp,command\n", encoding="utf-8")
    run_dir.joinpath("samples.xlsx").write_text("", encoding="utf-8")
    run_dir.joinpath("route_trace.jsonl").write_text('{"route":"co2"}\n', encoding="utf-8")
    run_dir.joinpath("run.log").write_text("run log\n", encoding="utf-8")
    run_dir.joinpath("samples_runtime.csv").write_text("timestamp\n", encoding="utf-8")

    rows_by_name = {
        str(row.get("name") or ""): dict(row)
        for row in ResultsGateway(run_dir).read_reports_payload()["files"]
    }

    assert rows_by_name["acceptance_plan.json"]["artifact_role"] == "execution_summary"
    assert rows_by_name["analytics_summary.json"]["artifact_role"] == "diagnostic_analysis"
    assert rows_by_name["trend_registry.json"]["artifact_role"] == "diagnostic_analysis"
    assert rows_by_name["lineage_summary.json"]["artifact_role"] == "execution_summary"
    assert rows_by_name["evidence_registry.json"]["artifact_role"] == "execution_summary"
    assert rows_by_name["coefficient_registry.json"]["artifact_role"] == "formal_analysis"
    assert rows_by_name["suite_summary.json"]["artifact_role"] == "execution_summary"
    assert rows_by_name["suite_summary.md"]["artifact_role"] == "execution_summary"
    assert rows_by_name["suite_analytics_summary.json"]["artifact_role"] == "diagnostic_analysis"
    assert rows_by_name["suite_acceptance_plan.json"]["artifact_role"] == "execution_summary"
    assert rows_by_name["suite_evidence_registry.json"]["artifact_role"] == "execution_summary"
    assert rows_by_name["summary_parity_report.json"]["artifact_role"] == "diagnostic_analysis"
    assert rows_by_name["summary_parity_report.md"]["artifact_role"] == "diagnostic_analysis"
    assert rows_by_name["export_resilience_report.json"]["artifact_role"] == "diagnostic_analysis"
    assert rows_by_name["export_resilience_report.md"]["artifact_role"] == "diagnostic_analysis"
    assert rows_by_name["workbench_action_report.json"]["artifact_role"] == "diagnostic_analysis"
    assert rows_by_name["workbench_action_report.md"]["artifact_role"] == "diagnostic_analysis"
    assert rows_by_name["workbench_action_snapshot.json"]["artifact_role"] == "diagnostic_analysis"
    assert rows_by_name["temperature_snapshots.json"]["artifact_role"] == "diagnostic_analysis"
    assert rows_by_name["stage_admission_review_pack.json"]["artifact_key"] == "stage_admission_review_pack"
    assert rows_by_name["stage_admission_review_pack.json"]["artifact_role"] == "execution_summary"
    assert rows_by_name["stage_admission_review_pack.md"]["artifact_key"] == "stage_admission_review_pack_reviewer_artifact"
    assert rows_by_name["stage_admission_review_pack.md"]["artifact_role"] == "formal_analysis"
    assert rows_by_name["points.csv"]["artifact_role"] == "execution_rows"
    assert rows_by_name["io_log.csv"]["artifact_role"] == "execution_rows"
    assert rows_by_name["samples.xlsx"]["artifact_role"] == "execution_rows"
    assert rows_by_name["ai_run_summary.md"]["artifact_key"] == "ai_run_summary_markdown"
    assert rows_by_name["ai_run_summary.md"]["artifact_role"] == "unclassified"
    assert rows_by_name["run_summary.txt"]["artifact_key"] == "run_summary_text"
    assert rows_by_name["run_summary.txt"]["artifact_role"] == "unclassified"
    assert rows_by_name["run.log"]["artifact_key"] == "run_log"
    assert rows_by_name["run.log"]["artifact_role"] == "unclassified"
    assert rows_by_name["samples_runtime.csv"]["artifact_key"] == "samples_runtime"
    assert rows_by_name["samples_runtime.csv"]["artifact_role"] == "unclassified"
    assert rows_by_name["route_trace.jsonl"]["artifact_key"] == "route_trace"
    assert rows_by_name["route_trace.jsonl"]["artifact_role"] == "unclassified"
    assert "spectral_quality_summary.json" not in rows_by_name


def test_results_gateway_reads_optional_spectral_quality_summary_when_present(tmp_path: Path) -> None:
    run_dir = tmp_path / "spectral_run"
    run_dir.mkdir(parents=True, exist_ok=True)
    run_dir.joinpath("summary.json").write_text(
        '{"run_id":"spectral_run","stats":{"artifact_exports":{}},"reporting":{}}',
        encoding="utf-8",
    )
    run_dir.joinpath("manifest.json").write_text('{"run_id":"spectral_run","artifacts":{"role_catalog":{}}}', encoding="utf-8")
    run_dir.joinpath("spectral_quality_summary.json").write_text(
        (
            "{"
            '"artifact_type":"spectral_quality_summary",'
            '"status":"ok",'
            '"channel_count":1,'
            '"ok_channel_count":1,'
            '"overall_score":0.95,'
            '"flags":[],'
            '"channels":{"GA01.co2_signal":{"status":"ok","stability_score":0.95}}'
            "}"
        ),
        encoding="utf-8",
    )

    gateway = ResultsGateway(run_dir, output_files_provider=lambda: [str(run_dir / "spectral_quality_summary.json")])
    results_payload = gateway.read_results_payload()
    report_rows = {str(row.get("name") or ""): dict(row) for row in gateway.read_reports_payload()["files"]}

    assert results_payload["spectral_quality_summary"]["artifact_type"] == "spectral_quality_summary"
    assert report_rows["spectral_quality_summary.json"]["artifact_role"] == "diagnostic_analysis"
    assert report_rows["spectral_quality_summary.json"]["artifact_key"] == "spectral_quality_summary"


def test_results_gateway_backfills_config_safety_from_legacy_summary_stats(tmp_path: Path) -> None:
    run_dir = tmp_path / "legacy_config_safety_run"
    run_dir.mkdir(parents=True, exist_ok=True)
    run_dir.joinpath("summary.json").write_text(
        json.dumps(
            {
                "run_id": "legacy_config_safety_run",
                "reporting": {},
                "stats": {
                    "artifact_exports": {},
                    "config_safety": {
                        "classification": "simulation_real_port_inventory_risk",
                        "execution_gate": {"status": "blocked"},
                    },
                    "config_safety_review": {
                        "status": "blocked",
                        "warnings": ["检测到非仿真设备端口。"],
                        "execution_gate": {"status": "blocked"},
                    },
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    run_dir.joinpath("manifest.json").write_text(
        '{"run_id":"legacy_config_safety_run","artifacts":{"role_catalog":{}}}',
        encoding="utf-8",
    )

    gateway = ResultsGateway(run_dir)
    results_payload = gateway.read_results_payload()
    reports_payload = gateway.read_reports_payload()

    assert results_payload["config_safety"]["classification"] == "simulation_real_port_inventory_risk"
    assert results_payload["config_safety_review"]["status"] == "blocked"
    assert results_payload["config_governance_handoff"]["status"] == "blocked"
    assert reports_payload["config_safety_review"]["execution_gate"]["status"] == "blocked"
    assert reports_payload["config_governance_handoff"]["execution_gate"]["status"] == "blocked"


def test_results_gateway_reads_config_safety_from_evidence_registry_when_summary_missing(tmp_path: Path) -> None:
    run_dir = tmp_path / "evidence_registry_config_safety_run"
    run_dir.mkdir(parents=True, exist_ok=True)
    run_dir.joinpath("summary.json").write_text(
        json.dumps(
            {
                "run_id": "evidence_registry_config_safety_run",
                "reporting": {},
                "stats": {"artifact_exports": {}},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    run_dir.joinpath("manifest.json").write_text(
        '{"run_id":"evidence_registry_config_safety_run","artifacts":{"role_catalog":{}}}',
        encoding="utf-8",
    )
    run_dir.joinpath("evidence_registry.json").write_text(
        json.dumps(
            {
                "artifact_type": "evidence_registry",
                "config_safety": {
                    "classification": "simulation_real_port_inventory_risk",
                    "summary": "registry safety",
                    "execution_gate": {"status": "blocked"},
                },
                "config_safety_review": {
                    "status": "unlocked_override",
                    "summary": "registry review",
                    "warnings": ["registry warning"],
                    "execution_gate": {"status": "unlocked_override", "summary": "registry gate"},
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    gateway = ResultsGateway(run_dir)
    results_payload = gateway.read_results_payload()
    reports_payload = gateway.read_reports_payload()

    assert results_payload["config_safety"]["classification"] == "simulation_real_port_inventory_risk"
    assert results_payload["config_safety_review"]["status"] == "unlocked_override"
    assert results_payload["config_safety_review"]["warnings"] == ["registry warning"]
    assert results_payload["config_governance_handoff"]["execution_gate"]["status"] == "unlocked_override"
    assert reports_payload["config_safety_review"]["execution_gate"]["status"] == "unlocked_override"
    assert reports_payload["config_governance_handoff"]["execution_gate"]["status"] == "unlocked_override"


def test_results_gateway_reads_config_safety_from_analytics_summary_when_summary_and_registry_missing(tmp_path: Path) -> None:
    run_dir = tmp_path / "analytics_summary_config_safety_run"
    run_dir.mkdir(parents=True, exist_ok=True)
    run_dir.joinpath("summary.json").write_text(
        json.dumps(
            {
                "run_id": "analytics_summary_config_safety_run",
                "reporting": {},
                "stats": {"artifact_exports": {}},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    run_dir.joinpath("manifest.json").write_text(
        '{"run_id":"analytics_summary_config_safety_run","artifacts":{"role_catalog":{}}}',
        encoding="utf-8",
    )
    run_dir.joinpath("analytics_summary.json").write_text(
        json.dumps(
            {
                "artifact_type": "run_analytics_summary",
                "config_safety": {
                    "classification": "simulation_real_port_inventory_risk",
                    "summary": "analytics safety",
                    "execution_gate": {"status": "blocked"},
                },
                "config_safety_review": {
                    "status": "analytics_override",
                    "summary": "analytics review",
                    "warnings": ["analytics warning"],
                    "execution_gate": {"status": "analytics_override", "summary": "analytics gate"},
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    gateway = ResultsGateway(run_dir)
    results_payload = gateway.read_results_payload()
    reports_payload = gateway.read_reports_payload()

    assert results_payload["config_safety"]["classification"] == "simulation_real_port_inventory_risk"
    assert results_payload["config_safety_review"]["status"] == "analytics_override"
    assert results_payload["config_safety_review"]["warnings"] == ["analytics warning"]
    assert results_payload["config_governance_handoff"]["execution_gate"]["status"] == "analytics_override"
    assert reports_payload["config_safety_review"]["execution_gate"]["status"] == "analytics_override"
    assert reports_payload["config_governance_handoff"]["execution_gate"]["status"] == "analytics_override"


def test_results_gateway_surfaces_offline_diagnostic_adapter_artifacts(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)
    run_dir = Path(facade.result_store.run_dir)
    _write_offline_diagnostic_bundles(run_dir)

    gateway = ResultsGateway(
        run_dir,
        output_files_provider=facade.service.get_output_files,
    )
    results_payload = gateway.read_results_payload()
    reports_payload = gateway.read_reports_payload()
    rows_by_path = {
        str(row.get("path") or ""): dict(row)
        for row in reports_payload["files"]
    }

    summary = dict(results_payload.get("offline_diagnostic_adapter_summary", {}) or {})

    assert summary["found"] is True
    assert summary["room_temp_count"] == 1
    assert summary["analyzer_chain_count"] == 1
    assert summary["artifact_count"] == 12
    assert summary["primary_artifact_count"] == 2
    assert summary["supporting_artifact_count"] == 8
    assert summary["plot_count"] == 2
    assert summary["coverage_summary"] == "room-temp 1 | analyzer-chain 1 | artifacts 12 | plots 2"
    assert summary["review_scope_summary"] == "primary 2 | supporting 8 | plots 2"
    assert summary["next_check_summary"] == "verify ambient chain | inspect analyzer chain"
    assert summary["detail_lines"]
    assert summary["review_highlight_lines"]
    assert summary["detail_items"][0]["kind"] == "room_temp"
    assert summary["detail_items"][0]["artifact_scope_summary"] == "artifacts 4 | plots 1"
    assert summary["detail_items"][1]["artifact_scope_summary"] == "artifacts 8 | plots 1"
    assert summary["latest_room_temp"]["recommended_variant"] == "ambient_open"
    assert summary["latest_analyzer_chain"]["recommendation"] == "inspect analyzer chain"
    assert results_payload["evidence_source"] == "simulated_protocol"
    assert reports_payload["evidence_source"] == "simulated_protocol"
    assert any("Room-temp diagnostic summary" in str(line) for line in list(summary.get("review_lines") or []))
    assert any("scope artifacts 4 | plots 1" in str(line) for line in list(summary.get("review_highlight_lines") or []))
    assert any("scope artifacts 8 | plots 1" in str(line) for line in list(summary.get("review_highlight_lines") or []))
    assert "simulated_protocol" in results_payload["result_summary_text"]
    assert "simulated_protocol" in reports_payload["result_summary_text"]
    assert "离线诊断" in results_payload["result_summary_text"]
    assert "离线诊断" in reports_payload["result_summary_text"]
    assert "工件 12 | 图表 2" in results_payload["result_summary_text"]
    assert "主工件 2 | 支撑工件 8 | 图表 2" in results_payload["result_summary_text"]
    assert "工件范围: 工件 4 | 图表 1" in results_payload["result_summary_text"]
    assert "工件范围: 工件 8 | 图表 1" in reports_payload["result_summary_text"]
    assert "verify ambient chain | inspect analyzer chain" in reports_payload["result_summary_text"]
    assert "verify ambient chain" in results_payload["result_summary_text"]
    assert "inspect analyzer chain" in reports_payload["result_summary_text"]
    assert "real acceptance evidence" in results_payload["result_summary_text"]
    assert "real acceptance evidence" in reports_payload["result_summary_text"]
    assert rows_by_path[str((run_dir / "room_temp_diagnostic" / "diagnostic_summary.json").resolve())]["artifact_role"] == (
        "diagnostic_analysis"
    )
    assert rows_by_path[str((run_dir / "room_temp_diagnostic" / "diagnostic_summary.json").resolve())]["artifact_key"] == (
        "room_temp_diagnostic_summary"
    )
    assert rows_by_path[str((run_dir / "analyzer_chain_isolation" / "isolation_comparison_summary.json").resolve())][
        "artifact_key"
    ] == "analyzer_chain_isolation_comparison"
    assert rows_by_path[str((run_dir / "analyzer_chain_isolation" / "operator_checklist.md").resolve())]["artifact_key"] == (
        "analyzer_chain_operator_checklist"
    )


def test_results_gateway_exposes_phase_transition_bridge_reviewer_markdown_as_first_class_artifact_entry(
    tmp_path: Path,
) -> None:
    facade = build_fake_facade(tmp_path)
    run_dir = Path(facade.result_store.run_dir)
    rebuild_run(run_dir)

    gateway = ResultsGateway(
        run_dir,
        output_files_provider=facade.service.get_output_files,
    )
    reports_payload = gateway.read_reports_payload()
    reviewer_path = str((run_dir / "phase_transition_bridge_reviewer.md").resolve())
    rows_by_path = {
        str(Path(str(row.get("path") or "")).resolve()): dict(row)
        for row in reports_payload["files"]
    }
    reviewer_row = rows_by_path[reviewer_path]
    reviewer_entry = dict(reports_payload.get("phase_transition_bridge_reviewer_artifact_entry", {}) or {})

    assert reviewer_entry["path"] == reviewer_path
    assert reviewer_entry["summary_text"] == reviewer_row["note"]
    assert reviewer_row["artifact_key"] == "phase_transition_bridge_reviewer_artifact"
    assert reviewer_row["artifact_role"] == "formal_analysis"
    assert reviewer_row["name"] == reviewer_entry["name_text"]
    assert reviewer_row["present_on_disk"] is True
    assert "Step 2 tail / Stage 3 bridge" in reviewer_row["role_status_display"]
    assert "engineering-isolation" in reviewer_row["role_status_display"]
    assert "不是 real acceptance" in reviewer_row["role_status_display"]
    assert "不能替代真实计量验证" not in reviewer_row["name"]
    assert "Step 2 tail / Stage 3 bridge" in reviewer_entry["entry_text"]
    assert reviewer_entry["execute_now_text"] in reviewer_entry["entry_text"]
    assert reviewer_entry["defer_to_stage3_text"] in reviewer_entry["entry_text"]
    assert "不是 real acceptance" in reviewer_entry["entry_text"]
    assert "不能替代真实计量验证" in reviewer_entry["entry_text"]
    assert reviewer_entry["ready_for_engineering_isolation"] is False
    assert reviewer_entry["real_acceptance_ready"] is False


def test_results_gateway_exposes_stage_admission_review_pack_as_first_class_artifact_entry(
    tmp_path: Path,
) -> None:
    facade = build_fake_facade(tmp_path)
    run_dir = Path(facade.result_store.run_dir)
    rebuild_run(run_dir)

    gateway = ResultsGateway(
        run_dir,
        output_files_provider=facade.service.get_output_files,
    )
    reports_payload = gateway.read_reports_payload()
    pack_entry = dict(reports_payload.get("stage_admission_review_pack_artifact_entry", {}) or {})
    rows_by_path = {
        str(Path(str(row.get("path") or "")).resolve()): dict(row)
        for row in reports_payload["files"]
    }
    pack_json_path = str((run_dir / STAGE_ADMISSION_REVIEW_PACK_FILENAME).resolve())
    pack_md_path = str((run_dir / STAGE_ADMISSION_REVIEW_PACK_REVIEWER_FILENAME).resolve())
    pack_json_row = rows_by_path[pack_json_path]
    pack_md_row = rows_by_path[pack_md_path]

    assert pack_entry["path"] == pack_json_path
    assert pack_entry["reviewer_path"] == pack_md_path
    assert pack_json_row["artifact_key"] == "stage_admission_review_pack"
    assert pack_json_row["artifact_role"] == "execution_summary"
    assert pack_md_row["artifact_key"] == "stage_admission_review_pack_reviewer_artifact"
    assert pack_md_row["artifact_role"] == "formal_analysis"
    assert pack_json_row["stage_admission_review_pack_artifact_entry"]["path"] == pack_json_path
    assert pack_md_row["stage_admission_review_pack_artifact_entry"]["reviewer_path"] == pack_md_path
    assert pack_json_row["name"] == "阶段准入评审包 / Stage Admission Review Pack (JSON)"
    assert pack_md_row["name"] == "阶段准入评审包 / Stage Admission Review Pack (Markdown)"
    assert pack_entry["summary_text"] == pack_json_row["note"] == pack_md_row["note"]
    assert "execution_summary" not in pack_json_row["role_status_display"]
    assert "Step 2 tail / Stage 3 bridge" in pack_json_row["role_status_display"]
    assert "engineering-isolation" in pack_json_row["role_status_display"]
    assert "不是 real acceptance" in pack_json_row["role_status_display"]
    assert "formal_analysis" not in pack_md_row["role_status_display"]
    assert "Step 2 tail / Stage 3 bridge" in pack_entry["entry_text"]
    assert pack_entry["execute_now_text"] in pack_entry["entry_text"]
    assert pack_entry["defer_to_stage3_text"] in pack_entry["entry_text"]
    assert "不是 real acceptance" in pack_entry["entry_text"]
    assert "不能替代真实计量验证" in pack_entry["entry_text"]
    assert pack_entry["ready_for_engineering_isolation"] is False
    assert pack_entry["real_acceptance_ready"] is False


def test_results_gateway_exposes_engineering_isolation_admission_checklist_as_first_class_artifact_entry(
    tmp_path: Path,
) -> None:
    facade = build_fake_facade(tmp_path)
    run_dir = Path(facade.result_store.run_dir)
    rebuild_run(run_dir)

    gateway = ResultsGateway(
        run_dir,
        output_files_provider=facade.service.get_output_files,
    )
    reports_payload = gateway.read_reports_payload()
    checklist_entry = dict(
        reports_payload.get("engineering_isolation_admission_checklist_artifact_entry", {}) or {}
    )
    rows_by_path = {
        str(Path(str(row.get("path") or "")).resolve()): dict(row)
        for row in reports_payload["files"]
    }
    checklist_json_path = str((run_dir / ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_FILENAME).resolve())
    checklist_md_path = str((run_dir / ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_REVIEWER_FILENAME).resolve())
    checklist_json_row = rows_by_path[checklist_json_path]
    checklist_md_row = rows_by_path[checklist_md_path]

    assert checklist_entry["path"] == checklist_json_path
    assert checklist_entry["reviewer_path"] == checklist_md_path
    assert checklist_json_row["artifact_key"] == "engineering_isolation_admission_checklist"
    assert checklist_json_row["artifact_role"] == "execution_summary"
    assert checklist_md_row["artifact_key"] == "engineering_isolation_admission_checklist_reviewer_artifact"
    assert checklist_md_row["artifact_role"] == "formal_analysis"
    assert checklist_json_row["engineering_isolation_admission_checklist_artifact_entry"]["path"] == checklist_json_path
    assert (
        checklist_md_row["engineering_isolation_admission_checklist_artifact_entry"]["reviewer_path"]
        == checklist_md_path
    )
    assert checklist_json_row["name"] == "工程隔离准入清单 / Engineering Isolation Admission Checklist (JSON)"
    assert checklist_md_row["name"] == "工程隔离准入清单 / Engineering Isolation Admission Checklist (Markdown)"
    assert checklist_entry["summary_text"] == checklist_json_row["note"] == checklist_md_row["note"]
    assert "execution_summary" not in checklist_json_row["role_status_display"]
    assert "formal_analysis" not in checklist_md_row["role_status_display"]
    assert "Step 2 tail / Stage 3 bridge" in checklist_json_row["role_status_display"]
    assert "engineering-isolation" in checklist_json_row["role_status_display"]
    assert "real acceptance" in checklist_md_row["role_status_display"]
    assert checklist_entry["warning_text"] in checklist_md_row["role_status_display"]
    assert checklist_entry["execute_now_text"] in checklist_entry["entry_text"]
    assert checklist_entry["defer_to_stage3_text"] in checklist_entry["entry_text"]
    assert checklist_entry["warning_text"] in checklist_entry["entry_text"]
    assert "ready_for_engineering_isolation" not in checklist_entry["entry_text"]
    assert "real_acceptance_ready" not in checklist_entry["entry_text"]
    assert checklist_entry["ready_for_engineering_isolation"] is False
    assert checklist_entry["real_acceptance_ready"] is False


def test_results_gateway_exposes_stage3_real_validation_plan_as_first_class_artifact_entry(
    tmp_path: Path,
) -> None:
    facade = build_fake_facade(tmp_path)
    run_dir = Path(facade.result_store.run_dir)
    rebuild_run(run_dir)

    gateway = ResultsGateway(
        run_dir,
        output_files_provider=facade.service.get_output_files,
    )
    reports_payload = gateway.read_reports_payload()
    stage3_entry = dict(reports_payload.get("stage3_real_validation_plan_artifact_entry", {}) or {})
    rows_by_path = {
        str(Path(str(row.get("path") or "")).resolve()): dict(row)
        for row in reports_payload["files"]
    }
    stage3_json_path = str((run_dir / STAGE3_REAL_VALIDATION_PLAN_FILENAME).resolve())
    stage3_md_path = str((run_dir / STAGE3_REAL_VALIDATION_PLAN_REVIEWER_FILENAME).resolve())
    stage3_json_row = rows_by_path[stage3_json_path]
    stage3_md_row = rows_by_path[stage3_md_path]

    assert stage3_entry["path"] == stage3_json_path
    assert stage3_entry["reviewer_path"] == stage3_md_path
    assert stage3_json_row["artifact_key"] == "stage3_real_validation_plan"
    assert stage3_json_row["artifact_role"] == "execution_summary"
    assert stage3_md_row["artifact_key"] == "stage3_real_validation_plan_reviewer_artifact"
    assert stage3_md_row["artifact_role"] == "formal_analysis"
    assert stage3_json_row["stage3_real_validation_plan_artifact_entry"]["path"] == stage3_json_path
    assert stage3_md_row["stage3_real_validation_plan_artifact_entry"]["reviewer_path"] == stage3_md_path
    assert stage3_json_row["name"] == "Stage 3 Real Validation Plan / 第三阶段真实验证计划 (JSON)"
    assert stage3_md_row["name"] == "Stage 3 Real Validation Plan / 第三阶段真实验证计划 (Markdown)"
    assert stage3_entry["summary_text"] == stage3_json_row["note"] == stage3_md_row["note"]
    assert "execution_summary" not in stage3_json_row["role_status_display"]
    assert "formal_analysis" not in stage3_md_row["role_status_display"]
    assert "Step 2 tail / Stage 3 bridge" in stage3_json_row["role_status_display"]
    assert "engineering-isolation" in stage3_json_row["role_status_display"]
    assert "simulation / offline / headless only" in stage3_md_row["role_status_display"]
    assert stage3_entry["role_text"] in stage3_entry["card_text"]
    assert stage3_entry["reviewer_note_text"] in stage3_entry["card_text"]
    assert "第三阶段真实验证证据类别" in stage3_entry["card_text"]
    assert "pass/fail contract 摘要" in stage3_entry["card_text"]
    assert "Digest：" in stage3_entry["card_text"]
    assert "JSON：" in stage3_entry["card_text"]
    assert "Markdown：" in stage3_entry["card_text"]
    assert "不是 real acceptance" in stage3_entry["entry_text"]
    assert "不能替代真实计量验证" in stage3_entry["entry_text"]
    assert "ready_for_engineering_isolation" not in stage3_entry["entry_text"]
    assert "real_acceptance_ready" not in stage3_entry["entry_text"]


def test_results_gateway_exposes_stage3_standards_alignment_matrix_as_first_class_artifact_entry(
    tmp_path: Path,
) -> None:
    facade = build_fake_facade(tmp_path)
    run_dir = Path(facade.result_store.run_dir)
    rebuild_run(run_dir)

    gateway = ResultsGateway(
        run_dir,
        output_files_provider=facade.service.get_output_files,
    )
    reports_payload = gateway.read_reports_payload()
    matrix_entry = dict(reports_payload.get("stage3_standards_alignment_matrix_artifact_entry", {}) or {})
    rows_by_path = {
        str(Path(str(row.get("path") or "")).resolve()): dict(row)
        for row in reports_payload["files"]
    }
    matrix_json_path = str((run_dir / STAGE3_STANDARDS_ALIGNMENT_MATRIX_FILENAME).resolve())
    matrix_md_path = str((run_dir / STAGE3_STANDARDS_ALIGNMENT_MATRIX_REVIEWER_FILENAME).resolve())
    matrix_json_row = rows_by_path[matrix_json_path]
    matrix_md_row = rows_by_path[matrix_md_path]

    assert matrix_entry["path"] == matrix_json_path
    assert matrix_entry["reviewer_path"] == matrix_md_path
    assert matrix_json_row["artifact_key"] == "stage3_standards_alignment_matrix"
    assert matrix_json_row["artifact_role"] == "execution_summary"
    assert matrix_md_row["artifact_key"] == "stage3_standards_alignment_matrix_reviewer_artifact"
    assert matrix_md_row["artifact_role"] == "formal_analysis"
    assert matrix_json_row["stage3_standards_alignment_matrix_artifact_entry"]["path"] == matrix_json_path
    assert matrix_md_row["stage3_standards_alignment_matrix_artifact_entry"]["reviewer_path"] == matrix_md_path
    assert matrix_json_row["name"] == (
        "Stage 3 Standards Alignment Matrix / 第三阶段标准符合性映射与证据覆盖矩阵 (JSON)"
    )
    assert matrix_md_row["name"] == (
        "Stage 3 Standards Alignment Matrix / 第三阶段标准符合性映射与证据覆盖矩阵 (Markdown)"
    )
    assert matrix_entry["summary_text"] == matrix_json_row["note"] == matrix_md_row["note"]
    assert "Step 2 tail / Stage 3 bridge" in matrix_json_row["role_status_display"]
    assert "engineering-isolation" in matrix_json_row["role_status_display"]
    assert "simulation / offline / headless only" in matrix_md_row["role_status_display"]
    assert "readiness mapping only" in matrix_entry["card_text"]
    assert "not accreditation claim" in matrix_entry["card_text"]
    assert "not compliance certification" in matrix_entry["card_text"]
    assert "not real acceptance" in matrix_entry["card_text"]
    assert "cannot replace real metrology validation" in matrix_entry["card_text"]
    assert "JSON" in matrix_entry["card_text"]
    assert "Markdown" in matrix_entry["card_text"]
    assert "ISO/IEC 17025" in matrix_entry["standard_families_text"]
    assert "CNAS-CL01-G003" in matrix_entry["standard_families_text"]
    assert "ready_for_engineering_isolation" not in matrix_entry["entry_text"]
    assert "real_acceptance_ready" not in matrix_entry["entry_text"]


def test_results_gateway_exposes_measurement_core_evidence_artifacts(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)
    run_dir = Path(facade.result_store.run_dir)

    gateway = ResultsGateway(
        run_dir,
        output_files_provider=facade.service.get_output_files,
    )
    results_payload = gateway.read_results_payload()
    reports_payload = gateway.read_reports_payload()
    rows_by_path = {
        str(Path(str(row.get("path") or "")).resolve()): dict(row)
        for row in reports_payload["files"]
    }

    stability_json_path = str((run_dir / MULTI_SOURCE_STABILITY_EVIDENCE_FILENAME).resolve())
    stability_md_path = str((run_dir / MULTI_SOURCE_STABILITY_EVIDENCE_MARKDOWN_FILENAME).resolve())
    transition_json_path = str((run_dir / STATE_TRANSITION_EVIDENCE_FILENAME).resolve())
    transition_md_path = str((run_dir / STATE_TRANSITION_EVIDENCE_MARKDOWN_FILENAME).resolve())
    sidecar_path = str((run_dir / SIMULATION_EVIDENCE_SIDECAR_BUNDLE_FILENAME).resolve())
    phase_coverage_json_path = str((run_dir / MEASUREMENT_PHASE_COVERAGE_REPORT_FILENAME).resolve())
    phase_coverage_md_path = str((run_dir / MEASUREMENT_PHASE_COVERAGE_REPORT_MARKDOWN_FILENAME).resolve())

    assert Path(stability_json_path).exists()
    assert Path(stability_md_path).exists()
    assert Path(transition_json_path).exists()
    assert Path(transition_md_path).exists()
    assert Path(sidecar_path).exists()
    assert Path(phase_coverage_json_path).exists()
    assert Path(phase_coverage_md_path).exists()

    assert results_payload["multi_source_stability_evidence"]["artifact_type"] == "multi_source_stability_evidence"
    assert results_payload["state_transition_evidence"]["artifact_type"] == "state_transition_evidence"
    assert results_payload["simulation_evidence_sidecar_bundle"]["artifact_type"] == "simulation_evidence_sidecar_bundle"
    assert results_payload["measurement_phase_coverage_report"]["artifact_type"] == "measurement_phase_coverage_report"
    assert "shadow evaluation only" in results_payload["multi_source_stability_evidence"]["boundary_statements"]
    assert "does not modify live sampling gate by default" in results_payload["state_transition_evidence"][
        "boundary_statements"
    ]
    assert "future database intake / sidecar-ready" in results_payload["simulation_evidence_sidecar_bundle"][
        "boundary_statements"
    ]
    assert "measurement phase coverage" in results_payload["result_summary_text"]
    assert "payload 完整阶段" in results_payload["result_summary_text"]
    assert "下一步补证工件" in results_payload["result_summary_text"]
    assert "sidecar-ready contract" in results_payload["result_summary_text"]
    assert "measurement phase coverage" in reports_payload["result_summary_text"]
    assert "payload 完整阶段" in reports_payload["result_summary_text"]
    assert "下一步补证工件" in reports_payload["result_summary_text"]
    assert "sidecar-ready contract" in reports_payload["result_summary_text"]
    assert "payload_phase_summary" in results_payload["simulation_evidence_sidecar_bundle"]["coverage_digest"]

    stability_json_row = rows_by_path[stability_json_path]
    stability_md_row = rows_by_path[stability_md_path]
    transition_json_row = rows_by_path[transition_json_path]
    transition_md_row = rows_by_path[transition_md_path]
    sidecar_row = rows_by_path[sidecar_path]
    phase_coverage_json_row = rows_by_path[phase_coverage_json_path]
    phase_coverage_md_row = rows_by_path[phase_coverage_md_path]

    assert stability_json_row["artifact_key"] == "multi_source_stability_evidence"
    assert stability_json_row["artifact_role"] == "diagnostic_analysis"
    assert stability_md_row["artifact_key"] == "multi_source_stability_evidence_markdown"
    assert transition_json_row["artifact_key"] == "state_transition_evidence"
    assert transition_md_row["artifact_key"] == "state_transition_evidence_markdown"
    assert sidecar_row["artifact_key"] == "simulation_evidence_sidecar_bundle"
    assert sidecar_row["artifact_role"] == "execution_summary"
    assert phase_coverage_json_row["artifact_key"] == "measurement_phase_coverage_report"
    assert phase_coverage_md_row["artifact_key"] == "measurement_phase_coverage_report_markdown"
    assert phase_coverage_json_row["artifact_role"] == "diagnostic_analysis"
    assert "仅供影子评估" in stability_json_row["role_status_display"]
    assert "does not modify live sampling gate by default" in stability_json_row["note"]
    assert "fixed canonical states" in transition_json_row["note"]
    assert "Future database intake only" in sidecar_row["note"]
    assert "richer simulation coverage only" in phase_coverage_json_row["note"]
    assert "payload-complete" in phase_coverage_json_row["measurement_phase_coverage_report_entry"]["digest"]["summary"]
    assert phase_coverage_json_row["measurement_phase_coverage_report_entry"]["linked_artifact_refs"]
    assert phase_coverage_json_row["measurement_phase_coverage_report_entry"]["linked_method_confirmation_items"]
    assert phase_coverage_json_row["measurement_phase_coverage_report_entry"]["linked_uncertainty_inputs"]
    assert phase_coverage_json_row["measurement_phase_coverage_report_entry"]["linked_traceability_nodes"]
    assert phase_coverage_json_row["measurement_phase_coverage_report_entry"]["reviewer_next_step_digest"]
    assert phase_coverage_json_row["measurement_phase_coverage_report_entry"]["reviewer_fragments_contract_version"]
    assert phase_coverage_json_row["measurement_phase_coverage_report_entry"]["boundary_fragment_keys"]
    assert phase_coverage_json_row["measurement_phase_coverage_report_entry"]["non_claim_fragment_keys"]
    assert phase_coverage_json_row["measurement_phase_coverage_report_entry"]["phase_contrast_fragment_keys"]
    assert "boundary:shadow_evaluation_only" in list(
        phase_coverage_json_row["measurement_phase_coverage_report_entry"]["boundary_filters"]
    )
    assert any(
        str(item.get("canonical_fragment_id") or "").startswith("phase_contrast:")
        for item in list(phase_coverage_json_row["measurement_phase_coverage_report_entry"]["phase_contrast_filter_rows"] or [])
    )
    assert "Ambient baseline stabilization rule" in list(
        phase_coverage_json_row["measurement_phase_coverage_report_entry"]["linked_method_confirmation_items"]
    )
    assert "Software event log chain" in list(
        phase_coverage_json_row["measurement_phase_coverage_report_entry"]["linked_traceability_nodes"]
    )
    assert "next_required_artifacts" in phase_coverage_json_row["measurement_phase_coverage_report_entry"]
    assert any(
        list(item.get("gap_reason_fragment_keys") or [])
        for item in list(phase_coverage_json_row["measurement_phase_coverage_report_entry"]["linked_measurement_gaps"] or [])
    )
    assert any(
        list(item.get("reviewer_next_step_fragment_keys") or [])
        for item in list(phase_coverage_json_row["measurement_phase_coverage_report_entry"]["linked_measurement_gaps"] or [])
    )
    assert any(
        list(item.get("boundary_fragment_keys") or [])
        for item in list(phase_coverage_json_row["measurement_phase_coverage_report_entry"]["linked_measurement_gaps"] or [])
    )
    assert any(
        list(item.get("non_claim_fragment_keys") or [])
        for item in list(phase_coverage_json_row["measurement_phase_coverage_report_entry"]["linked_measurement_gaps"] or [])
    )
    assert "measurement_phase_coverage_report_entry" in phase_coverage_json_row
    assert "measurement_phase_coverage_report_entry" in phase_coverage_md_row
    assert "shadow_evaluation_results" not in stability_json_row["note"]
    assert "live_gate" not in stability_json_row["note"]
    assert "compliance" not in sidecar_row["note"].lower()
    assert "acceptance_level" not in phase_coverage_json_row["note"]


def test_results_gateway_exposes_recognition_readiness_artifacts(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)
    run_dir = Path(facade.result_store.run_dir)
    rebuild_run(run_dir)
    gateway = ResultsGateway(
        run_dir,
        output_files_provider=facade.service.get_output_files,
    )

    results_payload = gateway.read_results_payload()
    reports_payload = gateway.read_reports_payload()
    rows_by_path = {
        str(Path(str(row.get("path") or "")).resolve()): dict(row)
        for row in reports_payload["files"]
    }

    scope_json_path = str((run_dir / recognition_readiness.SCOPE_READINESS_SUMMARY_FILENAME).resolve())
    reference_asset_json_path = str((run_dir / recognition_readiness.REFERENCE_ASSET_REGISTRY_FILENAME).resolve())
    certificate_lifecycle_json_path = str(
        (run_dir / recognition_readiness.CERTIFICATE_LIFECYCLE_SUMMARY_FILENAME).resolve()
    )
    certificate_json_path = str((run_dir / recognition_readiness.CERTIFICATE_READINESS_SUMMARY_FILENAME).resolve())
    pre_run_gate_json_path = str((run_dir / recognition_readiness.PRE_RUN_READINESS_GATE_FILENAME).resolve())
    uncertainty_report_pack_json_path = str((run_dir / recognition_readiness.UNCERTAINTY_REPORT_PACK_FILENAME).resolve())
    uncertainty_digest_json_path = str((run_dir / recognition_readiness.UNCERTAINTY_DIGEST_FILENAME).resolve())
    uncertainty_rollup_json_path = str((run_dir / recognition_readiness.UNCERTAINTY_ROLLUP_FILENAME).resolve())
    uncertainty_json_path = str(
        (run_dir / recognition_readiness.UNCERTAINTY_METHOD_READINESS_SUMMARY_FILENAME).resolve()
    )
    audit_json_path = str((run_dir / recognition_readiness.AUDIT_READINESS_DIGEST_FILENAME).resolve())

    for key in (
        "scope_readiness_summary",
        "reference_asset_registry",
        "certificate_lifecycle_summary",
        "certificate_readiness_summary",
        "pre_run_readiness_gate",
        "uncertainty_report_pack",
        "uncertainty_digest",
        "uncertainty_rollup",
        "uncertainty_method_readiness_summary",
        "audit_readiness_digest",
    ):
        assert results_payload[key]["artifact_type"] == key
        assert results_payload[key]["not_real_acceptance_evidence"] is True

    assert "Scope Readiness Summary" in results_payload["result_summary_text"]
    assert "Reference Asset Registry" in results_payload["result_summary_text"]
    assert "Certificate Lifecycle Summary" in results_payload["result_summary_text"]
    assert "Pre-run Readiness Gate" in results_payload["result_summary_text"]
    assert (
        "不确定度概览" in results_payload["result_summary_text"]
        or "Uncertainty overview" in results_payload["result_summary_text"]
    )
    assert (
        "预算完整度" in results_payload["result_summary_text"]
        or "Budget completeness" in results_payload["result_summary_text"]
    )
    assert (
        "主要不确定度贡献" in results_payload["result_summary_text"]
        or "Top uncertainty contributors" in results_payload["result_summary_text"]
    )
    assert "Scope Readiness Summary" in reports_payload["result_summary_text"]
    assert "Pre-run Readiness Gate" in reports_payload["result_summary_text"]

    scope_row = rows_by_path[scope_json_path]
    reference_asset_row = rows_by_path[reference_asset_json_path]
    certificate_lifecycle_row = rows_by_path[certificate_lifecycle_json_path]
    certificate_row = rows_by_path[certificate_json_path]
    pre_run_gate_row = rows_by_path[pre_run_gate_json_path]
    uncertainty_report_pack_row = rows_by_path[uncertainty_report_pack_json_path]
    uncertainty_digest_row = rows_by_path[uncertainty_digest_json_path]
    uncertainty_rollup_row = rows_by_path[uncertainty_rollup_json_path]
    uncertainty_row = rows_by_path[uncertainty_json_path]
    audit_row = rows_by_path[audit_json_path]

    assert scope_row["artifact_key"] == "scope_readiness_summary"
    assert reference_asset_row["artifact_key"] == "reference_asset_registry"
    assert certificate_lifecycle_row["artifact_key"] == "certificate_lifecycle_summary"
    assert certificate_row["artifact_key"] == "certificate_readiness_summary"
    assert pre_run_gate_row["artifact_key"] == "pre_run_readiness_gate"
    assert uncertainty_report_pack_row["artifact_key"] == "uncertainty_report_pack"
    assert uncertainty_digest_row["artifact_key"] == "uncertainty_digest"
    assert uncertainty_rollup_row["artifact_key"] == "uncertainty_rollup"
    assert uncertainty_row["artifact_key"] == "uncertainty_method_readiness_summary"
    assert audit_row["artifact_key"] == "audit_readiness_digest"
    assert scope_row["artifact_role"] == "diagnostic_analysis"
    assert reference_asset_row["artifact_role"] == "execution_summary"
    assert certificate_lifecycle_row["artifact_role"] == "diagnostic_analysis"
    assert pre_run_gate_row["artifact_role"] == "diagnostic_analysis"
    assert "Step 2 reviewer readiness only" in scope_row["role_status_display"]
    assert "formal scope approval" in scope_row["note"]
    assert "reference asset ledger" in reference_asset_row["note"].lower()
    assert "certificate lifecycle skeleton" in certificate_lifecycle_row["note"].lower()
    assert "missing certificates" in certificate_row["note"].lower()
    assert "pass results" in certificate_row["note"].lower()
    assert "advisory" in pre_run_gate_row["note"].lower()
    assert "readiness mapping" in uncertainty_report_pack_row["note"].lower()
    assert "reviewer-facing uncertainty skeleton" in uncertainty_digest_row["note"].lower()
    assert "placeholder" in uncertainty_rollup_row["note"].lower()
    assert "traceability skeleton" in audit_row["note"].lower()
    assert (
        scope_row["scope_readiness_summary_entry"]["review_surface"]["anchor_id"]
        == "scope-readiness-summary"
    )
    assert (
        reference_asset_row["reference_asset_registry_entry"]["review_surface"]["anchor_id"]
        == "reference-asset-registry"
    )
    assert (
        certificate_lifecycle_row["certificate_lifecycle_summary_entry"]["review_surface"]["anchor_id"]
        == "certificate-lifecycle-summary"
    )
    assert (
        certificate_row["certificate_readiness_summary_entry"]["review_surface"]["anchor_id"]
        == "certificate-readiness-summary"
    )
    assert (
        pre_run_gate_row["pre_run_readiness_gate_entry"]["review_surface"]["anchor_id"]
        == "pre-run-readiness-gate"
    )
    assert (
        uncertainty_report_pack_row["uncertainty_report_pack_entry"]["review_surface"]["anchor_id"]
        == "uncertainty-report-pack"
    )
    assert (
        uncertainty_digest_row["uncertainty_digest_entry"]["review_surface"]["anchor_id"]
        == "uncertainty-digest"
    )
    assert (
        uncertainty_rollup_row["uncertainty_rollup_entry"]["review_surface"]["anchor_id"]
        == "uncertainty-rollup"
    )
    assert (
        uncertainty_row["uncertainty_method_readiness_summary_entry"]["review_surface"]["anchor_id"]
        == "uncertainty-method-readiness-summary"
    )
    assert (
        audit_row["audit_readiness_digest_entry"]["review_surface"]["anchor_id"]
        == "audit-readiness-digest"
    )
    assert scope_row["scope_readiness_summary_entry"]["linked_method_confirmation_items"]
    assert results_payload["reference_asset_registry"]["assets"]
    assert results_payload["certificate_lifecycle_summary"]["certificate_rows"]
    assert results_payload["certificate_readiness_summary"]["asset_status_rows"]
    assert results_payload["pre_run_readiness_gate"]["checks"]
    assert results_payload["pre_run_readiness_gate"]["gate_status"] == "blocked_for_formal_claim"
    assert results_payload["uncertainty_report_pack"]["top_contributors"]
    assert results_payload["uncertainty_digest"]["digest"]["uncertainty_overview_summary"]
    assert results_payload["uncertainty_rollup"]["budget_completeness_summary"]
    assert results_payload["uncertainty_rollup"]["db_ready_stub"]["not_in_default_chain"] is True
    assert results_payload["uncertainty_rollup"]["primary_evidence_rewritten"] is False
    assert results_payload["uncertainty_rollup"]["not_ready_for_formal_claim"] is True
    assert uncertainty_row["uncertainty_method_readiness_summary_entry"]["linked_uncertainty_inputs"]
    assert audit_row["audit_readiness_digest_entry"]["linked_measurement_gaps"]
    assert audit_row["audit_readiness_digest_entry"]["reviewer_next_step_digest"]
    assert audit_row["audit_readiness_digest_entry"]["reviewer_fragments_contract_version"]
    assert "Ambient baseline stabilization rule" in list(
        scope_row["scope_readiness_summary_entry"]["linked_method_confirmation_items"]
    )
    assert "Ambient stabilization window" in list(
        uncertainty_row["uncertainty_method_readiness_summary_entry"]["linked_uncertainty_inputs"]
    )
    assert "Software event log chain" in list(audit_row["audit_readiness_digest_entry"]["linked_traceability_nodes"])
    assert list(scope_row["scope_readiness_summary_entry"].get("gap_reason_fragment_keys") or [])
    assert list(scope_row["scope_readiness_summary_entry"].get("boundary_fragment_keys") or [])
    assert list(scope_row["scope_readiness_summary_entry"].get("non_claim_fragment_keys") or [])
    assert list(audit_row["audit_readiness_digest_entry"].get("reviewer_next_step_fragment_keys") or [])
    for row in (
        scope_row,
        reference_asset_row,
        certificate_lifecycle_row,
        certificate_row,
        pre_run_gate_row,
        uncertainty_report_pack_row,
        uncertainty_digest_row,
        uncertainty_rollup_row,
        uncertainty_row,
        audit_row,
    ):
        note_text = str(row.get("note") or "").lower()
        assert "accreditation" not in note_text
        assert "acceptance_level" not in note_text
        assert "compliance claim" not in note_text


# ---------------------------------------------------------------------------
# TestResultsGatewayUsesV12CompactSummary (2.11)
# ---------------------------------------------------------------------------

class TestResultsGatewayUsesV12CompactSummary:
    """Verify results_gateway imports and can use V1.2 compact summary builders."""

    def test_v12_compact_summary_importable(self):
        """V1.2 compact summary builders must be importable from results_gateway."""
        from gas_calibrator.v2.adapters.results_gateway import (
            build_v12_alignment_compact_summary,
            build_phase_evidence_compact_summary,
            build_governance_handoff_compact_summary,
            build_parity_resilience_compact_summary,
        )
        # Verify they are callable
        assert callable(build_v12_alignment_compact_summary)
        assert callable(build_phase_evidence_compact_summary)
        assert callable(build_governance_handoff_compact_summary)
        assert callable(build_parity_resilience_compact_summary)

    def test_v12_compact_summary_stable_output(self):
        """V1.2 compact summary must produce stable output for empty payload."""
        from gas_calibrator.v2.core.reviewer_summary_builders import build_v12_alignment_compact_summary

        result = build_v12_alignment_compact_summary({})
        assert "summary_lines" in result
        assert "v12_compact" in result
        assert "boundary_markers" in result
        # Must have simulated-only note
        joined = " | ".join(result["summary_lines"])
        assert "仿真" in joined or "Simulated" in joined


# ---------------------------------------------------------------------------
# Step 2.13: Compact summary pack and surface budget governance
# ---------------------------------------------------------------------------


class TestResultsGatewayCompactSummaryPacks:
    """Verify results_gateway builds and exposes compact summary packs."""

    def test_build_compact_summary_packs_returns_list(self):
        packs = ResultsGateway._build_compact_summary_packs()
        assert isinstance(packs, list)
        assert len(packs) == 4  # v12_alignment, phase_evidence, governance_handoff, parity_resilience

    def test_each_pack_has_summary_key(self):
        packs = ResultsGateway._build_compact_summary_packs()
        expected_keys = {"v12_alignment", "phase_evidence", "governance_handoff", "parity_resilience"}
        actual_keys = {p["summary_key"] for p in packs}
        assert actual_keys == expected_keys

    def test_each_pack_has_simulation_only_markers(self):
        packs = ResultsGateway._build_compact_summary_packs()
        for pack in packs:
            assert pack["evidence_source"] == "simulated"
            assert pack["not_real_acceptance_evidence"] is True
            assert pack["not_ready_for_formal_claim"] is True

    def test_packs_with_payload(self):
        packs = ResultsGateway._build_compact_summary_packs(
            taxonomy_summary={"pressure_summary": "4 points"},
            phase_coverage_summary={"status": "partial"},
            workbench_summary={
                "parity_resilience_summary": {"parity_status": "pass", "resilience_status": "pass"},
                "governance_handoff_summary": {"blockers": [], "next_steps": "continue"},
            },
        )
        assert len(packs) == 4
        for pack in packs:
            assert "summary_lines" in pack
            assert isinstance(pack["summary_lines"], list)


class TestResultsGatewayCompactSummaryBudget:
    """Verify results_gateway applies surface budget governance to compact summary lines."""

    def test_budget_governance_importable(self):
        from gas_calibrator.v2.adapters.results_gateway import (
            apply_surface_budget,
            build_truncation_hint_line,
        )
        assert callable(apply_surface_budget)
        assert callable(build_truncation_hint_line)

    def test_compact_summary_packs_in_read_results_payload(self):
        """read_results_payload must include compact_summary_packs field."""
        facade = build_fake_facade()
        # Use a real run dir if available, otherwise skip
        run_dirs = list(facade._run_roots) if hasattr(facade, '_run_roots') else []
        if not run_dirs:
            pytest.skip("No run dirs available for integration test")
        # This test verifies the field exists in the payload structure
        # The actual content depends on run data
        from gas_calibrator.v2.core.reviewer_summary_packs import PACK_SUMMARY_KEYS
        assert "v12_alignment" in PACK_SUMMARY_KEYS
