from pathlib import Path
import json
import sys

from gas_calibrator.v2.adapters.results_gateway import ResultsGateway
from gas_calibrator.v2.ui_v2.artifact_registry_governance import build_role_by_key

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
    assert "配置安全" in reports_payload["result_summary_text"]
    assert "工作台诊断证据" in reports_payload["result_summary_text"]


def test_results_gateway_reads_top_level_handoffs_when_stats_sections_are_missing(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)
    summary_path = Path(facade.result_store.run_dir) / "summary.json"
    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    stats = dict(payload.get("stats", {}) or {})
    stats.pop("artifact_role_summary", None)
    stats.pop("workbench_evidence_summary", None)
    payload["stats"] = stats
    payload["artifact_role_summary"] = {
        "execution_summary": {
            "count": 9,
            "artifacts": ["summary.json"],
            "status_counts": {"ok": 9},
        }
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
    assert results_payload["evidence_source"] == "simulated_protocol"
    assert reports_payload["artifact_role_summary"]["execution_summary"]["status_counts"]["ok"] == 9
    assert reports_payload["workbench_evidence_summary"]["summary_line"] == "top-level workbench summary"


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
    assert summary["latest_room_temp"]["recommended_variant"] == "ambient_open"
    assert summary["latest_analyzer_chain"]["recommendation"] == "inspect analyzer chain"
    assert results_payload["evidence_source"] == "simulated_protocol"
    assert reports_payload["evidence_source"] == "simulated_protocol"
    assert any("Room-temp diagnostic summary" in str(line) for line in list(summary.get("review_lines") or []))
    assert "simulated_protocol" in results_payload["result_summary_text"]
    assert "simulated_protocol" in reports_payload["result_summary_text"]
    assert "离线诊断" in results_payload["result_summary_text"]
    assert "离线诊断" in reports_payload["result_summary_text"]
    assert "artifacts 12 | plots 2" in results_payload["result_summary_text"]
    assert "primary 2 | supporting 8 | plots 2" in results_payload["result_summary_text"]
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
