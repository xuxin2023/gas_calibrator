import json
from pathlib import Path
import sys

import pytest

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from gas_calibrator.v2.config import AppConfig
from gas_calibrator.v2.core.route_planner import RoutePlanner
from gas_calibrator.v2.domain.pressure_selection import (
    AMBIENT_PRESSURE_LABEL,
    AMBIENT_PRESSURE_TOKEN,
)
from gas_calibrator.v2.ui_v2.controllers.app_facade import AppFacade
from ui_v2_support import build_fake_facade


def _execution_signature(payload) -> list[tuple[float, str, float | None, float | None, float | None, float | None]]:
    return [
        (
            float(point.temperature_c),
            str(point.route),
            None if point.target_pressure_hpa is None else float(point.target_pressure_hpa),
            None if point.co2_ppm is None else float(point.co2_ppm),
            None if point.hgen_temp_c is None else float(point.hgen_temp_c),
            None if point.hgen_rh_pct is None else float(point.hgen_rh_pct),
        )
        for point in payload
    ]


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


def test_app_facade_builds_run_qc_and_results_snapshots(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)

    snapshot = facade.build_snapshot()

    assert snapshot["run"]["run_id"] == facade.session.run_id
    assert snapshot["run"]["route"] == "co2"
    assert snapshot["run"]["retry"] == 1
    assert snapshot["qc"]["total_points"] == 2
    assert snapshot["qc"]["invalid_points"] == 1
    assert snapshot["results"]["summary"]["run_id"] == facade.session.run_id
    assert "运行状态稳定" in snapshot["results"]["ai_summary_text"]
    assert snapshot["results"]["reporting"]["mode"] == "formal_default"
    assert "run_summary" in snapshot["results"]["artifact_exports"]
    assert "execution_summary" in snapshot["results"]["artifact_role_summary"]
    assert snapshot["results"]["acceptance_plan"]["promotion_state"] == "dry_run_only"
    assert snapshot["results"]["analytics_summary"]["artifact_type"] == "run_analytics_summary"
    assert snapshot["results"]["analytics_summary"]["qc_overview"]["run_gate"]["status"] == "warn"
    assert snapshot["results"]["analytics_summary"]["config_safety"]["classification"] == (
        "simulation_real_port_inventory_risk"
    )
    assert snapshot["results"]["analytics_summary"]["config_safety_review"]["status"] == "blocked"
    assert snapshot["results"]["analytics_summary"]["config_governance_handoff"]["execution_gate"]["status"] == "blocked"
    assert snapshot["results"]["analytics_summary"]["qc_reviewer_card"]["lines"]
    assert snapshot["results"]["analytics_summary"]["qc_evidence_section"]["cards"]
    assert snapshot["results"]["analytics_summary"]["qc_review_cards"]
    assert snapshot["results"]["acceptance_readiness_summary"]["simulated_readiness_only"] is True
    assert "离线回归" in snapshot["results"]["acceptance_readiness_summary"]["summary_display"]
    assert "覆盖" in snapshot["results"]["analytics_summary_digest"]["summary_display"]
    assert "质控" in snapshot["results"]["analytics_summary_digest"]["summary_display"]
    assert "质控摘要" in snapshot["results"]["qc_summary_text"]
    assert "不代表 real acceptance" in snapshot["results"]["qc_summary_text"]
    assert snapshot["results"]["qc_reviewer_card"]["lines"]
    assert snapshot["results"]["qc_evidence_section"]["lines"]
    assert snapshot["results"]["qc_evidence_section"]["review_card_lines"]
    assert snapshot["results"]["qc_evidence_section"]["cards"]
    assert snapshot["results"]["qc_review_cards"]
    assert snapshot["results"]["qc_evidence_section"]["run_gate"]["status"] == "warn"
    assert snapshot["results"]["qc_evidence_section"]["point_gate_summary"]["status"] == "warn"
    assert snapshot["results"]["qc_evidence_section"]["not_real_acceptance_evidence"] is True
    assert snapshot["results"]["lineage_digest"]["config_version"].startswith("cfg-")
    assert snapshot["results"]["review_center"]["acceptance_readiness"]["simulated_only"] is True
    assert snapshot["results"]["review_center"]["evidence_items"]
    analytics_item = next(item for item in snapshot["results"]["review_center"]["evidence_items"] if item["type"] == "analytics")
    assert analytics_item["detail_qc_summary"]
    assert analytics_item["detail_qc_cards"]
    assert any("运行门禁" in str(line) for line in list(analytics_item["detail_qc_summary"]))
    assert any("证据边界" in str(line) for line in list(analytics_item["detail_qc_summary"]))
    assert "可读点表" in snapshot["results"]["result_summary_text"]
    assert "工件角色" in snapshot["results"]["result_summary_text"]
    assert "工作台诊断证据" in snapshot["results"]["result_summary_text"]
    assert "facade.role_summary_item" not in snapshot["results"]["result_summary_text"]
    assert any(item.get("detail_qc_cards") for item in snapshot["results"]["review_center"]["evidence_items"])
    assert snapshot["devices"]["enabled_count"] == 2
    assert snapshot["devices"]["workbench"]["meta"]["simulated"] is True
    assert snapshot["algorithms"]["default_algorithm"] == "amt"
    assert snapshot["reports"]["run_dir"].endswith(facade.session.run_id)
    assert snapshot["reports"]["files"]
    assert snapshot["reports"]["files"][0]["listed_in_current_run"] is True
    assert snapshot["reports"]["files"][0]["artifact_origin"] == "current_run"
    report_rows = {str(row.get("name") or ""): dict(row) for row in snapshot["reports"]["files"]}
    assert any(str(row.get("artifact_role") or "") == "execution_summary" for row in report_rows.values())
    assert any(str(row.get("export_status") or "") == "ok" for row in report_rows.values())
    assert report_rows["summary.json"]["artifact_role"] == "execution_summary"
    assert report_rows["summary.json"]["export_status"] == "ok"
    assert report_rows["acceptance_plan.json"]["artifact_role"] == "execution_summary"
    assert report_rows["analytics_summary.json"]["artifact_role"] == "diagnostic_analysis"
    assert report_rows["trend_registry.json"]["artifact_role"] == "diagnostic_analysis"
    assert report_rows["lineage_summary.json"]["artifact_role"] == "execution_summary"
    assert report_rows["evidence_registry.json"]["artifact_role"] == "execution_summary"
    assert report_rows["coefficient_registry.json"]["artifact_role"] == "formal_analysis"
    assert report_rows["ai_run_summary.md"]["artifact_role"] == "unclassified"
    assert report_rows["ai_run_summary.md"]["export_status_known"] is False
    summary_payload = json.loads((Path(facade.result_store.run_dir) / "summary.json").read_text(encoding="utf-8"))
    assert summary_payload["config_safety"]["execution_gate"]["status"] == "blocked"
    assert summary_payload["config_safety_review"]["status"] == "blocked"
    assert summary_payload["config_safety_review"]["execution_gate"]["status"] == "blocked"
    assert summary_payload["stats"]["config_safety"]["execution_gate"]["status"] == "blocked"
    assert summary_payload["stats"]["config_safety_review"]["review_lines"]
    assert summary_payload["stats"]["config_safety_review"]["execution_gate"]["status"] == "blocked"
    assert summary_payload["stats"]["config_safety_review"]["warnings"]
    assert snapshot["results"]["config_safety"]["classification"] == "simulation_real_port_inventory_risk"
    assert snapshot["results"]["config_safety_review"]["status"] == "blocked"
    assert snapshot["results"]["config_safety_review"]["execution_gate"]["status"] == "blocked"
    assert snapshot["results"]["config_governance_handoff"]["blocked_reason_details"]
    assert snapshot["results"]["config_safety_review"]["warnings"]
    assert "配置安全" in snapshot["results"]["result_summary_text"]
    assert snapshot["reports"]["review_center"]["evidence_items"]
    assert snapshot["reports"]["evidence_source"] == "simulated_protocol"
    assert snapshot["reports"]["not_real_acceptance_evidence"] is True
    assert snapshot["reports"]["acceptance_level"] == "offline_regression"
    assert snapshot["reports"]["promotion_state"] == "dry_run_only"
    assert snapshot["reports"]["config_safety"]["classification"] == "simulation_real_port_inventory_risk"
    assert snapshot["reports"]["config_safety_review"]["execution_gate"]["status"] == "blocked"
    assert snapshot["reports"]["config_governance_handoff"]["execution_gate"]["status"] == "blocked"
    assert "配置安全" in snapshot["reports"]["result_summary_text"]
    assert "工作台诊断证据" in snapshot["reports"]["result_summary_text"]
    assert snapshot["reports"]["qc_evidence_section"]["reviewer_card"]["lines"]
    assert snapshot["reports"]["qc_evidence_section"]["cards"]
    assert "质控摘要" in snapshot["reports"]["qc_summary_text"]
    assert snapshot["reports"]["qc_review_cards"]
    assert "simulated_protocol" in snapshot["reports"]["result_summary_text"]
    assert any(card["id"] == "boundary" for card in snapshot["results"]["analytics_summary"]["qc_review_cards"])
    assert snapshot["timeseries"]["series"]["temperature_c"]
    assert snapshot["qc_overview"]["grade"] == "B"
    assert snapshot["winner"]["winner"] == "amt"
    assert snapshot["export"]["artifact_count"] >= 1
    assert snapshot["route_progress"]["route"] == "co2"
    assert snapshot["route_progress"]["route_display"] == "气路"
    assert snapshot["reject_reasons_chart"]["rows"][0]["reason"] == "outlier_ratio_too_high"
    assert snapshot["residuals"]["series"]
    assert snapshot["analyzer_health"]["rows"]
    assert snapshot["notifications"]["items"]
    assert snapshot["validation"]["promotion_state"] == "dry_run_only"


def test_app_facade_exports_artifacts(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)

    result = facade.export_artifacts("json")

    assert result["ok"] is True
    assert Path(result["directory"]).exists()
    assert any(Path(item).suffix.lower() == ".json" for item in result["exported_files"])


def test_app_facade_exports_review_scope_manifest(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)
    history_dir = Path(tmp_path) / "history" / "review_scope_case"
    history_dir.mkdir(parents=True, exist_ok=True)
    suite_summary = history_dir / "suite_summary.json"
    suite_summary.write_text("{}", encoding="utf-8")
    spectral_summary = Path(facade.result_store.run_dir) / "spectral_quality_summary.json"
    spectral_summary.write_text(
        json.dumps(
            {
                "artifact_type": "spectral_quality_summary",
                "status": "ok",
                "channel_count": 1,
                "ok_channel_count": 1,
                "overall_score": 0.91,
                "flags": ["low_frequency_drift"],
                "not_real_acceptance_evidence": True,
                "channels": {
                    "GA01.co2_signal": {
                        "status": "ok",
                        "stability_score": 0.91,
                        "low_freq_energy_ratio": 0.72,
                        "dominant_frequency_hz": 0.1,
                        "anomaly_flags": ["low_frequency_drift"],
                    }
                },
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    facade.service.orchestrator.run_state.artifacts.output_files.append(str(spectral_summary))

    result_a = facade.export_review_scope_manifest(
        selection={
            "scope": "source",
            "selected_source_label_display": "review_scope_case",
            "selected_source_dir": str(history_dir),
            "selected_source_artifact_paths": [str(suite_summary)],
        }
    )
    result_b = facade.export_review_scope_manifest(
        selection={
            "scope": "source",
            "selected_source_label_display": "review_scope_case",
            "selected_source_dir": str(history_dir),
            "selected_source_artifact_paths": [str(suite_summary)],
        }
    )

    assert result_a["ok"] is True
    assert result_b["ok"] is True
    assert result_a["batch_id"] != result_b["batch_id"]
    json_path_a = Path(result_a["json_path"])
    json_path_b = Path(result_b["json_path"])
    markdown_path_a = Path(result_a["markdown_path"])
    payload_a = json.loads(json_path_a.read_text(encoding="utf-8"))
    index_payload = json.loads(Path(result_b["index_path"]).read_text(encoding="utf-8"))

    assert json_path_a.exists()
    assert json_path_b.exists()
    assert markdown_path_a.exists()
    assert payload_a["selection"]["scope"] == "source"
    assert payload_a["scope_summary"]["scope_visible_count"] >= 1
    assert payload_a["disclaimer"]["not_real_acceptance_evidence"] is True
    assert any(row["artifact_origin"] == "review_reference" for row in payload_a["rows"])
    assert result_b["batch_id"] in result_b["message"]
    assert str(json_path_b) in result_b["message"]
    assert index_payload["entry_count"] >= 2
    assert index_payload["latest"]["batch_id"] == result_b["batch_id"]
    assert index_payload["previous"]["batch_id"] == result_a["batch_id"]
    assert index_payload["latest"]["selection_snapshot"]["selected_source_label_display"] == "review_scope_case"
    assert index_payload["latest"]["summary_counts"]["scope_visible_count"] >= 1
    assert index_payload["latest"]["disclaimer_flags"]["offline_review_only"] is True
    assert index_payload["latest"]["disclaimer_flags"]["not_real_acceptance_evidence"] is True
    assert index_payload["latest"]["spectral_quality"]["status"] == "ok"
    assert index_payload["latest"]["spectral_quality"]["not_real_acceptance_evidence"] is True


def test_app_facade_rejects_unsupported_export_format_in_chinese(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)

    result = facade.export_artifacts("xlsx")

    assert result["ok"] is False
    assert "不支持的导出格式" in result["message"]
    assert facade.get_error_snapshot()["message"] == result["message"]


def test_app_facade_builds_error_busy_and_notification_snapshots(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)
    facade.service.is_running = True
    facade.session.add_error("simulated error")

    snapshot = facade.build_snapshot()

    assert snapshot["error"]["visible"] is True
    assert "simulated error" in snapshot["error"]["message"]
    assert snapshot["busy"]["active"] is True
    assert snapshot["notifications"]["items"]


def test_app_facade_surfaces_spectral_quality_summary_as_sidecar_review_data(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)
    run_dir = Path(facade.result_store.run_dir)
    spectral_summary = run_dir / "spectral_quality_summary.json"
    spectral_summary.write_text(
        json.dumps(
            {
                "artifact_type": "spectral_quality_summary",
                "status": "ok",
                "channel_count": 1,
                "ok_channel_count": 1,
                "overall_score": 0.95,
                "flags": [],
                "not_real_acceptance_evidence": True,
                "channels": {
                    "GA01.co2_signal": {
                        "status": "ok",
                        "stability_score": 0.95,
                        "low_freq_energy_ratio": 0.18,
                        "dominant_frequency_hz": 0.125,
                        "anomaly_flags": [],
                    }
                },
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    facade.service.orchestrator.run_state.artifacts.output_files.append(str(spectral_summary))

    snapshot = facade.build_snapshot()
    analytics_item = next(item for item in snapshot["results"]["review_center"]["evidence_items"] if item["type"] == "analytics")
    report_rows = {str(row.get("name") or ""): dict(row) for row in snapshot["reports"]["files"]}

    assert snapshot["results"]["spectral_quality_summary"]["artifact_type"] == "spectral_quality_summary"
    assert snapshot["results"]["spectral_quality_digest"]["status"] == "ok"
    assert analytics_item["detail_qc_summary"]
    assert analytics_item["detail_spectral_summary"]
    assert any("GA01.co2_signal" in str(line) for line in list(analytics_item["detail_spectral_summary"]))
    assert report_rows["spectral_quality_summary.json"]["artifact_role"] == "diagnostic_analysis"


def test_app_facade_surfaces_offline_diagnostic_adapter_review_items(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)
    run_dir = Path(facade.result_store.run_dir)
    _write_offline_diagnostic_bundles(run_dir)

    results_snapshot = facade.build_results_snapshot()
    reports_snapshot = facade.get_reports_snapshot(results_snapshot=results_snapshot)
    offline_summary = dict(results_snapshot.get("offline_diagnostic_adapter_summary", {}) or {})
    offline_items = [
        dict(item)
        for item in list(results_snapshot["review_center"]["evidence_items"] or [])
        if item["type"] == "offline_diagnostic"
    ]
    report_rows = {
        str(row.get("path") or ""): dict(row)
        for row in reports_snapshot["files"]
    }

    assert offline_summary["found"] is True
    assert offline_summary["room_temp_count"] == 1
    assert offline_summary["analyzer_chain_count"] == 1
    assert offline_summary["artifact_count"] == 12
    assert offline_summary["primary_artifact_count"] == 2
    assert offline_summary["supporting_artifact_count"] == 8
    assert offline_summary["plot_count"] == 2
    assert offline_summary["coverage_summary"] == "room-temp 1 | analyzer-chain 1 | artifacts 12 | plots 2"
    assert offline_summary["review_scope_summary"] == "primary 2 | supporting 8 | plots 2"
    assert offline_summary["next_check_summary"] == "verify ambient chain | inspect analyzer chain"
    assert offline_summary["detail_lines"]
    assert offline_summary["review_highlight_lines"]
    assert offline_summary["detail_items"][0]["artifact_scope_summary"] == "artifacts 4 | plots 1"
    assert offline_summary["detail_items"][1]["artifact_scope_summary"] == "artifacts 8 | plots 1"
    assert offline_summary["latest_room_temp"]["recommended_variant"] == "ambient_open"
    assert offline_summary["latest_analyzer_chain"]["recommendation"] == "inspect analyzer chain"
    assert reports_snapshot["evidence_source"] == "simulated_protocol"
    assert reports_snapshot["not_real_acceptance_evidence"] is True
    assert "simulated_protocol" in results_snapshot["result_summary_text"]
    assert "simulated_protocol" in reports_snapshot["result_summary_text"]
    assert "离线诊断" in results_snapshot["result_summary_text"]
    assert "离线诊断" in reports_snapshot["result_summary_text"]
    assert "工件 12 | 图表 2" in results_snapshot["result_summary_text"]
    assert "主工件 2 | 支撑工件 8 | 图表 2" in results_snapshot["result_summary_text"]
    assert "工件范围: 工件 4 | 图表 1" in results_snapshot["result_summary_text"]
    assert "工件范围: 工件 8 | 图表 1" in reports_snapshot["result_summary_text"]
    assert "verify ambient chain | inspect analyzer chain" in reports_snapshot["result_summary_text"]
    assert "verify ambient chain" in results_snapshot["result_summary_text"]
    assert "inspect analyzer chain" in reports_snapshot["result_summary_text"]
    assert "real acceptance evidence" in results_snapshot["result_summary_text"]
    assert results_snapshot["review_center"]["latest"]["offline_diagnostic"]["available"] is True
    assert any(
        item["id"] == "offline_diagnostic"
        for item in list(results_snapshot["review_center"]["filters"]["type_options"] or [])
    )
    assert len(offline_items) == 2
    assert all(item["type_display"] for item in offline_items)
    assert all(item["detail_analytics_summary"] for item in offline_items)
    assert all(item["detail_lineage_summary"] for item in offline_items)
    assert all(
        any("工件范围" in str(line) for line in list(item["detail_analytics_summary"] or []))
        for item in offline_items
    )
    assert all(
        any("工件范围" in str(line) for line in list(item["detail_lineage_summary"] or []))
        for item in offline_items
    )
    assert any(
        any("工件 4 | 图表 1" in str(line) for line in list(item["detail_analytics_summary"] or []))
        for item in offline_items
    )
    assert any(
        any("工件 8 | 图表 1" in str(line) for line in list(item["detail_analytics_summary"] or []))
        for item in offline_items
    )
    assert any(item["path"].endswith("diagnostic_summary.json") for item in offline_items)
    assert any(item["path"].endswith("isolation_comparison_summary.json") for item in offline_items)
    assert report_rows[str((run_dir / "room_temp_diagnostic" / "diagnostic_summary.json").resolve())]["artifact_key"] == (
        "room_temp_diagnostic_summary"
    )
    assert report_rows[str((run_dir / "analyzer_chain_isolation" / "operator_checklist.md").resolve())]["artifact_key"] == (
        "analyzer_chain_operator_checklist"
    )


def test_app_facade_surfaces_point_taxonomy_summary_in_results_and_reports(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)
    _inject_point_taxonomy_summary(facade.result_store.run_dir)

    results_snapshot = facade.build_results_snapshot()
    reports_snapshot = facade.get_reports_snapshot(results_snapshot=results_snapshot)
    taxonomy = dict(results_snapshot.get("point_taxonomy_summary", {}) or {})

    assert taxonomy["pressure_summary"] == "ambient 1 | ambient_open 1"
    assert taxonomy["pressure_mode_summary"] == "ambient_open 2"
    assert taxonomy["pressure_target_label_summary"] == "ambient 1 | ambient_open 1"
    assert taxonomy["flush_gate_summary"] == "pass 1 | veto 1 | rebound 1"
    assert taxonomy["preseal_summary"] == "points 1 | max overshoot 4.2 hPa | max sealed wait 1200 ms"
    assert taxonomy["postseal_summary"] == "timeout blocked 1 | late rebound 1"
    assert taxonomy["stale_gauge_summary"] == "points 1 | worst 25%"
    assert reports_snapshot["point_taxonomy_summary"] == taxonomy
    assert "ambient 1 | ambient_open 1" in results_snapshot["result_summary_text"]
    assert "ambient_open 2" in results_snapshot["result_summary_text"]
    assert "pass 1 | veto 1 | rebound 1" in results_snapshot["result_summary_text"]
    assert "points 1 | worst 25%" in results_snapshot["result_summary_text"]
    assert "ambient 1 | ambient_open 1" in reports_snapshot["result_summary_text"]
    assert "timeout blocked 1 | late rebound 1" in reports_snapshot["result_summary_text"]


def test_app_facade_prefers_stored_point_taxonomy_handoff(tmp_path: Path) -> None:
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

    results_snapshot = facade.build_results_snapshot()
    reports_snapshot = facade.get_reports_snapshot(results_snapshot=results_snapshot)

    assert results_snapshot["point_taxonomy_summary"]["pressure_summary"] == "stored pressure taxonomy"
    assert results_snapshot["point_taxonomy_summary"]["pressure_mode_summary"] == "stored pressure mode taxonomy"
    assert results_snapshot["point_taxonomy_summary"]["flush_gate_summary"] == "stored flush taxonomy"
    assert reports_snapshot["point_taxonomy_summary"]["postseal_summary"] == "stored postseal taxonomy"
    assert "stored pressure taxonomy" in results_snapshot["result_summary_text"]
    assert "stored pressure mode taxonomy" in results_snapshot["result_summary_text"]
    assert "stored stale taxonomy" in reports_snapshot["result_summary_text"]


def test_app_facade_preferences_recent_runs_and_app_info(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)

    preferences = facade.get_preferences()
    saved = facade.save_preferences({"simulation_default": True, "screenshot_format": "txt"})
    recent_runs = facade.get_recent_runs()
    app_info = facade.get_app_info()

    assert preferences["auto_start_feed"] is True
    assert saved["simulation_default"] is True
    assert saved["screenshot_format"] == "txt"
    assert recent_runs
    assert recent_runs[0]["path"].endswith(facade.session.run_id)
    assert app_info["product_name"] == "气体校准 V2 驾驶舱"


def test_app_facade_from_config_path_uses_shared_v2_service_builder(tmp_path: Path) -> None:
    config_dir = tmp_path / "configs"
    config_dir.mkdir()
    points_path = config_dir / "points.json"
    points_path.write_text('{"points": []}', encoding="utf-8")
    config_path = config_dir / "app.json"
    config_path.write_text(
        (
            "{"
            '"devices": {"gas_analyzers": [{"port": "SIM-GA1", "enabled": true}]},'
            '"paths": {"points_excel": "points.json", "output_dir": "output", "logs_dir": "logs"},'
            '"features": {"simulation_mode": true}'
            "}"
        ),
        encoding="utf-8",
    )

    facade = AppFacade.from_config_path(str(config_path), simulation=True)

    assert facade.service._raw_cfg is not None
    assert Path(facade.service.config.paths.points_excel) == points_path.resolve()
    assert facade.service.config.features.simulation_mode is True


def test_app_facade_from_config_path_blocks_unsafe_step2_config_without_dual_unlock(tmp_path: Path, monkeypatch) -> None:
    config_dir = tmp_path / "configs"
    config_dir.mkdir()
    points_path = config_dir / "points.json"
    points_path.write_text('{"points": []}', encoding="utf-8")
    config_path = config_dir / "unsafe.json"
    config_path.write_text(
        json.dumps(
            {
                "devices": {
                    "pressure_controller": {"port": "COM31", "enabled": True},
                    "gas_analyzers": [{"port": "SIM-GA1", "enabled": True}],
                },
                "paths": {"points_excel": "points.json", "output_dir": "output", "logs_dir": "logs"},
                "features": {"simulation_mode": True},
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.delenv("GAS_CALIBRATOR_V2_ALLOW_UNSAFE_CONFIG", raising=False)

    with pytest.raises(RuntimeError, match="Step 2"):
        AppFacade.from_config_path(str(config_path), simulation=True)

    monkeypatch.setenv("GAS_CALIBRATOR_V2_ALLOW_UNSAFE_CONFIG", "1")
    facade = AppFacade.from_config_path(
        str(config_path),
        simulation=True,
        allow_unsafe_step2_config=True,
    )

    gate = dict(getattr(facade.service.config, "_step2_execution_gate", {}) or {})

    assert gate["status"] == "unlocked_override"


def test_app_facade_from_config_path_blocks_capture_then_hold_without_dual_unlock(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config_dir = tmp_path / "configs"
    config_dir.mkdir()
    points_path = config_dir / "points.json"
    points_path.write_text('{"points": []}', encoding="utf-8")
    config_path = config_dir / "capture_then_hold.json"
    config_path.write_text(
        json.dumps(
            {
                "devices": {
                    "pressure_controller": {"port": "SIM-PACE5000", "enabled": True},
                    "gas_analyzers": [{"port": "SIM-GA1", "enabled": True}],
                },
                "paths": {"points_excel": "points.json", "output_dir": "output", "logs_dir": "logs"},
                "features": {"simulation_mode": True},
                "workflow": {
                    "pressure": {
                        "capture_then_hold_enabled": True,
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.delenv("GAS_CALIBRATOR_V2_ALLOW_UNSAFE_CONFIG", raising=False)

    with pytest.raises(RuntimeError, match="Step 2"):
        AppFacade.from_config_path(str(config_path), simulation=True)

    with pytest.raises(RuntimeError, match="Step 2"):
        AppFacade.from_config_path(
            str(config_path),
            simulation=True,
            allow_unsafe_step2_config=True,
        )

    monkeypatch.setenv("GAS_CALIBRATOR_V2_ALLOW_UNSAFE_CONFIG", "1")
    facade = AppFacade.from_config_path(
        str(config_path),
        simulation=True,
        allow_unsafe_step2_config=True,
    )

    gate = dict(getattr(facade.service.config, "_step2_execution_gate", {}) or {})

    assert gate["status"] == "unlocked_override"
    assert facade.service.config.workflow.pressure["capture_then_hold_enabled"] is True


def test_app_facade_builds_points_preview_rows(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)

    preview = facade.preview_points()

    assert preview["ok"] is True
    assert "按真实执行顺序预览" in preview["summary"]
    assert preview["rows"][0]["route"] == "水路"
    assert preview["rows"][1]["route"] == "气路"

def test_app_facade_default_profile_preview_displays_ambient_pressure_label(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)
    gateway = facade.get_plan_gateway()
    gateway.save_profile(
        {
            "name": "ambient_default",
            "is_default": True,
            "temperatures": [{"temperature_c": 25.0, "enabled": True}],
            "humidities": [{"hgen_temp_c": 25.0, "hgen_rh_pct": 35.0, "enabled": True}],
            "gas_points": [{"co2_ppm": 400.0, "co2_group": "B", "enabled": True}],
            "pressures": [{"pressure_hpa": 1100.0, "enabled": True}, {"pressure_hpa": 900.0, "enabled": True}],
            "ordering": {"selected_pressure_points": [AMBIENT_PRESSURE_TOKEN, 900.0]},
        }
    )

    preview = facade.preview_points(points_source="use_default_profile")
    compiled = gateway.build_default_runtime_points_file()
    preview_parser = facade._build_preview_parser()
    preview_planner = RoutePlanner(facade.config, preview_parser)
    preview_points = facade._load_preview_points(
        Path(compiled["path"]),
        point_parser=preview_parser,
        route_planner=preview_planner,
    )
    ambient_point = next(point for point in preview_points if point.is_ambient_pressure_point)

    assert preview["ok"] is True
    assert any(row["pressure"] == AMBIENT_PRESSURE_LABEL for row in preview["rows"])
    assert AMBIENT_PRESSURE_LABEL in AppFacade._point_to_text(ambient_point)


def test_app_facade_applies_analyzer_setup_from_default_profile_at_start(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)
    gateway = facade.get_plan_gateway()
    gateway.save_profile(
        {
            "name": "default_runtime",
            "is_default": True,
            "analyzer_setup": {
                "software_version": "pre_v5",
                "device_id_assignment_mode": "manual",
                "start_device_id": "7",
                "manual_device_ids": ["011", "012"],
            },
            "temperatures": [{"temperature_c": 25.0, "enabled": True}],
            "gas_points": [{"co2_ppm": 400.0, "co2_group": "B", "cylinder_nominal_ppm": 405.0, "enabled": True}],
            "pressures": [{"pressure_hpa": 1000.0, "enabled": True}],
        }
    )

    ok, _ = facade.start(points_source="use_default_profile")

    assert ok is True
    assert facade.service.config.workflow.analyzer_setup["software_version"] == "pre_v5"
    assert facade.service.config.workflow.analyzer_setup["device_id_assignment_mode"] == "manual"
    assert facade.service.config.workflow.analyzer_setup["start_device_id"] == "007"
    assert facade.service.config.workflow.analyzer_setup["manual_device_ids"] == ["011", "012"]


def test_app_facade_preview_honors_runtime_water_first_threshold_and_all_temps(tmp_path: Path) -> None:
    points_path = tmp_path / "points.json"
    points_path.write_text(
        json.dumps(
            {
                "points": [
                    {"index": 1, "temperature_c": 20.0, "co2_ppm": 400.0, "pressure_hpa": 1000.0, "route": "co2"},
                    {"index": 2, "temperature_c": 20.0, "humidity_pct": 35.0, "pressure_hpa": 1000.0, "route": "h2o"},
                    {"index": 3, "temperature_c": 30.0, "co2_ppm": 800.0, "pressure_hpa": 1000.0, "route": "co2"},
                    {"index": 4, "temperature_c": 30.0, "humidity_pct": 40.0, "pressure_hpa": 1000.0, "route": "h2o"},
                ]
            }
        ),
        encoding="utf-8",
    )

    threshold_facade = AppFacade(
        config=AppConfig.from_dict(
            {
                "workflow": {"water_first_temp_gte": 25.0},
                "paths": {"points_excel": str(points_path), "output_dir": str(tmp_path / "threshold_out")},
                "features": {"simulation_mode": True},
            }
        ),
        simulation=True,
    )
    threshold_parser = threshold_facade._build_preview_parser()
    threshold_planner = RoutePlanner(threshold_facade.config, threshold_parser)
    threshold_points = threshold_facade._load_preview_points(
        points_path,
        point_parser=threshold_parser,
        route_planner=threshold_planner,
    )

    assert [point.route for point in threshold_points if point.temperature_c == 30.0] == ["h2o", "co2"]
    assert [point.route for point in threshold_points if point.temperature_c == 20.0] == ["co2", "h2o"]

    all_temps_facade = AppFacade(
        config=AppConfig.from_dict(
            {
                "workflow": {
                    "water_first_all_temps": True,
                    "water_first_temp_gte": 25.0,
                },
                "paths": {"points_excel": str(points_path), "output_dir": str(tmp_path / "all_temps_out")},
                "features": {"simulation_mode": True},
            }
        ),
        simulation=True,
    )
    all_temps_parser = all_temps_facade._build_preview_parser()
    all_temps_planner = RoutePlanner(all_temps_facade.config, all_temps_parser)
    all_temps_points = all_temps_facade._load_preview_points(
        points_path,
        point_parser=all_temps_parser,
        route_planner=all_temps_planner,
    )

    assert [point.route for point in all_temps_points if point.temperature_c == 30.0] == ["h2o", "co2"]
    assert [point.route for point in all_temps_points if point.temperature_c == 20.0] == ["h2o", "co2"]


def test_app_facade_preview_execution_order_matches_runtime_execution_order(tmp_path: Path) -> None:
    points_path = tmp_path / "points.json"
    points_path.write_text(
        json.dumps(
            {
                "points": [
                    {"index": 1, "temperature_c": 30.0, "co2_ppm": 800.0, "pressure_hpa": 1000.0, "route": "co2"},
                    {"index": 2, "temperature_c": 30.0, "humidity_pct": 45.0, "pressure_hpa": 1000.0, "route": "h2o"},
                    {"index": 3, "temperature_c": 20.0, "co2_ppm": 400.0, "pressure_hpa": 1000.0, "route": "co2"},
                    {"index": 4, "temperature_c": 20.0, "humidity_pct": 35.0, "pressure_hpa": 1000.0, "route": "h2o"},
                    {"index": 5, "temperature_c": -10.0, "co2_ppm": 0.0, "pressure_hpa": 1000.0, "route": "co2"},
                    {"index": 6, "temperature_c": -10.0, "humidity_pct": 15.0, "pressure_hpa": 1000.0, "route": "h2o"},
                ]
            }
        ),
        encoding="utf-8",
    )
    facade = AppFacade(
        config=AppConfig.from_dict(
            {
                "workflow": {
                    "selected_temps_c": [30.0, 20.0, -10.0],
                    "water_first_temp_gte": 25.0,
                },
                "paths": {"points_excel": str(points_path), "output_dir": str(tmp_path / "run_out")},
                "features": {"simulation_mode": True},
            }
        ),
        simulation=True,
    )

    preview = facade.preview_points()
    assert preview["ok"] is True

    preview_parser = facade._build_preview_parser()
    preview_planner = RoutePlanner(facade.config, preview_parser)
    preview_points = facade._load_preview_points(
        points_path,
        point_parser=preview_parser,
        route_planner=preview_planner,
    )
    preview_execution = facade._preview_points_in_execution_order(
        preview_points,
        route_planner=preview_planner,
    )

    facade.service.load_points(str(points_path))
    runtime_execution = facade._preview_points_in_execution_order(
        list(facade.service._points),
        route_planner=facade.service.orchestrator.route_planner,
    )

    assert _execution_signature(preview_execution) == _execution_signature(runtime_execution)
    assert all(point.route == "co2" for point in runtime_execution if point.temperature_c < 0.0)


def test_app_facade_opens_points_file_for_editing(tmp_path: Path, monkeypatch) -> None:
    facade = build_fake_facade(tmp_path)
    opened: list[str] = []

    monkeypatch.setattr(
        "gas_calibrator.v2.ui_v2.controllers.app_facade.os.startfile",
        lambda path: opened.append(str(path)),
        raising=False,
    )

    ok, message = facade.edit_points_file()

    assert ok is True
    assert message.startswith("正在编辑点表：")
    assert opened == [str(facade.service.config.paths.points_excel)]


def test_app_facade_exposes_plan_gateway_without_repeating_profile_api(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)
    gateway = facade.get_plan_gateway()
    payload = {
        "name": "ui_profile",
        "description": "UI editable plan",
        "temperatures": [{"temperature_c": 25.0, "enabled": True}],
        "humidities": [{"hgen_temp_c": 25.0, "hgen_rh_pct": 45.0, "enabled": True}],
        "gas_points": [{"co2_ppm": 400.0, "enabled": True}],
        "pressures": [{"pressure_hpa": 1000.0, "enabled": True}],
        "ordering": {
            "water_first": True,
            "selected_temps_c": [25.0],
            "skip_co2_ppm": [0],
            "temperature_descending": True,
        },
    }

    saved = gateway.save_profile(payload, set_default=True)
    loaded = gateway.load_profile("ui_profile")
    listed = gateway.list_profiles()
    preview = gateway.compile_profile_preview("ui_profile")

    assert facade.plan_gateway is gateway
    assert saved["name"] == "ui_profile"
    assert loaded is not None
    assert loaded["ordering"]["skip_co2_ppm"] == [0]
    assert listed[0]["name"] == "ui_profile"
    assert listed[0]["is_default"] is True
    assert preview["ok"] is True
    assert preview["rows"]
    assert preview["runtime_payload"]["points"]


def test_app_facade_can_preview_and_start_from_default_profile_without_replacing_points_file(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)
    gateway = facade.get_plan_gateway()
    gateway.save_profile(
        {
            "name": "default_run",
            "is_default": True,
            "run_mode": "co2_measurement",
            "temperatures": [{"temperature_c": 25.0, "enabled": True}],
            "humidities": [{"hgen_temp_c": 25.0, "hgen_rh_pct": 35.0, "enabled": True}],
            "gas_points": [{"co2_ppm": 400.0, "enabled": True}],
            "pressures": [{"pressure_hpa": 1000.0, "enabled": True}],
            "ordering": {"skip_co2_ppm": [0], "water_first": True},
        }
    )

    preview = facade.preview_points(points_source="use_default_profile")
    ok_start, message = facade.start(points_source="use_default_profile")
    compiled_path = Path(facade.service.start_calls[-1])

    assert preview["ok"] is True
    assert "默认配置档 default_run" in preview["summary"]
    assert preview["run_mode"] == "co2_measurement"
    assert ok_start is True
    assert "默认配置档" in message
    assert compiled_path.exists()
    assert facade.service.config.workflow.run_mode == "co2_measurement"
    assert facade.service.config.workflow.route_mode == "co2_only"
    assert json.loads(compiled_path.read_text(encoding="utf-8"))["points"]
    assert Path(facade.config.paths.points_excel).exists()


def test_app_facade_preview_points_can_apply_points_file_run_mode_override(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)

    preview = facade.preview_points(run_mode="h2o_measurement")

    assert preview["ok"] is True
    assert preview["run_mode"] == "h2o_measurement"
    assert preview["route_mode"] == "h2o_only"
    assert preview["formal_calibration_report"] is False


def test_app_facade_build_snapshot_prefers_current_primary_validation_status(tmp_path: Path, monkeypatch) -> None:
    facade = build_fake_facade(tmp_path)
    validation_root = tmp_path / "validation_compare"
    validation_root.mkdir(parents=True, exist_ok=True)
    stale_h2o_path = validation_root / "h2o_only_replacement_latest.json"
    stale_h2o_path.write_text(
        json.dumps(
            {
                "validation_profile": "h2o_only_replacement",
                "checklist_gate": "12B",
                "evidence_state": "stale_diagnostic_only",
                "compare_status": "NOT_EXECUTED",
                "first_failure_phase": "v2:precheck.sensor_check",
                "entered_target_route": {"v1": True, "v2": False},
                "target_route_event_count": {"v1": 12, "v2": 0},
                "bench_context": {
                    "co2_0ppm_available": False,
                    "other_gases_available": True,
                    "primary_replacement_route": "skip0_co2_only_replacement",
                    "validation_role": "diagnostic",
                },
                "route_execution_summary": {
                    "target_route": "h2o",
                    "bench_context": {"co2_0ppm_available": False, "other_gases_available": True},
                },
                "report_dir": str(validation_root / "run_1"),
                "artifacts": {
                    "h2o_only_replacement_bundle": str(validation_root / "run_1" / "h2o_only_replacement_bundle.json"),
                },
            }
        ),
        encoding="utf-8",
    )
    latest_path = validation_root / "skip0_co2_only_replacement_latest.json"
    latest_path.write_text(
        json.dumps(
            {
                "validation_profile": "skip0_co2_only_replacement",
                "checklist_gate": "12A",
                "compare_status": "NOT_EXECUTED",
                "first_failure_phase": "v2:startup.sensor_precheck",
                "entered_target_route": {"v1": False, "v2": False},
                "target_route_event_count": {"v1": 0, "v2": 0},
                "bench_context": {
                    "co2_0ppm_available": False,
                    "other_gases_available": True,
                    "h2o_route_available": False,
                    "humidity_generator_humidity_feedback_valid": False,
                    "primary_replacement_route": "skip0_co2_only_replacement",
                    "validation_role": "primary",
                    "target_route": "co2",
                },
                "route_execution_summary": {
                    "target_route": "co2",
                    "route_physical_state_match": {"v1": True, "v2": True},
                    "relay_physical_mismatch": {"v1": False, "v2": False},
                    "sides": {
                        "v1": {
                            "target_open_valves": [3, 7],
                            "actual_open_valves": [3, 7],
                            "target_relay_state": {"relay_a": {"3": True}},
                            "actual_relay_state": {"relay_a": {"3": True}},
                            "cleanup_all_relays_off": True,
                            "cleanup_relay_state": {"relay_a": {"3": False}},
                        },
                        "v2": {
                            "target_open_valves": [3, 7],
                            "actual_open_valves": [3, 7],
                            "target_relay_state": {"relay_a": {"3": True}},
                            "actual_relay_state": {"relay_a": {"3": True}},
                            "cleanup_all_relays_off": True,
                            "cleanup_relay_state": {"relay_a": {"3": False}},
                        },
                    },
                    "bench_context": {"co2_0ppm_available": False, "other_gases_available": True},
                },
                "reference_quality": {
                    "reference_quality": "healthy",
                    "reference_integrity": "healthy",
                    "reference_quality_degraded": False,
                    "thermometer_reference_status": "healthy",
                    "pressure_reference_status": "healthy",
                },
                "report_dir": str(validation_root / "run_2"),
                "artifacts": {
                    "skip0_co2_only_replacement_bundle": str(
                        validation_root / "run_2" / "skip0_co2_only_replacement_bundle.json"
                    ),
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "gas_calibrator.v2.ui_v2.controllers.app_facade.VALIDATION_LATEST_INDEXES",
        (
            ("h2o_only_replacement", stale_h2o_path),
            ("skip0_co2_only_replacement", latest_path),
        ),
    )

    snapshot = facade.build_snapshot()

    assert snapshot["validation"]["available"] is True
    assert snapshot["validation"]["validation_profile"] == "skip0_co2_only_replacement"
    assert snapshot["validation"]["compare_status"] == "NOT_EXECUTED"
    assert snapshot["validation"]["gate_state"]["checklist_gate"] == "12A"
    assert snapshot["validation"]["evidence_source"] == "real"
    assert snapshot["validation"]["bench_context"]["h2o_route_available"] is False
    assert snapshot["validation"]["reference_quality"]["reference_quality"] == "healthy"
    assert snapshot["validation"]["route_physical_validation"]["route_physical_state_match"]["v2"] is True
    assert snapshot["run"]["validation"]["artifact_bundle_path"].endswith("skip0_co2_only_replacement_bundle.json")
    assert snapshot["validation"]["evidence_layers"][0]["tier"] == "primary"


def test_app_facade_build_snapshot_deprioritizes_legacy_mixed_route_status(tmp_path: Path, monkeypatch) -> None:
    facade = build_fake_facade(tmp_path)
    validation_root = tmp_path / "validation_compare"
    validation_root.mkdir(parents=True, exist_ok=True)
    legacy_path = validation_root / "skip0_replacement_latest.json"
    legacy_path.write_text(
        json.dumps(
            {
                "validation_profile": "skip0_replacement",
                "checklist_gate": "12A",
                "evidence_state": "superseded_mixed_route_validation",
                "bench_context": {
                    "primary_replacement_route": "skip0_co2_only_replacement",
                    "validation_role": "legacy_mixed_route",
                },
            }
        ),
        encoding="utf-8",
    )
    diagnostic_path = validation_root / "h2o_only_replacement_latest.json"
    diagnostic_path.write_text(
        json.dumps(
            {
                "validation_profile": "h2o_only_replacement",
                "checklist_gate": "12B",
                "bench_context": {
                    "primary_replacement_route": "skip0_co2_only_replacement",
                    "validation_role": "diagnostic",
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "gas_calibrator.v2.ui_v2.controllers.app_facade.VALIDATION_LATEST_INDEXES",
        (
            ("skip0_replacement", legacy_path),
            ("h2o_only_replacement", diagnostic_path),
        ),
    )

    snapshot = facade.build_snapshot()

    assert snapshot["validation"]["validation_profile"] == "skip0_replacement"
    assert snapshot["validation"]["gate_state"]["checklist_gate"] == "12A"


def test_app_facade_build_snapshot_marks_primary_latest_missing_instead_of_falling_back(
    tmp_path: Path,
    monkeypatch,
) -> None:
    facade = build_fake_facade(tmp_path)
    validation_root = tmp_path / "validation_compare"
    validation_root.mkdir(parents=True, exist_ok=True)
    diagnostic_path = validation_root / "skip0_co2_only_diagnostic_relaxed_latest.json"
    diagnostic_path.write_text(
        json.dumps(
            {
                "validation_profile": "skip0_co2_only_diagnostic_relaxed",
                "checklist_gate": "12A",
                "compare_status": "NOT_EXECUTED",
                "evidence_state": "route_unblock_diagnostic",
                "diagnostic_only": True,
                "acceptance_evidence": False,
                "bench_context": {
                    "primary_replacement_route": "skip0_co2_only_replacement",
                    "validation_role": "diagnostic_route_unblock",
                    "target_route": "co2",
                    "h2o_route_available": False,
                },
                "route_execution_summary": {
                    "target_route": "co2",
                    "compare_status": "NOT_EXECUTED",
                    "valid_for_route_diff": False,
                },
                "source_latest_index_path": str(diagnostic_path),
            }
        ),
        encoding="utf-8",
    )
    stale_h2o_path = validation_root / "h2o_only_replacement_latest.json"
    stale_h2o_path.write_text(
        json.dumps(
            {
                "validation_profile": "h2o_only_replacement",
                "checklist_gate": "12B",
                "compare_status": "NOT_EXECUTED",
                "evidence_state": "stale_diagnostic_only",
                "diagnostic_only": True,
                "bench_context": {
                    "primary_replacement_route": "skip0_co2_only_replacement",
                    "validation_role": "diagnostic",
                },
                "source_latest_index_path": str(stale_h2o_path),
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "gas_calibrator.v2.ui_v2.controllers.app_facade.VALIDATION_LATEST_INDEXES",
        (
            ("skip0_co2_only_diagnostic_relaxed", diagnostic_path),
            ("h2o_only_replacement", stale_h2o_path),
            ("skip0_co2_only_replacement", validation_root / "skip0_co2_only_replacement_latest.json"),
        ),
    )

    snapshot = facade.build_snapshot()

    assert snapshot["validation"]["validation_profile"] == "skip0_co2_only_replacement"
    assert snapshot["validation"]["compare_status"] == "PRIMARY_REAL_VALIDATION_LATEST_MISSING"
    assert snapshot["validation"]["primary_latest_missing"] is True
    assert snapshot["validation"]["primary_real_latest_missing"] is True
    assert snapshot["validation"]["diagnostic_only"] is False
    assert snapshot["validation"]["acceptance_evidence"] is True
    assert snapshot["validation"]["evidence_state"] == "primary_validation_latest_missing"
    assert snapshot["validation"]["gate_state"]["checklist_gate"] == "12A"
    assert snapshot["validation"]["gate_state"]["target_route"] == "co2"
    assert snapshot["validation"]["fallback_candidates"][0]["validation_profile"] == (
        "skip0_co2_only_diagnostic_relaxed"
    )
    assert snapshot["validation"]["evidence_layers"][0]["tier"] == "primary"
    assert snapshot["validation"]["evidence_layers"][1]["tier"] == "diagnostic"
    assert snapshot["validation"]["evidence_layers"][2]["tier"] == "stale"


def test_app_facade_build_snapshot_keeps_simulated_evidence_separate_from_real_primary(
    tmp_path: Path,
    monkeypatch,
) -> None:
    facade = build_fake_facade(tmp_path)
    validation_root = tmp_path / "validation_compare"
    validation_root.mkdir(parents=True, exist_ok=True)
    simulated_path = validation_root / "replacement_full_route_simulated_latest.json"
    simulated_path.write_text(
        json.dumps(
            {
                "validation_profile": "replacement_full_route_simulated",
                "checklist_gate": "SIM-FULL",
                "compare_status": "MATCH",
                "evidence_source": "simulated",
                "evidence_state": "simulated_acceptance_like_coverage",
                "diagnostic_only": False,
                "acceptance_evidence": False,
                "not_real_acceptance_evidence": True,
                "bench_context": {"target_route": "h2o_then_co2", "validation_role": "simulated_acceptance_like_coverage"},
                "simulation_context": {"scenario": "full_route_success_all_temps_all_sources"},
                "source_latest_index_path": str(simulated_path),
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "gas_calibrator.v2.ui_v2.controllers.app_facade.VALIDATION_LATEST_INDEXES",
        (("skip0_co2_only_replacement", validation_root / "skip0_co2_only_replacement_latest.json"),),
    )
    monkeypatch.setattr(
        "gas_calibrator.v2.ui_v2.controllers.app_facade.SIMULATED_VALIDATION_LATEST_INDEXES",
        (("replacement_full_route_simulated", simulated_path),),
    )

    snapshot = facade.build_snapshot()

    assert snapshot["validation"]["compare_status"] == "PRIMARY_REAL_VALIDATION_LATEST_MISSING"
    assert snapshot["validation"]["evidence_layers"][1]["tier"] == "simulated_coverage"
    assert snapshot["validation"]["evidence_layers"][1]["evidence_source"] == "simulated_protocol"


def test_app_facade_marks_profile_disabled_devices_as_skipped(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)

    snapshot = facade.build_snapshot()

    device_rows = {row["name"]: row for row in snapshot["run"]["device_rows"]}
    assert device_rows["humidity_generator"]["status"] == "skipped_by_profile"
    assert device_rows["dewpoint_meter"]["status"] == "skipped_by_profile"
    assert sorted(snapshot["run"]["profile_skipped_devices"]) == ["dewpoint_meter", "humidity_generator"]
    health_rows = {row["analyzer"]: row for row in snapshot["analyzer_health"]["rows"]}
    assert "skipped_by_profile" in health_rows["humidity_generator"]["note"]


def test_app_facade_builds_review_digest_for_offline_evidence(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)
    run_dir = Path(facade.result_store.run_dir)
    run_dir.joinpath("suite_summary.json").write_text(
        json.dumps(
            {
                "suite": "regression",
                "counts": {"passed": 5, "total": 5},
                "all_passed": True,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    run_dir.joinpath("summary_parity_report.json").write_text(
        json.dumps(
            {
                "status": "MATCH",
                "summary": {"cases_matched": 3, "cases_total": 3, "failed_cases": []},
                "evidence_source": "simulated",
                "not_real_acceptance_evidence": True,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    run_dir.joinpath("export_resilience_report.json").write_text(
        json.dumps(
            {
                "status": "MATCH",
                "cases": [{"status": "MATCH"}, {"status": "MATCH"}],
                "evidence_source": "simulated",
                "not_real_acceptance_evidence": True,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    facade.execute_device_workbench_action(
        "relay",
        "run_preset",
        preset_id="stuck_channel",
        relay_name="relay_8",
        channel=1,
    )
    facade.execute_device_workbench_action(
        "workbench",
        "generate_diagnostic_evidence",
        current_device="relay",
        current_action="run_preset",
    )

    results_snapshot = facade.build_results_snapshot()
    reports_snapshot = facade.get_reports_snapshot(results_snapshot=results_snapshot)
    digest = results_snapshot["review_digest"]
    report_rows = {str(row.get("name") or ""): dict(row) for row in reports_snapshot["files"]}

    assert digest["items"]["suite"]["available"] is True
    assert digest["items"]["parity"]["available"] is True
    assert digest["items"]["resilience"]["available"] is True
    assert digest["items"]["workbench"]["available"] is True
    assert digest["items"]["acceptance_readiness"]["simulated_readiness_only"] is True
    assert report_rows["summary_parity_report.json"]["artifact_role"] == "diagnostic_analysis"
    assert report_rows["summary_parity_report.json"]["export_status_known"] is False
    assert report_rows["export_resilience_report.json"]["artifact_role"] == "diagnostic_analysis"
    assert report_rows["export_resilience_report.json"]["export_status_known"] is False
    assert results_snapshot["workbench_action_report"]["evidence_source"] == "simulated_protocol"
    assert results_snapshot["workbench_action_report"]["config_safety"]["classification"] == (
        "simulation_real_port_inventory_risk"
    )
    assert "不代表真实 acceptance" in digest["summary_text"]
    assert "最新套件" in reports_snapshot["review_digest_text"]


def test_app_facade_prefers_top_level_summary_config_safety_review(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)
    summary_path = Path(facade.result_store.run_dir) / "summary.json"
    summary_payload = json.loads(summary_path.read_text(encoding="utf-8"))
    summary_payload["config_safety"] = dict(summary_payload["stats"]["config_safety"])
    summary_payload["config_safety_review"] = dict(summary_payload["stats"]["config_safety_review"])
    summary_payload["config_safety_review"]["status"] = "unlocked_override"
    summary_payload["config_safety_review"]["summary"] = "top-level review override"
    summary_payload["config_safety_review"]["warnings"] = ["top-level warning"]
    summary_payload["config_safety_review"]["execution_gate"] = {
        **dict(summary_payload["config_safety_review"].get("execution_gate") or {}),
        "status": "unlocked_override",
        "summary": "top-level gate override",
    }
    summary_payload["stats"].pop("config_safety", None)
    summary_payload["stats"].pop("config_safety_review", None)
    summary_path.write_text(json.dumps(summary_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    results_snapshot = facade.build_results_snapshot()
    reports_snapshot = facade.get_reports_snapshot(results_snapshot=results_snapshot)

    assert results_snapshot["config_safety_review"]["status"] == "unlocked_override"
    assert results_snapshot["config_safety_review"]["summary"] == "top-level gate override"
    assert results_snapshot["config_safety_review"]["warnings"] == ["top-level warning"]
    assert results_snapshot["config_safety_review"]["execution_gate"]["status"] == "unlocked_override"
    assert results_snapshot["config_governance_handoff"]["execution_gate"]["status"] == "unlocked_override"
    assert reports_snapshot["config_safety_review"]["execution_gate"]["status"] == "unlocked_override"
    assert reports_snapshot["config_governance_handoff"]["execution_gate"]["status"] == "unlocked_override"


def test_app_facade_reads_config_safety_from_evidence_registry_when_summary_missing(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)
    run_dir = Path(facade.result_store.run_dir)
    summary_path = run_dir / "summary.json"
    evidence_registry_path = run_dir / "evidence_registry.json"

    summary_payload = json.loads(summary_path.read_text(encoding="utf-8"))
    summary_payload.pop("config_safety", None)
    summary_payload.pop("config_safety_review", None)
    summary_payload.get("stats", {}).pop("config_safety", None)
    summary_payload.get("stats", {}).pop("config_safety_review", None)
    summary_path.write_text(json.dumps(summary_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    evidence_registry = json.loads(evidence_registry_path.read_text(encoding="utf-8"))
    evidence_registry["config_safety"] = {
        "classification": "simulation_real_port_inventory_risk",
        "summary": "registry safety",
        "execution_gate": {"status": "blocked"},
    }
    evidence_registry["config_safety_review"] = {
        "status": "unlocked_override",
        "summary": "registry override",
        "warnings": ["registry warning"],
        "execution_gate": {"status": "unlocked_override", "summary": "registry gate"},
    }
    evidence_registry_path.write_text(json.dumps(evidence_registry, ensure_ascii=False, indent=2), encoding="utf-8")

    results_snapshot = facade.build_results_snapshot()
    reports_snapshot = facade.get_reports_snapshot(results_snapshot=results_snapshot)

    assert results_snapshot["config_safety"]["classification"] == "simulation_real_port_inventory_risk"
    assert results_snapshot["config_safety_review"]["status"] == "unlocked_override"
    assert results_snapshot["config_safety_review"]["warnings"] == ["registry warning"]
    assert results_snapshot["config_governance_handoff"]["execution_gate"]["status"] == "unlocked_override"
    assert reports_snapshot["config_safety_review"]["execution_gate"]["status"] == "unlocked_override"
    assert reports_snapshot["config_governance_handoff"]["execution_gate"]["status"] == "unlocked_override"


def test_app_facade_reads_config_safety_from_analytics_summary_when_summary_and_registry_missing(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)
    run_dir = Path(facade.result_store.run_dir)
    summary_path = run_dir / "summary.json"
    evidence_registry_path = run_dir / "evidence_registry.json"
    analytics_summary_path = run_dir / "analytics_summary.json"

    summary_payload = json.loads(summary_path.read_text(encoding="utf-8"))
    summary_payload.pop("config_safety", None)
    summary_payload.pop("config_safety_review", None)
    summary_payload.get("stats", {}).pop("config_safety", None)
    summary_payload.get("stats", {}).pop("config_safety_review", None)
    summary_path.write_text(json.dumps(summary_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    evidence_registry = json.loads(evidence_registry_path.read_text(encoding="utf-8"))
    evidence_registry.pop("config_safety", None)
    evidence_registry.pop("config_safety_review", None)
    evidence_registry_path.write_text(json.dumps(evidence_registry, ensure_ascii=False, indent=2), encoding="utf-8")

    analytics_summary = json.loads(analytics_summary_path.read_text(encoding="utf-8"))
    analytics_summary["config_safety"] = {
        "classification": "simulation_real_port_inventory_risk",
        "summary": "analytics safety",
        "execution_gate": {"status": "blocked"},
    }
    analytics_summary["config_safety_review"] = {
        "status": "analytics_override",
        "summary": "analytics override",
        "warnings": ["analytics warning"],
        "execution_gate": {"status": "analytics_override", "summary": "analytics gate"},
    }
    analytics_summary_path.write_text(json.dumps(analytics_summary, ensure_ascii=False, indent=2), encoding="utf-8")

    results_snapshot = facade.build_results_snapshot()
    reports_snapshot = facade.get_reports_snapshot(results_snapshot=results_snapshot)

    assert results_snapshot["analytics_summary"]["config_safety_review"]["status"] == "analytics_override"
    assert results_snapshot["config_safety"]["classification"] == "simulation_real_port_inventory_risk"
    assert results_snapshot["config_safety_review"]["status"] == "analytics_override"
    assert results_snapshot["config_safety_review"]["warnings"] == ["analytics warning"]
    assert results_snapshot["config_governance_handoff"]["execution_gate"]["status"] == "analytics_override"
    assert reports_snapshot["config_safety_review"]["execution_gate"]["status"] == "analytics_override"
    assert reports_snapshot["config_governance_handoff"]["execution_gate"]["status"] == "analytics_override"
