import json
from pathlib import Path
import sys
import time

from gas_calibrator.v2.core.phase_transition_bridge_presenter import (
    build_phase_transition_bridge_panel_payload,
)
from gas_calibrator.v2.core.phase_transition_bridge_reviewer_artifact import (
    build_phase_transition_bridge_reviewer_artifact,
)
from gas_calibrator.v2.core.phase_transition_bridge_reviewer_artifact_entry import (
    build_phase_transition_bridge_reviewer_artifact_entry,
)
from gas_calibrator.v2.core.stage_admission_review_pack import STAGE_ADMISSION_REVIEW_PACK_REVIEWER_FILENAME
from gas_calibrator.v2.scripts.build_offline_governance_artifacts import rebuild_run
from gas_calibrator.v2.ui_v2.i18n import t
from gas_calibrator.v2.ui_v2.widgets.review_center_panel import ReviewCenterPanel

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import build_fake_facade, make_root


def _build_phase_transition_bridge_payload() -> dict:
    return {
        "artifact_type": "phase_transition_bridge",
        "phase": "step2_tail_stage3_bridge",
        "overall_status": "ready_for_engineering_isolation",
        "recommended_next_stage": "engineering_isolation",
        "ready_for_engineering_isolation": True,
        "real_acceptance_ready": False,
        "execute_now_in_step2_tail": [
            "contract_schema_digest_reporting",
            "governance_artifact_export",
        ],
        "defer_to_stage3_real_validation": [
            "real_reference_instrument_enforcement",
            "real_acceptance_pass_fail",
        ],
        "blocking_items": [],
        "warning_items": ["phase_transition_bridge_not_real_acceptance"],
        "reviewer_display": {
            "summary_text": "阶段桥工件：统一说明离第三阶段真实验证还有多远，不是 real acceptance。",
            "status_line": "阶段状态：当前仍处于 Step 2 tail / Stage 3 bridge，但已具备 engineering-isolation 准备。不是 real acceptance。",
            "current_stage_text": "当前阶段：Step 2 tail / Stage 3 bridge。",
            "next_stage_text": "下一阶段：进入 engineering-isolation，继续准备 Stage 3 real validation。",
            "execute_now_text": "现在执行：contract_schema_digest_reporting / governance_artifact_export。",
            "defer_to_stage3_text": "第三阶段执行：real_reference_instrument_enforcement / real_acceptance_pass_fail。",
            "blocking_text": "阻塞项：无。",
            "warning_text": "提示：不能替代真实计量验证。",
            "gate_lines": [],
        },
    }


def _build_phase_transition_bridge_reviewer_entry() -> dict:
    bridge = _build_phase_transition_bridge_payload()
    reviewer_artifact = build_phase_transition_bridge_reviewer_artifact(bridge)
    return build_phase_transition_bridge_reviewer_artifact_entry(
        artifact_path="D:/tmp/history_run/phase_transition_bridge_reviewer.md",
        manifest_section={
            "artifact_type": reviewer_artifact["artifact_type"],
            "path": "D:/tmp/history_run/phase_transition_bridge_reviewer.md",
            "available": True,
            "summary_text": reviewer_artifact["display"]["summary_text"],
            "status_line": reviewer_artifact["display"]["status_line"],
            "current_stage_text": reviewer_artifact["display"]["current_stage_text"],
            "next_stage_text": reviewer_artifact["display"]["next_stage_text"],
            "engineering_isolation_text": reviewer_artifact["display"]["engineering_isolation_text"],
            "real_acceptance_text": reviewer_artifact["display"]["real_acceptance_text"],
            "execute_now_text": reviewer_artifact["display"]["execute_now_text"],
            "defer_to_stage3_text": reviewer_artifact["display"]["defer_to_stage3_text"],
            "blocking_text": reviewer_artifact["display"]["blocking_text"],
            "warning_text": reviewer_artifact["display"]["warning_text"],
            "not_real_acceptance_evidence": True,
        },
        reviewer_section=reviewer_artifact["section"],
    )


def test_review_center_aggregates_multi_evidence_and_acceptance_readiness(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)
    run_dir = Path(facade.result_store.run_dir)
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "suite_summary.json").write_text(
        json.dumps(
            {
                "suite": "regression",
                "generated_at": "2099-03-27T08:00:00",
                "counts": {"passed": 5, "total": 5},
                "all_passed": True,
                "evidence_source": "simulated_protocol",
                "evidence_state": "collected",
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (run_dir / "summary_parity_report.json").write_text(
        json.dumps(
            {
                "generated_at": "2099-03-27T09:00:00",
                "status": "MISMATCH",
                "evidence_source": "diagnostic",
                "evidence_state": "collected",
                "not_real_acceptance_evidence": True,
                "summary": {
                    "cases_matched": 7,
                    "cases_total": 8,
                    "failed_cases": ["summary_export"],
                },
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (run_dir / "export_resilience_report.json").write_text(
        json.dumps(
            {
                "generated_at": "2099-03-27T10:00:00",
                "status": "MATCH",
                "evidence_source": "diagnostic",
                "evidence_state": "collected",
                "not_real_acceptance_evidence": True,
                "cases": [{"name": "json_export", "status": "MATCH"}],
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (run_dir / "analytics_summary.json").write_text(
        json.dumps(
            {
                "generated_at": "2099-03-27T10:30:00",
                "evidence_source": "simulated",
                "evidence_state": "collected",
                "not_real_acceptance_evidence": True,
                "analyzer_coverage": {"coverage_text": "1/1"},
                "reference_quality_statistics": {
                    "reference_quality": "degraded",
                    "reference_quality_trend": "drift",
                },
                "export_resilience_status": {"overall_status": "degraded"},
                "qc_overview": {
                    "run_gate": {"status": "warn"},
                    "reviewer_digest": {
                        "summary": "运行 run-1 质控评分 -- / 等级 --；通过 1，预警 1，拒绝 0，跳过 0；门禁 warn。"
                    },
                },
                "unified_review_summary": {
                    "summary": "离线分析摘要：点位 2，覆盖 1/1，质控 warn，漂移 stable，控制图 in_control，健康 attention，主故障 none。",
                    "reviewer_notes": [
                        "离线分析摘要：点位 2，覆盖 1/1，质控 warn，漂移 stable，控制图 in_control，健康 attention，主故障 none。",
                        "运行 run-1 质控评分 -- / 等级 --；通过 1，预警 1，拒绝 0，跳过 0；门禁 warn。",
                    ],
                },
                "digest": {
                    "summary": "coverage 1/1 | reference degraded | exports degraded | lineage cfg-001",
                    "health": "attention",
                },
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (run_dir / "spectral_quality_summary.json").write_text(
        json.dumps(
            {
                "artifact_type": "spectral_quality_summary",
                "status": "ok",
                "channel_count": 1,
                "ok_channel_count": 1,
                "overall_score": 0.94,
                "flags": [],
                "not_real_acceptance_evidence": True,
                "channels": {
                    "GA01.co2_signal": {
                        "status": "ok",
                        "stability_score": 0.94,
                        "low_freq_energy_ratio": 0.12,
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
    facade.service.orchestrator.run_state.artifacts.output_files.append(str(run_dir / "spectral_quality_summary.json"))
    (run_dir / "lineage_summary.json").write_text(
        json.dumps(
            {
                "generated_at": "2099-03-27T10:31:00",
                "config_version": "cfg-001",
                "points_version": "pts-001",
                "profile_version": "profile-001",
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    facade.execute_device_workbench_action("pressure_gauge", "run_preset", preset_id="wrong_unit")
    facade.execute_device_workbench_action(
        "workbench",
        "generate_diagnostic_evidence",
        current_device="pressure_gauge",
        current_action="run_preset",
    )

    results = facade.build_results_snapshot()
    review_center = results["review_center"]
    follow_up_review_center = facade.build_results_snapshot()["review_center"]
    evidence_types = {item["type"] for item in review_center["evidence_items"]}
    risk_summary = review_center["risk_summary"]
    diagnostics = dict(review_center.get("diagnostics", {}) or {})

    assert {"suite", "parity", "resilience", "workbench", "analytics"} <= evidence_types
    assert review_center["acceptance_readiness"]["simulated_only"] is True
    assert review_center["operator_focus"]["summary"]
    assert review_center["reviewer_focus"]["summary"]
    assert review_center["approver_focus"]["summary"]
    assert risk_summary["level"] == "high"
    assert risk_summary["failed_count"] >= 1
    assert risk_summary["degraded_count"] >= 1
    assert risk_summary["summary"]
    assert review_center["index_summary"]["analytics_count"] >= 1
    assert review_center["index_summary"]["source_kind_counts"]["run"] >= 1
    assert review_center["index_summary"]["sources"]
    assert review_center["index_summary"]["source_kind_summary"]
    assert review_center["index_summary"]["coverage_summary"]
    assert review_center["index_summary"]["diagnostics_summary"]
    assert review_center["filters"]["source_options"]
    assert review_center["filters"]["time_options"]
    assert set(diagnostics) >= {"cache_hit", "scanned_root_count", "scanned_candidate_count", "elapsed_ms", "scan_budget_used"}
    assert isinstance(diagnostics["cache_hit"], bool)
    assert diagnostics["elapsed_ms"] >= 0
    assert any(str(item.get("detail_summary") or "").strip() for item in review_center["evidence_items"])
    assert any(list(item.get("detail_key_fields") or []) for item in review_center["evidence_items"])
    assert any(list(item.get("detail_artifact_paths") or []) for item in review_center["evidence_items"])
    assert any(str(item.get("detail_acceptance_hint") or "").strip() for item in review_center["evidence_items"])
    assert all(item.get("detail_analytics_summary") for item in review_center["evidence_items"])
    assert all(item.get("detail_lineage_summary") for item in review_center["evidence_items"])
    analytics_item = next(item for item in review_center["evidence_items"] if item["type"] == "analytics")
    suite_item = next(item for item in review_center["evidence_items"] if item["type"] == "suite")
    workbench_item = next(item for item in review_center["evidence_items"] if item["type"] == "workbench")
    assert analytics_item["detail_analytics_summary"]
    assert analytics_item["detail_qc_summary"]
    assert analytics_item["detail_qc_cards"]
    assert any(card["id"] == "boundary" for card in analytics_item["detail_qc_cards"])
    assert analytics_item["detail_spectral_summary"]
    assert analytics_item["detail_lineage_summary"]
    assert analytics_item["evidence_source"] == "simulated_protocol"
    assert workbench_item["evidence_source"] == "simulated_protocol"
    assert workbench_item["detail_qc_cards"]
    assert any("配置安全" in str(line) for line in list(analytics_item["detail_analytics_summary"]))
    assert any("配置安全" in str(line) for line in list(workbench_item["detail_analytics_summary"]))
    assert any("运行门禁" in str(line) for line in list(analytics_item["detail_qc_summary"]))
    assert any("结果分级" in str(line) for line in list(analytics_item["detail_qc_summary"]))
    assert any("证据边界" in str(line) for line in list(analytics_item["detail_qc_summary"]))
    assert any("质控" in str(line) for line in list(analytics_item["detail_analytics_summary"]))
    assert any("质控" in str(line) for line in list(workbench_item["detail_qc_summary"]))
    assert any("默认工作流" in str(line) or "阻断原因" in str(line) for line in list(workbench_item["detail_analytics_summary"]))
    assert any(
        str(review_center["index_summary"]["source_kind_summary"] or "") in str(line)
        for line in list(suite_item["detail_analytics_summary"])
    )
    assert any(
        str(review_center["index_summary"]["coverage_summary"] or "") in str(line)
        for line in list(suite_item["detail_lineage_summary"])
    )
    assert any(item["id"] == "analytics" for item in review_center["filters"]["type_options"])
    assert any(item["id"] == "run" for item in review_center["filters"]["source_options"])
    assert any(item["id"] == "30d" for item in review_center["filters"]["time_options"])
    assert any(
        item["type"] == "analytics" and bool(item.get("not_real_acceptance_evidence", False))
        for item in review_center["evidence_items"]
    )
    assert any(risk_summary["summary"] in detail for detail in review_center["acceptance_readiness"]["detail"])
    assert "acceptance" in review_center["disclaimer"].lower()
    assert follow_up_review_center["diagnostics"]["cache_hit"] is True
    reports = facade.get_reports_snapshot(results_snapshot=results)
    assert reports["review_center"]["evidence_items"]


def test_review_center_panel_filters_by_type_status_and_source_without_implying_real_acceptance() -> None:
    root = make_root()
    try:
        panel = ReviewCenterPanel(root)
        now = time.time()
        payload = {
            "operator_focus": {"summary": "operator summary"},
            "reviewer_focus": {"summary": "reviewer summary"},
            "approver_focus": {"summary": "approver summary"},
                "risk_summary": {
                    "level": "high",
                    "level_display": t("results.review_center.risk.high"),
                    "summary": t(
                        "results.review_center.risk.summary",
                        level=t("results.review_center.risk.high"),
                        failed=1,
                        degraded=1,
                        diagnostic=1,
                        missing=0,
                        coverage="suite / parity / resilience / workbench / analytics",
                    ),
                },
            "acceptance_readiness": {"summary": "offline readiness only"},
            "analytics_summary": {"summary": "analytics summary"},
            "lineage_summary": {"summary": "lineage summary"},
            "index_summary": {
                "summary": "recent sources 1 | suite 1 | parity 1 | resilience 1 | workbench 1 | analytics 1",
                "source_kind_summary": "sources by kind | run 1 | suite 1 | workbench 1",
                "coverage_summary": "coverage | complete 1 | gapped 0 | no gaps",
                "sources": [
                    {
                        "source_label": "offline_run_1",
                        "latest_display": "03-27 11:00",
                        "coverage_display": "5/5 | suite / parity / resilience / workbench / analytics",
                        "gaps_display": "no gaps",
                    }
                ],
            },
            "filters": {
                "selected_type": "all",
                "selected_status": "all",
                "selected_time": "all",
                "selected_source": "all",
                "type_options": [
                    {"id": "all", "label": t("results.review_center.filter.all_types")},
                    {"id": "suite", "label": t("results.review_center.type.suite")},
                    {"id": "parity", "label": t("results.review_center.type.parity")},
                    {"id": "resilience", "label": t("results.review_center.type.resilience")},
                    {"id": "workbench", "label": t("results.review_center.type.workbench")},
                    {"id": "analytics", "label": t("results.review_center.type.analytics")},
                ],
                "status_options": [
                    {"id": "all", "label": t("results.review_center.filter.all_statuses")},
                    {"id": "passed", "label": t("results.review_center.status.passed")},
                    {"id": "failed", "label": t("results.review_center.status.failed")},
                    {"id": "degraded", "label": t("results.review_center.status.degraded")},
                    {"id": "diagnostic_only", "label": t("results.review_center.status.diagnostic_only")},
                ],
                "time_options": [
                    {"id": "all", "label": t("results.review_center.filter.all_time"), "window_seconds": None},
                    {"id": "24h", "label": t("results.review_center.filter.time_24h"), "window_seconds": 86400},
                    {"id": "7d", "label": t("results.review_center.filter.time_7d"), "window_seconds": 604800},
                ],
                "source_options": [
                    {"id": "all", "label": t("results.review_center.filter.all_sources")},
                    {"id": "suite", "label": t("results.review_center.source_kind.suite")},
                    {"id": "run", "label": t("results.review_center.source_kind.run")},
                    {"id": "workbench", "label": t("results.review_center.source_kind.workbench")},
                ],
            },
            "evidence_items": [
                {
                    "type": "suite",
                    "type_display": t("results.review_center.type.suite"),
                    "status": "passed",
                    "status_display": t("results.review_center.status.passed"),
                    "generated_at_display": "03-27 08:00",
                    "sort_key": now - 3600,
                    "summary": "suite passed",
                    "detail_text": "suite detail",
                    "detail_summary": "suite summary detail",
                    "detail_risk": "low | passed",
                    "detail_key_fields": ["regression", "5/5", "simulated_protocol"],
                    "detail_artifact_paths": ["D:/tmp/run_1/suite_summary.json"],
                    "detail_acceptance_hint": "offline readiness only | run coverage complete",
                    "detail_analytics_summary": ["coverage 1/1", "reference healthy", "exports healthy"],
                    "detail_lineage_summary": ["cfg-001 / pts-001 / profile-001"],
                    "source_kind": "suite",
                },
                {
                    "type": "parity",
                    "type_display": t("results.review_center.type.parity"),
                    "status": "failed",
                    "status_display": t("results.review_center.status.failed"),
                    "generated_at_display": "03-27 09:00",
                    "sort_key": now - 7200,
                    "summary": "parity failed",
                    "detail_text": "parity detail",
                    "source_kind": "run",
                },
                {
                    "type": "resilience",
                    "type_display": t("results.review_center.type.resilience"),
                    "status": "passed",
                    "status_display": t("results.review_center.status.passed"),
                    "generated_at_display": "03-27 10:00",
                    "sort_key": now - 10800,
                    "summary": "resilience passed",
                    "detail_text": "resilience detail",
                    "source_kind": "run",
                },
                {
                    "type": "analytics",
                    "type_display": t("results.review_center.type.analytics"),
                    "status": "degraded",
                    "status_display": t("results.review_center.status.degraded"),
                    "generated_at_display": "03-27 10:30",
                    "sort_key": now - 14400,
                    "summary": "analytics degraded",
                    "detail_text": "analytics detail",
                    "detail_qc_summary": ["质控摘要: 运行门禁 warn", "点级门禁: warn | 关注路由: co2"],
                    "detail_analytics_summary": ["coverage 1/1", "reference drift", "exports degraded"],
                    "detail_spectral_summary": ["GA01.co2_signal | 正常 | 稳定性 0.940"],
                    "detail_lineage_summary": ["cfg-001 / pts-001 / profile-001", "D:/tmp/run_1/lineage_summary.json"],
                    "source_kind": "run",
                },
                {
                    "type": "workbench",
                    "type_display": t("results.review_center.type.workbench"),
                    "status": "diagnostic_only",
                    "status_display": t("results.review_center.status.diagnostic_only"),
                    "generated_at_display": "03-27 11:00",
                    "sort_key": now - 18000,
                    "summary": "workbench diagnostic",
                    "detail_text": "workbench detail",
                    "source_kind": "workbench",
                },
            ],
            "detail_hint": "select evidence to inspect details",
            "empty_detail": "no evidence",
            "disclaimer": "offline simulated/replay evidence only; not real acceptance.",
        }
        panel.render(payload)

        assert "real acceptance" in panel.disclaimer_var.get()
        assert panel.operator_var.get()
        assert panel.reviewer_var.get()
        assert panel.approver_var.get()
        assert panel.risk_var.get() == payload["risk_summary"]["summary"]
        assert "sources by kind" in panel.index_var.get()
        assert "覆盖 | 完整 1 | 缺口 0 | 无缺口" in panel.index_var.get()
        assert panel.time_filter["values"]
        assert panel.source_filter["values"]
        assert len(panel.source_tree.get_children()) == 1
        assert len(panel.tree.get_children()) == 5
        assert panel.detail_summary_var.get() == "suite summary detail"
        assert "low" in panel.detail_risk_var.get()
        assert "regression" in panel.detail_key_fields_var.get()
        assert "suite_summary.json" in panel.detail_artifacts_var.get()
        assert "offline readiness only" in panel.detail_acceptance_var.get()
        assert "coverage 1/1" in panel.detail_analytics_var.get()
        assert "cfg-001" in panel.detail_lineage_var.get()

        panel.type_filter_var.set(t("results.review_center.type.parity"))
        panel.status_filter_var.set(t("results.review_center.status.failed"))
        panel._apply_filters()

        rows = panel.tree.get_children()
        assert len(rows) == 1
        values = panel.tree.item(rows[0], "values")
        assert values[1] == t("results.review_center.type.parity")
        assert values[2] == t("results.review_center.status.failed")

        panel.type_filter_var.set(t("results.review_center.filter.all_types"))
        panel.status_filter_var.set(t("results.review_center.filter.all_statuses"))
        panel.source_filter_var.set(t("results.review_center.source_kind.workbench"))
        panel._apply_filters()

        rows = panel.tree.get_children()
        assert len(rows) == 1
        values = panel.tree.item(rows[0], "values")
        assert values[1] == t("results.review_center.type.workbench")

        panel.source_filter_var.set(t("results.review_center.filter.all_sources"))
        panel.type_filter_var.set(t("results.review_center.type.analytics"))
        panel.status_filter_var.set(t("results.review_center.status.degraded"))
        panel._apply_filters()
        rows = panel.tree.get_children()
        assert len(rows) == 1
        values = panel.tree.item(rows[0], "values")
        assert values[1] == t("results.review_center.type.analytics")
        assert "运行门禁 warn" in panel.detail_qc_var.get()
        assert "reference drift" in panel.detail_analytics_var.get()
        assert "GA01.co2_signal" in panel.detail_spectral_var.get()
        assert "lineage_summary.json" in panel.detail_lineage_var.get()

        panel.type_filter_var.set(t("results.review_center.filter.all_types"))
        panel.status_filter_var.set(t("results.review_center.filter.all_statuses"))
        panel.time_filter_var.set(t("results.review_center.filter.time_24h"))
        panel._apply_filters()
        assert len(panel.tree.get_children()) == 5
    finally:
        root.destroy()


def test_review_center_panel_merges_phase_transition_bridge_into_readiness_and_analytics_summary() -> None:
    root = make_root()
    try:
        panel = ReviewCenterPanel(root, compact=True)
        payload = {
            "operator_focus": {"summary": "operator"},
            "reviewer_focus": {"summary": "reviewer"},
            "approver_focus": {"summary": "approver"},
            "risk_summary": {"level": "low", "level_display": "low", "summary": "risk summary"},
            "acceptance_readiness": {"summary": "offline readiness only"},
            "analytics_summary": {
                "summary": "analytics summary",
                "detail": {
                    "phase_transition_bridge": _build_phase_transition_bridge_payload(),
                },
            },
            "lineage_summary": {"summary": "lineage summary"},
            "index_summary": {
                "summary": "recent sources 1",
                "source_kind_summary": "sources by kind | run 1",
                "coverage_summary": "coverage | complete 1 | gapped 0 | no gaps",
                "sources": [],
            },
            "filters": {
                "selected_type": "all",
                "selected_status": "all",
                "selected_time": "all",
                "selected_source": "all",
                "type_options": [{"id": "all", "label": t("results.review_center.filter.all_types")}],
                "status_options": [{"id": "all", "label": t("results.review_center.filter.all_statuses")}],
                "time_options": [{"id": "all", "label": t("results.review_center.filter.all_time"), "window_seconds": None}],
                "source_options": [{"id": "all", "label": t("results.review_center.filter.all_sources")}],
            },
            "evidence_items": [],
            "detail_hint": "select evidence",
            "empty_detail": "no evidence",
            "disclaimer": "offline evidence only; not real acceptance.",
        }

        panel.render(payload)

        assert "offline readiness only" in panel.readiness_var.get()
        assert "Step 2 tail / Stage 3 bridge" in panel.readiness_var.get()
        assert "不是 real acceptance" in panel.readiness_var.get()
        assert "analytics summary" in panel.analytics_var.get()
        assert "阶段桥工件" in panel.analytics_var.get()
        assert "real acceptance passed" not in panel.analytics_var.get()
    finally:
        root.destroy()


def test_review_center_panel_exposes_phase_transition_bridge_as_dedicated_card() -> None:
    root = make_root()
    try:
        panel = ReviewCenterPanel(root, compact=True)
        payload = {
            "operator_focus": {"summary": "operator"},
            "reviewer_focus": {"summary": "reviewer"},
            "approver_focus": {"summary": "approver"},
            "risk_summary": {"level": "low", "level_display": "low", "summary": "risk summary"},
            "acceptance_readiness": {"summary": "offline readiness only"},
            "analytics_summary": {
                "summary": "analytics summary",
                "detail": {
                    "phase_transition_bridge": _build_phase_transition_bridge_payload(),
                },
            },
            "lineage_summary": {"summary": "lineage summary"},
            "index_summary": {
                "summary": "recent sources 1",
                "source_kind_summary": "sources by kind | run 1",
                "coverage_summary": "coverage | complete 1 | gapped 0 | no gaps",
                "sources": [],
            },
            "filters": {
                "selected_type": "all",
                "selected_status": "all",
                "selected_time": "all",
                "selected_source": "all",
                "type_options": [{"id": "all", "label": t("results.review_center.filter.all_types")}],
                "status_options": [{"id": "all", "label": t("results.review_center.filter.all_statuses")}],
                "time_options": [{"id": "all", "label": t("results.review_center.filter.all_time"), "window_seconds": None}],
                "source_options": [{"id": "all", "label": t("results.review_center.filter.all_sources")}],
            },
            "evidence_items": [],
            "detail_hint": "select evidence",
            "empty_detail": "no evidence",
            "disclaimer": "offline only; not real acceptance.",
        }

        panel.render(payload)

        assert "Step 2 tail / Stage 3 bridge" in panel.phase_bridge_var.get()
        assert "engineering-isolation" in panel.phase_bridge_var.get()
        assert "engineering-isolation 准备：已具备。" in panel.phase_bridge_var.get()
        assert "real acceptance 准备：尚未具备。" in panel.phase_bridge_var.get()
        assert "现在执行" in panel.phase_bridge_var.get()
        assert "第三阶段执行" in panel.phase_bridge_var.get()
        assert "不是 real acceptance" in panel.phase_bridge_var.get()
        assert "不能替代真实计量验证" in panel.phase_bridge_var.get()
        assert "ready_for_engineering_isolation" not in panel.phase_bridge_var.get()
        assert "real_acceptance_ready" not in panel.phase_bridge_var.get()
    finally:
        root.destroy()


def test_review_center_phase_transition_bridge_card_matches_presenter_payload() -> None:
    root = make_root()
    try:
        panel = ReviewCenterPanel(root, compact=True)
        bridge = _build_phase_transition_bridge_payload()
        expected_panel = build_phase_transition_bridge_panel_payload(bridge)
        payload = {
            "operator_focus": {"summary": "operator"},
            "reviewer_focus": {"summary": "reviewer"},
            "approver_focus": {"summary": "approver"},
            "risk_summary": {"level": "low", "level_display": "low", "summary": "risk summary"},
            "acceptance_readiness": {"summary": "offline readiness only"},
            "analytics_summary": {
                "summary": "analytics summary",
                "detail": {
                    "phase_transition_bridge": bridge,
                },
            },
            "lineage_summary": {"summary": "lineage summary"},
            "index_summary": {
                "summary": "recent sources 1",
                "source_kind_summary": "sources by kind | run 1",
                "coverage_summary": "coverage | complete 1 | gapped 0 | no gaps",
                "sources": [],
            },
            "filters": {
                "selected_type": "all",
                "selected_status": "all",
                "selected_time": "all",
                "selected_source": "all",
                "type_options": [{"id": "all", "label": t("results.review_center.filter.all_types")}],
                "status_options": [{"id": "all", "label": t("results.review_center.filter.all_statuses")}],
                "time_options": [{"id": "all", "label": t("results.review_center.filter.all_time"), "window_seconds": None}],
                "source_options": [{"id": "all", "label": t("results.review_center.filter.all_sources")}],
            },
            "evidence_items": [],
            "detail_hint": "select evidence",
            "empty_detail": "no evidence",
            "disclaimer": "offline only; not real acceptance.",
        }

        panel.render(payload)
        bridge_text = panel.phase_bridge_var.get().strip()

        assert bridge_text == expected_panel["display"]["card_text"]
        assert "Step 2 tail / Stage 3 bridge" in bridge_text
        assert "engineering-isolation" in bridge_text
        assert expected_panel["display"]["engineering_isolation_text"] in bridge_text
        assert expected_panel["display"]["real_acceptance_text"] in bridge_text
        assert expected_panel["display"]["execute_now_text"] in bridge_text
        assert expected_panel["display"]["defer_to_stage3_text"] in bridge_text
        assert "不是 real acceptance" in bridge_text
        assert "不能替代真实计量验证" in bridge_text
        assert "ready_for_engineering_isolation" not in bridge_text
        assert "real_acceptance_ready" not in bridge_text
    finally:
        root.destroy()


def test_review_center_panel_exposes_phase_transition_bridge_reviewer_artifact_as_dedicated_entry() -> None:
    root = make_root()
    try:
        panel = ReviewCenterPanel(root, compact=True)
        reviewer_entry = _build_phase_transition_bridge_reviewer_entry()
        payload = {
            "operator_focus": {"summary": "operator"},
            "reviewer_focus": {"summary": "reviewer"},
            "approver_focus": {"summary": "approver"},
            "risk_summary": {"level": "low", "level_display": "low", "summary": "risk summary"},
            "acceptance_readiness": {"summary": "offline readiness only"},
            "analytics_summary": {
                "summary": "analytics summary",
                "detail": {
                    "phase_transition_bridge": _build_phase_transition_bridge_payload(),
                },
            },
            "lineage_summary": {"summary": "lineage summary"},
            "index_summary": {
                "summary": "recent sources 1",
                "source_kind_summary": "sources by kind | run 1",
                "coverage_summary": "coverage | complete 1 | gapped 0 | no gaps",
                "sources": [],
            },
            "phase_transition_bridge_reviewer_artifact_entry": reviewer_entry,
            "filters": {
                "selected_type": "all",
                "selected_status": "all",
                "selected_time": "all",
                "selected_source": "all",
                "type_options": [{"id": "all", "label": t("results.review_center.filter.all_types")}],
                "status_options": [{"id": "all", "label": t("results.review_center.filter.all_statuses")}],
                "time_options": [{"id": "all", "label": t("results.review_center.filter.all_time"), "window_seconds": None}],
                "source_options": [{"id": "all", "label": t("results.review_center.filter.all_sources")}],
            },
            "evidence_items": [],
            "detail_hint": "select evidence",
            "empty_detail": "no evidence",
            "disclaimer": "offline only; not real acceptance.",
        }

        panel.render(payload)
        root.update_idletasks()

        assert panel.phase_bridge_artifact_frame.winfo_manager() == "grid"
        assert panel.phase_bridge_artifact_title_var.get() == reviewer_entry["name_text"]
        assert panel.phase_bridge_artifact_status_var.get() == reviewer_entry["role_status_display"]
        assert panel.phase_bridge_artifact_path_var.get() == reviewer_entry["path"]
        assert panel.phase_bridge_artifact_note_var.get() == reviewer_entry["note_text"]
        assert "Step 2 tail / Stage 3 bridge" in panel.phase_bridge_artifact_status_var.get()
        assert "engineering-isolation" in panel.phase_bridge_artifact_status_var.get()
        assert "不是 real acceptance" in panel.phase_bridge_artifact_status_var.get()
        assert "不能替代真实计量验证" in panel.phase_bridge_artifact_status_var.get()
        assert "ready_for_engineering_isolation" not in panel.phase_bridge_artifact_status_var.get()
        assert "real_acceptance_ready" not in panel.phase_bridge_artifact_status_var.get()
    finally:
        root.destroy()


def test_review_center_keeps_stage_admission_review_pack_markdown_aligned_with_phase_bridge_entry(
    tmp_path: Path,
) -> None:
    facade = build_fake_facade(tmp_path)
    run_dir = Path(facade.result_store.run_dir)
    rebuild_run(run_dir)

    results_snapshot = facade.build_results_snapshot()
    reviewer_entry = dict(
        results_snapshot["review_center"].get("phase_transition_bridge_reviewer_artifact_entry", {}) or {}
    )
    pack_markdown = (run_dir / STAGE_ADMISSION_REVIEW_PACK_REVIEWER_FILENAME).read_text(encoding="utf-8")

    assert reviewer_entry["summary_text"] in pack_markdown
    assert reviewer_entry["status_line"] in pack_markdown
    assert reviewer_entry["engineering_isolation_text"] in pack_markdown
    assert reviewer_entry["real_acceptance_text"] in pack_markdown
    assert reviewer_entry["execute_now_text"] in pack_markdown
    assert reviewer_entry["defer_to_stage3_text"] in pack_markdown
    assert reviewer_entry["warning_text"] in pack_markdown
    assert "Step 2 tail / Stage 3 bridge" in pack_markdown
    assert "engineering-isolation" in pack_markdown
    assert "不是 real acceptance" in pack_markdown
    assert "不能替代真实计量验证" in pack_markdown
    assert "ready_for_engineering_isolation" not in pack_markdown
    assert "real_acceptance_ready" not in pack_markdown


def test_review_center_panel_source_drilldown_syncs_list_detail_and_scope_summaries() -> None:
    root = make_root()
    try:
        panel = ReviewCenterPanel(root, compact=True)
        now = time.time()
        payload = {
            "operator_focus": {"summary": "operator"},
            "reviewer_focus": {"summary": "reviewer"},
            "approver_focus": {"summary": "approver"},
            "risk_summary": {
                "level": "medium",
                "level_display": t("results.review_center.risk.medium"),
                "summary": t(
                    "results.review_center.risk.summary",
                    level=t("results.review_center.risk.medium"),
                    failed=0,
                    degraded=1,
                    diagnostic=0,
                    missing=0,
                    coverage="suite / analytics",
                ),
            },
            "acceptance_readiness": {"summary": "offline readiness only"},
            "analytics_summary": {"summary": "analytics"},
            "lineage_summary": {"summary": "lineage"},
            "index_summary": {
                "summary": "recent sources 2 | suite 1 | parity 1 | resilience 0 | workbench 0 | analytics 2",
                "source_kind_summary": "sources by kind | run 2 | suite 0 | workbench 0",
                "coverage_summary": "coverage | complete 0 | gapped 2 | missing parity / resilience",
                "diagnostics_summary": "diagnostics | cache no | roots 2 | candidates 6 | elapsed 18 ms | budget 10",
                "sources": [
                    {
                        "source_id": "run-a",
                        "source_label": "review_run_a",
                        "source_scope": "run",
                        "latest_display": "03-27 10:00",
                        "coverage_display": "2/5 | suite / analytics",
                        "gaps_display": "missing parity / resilience / workbench",
                        "evidence_count": 2,
                    },
                    {
                        "source_id": "run-b",
                        "source_label": "review_run_b",
                        "source_scope": "run",
                        "latest_display": "03-27 11:00",
                        "coverage_display": "2/5 | parity / analytics",
                        "gaps_display": "missing suite / resilience / workbench",
                        "evidence_count": 2,
                    },
                ],
            },
            "filters": {
                "selected_type": "all",
                "selected_status": "all",
                "selected_time": "all",
                "selected_source": "all",
                "type_options": [
                    {"id": "all", "label": t("results.review_center.filter.all_types")},
                    {"id": "suite", "label": t("results.review_center.type.suite")},
                    {"id": "parity", "label": t("results.review_center.type.parity")},
                    {"id": "analytics", "label": t("results.review_center.type.analytics")},
                ],
                "status_options": [
                    {"id": "all", "label": t("results.review_center.filter.all_statuses")},
                    {"id": "passed", "label": t("results.review_center.status.passed")},
                    {"id": "failed", "label": t("results.review_center.status.failed")},
                    {"id": "degraded", "label": t("results.review_center.status.degraded")},
                ],
                "time_options": [
                    {"id": "all", "label": t("results.review_center.filter.all_time"), "window_seconds": None},
                    {"id": "24h", "label": t("results.review_center.filter.time_24h"), "window_seconds": 86400},
                ],
                "source_options": [
                    {"id": "all", "label": t("results.review_center.filter.all_sources")},
                    {"id": "run", "label": t("results.review_center.source_kind.run")},
                ],
            },
            "evidence_items": [
                {
                    "type": "suite",
                    "type_display": t("results.review_center.type.suite"),
                    "status": "passed",
                    "status_display": t("results.review_center.status.passed"),
                    "generated_at_display": "03-27 10:00",
                    "sort_key": now - 3600,
                    "summary": "suite passed",
                    "detail_text": "suite detail a",
                    "detail_analytics_summary": ["coverage 1/1", "reference healthy"],
                    "detail_lineage_summary": ["cfg-a / pts-a / profile-a"],
                    "source_kind": "run",
                    "source_scope": "run",
                    "source_id": "run-a",
                    "source_label": "review_run_a",
                },
                {
                    "type": "analytics",
                    "type_display": t("results.review_center.type.analytics"),
                    "status": "degraded",
                    "status_display": t("results.review_center.status.degraded"),
                    "generated_at_display": "03-27 10:10",
                    "sort_key": now - 3300,
                    "summary": "analytics degraded a",
                    "detail_text": "analytics detail a",
                    "detail_analytics_summary": ["coverage 1/1", "reference drift a"],
                    "detail_lineage_summary": ["cfg-a / pts-a / profile-a"],
                    "source_kind": "run",
                    "source_scope": "run",
                    "source_id": "run-a",
                    "source_label": "review_run_a",
                },
                {
                    "type": "parity",
                    "type_display": t("results.review_center.type.parity"),
                    "status": "failed",
                    "status_display": t("results.review_center.status.failed"),
                    "generated_at_display": "03-27 11:00",
                    "sort_key": now - 1800,
                    "summary": "parity failed b",
                    "detail_text": "parity detail b",
                    "detail_analytics_summary": ["coverage 1/1", "reference drift b"],
                    "detail_lineage_summary": ["cfg-b / pts-b / profile-b"],
                    "source_kind": "run",
                    "source_scope": "run",
                    "source_id": "run-b",
                    "source_label": "review_run_b",
                },
                {
                    "type": "analytics",
                    "type_display": t("results.review_center.type.analytics"),
                    "status": "degraded",
                    "status_display": t("results.review_center.status.degraded"),
                    "generated_at_display": "03-27 11:10",
                    "sort_key": now - 1500,
                    "summary": "analytics degraded b",
                    "detail_text": "analytics detail b",
                    "detail_analytics_summary": ["coverage 1/1", "reference drift b"],
                    "detail_lineage_summary": ["cfg-b / pts-b / profile-b"],
                    "source_kind": "run",
                    "source_scope": "run",
                    "source_id": "run-b",
                    "source_label": "review_run_b",
                },
            ],
            "detail_hint": "select evidence to inspect details",
            "empty_detail": "no evidence",
            "disclaimer": "offline simulated/replay evidence only; not real acceptance.",
        }

        panel.render(payload)
        assert len(panel.tree.get_children()) == 4
        assert str(panel.clear_source_button["state"]) == "disabled"

        panel.source_tree.selection_set("source-1")
        panel._on_source_selected()

        assert len(panel.tree.get_children()) == 2
        assert "review_run_b" in panel.source_scope_var.get()
        assert str(panel.clear_source_button["state"]) == "normal"
        assert "review_run_b" in panel.index_var.get()
        assert "2/5 | parity / analytics" in panel.index_var.get()
        assert "缺少 suite / resilience / workbench" in panel.index_var.get()
        assert "acceptance" in panel.readiness_var.get().lower()
        assert "cfg-b" in panel.detail_lineage_var.get()
        assert "reference drift b" in panel.detail_analytics_var.get()

        panel._clear_source_drilldown()

        assert len(panel.tree.get_children()) == 4
        assert str(panel.clear_source_button["state"]) == "disabled"
        assert t("results.review_center.filter.all_sources") in panel.source_scope_var.get()
    finally:
        root.destroy()


def test_review_center_panel_exposes_selection_contract_and_visible_source_disambiguation() -> None:
    root = make_root()
    try:
        panel = ReviewCenterPanel(root, compact=True)
        now = time.time()
        payload = {
            "operator_focus": {"summary": "operator"},
            "reviewer_focus": {"summary": "reviewer"},
            "approver_focus": {"summary": "approver"},
            "risk_summary": {
                "level": "medium",
                "level_display": t("results.review_center.risk.medium"),
                "summary": "risk summary",
            },
            "acceptance_readiness": {"summary": "offline readiness only"},
            "analytics_summary": {"summary": "analytics"},
            "lineage_summary": {"summary": "lineage"},
            "index_summary": {
                "summary": "recent sources 2",
                "source_kind_summary": "sources by kind",
                "coverage_summary": "coverage summary",
                "sources": [
                    {
                        "source_id": "D:/tmp/branch_a/shared_run",
                        "source_label": "shared_run",
                        "source_dir": "D:/tmp/branch_a/shared_run",
                        "source_scope": "run",
                        "latest_display": "03-27 10:00",
                        "coverage_display": "2/5 | suite / analytics",
                        "gaps_display": "missing parity / resilience / workbench",
                        "evidence_count": 2,
                    },
                    {
                        "source_id": "D:/tmp/branch_b/shared_run",
                        "source_label": "shared_run",
                        "source_dir": "D:/tmp/branch_b/shared_run",
                        "source_scope": "run",
                        "latest_display": "03-27 11:00",
                        "coverage_display": "1/5 | parity",
                        "gaps_display": "missing suite / resilience / workbench / analytics",
                        "evidence_count": 1,
                    },
                ],
            },
            "filters": {
                "selected_type": "all",
                "selected_status": "all",
                "selected_time": "all",
                "selected_source": "all",
                "type_options": [
                    {"id": "all", "label": t("results.review_center.filter.all_types")},
                    {"id": "analytics", "label": t("results.review_center.type.analytics")},
                ],
                "status_options": [
                    {"id": "all", "label": t("results.review_center.filter.all_statuses")},
                    {"id": "degraded", "label": t("results.review_center.status.degraded")},
                ],
                "time_options": [
                    {"id": "all", "label": t("results.review_center.filter.all_time"), "window_seconds": None},
                ],
                "source_options": [
                    {"id": "all", "label": t("results.review_center.filter.all_sources")},
                    {"id": "run", "label": t("results.review_center.source_kind.run")},
                ],
            },
            "evidence_items": [
                {
                    "type": "suite",
                    "type_display": t("results.review_center.type.suite"),
                    "status": "passed",
                    "status_display": t("results.review_center.status.passed"),
                    "generated_at_display": "03-27 10:00",
                    "sort_key": now - 10,
                    "summary": "suite a",
                    "detail_text": "suite detail a",
                    "detail_artifact_paths": ["D:/tmp/branch_a/shared_run/suite_summary.json"],
                    "source_kind": "run",
                    "source_scope": "run",
                    "source_id": "D:/tmp/branch_a/shared_run",
                    "source_label": "shared_run",
                    "source_dir": "D:/tmp/branch_a/shared_run",
                },
                {
                    "type": "analytics",
                    "type_display": t("results.review_center.type.analytics"),
                    "status": "degraded",
                    "status_display": t("results.review_center.status.degraded"),
                    "generated_at_display": "03-27 10:05",
                    "sort_key": now - 5,
                    "summary": "analytics a",
                    "detail_text": "analytics detail a",
                    "detail_artifact_paths": ["D:/tmp/branch_a/shared_run/analytics_summary.json"],
                    "source_kind": "run",
                    "source_scope": "run",
                    "source_id": "D:/tmp/branch_a/shared_run",
                    "source_label": "shared_run",
                    "source_dir": "D:/tmp/branch_a/shared_run",
                },
                {
                    "type": "analytics",
                    "type_display": t("results.review_center.type.analytics"),
                    "status": "degraded",
                    "status_display": t("results.review_center.status.degraded"),
                    "generated_at_display": "03-27 11:00",
                    "sort_key": now - 1,
                    "summary": "analytics b",
                    "detail_text": "analytics detail b",
                    "detail_artifact_paths": ["D:/tmp/branch_b/shared_run/analytics_summary.json"],
                    "source_kind": "run",
                    "source_scope": "run",
                    "source_id": "D:/tmp/branch_b/shared_run",
                    "source_label": "shared_run",
                    "source_dir": "D:/tmp/branch_b/shared_run",
                },
            ],
            "detail_hint": "select evidence",
            "empty_detail": "no evidence",
            "disclaimer": "offline only",
        }

        panel.render(payload)
        initial_snapshot = panel.get_selection_snapshot()
        source_values = [panel.source_tree.item(item, "values") for item in panel.source_tree.get_children()]

        assert initial_snapshot["scope"] == "all"
        assert len(source_values) == 2
        assert source_values[0][0] != source_values[1][0]
        assert "shared_run" in source_values[0][0]

        panel.type_filter_var.set(t("results.review_center.type.analytics"))
        panel._apply_filters()
        filtered_source_values = panel.source_tree.item(panel.source_tree.get_children()[0], "values")
        assert "1/2" in filtered_source_values[2]

        panel.source_tree.selection_set("source-0")
        panel._on_source_selected()
        source_snapshot = panel.get_selection_snapshot()

        assert source_snapshot["scope"] == "source"
        assert source_snapshot["selected_source_id"] == "D:/tmp/branch_a/shared_run"
        assert source_snapshot["selected_source_visible_count"] == 1
        assert source_snapshot["selected_source_total_count"] == 2

        panel.tree.selection_set("0")
        panel._on_tree_selected()
        evidence_snapshot = panel.get_selection_snapshot()

        assert evidence_snapshot["scope"] == "evidence"
        assert evidence_snapshot["selected_evidence_artifact_paths"] == [
            "D:/tmp/branch_a/shared_run/analytics_summary.json"
        ]
    finally:
        root.destroy()
