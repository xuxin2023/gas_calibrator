from pathlib import Path
import sys

from gas_calibrator.v2.ui_v2.i18n import t
from gas_calibrator.v2.ui_v2.pages.results_page import ResultsPage

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import make_root


def test_results_page_displays_artifact_sections() -> None:
    root = make_root()
    try:
        page = ResultsPage(root)
        page.render(
            {
                "overview_text": "运行 ID: run_1",
                "algorithm_compare_text": "默认算法: amt",
                "result_summary_text": "结果文件：已生成",
                "coefficient_summary_text": "calibration_coefficients.xlsx",
                "qc_summary_text": "质控摘要：运行门禁 warn | 点级门禁 warn\n证据边界：仅供 simulation/offline 审阅，不代表 real acceptance evidence。",
                "measurement_core_summary_text": "measurement-core phase coverage: ambient / preseal / pressure_stable / sample_ready\npayload-backed simulated phases: ambient / sample_ready",
                "ai_summary_text": "# AI 运行摘要\n运行状态稳定。",
                "review_center": {
                    "operator_focus": {"summary": "最近执行健康"},
                    "reviewer_focus": {"summary": "证据完整"},
                    "approver_focus": {"summary": "验收 readiness 未闭环"},
                    "risk_summary": {
                        "level": "high",
                        "level_display": t("results.review_center.risk.high"),
                        "summary": t(
                            "results.review_center.risk.summary",
                            level=t("results.review_center.risk.high"),
                            failed=1,
                            degraded=0,
                            diagnostic=0,
                            missing=2,
                            coverage="suite / analytics",
                        ),
                    },
                    "acceptance_readiness": {"summary": "仅离线 readiness"},
                    "analytics_summary": {"summary": "analytics 摘要"},
                    "lineage_summary": {"summary": "lineage 摘要"},
                    "index_summary": {
                        "summary": "最近来源 1 | suite 1 | parity 0 | resilience 0 | workbench 0 | analytics 1",
                        "sources": [
                            {
                                "source_label": "run_1",
                                "latest_display": "03-27 10:00",
                                "coverage_display": "2/5 | suite / analytics",
                                "gaps_display": "缺 parity / resilience / workbench",
                            }
                        ],
                    },
                    "filters": {
                        "selected_type": "all",
                        "selected_status": "all",
                        "selected_time": "all",
                        "selected_source": "all",
                        "type_options": [{"id": "all", "label": t("results.review_center.filter.all_types")}],
                        "status_options": [{"id": "all", "label": t("results.review_center.filter.all_statuses")}],
                        "time_options": [{"id": "all", "label": t("results.review_center.filter.all_time")}],
                        "source_options": [{"id": "all", "label": t("results.review_center.filter.all_sources")}],
                    },
                    "evidence_items": [
                        {
                            "type": "suite",
                            "type_display": t("results.review_center.type.suite"),
                            "status": "passed",
                            "status_display": t("results.review_center.status.passed"),
                            "generated_at_display": "03-27 10:00",
                            "summary": "regression 5/5 通过",
                            "detail_text": "离线 suite 证据",
                            "detail_summary": "regression 5/5 通过",
                            "detail_risk": "low | passed",
                            "detail_key_fields": ["regression", "5/5", "simulated_protocol"],
                            "detail_artifact_paths": ["D:/tmp/run_1/suite_summary.json"],
                            "detail_acceptance_hint": "仅离线 readiness",
                            "detail_qc_summary": ["运行门禁: warn | 原因: 存在预警点", "结果分级: 通过 1 / 预警 1 / 拒绝 0 / 跳过 0"],
                            "detail_qc_cards": [{"title": "质控卡片", "summary": "运行门禁 warn，需离线复核"}],
                            "detail_analytics_summary": ["coverage 1/1", "reference healthy", "exports healthy"],
                            "detail_lineage_summary": ["cfg-001 / pts-001 / profile-001"],
                            "source_kind": "suite",
                        }
                    ],
                    "detail_hint": "选择证据查看详情",
                    "empty_detail": "暂无证据",
                    "disclaimer": "以下均为离线证据，不代表真实 acceptance。",
                },
                "residuals": {"series": [{"algorithm": "amt", "residuals": [0.5, -0.2, 0.1]}]},
            }
        )

        assert page.page_scaffold is not None
        assert "run_1" in page.overview.get("1.0", "end")
        assert "amt" in page.algorithm.get("1.0", "end")
        assert "已生成" in page.result_summary.get("1.0", "end")
        assert "coefficients" in page.coefficient_summary.get("1.0", "end")
        assert "质控摘要" in page.qc_summary.get("1.0", "end")
        assert "不代表 real acceptance evidence" in page.qc_summary.get("1.0", "end")
        assert "measurement-core phase coverage" in page.measurement_core_summary.get("1.0", "end")
        assert "payload-backed simulated phases" in page.measurement_core_summary.get("1.0", "end")
        assert "运行状态稳定" in page.ai_summary.get("1.0", "end")
        assert page.review_center.risk_var.get()
        assert len(page.review_center.source_tree.get_children()) == 1
        assert len(page.review_center.tree.get_children()) == 1
        assert "regression" in page.review_center.detail_summary_var.get()
        assert "suite_summary.json" in page.review_center.detail_artifacts_var.get()
        assert "运行门禁" in page.review_center.detail_qc_var.get()
        assert "质控卡片" in page.review_center.detail_qc_var.get()
        assert "coverage 1/1" in page.review_center.detail_analytics_var.get()
        assert "cfg-001" in page.review_center.detail_lineage_var.get()
        assert page.residual_chart.canvas.find_all()
    finally:
        root.destroy()


def test_results_page_renders_measurement_core_review_filters() -> None:
    root = make_root()
    try:
        page = ResultsPage(root)
        page.render(
            {
                "overview_text": "run_2",
                "algorithm_compare_text": "amt",
                "result_summary_text": "multi-source stability shadow ready",
                "coefficient_summary_text": "coefficients",
                "qc_summary_text": "simulation-only",
                "measurement_core_summary_text": "measurement-core phase coverage: pressure_stable only\npayload completeness: complete 1 | trace_only 0",
                "ai_summary_text": "shadow evaluation only",
                "review_center": {
                    "operator_focus": {"summary": "operator"},
                    "reviewer_focus": {"summary": "reviewer"},
                    "approver_focus": {"summary": "approver"},
                    "risk_summary": {"level": "medium", "level_display": "medium", "summary": "review only"},
                    "acceptance_readiness": {"summary": "not real acceptance"},
                    "analytics_summary": {"summary": "analytics"},
                    "lineage_summary": {"summary": "lineage"},
                    "index_summary": {"summary": "index", "sources": []},
                    "filters": {
                        "selected_type": "stability",
                        "selected_status": "degraded",
                        "selected_time": "all",
                        "selected_source": "all",
                        "selected_phase": "pressure_stable",
                        "selected_artifact_role": "diagnostic_analysis",
                        "selected_standard_family": "all",
                        "selected_evidence_category": "measurement_core",
                        "selected_boundary": "boundary:shadow_evaluation_only",
                        "selected_anchor": "multi-source-stability-evidence",
                        "selected_route": "gas",
                        "selected_signal_family": "analyzer_raw",
                        "selected_decision_result": "partial_coverage_gap",
                        "selected_policy_version": "shadow_gas_v1",
                        "selected_evidence_source": "actual_simulated_run",
                        "type_options": [
                            {"id": "all", "label": "All Types"},
                            {"id": "stability", "label": "Stability"},
                        ],
                        "status_options": [
                            {"id": "all", "label": "All Statuses"},
                            {"id": "degraded", "label": "Degraded"},
                        ],
                        "time_options": [{"id": "all", "label": "All Time"}],
                        "source_options": [{"id": "all", "label": "All Sources"}],
                        "phase_options": [{"id": "pressure_stable", "label": "pressure_stable"}],
                        "artifact_role_options": [{"id": "diagnostic_analysis", "label": "diagnostic_analysis"}],
                        "standard_family_options": [{"id": "all", "label": "All Standard Families"}],
                        "evidence_category_options": [{"id": "measurement_core", "label": "measurement_core"}],
                        "boundary_options": [
                            {
                                "id": "boundary:shadow_evaluation_only",
                                "label": t(
                                    "reviewer_fragments.boundary.shadow_evaluation_only",
                                    default="仅影子评估",
                                ),
                            }
                        ],
                        "anchor_options": [
                            {"id": "multi-source-stability-evidence", "label": "multi-source-stability-evidence"}
                        ],
                        "route_options": [{"id": "gas", "label": "gas"}],
                        "signal_family_options": [{"id": "analyzer_raw", "label": "analyzer_raw"}],
                        "decision_result_options": [{"id": "partial_coverage_gap", "label": "partial_coverage_gap"}],
                        "policy_version_options": [{"id": "shadow_gas_v1", "label": "shadow_gas_v1"}],
                        "evidence_source_options": [
                            {"id": "actual_simulated_run", "label": "actual_simulated_run"}
                        ],
                    },
                    "evidence_items": [
                        {
                            "type": "stability",
                            "type_display": "Stability",
                            "status": "degraded",
                            "status_display": "Degraded",
                            "generated_at_display": "04-08 12:00",
                            "summary": "shadow evaluation only | partial coverage",
                            "detail_text": "multi-source stability evidence",
                            "detail_summary": "partial coverage on analyzer_raw",
                            "detail_risk": "review-only",
                            "detail_key_fields": ["shadow_gas_v1", "analyzer_raw", "partial_coverage_gap"],
                            "detail_artifact_paths": ["D:/tmp/run_2/multi_source_stability_evidence.json"],
                            "detail_acceptance_hint": t(
                                "reviewer_fragments.boundary.shadow_evaluation_only",
                                default="仅影子评估",
                            ),
                            "detail_qc_summary": [],
                            "detail_qc_cards": [],
                            "detail_analytics_summary": [],
                            "detail_lineage_summary": [],
                            "source_kind": "run",
                            "phase_filters": ["pressure_stable"],
                            "artifact_role_filters": ["diagnostic_analysis"],
                            "evidence_category_filters": ["measurement_core"],
                            "boundary_filters": ["boundary:shadow_evaluation_only"],
                            "route_filters": ["gas"],
                            "signal_family_filters": ["analyzer_raw"],
                            "decision_result_filters": ["partial_coverage_gap"],
                            "policy_version_filters": ["shadow_gas_v1"],
                            "evidence_source_filters": ["actual_simulated_run"],
                            "anchor_id": "multi-source-stability-evidence",
                            "anchor_label": "multi-source-stability-evidence",
                        },
                        {
                            "type": "stability",
                            "type_display": "Stability",
                            "status": "passed",
                            "status_display": "Passed",
                            "generated_at_display": "04-08 12:01",
                            "summary": "water route",
                            "detail_text": "should be filtered out",
                            "detail_summary": "water route evidence",
                            "detail_risk": "review-only",
                            "detail_key_fields": ["shadow_water_v1", "output", "stable_shadow_pass"],
                            "detail_artifact_paths": ["D:/tmp/run_2/ignored.json"],
                            "detail_acceptance_hint": t(
                                "reviewer_fragments.boundary.shadow_evaluation_only",
                                default="仅影子评估",
                            ),
                            "detail_qc_summary": [],
                            "detail_qc_cards": [],
                            "detail_analytics_summary": [],
                            "detail_lineage_summary": [],
                            "source_kind": "run",
                            "phase_filters": ["preseal"],
                            "artifact_role_filters": ["diagnostic_analysis"],
                            "evidence_category_filters": ["measurement_core"],
                            "boundary_filters": ["boundary:shadow_evaluation_only"],
                            "route_filters": ["water"],
                            "signal_family_filters": ["output"],
                            "decision_result_filters": ["stable_shadow_pass"],
                            "policy_version_filters": ["shadow_water_v1"],
                            "evidence_source_filters": ["model_only"],
                            "anchor_id": "multi-source-stability-evidence",
                            "anchor_label": "multi-source-stability-evidence",
                        }
                    ],
                    "detail_hint": "measurement-core evidence",
                    "empty_detail": "none",
                    "disclaimer": "not real acceptance",
                },
                "residuals": {"series": []},
            }
        )

        assert len(page.review_center.tree.get_children()) == 1
        assert "partial coverage" in page.review_center.detail_summary_var.get()
        assert "multi_source_stability_evidence.json" in page.review_center.detail_artifacts_var.get()
        assert "影子" in page.review_center.detail_acceptance_var.get()
        assert tuple(page.review_center.route_filter["values"]) == ("gas",)
        assert tuple(page.review_center.signal_family_filter["values"]) == ("analyzer_raw",)
        assert tuple(page.review_center.decision_result_filter["values"]) == ("partial_coverage_gap",)
        assert tuple(page.review_center.policy_version_filter["values"]) == ("shadow_gas_v1",)
        assert tuple(page.review_center.evidence_source_filter["values"]) == ("actual_simulated_run",)
        assert "pressure_stable only" in page.measurement_core_summary.get("1.0", "end")
        assert "payload completeness" in page.measurement_core_summary.get("1.0", "end")
    finally:
        root.destroy()


def test_results_page_renders_recognition_readiness_summary_lines() -> None:
    root = make_root()
    try:
        page = ResultsPage(root)
        page.render(
            {
                "overview_text": "run_3",
                "algorithm_compare_text": "amt",
                "result_summary_text": (
                    "scope readiness: Step 2 reviewer readiness | scope package + decision rule profile | payload-complete 0 | payload-partial 0 | trace-only 0 | gap 9\n"
                    "reference/certificate readiness: reference asset / certificate readiness | assets 8 | certificate gaps 7 | intermediate-check gaps 7\n"
                    "uncertainty/method readiness: uncertainty / method confirmation readiness | matrix rows 4 | missing evidence 4\n"
                    "software validation / audit readiness: software validation / audit readiness | trace rows 3 | file-artifact-first reviewer digest"
                ),
                "coefficient_summary_text": "coefficients",
                "qc_summary_text": "simulation-only",
                "measurement_core_summary_text": "measurement-core phase coverage: ambient",
                "ai_summary_text": "review only",
                "review_center": {
                    "operator_focus": {"summary": "operator"},
                    "reviewer_focus": {"summary": "reviewer"},
                    "approver_focus": {"summary": "approver"},
                    "risk_summary": {"level": "medium", "level_display": "medium", "summary": "review only"},
                    "acceptance_readiness": {"summary": "not real acceptance"},
                    "analytics_summary": {"summary": "analytics"},
                    "lineage_summary": {"summary": "lineage"},
                    "index_summary": {"summary": "index", "sources": []},
                    "filters": {
                        "selected_type": "all",
                        "selected_status": "all",
                        "selected_time": "all",
                        "selected_source": "all",
                        "type_options": [{"id": "all", "label": "all"}],
                        "status_options": [{"id": "all", "label": "all"}],
                        "time_options": [{"id": "all", "label": "all"}],
                        "source_options": [{"id": "all", "label": "all"}],
                    },
                    "evidence_items": [],
                    "detail_hint": "ready",
                    "empty_detail": "none",
                    "disclaimer": "not real acceptance",
                },
                "residuals": {"series": []},
            }
        )

        rendered = page.result_summary.get("1.0", "end")
        assert "scope package + decision rule profile" in rendered
        assert "reference asset / certificate readiness" in rendered
        assert "uncertainty / method confirmation readiness" in rendered
        assert "software validation / audit readiness" in rendered
    finally:
        root.destroy()
