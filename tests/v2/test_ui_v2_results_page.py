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
