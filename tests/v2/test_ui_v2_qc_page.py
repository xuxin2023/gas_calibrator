from pathlib import Path
import sys

from gas_calibrator.v2.ui_v2.pages.qc_page import QCPage

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import make_root


def test_qc_page_displays_qc_summary_and_rows() -> None:
    root = make_root()
    try:
        page = QCPage(root)
        page.render(
            {
                "overall_score": 0.81,
                "grade": "B",
                "valid_points": 1,
                "invalid_points": 1,
                "total_points": 2,
                "point_rows": [
                    {"point_index": 1, "route": "co2", "temperature_c": 25.0, "co2_ppm": 400.0, "quality_score": 0.92, "valid": True, "reason": "passed"},
                    {"point_index": 2, "route": "co2", "temperature_c": 25.0, "co2_ppm": 0.0, "quality_score": 0.58, "valid": False, "reason": "outlier_ratio_too_high"},
                ],
                "invalid_reasons": ["outlier_ratio_too_high"],
                "recommendations": ["Review invalid points before fitting."],
                "decision_counts": {"pass": 1, "warn": 1, "reject": 0, "skipped": 0},
                "run_gate": {"status": "warn", "reason": "review_required"},
                "point_gate_summary": {"status": "warn", "flagged_point_count": 1, "flagged_routes": ["co2"]},
                "route_decision_breakdown": {"co2": {"pass": 1, "warn": 1, "reject": 0, "skipped": 0}},
                "reject_reason_taxonomy": [{"code": "outlier_ratio_too_high", "category": "outlier", "count": 1}],
                "failed_check_taxonomy": [{"code": "signal_span", "category": "signal", "count": 1}],
                "reviewer_digest": {
                    "summary": "运行 run-1 质控评分 0.81 / 等级 B；通过 1，预警 1，拒绝 0，跳过 0；门禁 warn。",
                    "lines": ["点级门禁: warn | 关注路由: co2", "失败检查: signal_span"],
                },
                "rule_profile": {"name": "default"},
                "threshold_profile": {"min_sample_count": 3, "pass_threshold": 0.8, "warn_threshold": 0.6, "reject_threshold": 0.4},
                "evidence_boundary": {"evidence_source": "simulated_protocol"},
                "overview": {"score": 0.81, "grade": "B", "valid_points": 1, "invalid_points": 1, "total_points": 2},
                "reject_reasons_chart": {"rows": [{"reason": "outlier_ratio_too_high", "count": 1}]},
            }
        )

        assert page.page_scaffold is not None
        assert page.grade_var.get() == "B"
        assert page.total_var.get() == "2"
        assert page.overview.score_card.value_var.get() == "0.81"
        assert page.reject_chart.canvas.find_all()
        assert len(page.tree.get_children()) == 2
        assert "Review invalid points" in page.details.get("1.0", "end")
        assert "质控摘要" in page.details.get("1.0", "end")
        assert "审阅卡片" in page.details.get("1.0", "end")
        assert "点级门禁" in page.details.get("1.0", "end")
        assert "路由分布" in page.details.get("1.0", "end")
        assert "signal_span" in page.details.get("1.0", "end")
        assert "失败检查分类" in page.details.get("1.0", "end")
        assert "结果分级" in page.details.get("1.0", "end")
        assert "运行门禁" in page.details.get("1.0", "end")
        assert "证据边界" in page.details.get("1.0", "end")
        assert "仅限 simulation/offline" in page.details.get("1.0", "end")
    finally:
        root.destroy()
