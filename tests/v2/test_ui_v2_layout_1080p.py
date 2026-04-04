from pathlib import Path
import sys

from gas_calibrator.v2.ui_v2.app import build_application
from gas_calibrator.v2.ui_v2.pages.devices_page import DevicesPage
from gas_calibrator.v2.ui_v2.pages.qc_page import QCPage
from gas_calibrator.v2.ui_v2.pages.reports_page import ReportsPage
from gas_calibrator.v2.ui_v2.pages.results_page import ResultsPage
from gas_calibrator.v2.ui_v2.styles import apply_styles

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import build_fake_facade, make_root


def _show_1080p(root) -> None:
    root.geometry("1920x1080+0+0")
    root.deiconify()
    root.update_idletasks()
    root.update()


def _mount_page(root, page) -> None:
    root.rowconfigure(0, weight=1)
    root.columnconfigure(0, weight=1)
    page.grid(row=0, column=0, sticky="nsew")
    _show_1080p(root)


def _assert_scroll_safety(page) -> None:
    assert page.page_scaffold is not None
    overflow = page.page_scaffold.has_overflow()
    assert (not overflow) or page.page_scaffold.is_scrollbar_visible()
    assert page.page_scaffold.canvas.winfo_height() > 0
    assert page.page_scaffold.canvas.winfo_height() <= page.winfo_height()


def test_shell_layout_is_1080p_friendly(tmp_path: Path) -> None:
    root = make_root()
    shell = None
    try:
        apply_styles(root)
        facade = build_fake_facade(tmp_path)
        _, shell, _ = build_application(root=root, facade=facade, start_feed=False)
        _show_1080p(root)

        assert shell.workspace.winfo_height() > shell.log_panel.winfo_height()
        assert shell.log_panel.text.winfo_height() > 0
        assert shell.main_split.winfo_height() <= root.winfo_height()
    finally:
        if shell is not None:
            shell.shutdown()
        root.destroy()


def test_devices_page_keeps_operator_controls_visible_or_scrollable_at_1080p(tmp_path: Path) -> None:
    root = make_root()
    try:
        apply_styles(root)
        facade = build_fake_facade(tmp_path)
        page = DevicesPage(root, facade=facade)
        _mount_page(root, page)
        page.render(facade.build_snapshot()["devices"])
        _show_1080p(root)

        _assert_scroll_safety(page)
        assert page.workbench.notebook.winfo_ismapped()
        assert page.workbench.engineer_frame.winfo_manager() == ""
    finally:
        root.destroy()


def test_results_qc_and_reports_pages_have_1080p_scroll_safety(tmp_path: Path) -> None:
    snapshots = {
        "results": {
            "overview_text": "运行 ID：run_1080",
            "algorithm_compare_text": "默认算法：amt",
            "result_summary_text": "结果文件：已生成",
            "coefficient_summary_text": "calibration_coefficients.xlsx",
            "ai_summary_text": "# AI 运行摘要\n页面布局已收口。",
            "residuals": {"series": [{"algorithm": "amt", "residuals": [0.5, -0.2, 0.1, 0.05]}]},
        },
        "qc": {
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
            "recommendations": ["拟合前请复核无效点。"],
            "overview": {"score": 0.81, "grade": "B", "valid_points": 1, "invalid_points": 1, "total_points": 2},
            "reject_reasons_chart": {"rows": [{"reason": "outlier_ratio_too_high", "count": 1}]},
        },
        "reports": {
            "run_dir": str(tmp_path / "run_1080"),
            "files": [{"name": "summary.json", "present": True, "path": str(tmp_path / "run_1080" / "summary.json")}],
            "ai_summary_text": "# AI 报告摘要\n导出栏与工件列表分栏显示。",
            "export": {"available_formats": ["json", "csv", "all"], "last_export_message": "就绪"},
        },
    }

    root = make_root()
    try:
        apply_styles(root)

        results_page = ResultsPage(root)
        _mount_page(root, results_page)
        results_page.render(snapshots["results"])
        _assert_scroll_safety(results_page)
        results_page.grid_remove()

        qc_page = QCPage(root)
        _mount_page(root, qc_page)
        qc_page.render(snapshots["qc"])
        _assert_scroll_safety(qc_page)
        qc_page.grid_remove()

        reports_page = ReportsPage(root)
        _mount_page(root, reports_page)
        reports_page.render(snapshots["reports"])
        _assert_scroll_safety(reports_page)
    finally:
        root.destroy()
