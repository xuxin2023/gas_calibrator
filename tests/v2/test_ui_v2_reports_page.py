from pathlib import Path
import sys

from gas_calibrator.v2.ui_v2.i18n import t
from gas_calibrator.v2.ui_v2.pages.reports_page import ReportsPage

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import make_root


def _build_review_center_payload() -> dict:
    return {
        "operator_focus": {"summary": "operator"},
        "reviewer_focus": {"summary": "reviewer"},
        "approver_focus": {"summary": "approver"},
        "risk_summary": {"summary": "risk"},
        "acceptance_readiness": {"summary": "offline readiness"},
        "analytics_summary": {"summary": "analytics"},
        "lineage_summary": {"summary": "lineage"},
        "index_summary": {
            "summary": "recent sources 2",
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
            "type_options": [{"id": "all", "label": t("results.review_center.filter.all_types")}],
            "status_options": [{"id": "all", "label": t("results.review_center.filter.all_statuses")}],
            "time_options": [{"id": "all", "label": t("results.review_center.filter.all_time"), "window_seconds": None}],
            "source_options": [{"id": "all", "label": t("results.review_center.filter.all_sources")}],
        },
        "evidence_items": [
            {
                "type": "suite",
                "type_display": t("results.review_center.type.suite"),
                "status": "passed",
                "status_display": t("results.review_center.status.passed"),
                "generated_at_display": "03-27 10:00",
                "sort_key": 100.0,
                "summary": "suite a",
                "detail_text": "suite detail a",
                "detail_summary": "suite a",
                "detail_qc_summary": ["运行门禁: warn | 原因: 存在预警点"],
                "detail_artifact_paths": [
                    "D:/tmp/branch_a/shared_run/summary.json",
                    "D:/tmp/branch_a/shared_run/manifest.json",
                ],
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
                "sort_key": 101.0,
                "summary": "analytics a",
                "detail_text": "analytics detail a",
                "detail_summary": "analytics a",
                "detail_artifact_paths": ["D:/tmp/branch_a/shared_run/analytics_summary.json"],
                "source_kind": "run",
                "source_scope": "run",
                "source_id": "D:/tmp/branch_a/shared_run",
                "source_label": "shared_run",
                "source_dir": "D:/tmp/branch_a/shared_run",
            },
            {
                "type": "parity",
                "type_display": t("results.review_center.type.parity"),
                "status": "failed",
                "status_display": t("results.review_center.status.failed"),
                "generated_at_display": "03-27 11:00",
                "sort_key": 102.0,
                "summary": "parity b",
                "detail_text": "parity detail b",
                "detail_summary": "parity b",
                "detail_artifact_paths": ["D:/tmp/branch_b/shared_run/summary_parity_report.json"],
                "source_kind": "run",
                "source_scope": "run",
                "source_id": "D:/tmp/branch_b/shared_run",
                "source_label": "shared_run",
                "source_dir": "D:/tmp/branch_b/shared_run",
            },
        ],
        "detail_hint": "select evidence",
        "empty_detail": "no evidence",
        "disclaimer": "offline only; not real acceptance.",
    }


def test_reports_page_displays_snapshot() -> None:
    root = make_root()
    try:
        exports: list[str] = []

        class _Exporter:
            def export_artifacts(self, export_format: str):
                exports.append(export_format)
                return True, f"exported {export_format}"

            def export_review_scope_manifest(self, *, selection):
                exports.append(f"manifest:{selection.get('scope', 'all')}")
                return {
                    "ok": True,
                    "message": "review_scope_20260328_142210_all.json",
                }

        page = ReportsPage(root, exporter=_Exporter())
        page.render(
            {
                "run_dir": "D:/tmp/run_1",
                "files": [
                    {
                        "name": "summary.json",
                        "present": True,
                        "path": "D:/tmp/run_1/summary.json",
                        "listed_in_current_run": True,
                        "artifact_origin": "current_run",
                        "artifact_role": "execution_summary",
                        "export_status": "ok",
                        "export_status_known": True,
                        "exportable_in_current_run": True,
                    }
                ],
                "review_center": _build_review_center_payload(),
                "qc_summary_text": "质控摘要：运行门禁 warn | 点级门禁 warn\n证据边界：仅供 simulation/offline 审阅，不代表 real acceptance evidence。",
                "ai_summary_text": "# AI Run Summary\nStable",
                "export": {"available_formats": ["json", "csv", "all"], "last_export_message": "Ready"},
            }
        )

        assert page.page_scaffold is not None
        assert page.run_dir_card.value_var.get() == "D:/tmp/run_1"
        assert t("pages.reports.artifact_scope.label_all") in page.run_dir_card.note_var.get()
        assert page.artifact_count_card.value_var.get() == "1"
        assert page.present_count_card.value_var.get() == "1"
        assert len(page.artifacts.tree.get_children()) == 1
        assert "执行摘要" in page.artifacts.tree.item(page.artifacts.tree.get_children()[0], "values")[3]
        assert "质控摘要" in page.qc_summary.get("1.0", "end")
        assert "Stable" in page.ai_summary.text.get("1.0", "end")

        page.export_bar.export_all()
        page.export_bar.export_review_manifest()

        assert exports == ["all", "manifest:all"]
        assert "review_scope_20260328_142210_all.json" in page.export_bar.status_var.get()
    finally:
        root.destroy()


def test_reports_page_artifact_list_follows_review_center_source_and_evidence_scope() -> None:
    root = make_root()
    try:
        page = ReportsPage(root)
        page.render(
            {
                "run_dir": "D:/tmp/run_1",
                "files": [
                    {"name": "summary.json", "present": True, "path": "D:/tmp/branch_a/shared_run/summary.json"},
                    {"name": "analytics_summary.json", "present": True, "path": "D:/tmp/branch_a/shared_run/analytics_summary.json"},
                    {"name": "summary_parity_report.json", "present": True, "path": "D:/tmp/branch_b/shared_run/summary_parity_report.json"},
                ],
                "review_center": _build_review_center_payload(),
                "qc_summary_text": "质控摘要：运行门禁 warn | 点级门禁 warn\n证据边界：仅供 simulation/offline 审阅，不代表 real acceptance evidence。",
                "ai_summary_text": "AI summary",
                "export": {"available_formats": ["json", "csv", "all"], "last_export_message": "Ready"},
            }
        )

        assert len(page.artifacts.tree.get_children()) == 3
        assert str(page.clear_artifact_scope_button["state"]) == "disabled"
        assert page.export_scope_notice_var.get() == ""

        page.review_center.source_tree.selection_set("source-0")
        page.review_center._on_source_selected()

        assert len(page.artifacts.tree.get_children()) == 3
        assert "shared_run" in page.artifact_scope_var.get()
        assert str(page.clear_artifact_scope_button["state"]) == "normal"
        assert page.artifact_count_card.value_var.get() == "3"
        assert page.present_count_card.value_var.get() == "2"
        assert "2/3" in page.present_count_card.note_var.get()
        assert page.export_scope_notice_var.get()

        page.review_center.tree.selection_set("0")
        page.review_center._on_tree_selected()

        assert len(page.artifacts.tree.get_children()) == 2
        assert "运行门禁" in page.review_center.detail_qc_var.get()
        first_values = page.artifacts.tree.item(page.artifacts.tree.get_children()[0], "values")
        assert t("widgets.artifact_list.origin_current_run") in first_values[2]
        assert "执行摘要" in first_values[3]
        assert "summary.json" in first_values[4]
        assert "offline" in page.artifact_scope_notice_var.get().lower()
        assert page.artifact_count_card.value_var.get() == "2"
        assert page.present_count_card.value_var.get() == "1"
        assert "1/2" in page.present_count_card.note_var.get()

        page._clear_artifact_scope()

        assert len(page.artifacts.tree.get_children()) == 3
        assert str(page.clear_artifact_scope_button["state"]) == "disabled"
        assert page.export_scope_notice_var.get() == ""
    finally:
        root.destroy()
