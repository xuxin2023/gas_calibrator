from pathlib import Path
import sys

from gas_calibrator.v2.core.phase_transition_bridge_presenter import (
    build_phase_transition_bridge_panel_payload,
)
from gas_calibrator.v2.ui_v2.i18n import t
import gas_calibrator.v2.ui_v2.pages.reports_page as reports_page_module
from gas_calibrator.v2.ui_v2.pages.reports_page import ReportsPage
from gas_calibrator.v2.ui_v2.review_center_presenter import build_artifact_scope_view

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import make_root


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
        "warning_items": [
            "phase_transition_bridge_not_real_acceptance",
        ],
        "reviewer_display": {
            "summary_text": "阶段桥工件：统一说明离第三阶段真实验证还有多远，不是 real acceptance。",
            "status_line": "阶段状态：当前仍处于 Step 2 tail / Stage 3 bridge，但已具备 engineering-isolation 准备。不是 real acceptance。",
            "current_stage_text": "当前阶段：Step 2 tail / Stage 3 bridge。",
            "next_stage_text": "下一阶段：进入 engineering-isolation，继续准备 Stage 3 real validation。",
            "execute_now_text": "现在执行：contract_schema_digest_reporting / governance_artifact_export。",
            "defer_to_stage3_text": "第三阶段执行：real_reference_instrument_enforcement / real_acceptance_pass_fail。",
            "blocking_text": "阻塞项：无。",
            "warning_text": "提示：不能替代真实计量验证。",
            "gate_lines": [
                "simulation_only_boundary：pass（simulation_only）",
            ],
        },
    }


def _build_review_center_payload() -> dict:
    return {
        "operator_focus": {"summary": "operator"},
        "reviewer_focus": {"summary": "reviewer"},
        "approver_focus": {"summary": "approver"},
        "risk_summary": {"summary": "risk"},
        "acceptance_readiness": {"summary": "offline readiness"},
        "analytics_summary": {"summary": "analytics"},
        "lineage_summary": {"summary": "lineage"},
        "analytics_summary": {
            "summary": "analytics",
            "detail": {
                "phase_transition_bridge": _build_phase_transition_bridge_payload(),
            },
        },
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
                "result_summary_text": "运行与治理摘要：离线诊断 room-temp 1 | analyzer-chain 1\n配置安全：blocked",
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
        assert "离线诊断 room-temp 1" in page.result_summary.get("1.0", "end")
        assert "质控摘要" in page.qc_summary.get("1.0", "end")
        assert "Stable" in page.ai_summary.text.get("1.0", "end")

        page.export_bar.export_all()
        page.export_bar.export_review_manifest()

        assert exports == ["all", "manifest:all"]
        assert "review_scope_20260328_142210_all.json" in page.export_bar.status_var.get()
    finally:
        root.destroy()


def test_reports_page_falls_back_to_review_digest_for_result_summary() -> None:
    root = make_root()
    try:
        page = ReportsPage(root)
        page.render(
            {
                "run_dir": "D:/tmp/run_2",
                "files": [],
                "review_center": _build_review_center_payload(),
                "review_digest_text": "离线诊断摘要：room-temp 1 | analyzer-chain 1",
                "qc_summary_text": "",
                "ai_summary_text": "",
                "export": {"available_formats": ["json"], "last_export_message": "Ready"},
            }
        )

        assert "离线诊断摘要" in page.result_summary.get("1.0", "end")
    finally:
        root.destroy()


def test_reports_page_builds_result_summary_from_top_level_handoff() -> None:
    root = make_root()
    try:
        page = ReportsPage(root)
        page.render(
            {
                "run_dir": "D:/tmp/run_3",
                "files": [],
                "review_center": _build_review_center_payload(),
                "review_digest_text": "offline diagnostic digest",
                "evidence_source": "simulated_protocol",
                "config_safety_review": {"summary": "blocked"},
                "offline_diagnostic_adapter_summary": {
                    "summary": "room-temp 2 | analyzer-chain 1",
                    "coverage_summary": "room-temp 2 | analyzer-chain 1 | artifacts 12 | plots 2",
                    "review_scope_summary": "primary 3 | supporting 7 | plots 2",
                    "next_check_summary": "verify ambient chain | inspect analyzer chain",
                    "review_highlight_lines": [
                        "room-temp latest | classification warn | variant ambient_open | dominant pressure_bias | next verify ambient chain",
                        "analyzer-chain latest | continue_s1 hold | conclusion chain mismatch | next inspect analyzer chain",
                        "证据边界: 仅限 simulation/offline/headless evidence，不代表 real acceptance evidence。",
                    ],
                },
                "point_taxonomy_summary": {
                    "pressure_summary": "ambient 1 | ambient_open 1",
                    "pressure_mode_summary": "ambient_open 2",
                    "pressure_target_label_summary": "ambient 1 | ambient_open 1",
                    "flush_gate_summary": "pass 1 | veto 1 | rebound 1",
                    "preseal_summary": "points 1 | max overshoot 4.2 hPa | max sealed wait 1200 ms",
                    "postseal_summary": "timeout blocked 1 | late rebound 1",
                    "stale_gauge_summary": "points 1 | worst 25%",
                },
                "workbench_evidence_summary": {"summary_line": "operator snapshot available"},
                "qc_summary_text": "",
                "ai_summary_text": "",
                "export": {"available_formats": ["json"], "last_export_message": "Ready"},
            }
        )

        summary_text = page.result_summary.get("1.0", "end")

        assert "offline diagnostic digest" in summary_text
        assert "simulated_protocol" in summary_text
        assert "blocked" in summary_text
        assert "room-temp 2 | analyzer-chain 1" in summary_text
        assert "工件 12 | 图表 2" in summary_text
        assert "主工件 3 | 支撑工件 7 | 图表 2" in summary_text
        assert "verify ambient chain | inspect analyzer chain" in summary_text
        assert "verify ambient chain" in summary_text
        assert "inspect analyzer chain" in summary_text
        assert "real acceptance evidence" in summary_text
        assert "ambient 1 | ambient_open 1" in summary_text
        assert "ambient_open 2" in summary_text
        assert "pass 1 | veto 1 | rebound 1" in summary_text
        assert "points 1 | worst 25%" in summary_text
        assert "operator snapshot available" in summary_text
    finally:
        root.destroy()


def test_reports_page_normalizes_offline_diagnostic_scope_lines_in_fallback() -> None:
    root = make_root()
    try:
        page = ReportsPage(root)
        page.render(
            {
                "run_dir": "D:/tmp/run_scope",
                "files": [],
                "review_center": _build_review_center_payload(),
                "offline_diagnostic_adapter_summary": {
                    "summary": "room-temp 1 | analyzer-chain 1",
                    "review_highlight_lines": [
                        "room-temp latest | classification warn | next verify ambient chain | scope artifacts 4 | plots 1",
                    ],
                    "detail_items": [
                        {
                            "detail_line": "analyzer-chain latest | continue_s1 hold | next inspect analyzer chain",
                            "artifact_scope_summary": "artifacts 8 | plots 1",
                        }
                    ],
                },
                "qc_summary_text": "",
                "ai_summary_text": "",
                "export": {"available_formats": ["json"], "last_export_message": "Ready"},
            }
        )

        summary_text = page.result_summary.get("1.0", "end")

        assert "\u5de5\u4ef6\u8303\u56f4: \u5de5\u4ef6 4 | \u56fe\u8868 1" in summary_text
        assert "\u5de5\u4ef6\u8303\u56f4: \u5de5\u4ef6 8 | \u56fe\u8868 1" in summary_text
        assert "scope artifacts 4 | plots 1" not in summary_text
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
        expected_source_scope = build_artifact_scope_view(
            page._artifact_rows,
            selection=page._artifact_scope_snapshot,
        )
        expected_source_reviewer_display = dict(expected_source_scope.get("reviewer_display", {}) or {})

        assert len(page.artifacts.tree.get_children()) == 3
        assert page.artifact_scope_var.get() == expected_source_reviewer_display["summary_text"]
        assert str(page.clear_artifact_scope_button["state"]) == "normal"
        assert page.artifact_count_card.value_var.get() == "3"
        assert page.present_count_card.value_var.get() == "2"
        assert page.run_dir_card.note_var.get() == expected_source_reviewer_display["run_dir_note_text"]
        assert page.artifact_count_card.note_var.get() == expected_source_reviewer_display["scope_note_text"]
        assert page.present_count_card.note_var.get() == expected_source_reviewer_display["present_note_text"]
        assert "2/3" in page.present_count_card.note_var.get()
        assert "可见" in page.artifact_count_card.note_var.get()
        assert "外部" in page.artifact_count_card.note_var.get()
        assert "当前运行基线" in page.artifact_count_card.note_var.get()
        assert "visible " not in page.artifact_count_card.note_var.get()
        assert "external " not in page.artifact_count_card.note_var.get()
        assert "catalog " not in page.artifact_count_card.note_var.get()
        assert "当前审阅视角" in page.run_dir_card.note_var.get()
        assert "当前运行基线" in page.run_dir_card.note_var.get()
        assert page.export_scope_notice_var.get() == expected_source_reviewer_display["export_warning_text"]
        assert "当前运行" in page.export_scope_notice_var.get()
        assert "scope " not in page.export_scope_notice_var.get()

        page.review_center.tree.selection_set("0")
        page.review_center._on_tree_selected()
        expected_evidence_scope = build_artifact_scope_view(
            page._artifact_rows,
            selection=page._artifact_scope_snapshot,
        )
        expected_evidence_reviewer_display = dict(expected_evidence_scope.get("reviewer_display", {}) or {})

        assert len(page.artifacts.tree.get_children()) == 2
        assert "运行门禁" in page.review_center.detail_qc_var.get()
        first_values = page.artifacts.tree.item(page.artifacts.tree.get_children()[0], "values")
        assert t("widgets.artifact_list.origin_current_run") in first_values[2]
        assert "执行摘要" in first_values[3]
        assert "summary.json" in first_values[4]
        assert "offline" in page.artifact_scope_notice_var.get().lower()
        assert page.artifact_count_card.value_var.get() == "2"
        assert page.present_count_card.value_var.get() == "1"
        assert page.run_dir_card.note_var.get() == expected_evidence_reviewer_display["run_dir_note_text"]
        assert page.artifact_count_card.note_var.get() == expected_evidence_reviewer_display["scope_note_text"]
        assert page.present_count_card.note_var.get() == expected_evidence_reviewer_display["present_note_text"]
        assert "1/2" in page.present_count_card.note_var.get()
        assert "缺失" in page.present_count_card.note_var.get()
        assert "catalog " not in page.present_count_card.note_var.get()

        page._clear_artifact_scope()

        assert len(page.artifacts.tree.get_children()) == 3
        assert str(page.clear_artifact_scope_button["state"]) == "disabled"
        assert page.export_scope_notice_var.get() == ""
    finally:
        root.destroy()


def test_reports_page_prefers_artifact_scope_reviewer_display_payload(monkeypatch) -> None:
    root = make_root()
    try:
        page = ReportsPage(root)

        def _fake_scope_view(_files, *, selection=None):
            return {
                "rows": [],
                "summary_text": "top-level summary",
                "empty_text": "top-level empty",
                "disclaimer_text": "top-level disclaimer",
                "run_dir_note_text": "top-level run dir",
                "scope_note_text": "top-level scope note",
                "present_note_text": "top-level present note",
                "export_warning_text": "top-level warning",
                "reviewer_display": {
                    "summary_text": "reviewer summary",
                    "empty_text": "reviewer empty",
                    "run_dir_note_text": "reviewer run dir",
                    "scope_note_text": "reviewer scope note",
                    "present_note_text": "reviewer present note",
                    "export_warning_text": "reviewer warning",
                },
                "clear_enabled": False,
                "visible_count": 0,
                "scope_present_count": 0,
                "scope_visible_count": 0,
                "scope_external_count": 0,
                "scope_missing_count": 0,
            }

        monkeypatch.setattr(reports_page_module, "build_artifact_scope_view", _fake_scope_view)

        page.render(
            {
                "run_dir": "D:/tmp/run_scope_payload",
                "files": [],
                "review_center": _build_review_center_payload(),
                "qc_summary_text": "",
                "ai_summary_text": "",
                "export": {"available_formats": ["json"], "last_export_message": "Ready"},
            }
        )

        assert page.artifact_scope_var.get() == "reviewer summary"
        assert page.artifact_scope_notice_var.get() == "reviewer empty"
        assert page.run_dir_card.note_var.get() == "reviewer run dir"
        assert page.artifact_count_card.note_var.get() == "reviewer scope note"
        assert page.present_count_card.note_var.get() == "reviewer present note"
        assert page.export_scope_notice_var.get() == "reviewer warning"
    finally:
        root.destroy()


def test_reports_page_includes_phase_transition_bridge_digest_in_result_summary_fallback() -> None:
    root = make_root()
    try:
        page = ReportsPage(root)
        page.render(
            {
                "run_dir": "D:/tmp/run_bridge",
                "files": [],
                "review_center": _build_review_center_payload(),
                "qc_summary_text": "",
                "ai_summary_text": "",
                "export": {"available_formats": ["json"], "last_export_message": "Ready"},
            }
        )

        summary_text = page.result_summary.get("1.0", "end")

        assert "阶段桥工件" in summary_text
        assert "Step 2 tail / Stage 3 bridge" in summary_text
        assert "engineering-isolation" in summary_text
        assert "现在执行：contract_schema_digest_reporting / governance_artifact_export。" in summary_text
        assert "第三阶段执行：real_reference_instrument_enforcement / real_acceptance_pass_fail。" in summary_text
        assert "不是 real acceptance" in summary_text
        assert "不能替代真实计量验证" in summary_text
    finally:
        root.destroy()


def test_reports_page_exposes_phase_transition_bridge_as_dedicated_section() -> None:
    root = make_root()
    try:
        page = ReportsPage(root)
        page.render(
            {
                "run_dir": "D:/tmp/run_bridge_section",
                "files": [],
                "review_center": _build_review_center_payload(),
                "result_summary_text": "已有运行摘要",
                "qc_summary_text": "",
                "ai_summary_text": "",
                "export": {"available_formats": ["json"], "last_export_message": "Ready"},
            }
        )

        bridge_text = page.phase_bridge_section.get("1.0", "end")
        result_summary_text = page.result_summary.get("1.0", "end")

        assert "阶段桥工件" in bridge_text
        assert "Step 2 tail / Stage 3 bridge" in bridge_text
        assert "engineering-isolation" in bridge_text
        assert "现在执行" in bridge_text
        assert "第三阶段执行" in bridge_text
        assert "不是 real acceptance" in bridge_text
        assert "不能替代真实计量验证" in bridge_text
        assert "已有运行摘要" in result_summary_text
    finally:
        root.destroy()


def test_reports_page_phase_transition_bridge_section_matches_presenter_payload() -> None:
    root = make_root()
    try:
        page = ReportsPage(root)
        bridge = _build_phase_transition_bridge_payload()
        expected_panel = build_phase_transition_bridge_panel_payload(bridge)
        page.render(
            {
                "run_dir": "D:/tmp/run_bridge_section_parity",
                "files": [],
                "review_center": _build_review_center_payload(),
                "result_summary_text": "已有运行摘要",
                "qc_summary_text": "",
                "ai_summary_text": "",
                "export": {"available_formats": ["json"], "last_export_message": "Ready"},
            }
        )

        bridge_text = page.phase_bridge_section.get("1.0", "end").strip()

        assert bridge_text == expected_panel["display"]["section_text"]
        assert "Step 2 tail / Stage 3 bridge" in bridge_text
        assert "engineering-isolation" in bridge_text
        assert expected_panel["display"]["execute_now_text"] in bridge_text
        assert expected_panel["display"]["defer_to_stage3_text"] in bridge_text
        assert "不是 real acceptance" in bridge_text
        assert "不能替代真实计量验证" in bridge_text
    finally:
        root.destroy()
