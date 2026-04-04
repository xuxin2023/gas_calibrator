from pathlib import Path
import sys

from gas_calibrator.v2.ui_v2.i18n import display_compare_status, display_phase, display_route, display_run_mode
from gas_calibrator.v2.ui_v2.pages.run_control_page import RunControlPage

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import make_root


class _FakeController:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str | None, str | None]] = []
        self.preview_calls: list[tuple[str, str | None, str | None]] = []

    def start(self, points_path=None, *, points_source="use_points_file", run_mode=None):
        self.calls.append(("start", points_source, points_path, run_mode))
        return True, "已开始"

    def preview_points(self, points_path=None, *, points_source="use_points_file", run_mode=None):
        self.preview_calls.append((points_source, points_path, run_mode))
        summary = (
            "默认配置档 demo | profile=demo | source=2 | runtime=2 | prepared=2 | preview=2"
            if points_source == "use_default_profile"
            else "点表 2 行 | 执行预览 2 点"
        )
        return {
            "ok": True,
            "run_mode": "co2_measurement" if points_source == "use_default_profile" else (run_mode or "auto_calibration"),
            "summary": summary,
            "rows": [
                {
                    "seq": "1",
                    "row": "2",
                    "temp": "25C",
                    "route": "H2O",
                    "hgen": "25C / 30%RH",
                    "co2": "--",
                    "pressure": "1000hPa",
                    "group": "--",
                    "status": "compiled",
                },
                {
                    "seq": "2",
                    "row": "3",
                    "temp": "25C",
                    "route": "CO2",
                    "hgen": "--",
                    "co2": "400ppm",
                    "pressure": "1000hPa",
                    "group": "A",
                    "status": "已编译",
                },
            ],
        }

    def edit_points_file(self, points_path=None, *, points_source="use_points_file"):
        self.calls.append(("edit", points_source, points_path, None))
        return True, "正在编辑"

    def pause(self):
        self.calls.append(("pause", "use_points_file", None, None))
        return True, "已暂停"

    def resume(self):
        self.calls.append(("resume", "use_points_file", None, None))
        return True, "已继续"

    def stop(self):
        self.calls.append(("stop", "use_points_file", None, None))
        return True, "已停止"


def test_run_control_page_renders_snapshot_and_controls(tmp_path: Path) -> None:
    root = make_root()
    try:
        controller = _FakeController()
        page = RunControlPage(root, controller=controller, initial_points_path=str(tmp_path / "points.json"))
        assert page.page_scaffold is not None
        page.render(
            {
                "run_id": "run_1",
                "phase": "co2_route",
                "phase_display": "气路执行",
                "current_point": "#1 T=25C",
                "progress_pct": 50.0,
                "route": "co2",
                "route_display": "气路",
                "retry": 1,
                "message": "Sampling",
                "message_display": "正在采样",
                "device_rows": [{"name": "gas_analyzer_0", "status": "online", "port": "COM2"}],
                "disabled_analyzers": ["gas_analyzer_1"],
                "route_state": {"pressure_ready": True},
                "route_phase": "co2_route",
                "source_point": "#1",
                "active_point": "#1",
                "point_tag": "co2-400ppm",
                "validation": {
                    "available": True,
                    "validation_profile": "skip0_co2_only_replacement",
                    "compare_status": "NOT_EXECUTED",
                    "evidence_source": "real",
                    "evidence_state": "--",
                    "first_failure_phase": "v2:startup.sensor_precheck",
                    "entered_target_route": {"v1": False, "v2": False},
                    "target_route_event_count": {"v1": 0, "v2": 0},
                    "bench_context": {
                        "co2_0ppm_available": False,
                        "other_gases_available": True,
                        "h2o_route_available": False,
                        "humidity_generator_humidity_feedback_valid": False,
                    },
                    "reference_quality": {
                        "reference_quality": "degraded",
                        "thermometer_reference_status": "stale",
                        "pressure_reference_status": "healthy",
                    },
                    "route_physical_validation": {
                        "route_physical_state_match": {"v1": True, "v2": False},
                        "relay_physical_mismatch": {"v1": False, "v2": True},
                    },
                    "artifact_bundle_path": str(tmp_path / "skip0_co2_only_replacement_bundle.json"),
                    "report_dir": str(tmp_path / "compare_run"),
                    "gate_state": {
                        "checklist_gate": "12A",
                        "target_route": "co2",
                        "single_temp": True,
                    },
                    "promotion_state": "dry_run_only",
                    "review_state": "pending",
                    "approval_state": "blocked",
                    "ready_for_promotion": False,
                    "missing_conditions": ["real acceptance evidence present", "approval granted"],
                    "readiness_summary": {"summary_display": "离线回归 | 仅 dry-run | 缺少 2 项条件"},
                },
                "results": {
                    "analytics_summary_digest": {"summary_display": "覆盖 1/1 | 参考降级 | 导出 0 错误 | 一致性缺失"},
                    "lineage_digest": {"config_version": "cfg-abc", "points_version": "pts-xyz", "profile_version": "2.5"},
                },
                "timeseries": {"series": {"temperature_c": [24.8, 25.0], "pressure_hpa": [998.0, 1000.0]}},
                "route_progress": {"route": "co2", "route_phase": "co2_route", "points_completed": 1, "points_total": 2, "steps": ["H2O", "CO2", "Finalize"]},
            }
        )

        assert page.run_id_var.get() == "run_1"
        assert page.phase_var.get() == display_phase("co2_route")
        assert page.route_var.get() == display_route("co2")
        assert page.validation_profile_var.get() == "skip0_co2_only_replacement"
        assert page.validation_status_var.get() == display_compare_status("NOT_EXECUTED")
        assert "真实" in page.validation_evidence_var.get()
        assert "验收" in page.validation_evidence_var.get()
        assert "12A / 气路 / 单温度" == page.validation_gate_var.get()
        assert page.readiness_var.get() == "离线回归 | 仅 dry-run | 缺少 2 项条件"
        assert page.analytics_var.get().startswith("覆盖 1/1")
        assert page.lineage_var.get() == "cfg-abc / pts-xyz / 2.5"
        validation_text = page._validation_text.get("1.0", "end")
        assert "验证档案" in validation_text
        assert "参考质量" in validation_text
        assert "路由物理一致" in validation_text
        assert "回退候选" not in validation_text
        assert "h2o_route_available" not in validation_text
        assert page.points_preview_hint_var.get() == "点表 2 行 | 执行预览 2 点"
        assert len(page._points_tree.get_children()) == 2
        assert page.timeseries.canvas.find_all()
        assert page.route_timeline.canvas.find_all()

        page._on_edit_points()
        page._on_start()
        page._on_pause()
        page._on_resume()
        page._on_stop()

        assert controller.calls == [
            ("edit", "use_points_file", str(tmp_path / "points.json"), None),
            ("start", "use_points_file", str(tmp_path / "points.json"), "auto_calibration"),
            ("pause", "use_points_file", None, None),
            ("resume", "use_points_file", None, None),
            ("stop", "use_points_file", None, None),
        ]
        assert controller.preview_calls == [("use_points_file", str(tmp_path / "points.json"), "auto_calibration")]
    finally:
        root.destroy()


def test_run_control_page_can_switch_to_default_profile_mode(tmp_path: Path) -> None:
    root = make_root()
    try:
        controller = _FakeController()
        page = RunControlPage(root, controller=controller, initial_points_path=str(tmp_path / "points.json"))

        page.points_source_var.set("use_default_profile")
        page._on_points_source_changed()
        page._on_start()

        assert str(page.points_entry.cget("state")) == "disabled"
        assert str(page.edit_points_button.cget("state")) == "disabled"
        assert str(page.run_mode_combo.cget("state")) == "disabled"
        assert page.run_mode_var.get() == display_run_mode("co2_measurement")
        assert "默认配置档 demo" in page.points_preview_hint_var.get()
        assert controller.calls[-1] == ("start", "use_default_profile", str(tmp_path / "points.json"), "co2_measurement")
        assert controller.preview_calls[-1] == ("use_default_profile", str(tmp_path / "points.json"), "auto_calibration")
    finally:
        root.destroy()


def test_run_control_page_renders_primary_latest_missing_status(tmp_path: Path) -> None:
    root = make_root()
    try:
        controller = _FakeController()
        page = RunControlPage(root, controller=controller, initial_points_path=str(tmp_path / "points.json"))
        page.render(
            {
                "validation": {
                    "available": True,
                    "validation_profile": "skip0_co2_only_replacement",
                    "compare_status": "PRIMARY_REAL_VALIDATION_LATEST_MISSING",
                    "evidence_source": "real",
                    "evidence_state": "primary_validation_latest_missing",
                    "diagnostic_only": False,
                    "acceptance_evidence": True,
                    "primary_real_latest_missing": True,
                    "primary_latest_missing": True,
                    "first_failure_phase": "primary_validation_latest_missing",
                    "entered_target_route": {},
                    "target_route_event_count": {},
                    "bench_context": {
                        "co2_0ppm_available": False,
                        "other_gases_available": True,
                        "h2o_route_available": False,
                        "humidity_generator_humidity_feedback_valid": False,
                    },
                    "fallback_candidates": [
                        {
                            "validation_profile": "skip0_co2_only_diagnostic_relaxed",
                            "compare_status": "NOT_EXECUTED",
                            "evidence_state": "route_unblock_diagnostic",
                            "diagnostic_only": True,
                        }
                    ],
                    "evidence_layers": [
                        {
                            "tier": "primary",
                            "validation_profile": "skip0_co2_only_replacement",
                            "compare_status": "PRIMARY_REAL_VALIDATION_LATEST_MISSING",
                            "evidence_source": "real",
                            "evidence_state": "primary_validation_latest_missing",
                            "diagnostic_only": False,
                        },
                        {
                            "tier": "diagnostic",
                            "validation_profile": "skip0_co2_only_diagnostic_relaxed",
                            "compare_status": "NOT_EXECUTED",
                            "evidence_source": "real",
                            "evidence_state": "route_unblock_diagnostic",
                            "diagnostic_only": True,
                        },
                    ],
                    "gate_state": {
                        "checklist_gate": "12A",
                        "target_route": "co2",
                        "single_temp": True,
                    },
                    "promotion_state": "dry_run_only",
                    "review_state": "pending",
                    "approval_state": "blocked",
                    "ready_for_promotion": False,
                    "missing_conditions": ["real acceptance evidence present"],
                    "readiness_summary": {"summary_display": "离线回归 | 仅 dry-run | 缺少 1 项条件"},
                }
            }
        )

        assert page.validation_status_var.get() == display_compare_status("PRIMARY_REAL_VALIDATION_LATEST_MISSING")
        assert "主证据缺失" in page.validation_evidence_var.get()
        assert page.readiness_var.get() == "离线回归 | 仅 dry-run | 缺少 1 项条件"
        validation_text = page._validation_text.get("1.0", "end")
        assert "证据层" in validation_text
        assert "回退候选" in validation_text
        assert "skip0_co2_only_diagnostic_relaxed" in validation_text
        assert "\"evidence_layers\"" not in validation_text
    finally:
        root.destroy()
