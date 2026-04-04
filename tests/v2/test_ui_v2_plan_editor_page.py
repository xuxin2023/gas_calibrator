import json
from pathlib import Path
import sys

from gas_calibrator.v2.config import AppConfig
from gas_calibrator.v2.storage import ProfileStore
from gas_calibrator.v2.ui_v2.controllers.plan_gateway import PlanGateway
from gas_calibrator.v2.ui_v2.i18n import (
    display_analyzer_software_version,
    display_device_id_assignment,
    display_run_mode,
)

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from gas_calibrator.v2.ui_v2.pages.plan_editor_page import PlanEditorPage
from ui_v2_support import build_fake_facade, make_root


def test_plan_editor_page_saves_loads_sets_default_and_compiles(tmp_path: Path) -> None:
    root = make_root()
    try:
        facade = build_fake_facade(tmp_path)
        page = PlanEditorPage(root, facade=facade)
        page.pack(fill="both", expand=True)

        page.profile_name_var.set("bench_profile")
        page.profile_version_var.set("2.1")
        page.description_var.set("bench plan")
        page.run_mode_var.set(display_run_mode("co2_measurement"))
        page.analyzer_version_var.set(display_analyzer_software_version("pre_v5"))
        page.device_id_assignment_var.set(display_device_id_assignment("manual"))
        page.start_device_id_var.set("7")
        page.manual_device_ids_var.set("011,012")
        page.selected_temps_var.set("25")
        page.skip_co2_var.set("0")
        page.water_first_var.set(True)
        page.water_first_temp_gte_var.set("25")

        page.temp_value_var.set("25")
        page._add_temperature()
        page.humidity_temp_var.set("25")
        page.humidity_rh_var.set("40")
        page._add_humidity()
        page.gas_value_var.set("400")
        page.gas_group_var.set("B")
        page.gas_cylinder_var.set("405")
        page._add_gas()
        page.pressure_value_var.set("1000")
        page._add_pressure()

        page._save_profile()
        page._set_default_profile()
        page._compile_preview()

        assert page.profile_name_var.get() == "bench_profile"
        assert facade.plan_gateway.load_profile("bench_profile")["profile_version"] == "2.1"
        assert facade.plan_gateway.list_profiles()[0]["name"] == "bench_profile"
        assert facade.plan_gateway.list_profiles()[0]["is_default"] is True
        assert page.default_status_var.get() == "默认档案：是"
        assert facade.plan_gateway.load_profile("bench_profile")["run_mode"] == "co2_measurement"
        assert facade.plan_gateway.load_profile("bench_profile")["analyzer_setup"]["software_version"] == "pre_v5"
        assert facade.plan_gateway.load_profile("bench_profile")["analyzer_setup"]["manual_device_ids"] == ["011", "012"]
        assert facade.plan_gateway.load_profile("bench_profile")["ordering"]["water_first_temp_gte"] == 25.0
        assert facade.plan_gateway.load_profile("bench_profile")["gas_points"][0]["co2_group"] == "B"
        assert facade.plan_gateway.load_profile("bench_profile")["gas_points"][0]["cylinder_nominal_ppm"] == 405.0
        assert len(page.preview_tree.get_children()) > 0
        assert any(page.preview_tree.item(item_id)["values"][7] == "B" for item_id in page.preview_tree.get_children())
        assert any(page.preview_tree.item(item_id)["values"][8] == "405ppm" for item_id in page.preview_tree.get_children())
        assert "profile=bench_profile" in page.preview_summary_var.get()

        page._new_profile()
        page.profile_listbox.selection_set(0)
        page._load_selected_profile()

        assert page.profile_name_var.get() == "bench_profile"
        assert page.profile_version_var.get() == "2.1"
        assert page.description_var.get() == "bench plan"
        assert page.run_mode_var.get() == display_run_mode("co2_measurement")
        assert page.analyzer_version_var.get() == display_analyzer_software_version("pre_v5")
        assert page.device_id_assignment_var.get() == display_device_id_assignment("manual")
        assert page.start_device_id_var.get() == "007"
        assert page.manual_device_ids_var.get() == "011, 012"
        assert page.water_first_temp_gte_var.get() == "25"
        assert page.gas_group_var.get() == "B"
        assert page.gas_cylinder_var.get() == "405"
        assert len(page._temperature_rows) == 1
        assert len(page._humidity_rows) == 1
        assert len(page._gas_rows) == 1
        assert len(page._pressure_rows) == 1
    finally:
        root.destroy()


def test_plan_editor_page_deletes_profile(tmp_path: Path, monkeypatch) -> None:
    root = make_root()
    try:
        facade = build_fake_facade(tmp_path)
        page = PlanEditorPage(root, facade=facade)
        page.pack(fill="both", expand=True)

        page.profile_name_var.set("delete_me")
        page.temp_value_var.set("20")
        page._add_temperature()
        page._save_profile()
        page._refresh_profile_list(select_name="delete_me")
        monkeypatch.setattr(
            "gas_calibrator.v2.ui_v2.pages.plan_editor_page.messagebox.askyesno",
            lambda *args, **kwargs: True,
        )

        page._delete_selected_profile()

        assert facade.plan_gateway.load_profile("delete_me") is None
        assert "已删除档案" in page.status_var.get()
    finally:
        root.destroy()


def test_plan_editor_page_duplicate_rename_import_export_and_validate(tmp_path: Path, monkeypatch) -> None:
    root = make_root()
    try:
        facade = build_fake_facade(tmp_path)
        page = PlanEditorPage(root, facade=facade)
        page.pack(fill="both", expand=True)

        page.profile_name_var.set("source_profile")
        page.profile_version_var.set("4.0")
        page.run_mode_var.set(display_run_mode("h2o_measurement"))
        page.analyzer_version_var.set(display_analyzer_software_version("v5_plus"))
        page.device_id_assignment_var.set(display_device_id_assignment("automatic"))
        page.start_device_id_var.set("3")
        page.temp_value_var.set("20,25")
        page._add_temperature()
        assert len(page._temperature_rows) == 2
        page.gas_value_var.set("500,700")
        page.gas_group_var.set("C")
        page.gas_cylinder_var.set("510")
        page._add_gas()
        assert len(page._gas_rows) == 2
        page._refresh_gas_tree(select_index=0)
        page._duplicate_selected(page.gas_tree, page._gas_rows, page._refresh_gas_tree, page._fill_gas_form)
        assert len(page._gas_rows) == 3
        page._save_profile()

        names = iter(["source_profile_copy", "renamed_profile"])
        monkeypatch.setattr(
            "gas_calibrator.v2.ui_v2.pages.plan_editor_page.simpledialog.askstring",
            lambda *args, **kwargs: next(names),
        )

        page._duplicate_selected_profile()
        assert facade.plan_gateway.load_profile("source_profile_copy") is not None
        assert page.profile_name_var.get() == "source_profile_copy"

        page._rename_selected_profile()
        assert facade.plan_gateway.load_profile("source_profile_copy") is None
        assert facade.plan_gateway.load_profile("renamed_profile") is not None
        assert page.profile_name_var.get() == "renamed_profile"

        export_path = tmp_path / "exported_profile.json"
        monkeypatch.setattr(
            "gas_calibrator.v2.ui_v2.pages.plan_editor_page.filedialog.asksaveasfilename",
            lambda *args, **kwargs: str(export_path),
        )
        page._export_selected_profile()
        exported_payload = json.loads(export_path.read_text(encoding="utf-8"))
        assert exported_payload["name"] == "renamed_profile"
        assert exported_payload["profile_version"] == "4.0"
        assert exported_payload["run_mode"] == "h2o_measurement"
        assert exported_payload["analyzer_setup"]["start_device_id"] == "003"
        assert len(exported_payload["temperatures"]) == 2
        assert len(exported_payload["gas_points"]) == 3
        assert exported_payload["step2_config_governance"]["config_safety"]["classification"] == (
            "simulation_real_port_inventory_risk"
        )
        assert "real_com_risk" in exported_payload["step2_config_governance"]["config_safety"]["badge_ids"]
        assert exported_payload["step2_config_governance"]["config_safety_review"]["status"] == "blocked"
        assert exported_payload["step2_config_governance"]["config_safety_review"]["execution_gate"]["status"] == "blocked"
        assert exported_payload["step2_config_governance"]["config_safety_review"]["warnings"]
        assert exported_payload["step2_config_governance"]["config_safety_review"]["real_port_device_count"] >= 1
        assert exported_payload["step2_config_governance"]["config_governance_handoff"]["execution_gate"]["status"] == "blocked"
        assert exported_payload["step2_config_governance"]["config_governance_handoff"]["devices_with_real_ports"]

        import_path = tmp_path / "import_profile.json"
        import_path.write_text(
            json.dumps(
                {
                    "name": "imported_profile",
                    "profile_version": "8.2",
                    "analyzer_setup": {"software_version": "pre_v5", "device_id_assignment_mode": "manual", "manual_device_ids": ["021"]},
                    "temperatures": [{"temperature_c": 15.0, "enabled": True}],
                    "gas_points": [{"co2_ppm": 700.0, "co2_group": "D", "cylinder_nominal_ppm": 701.0, "enabled": True}],
                }
            ),
            encoding="utf-8",
        )
        monkeypatch.setattr(
            "gas_calibrator.v2.ui_v2.pages.plan_editor_page.filedialog.askopenfilename",
            lambda *args, **kwargs: str(import_path),
        )
        page._import_profile()
        assert page.profile_name_var.get() == "imported_profile"
        assert page.profile_version_var.get() == "8.2"
        assert page.run_mode_var.get() == display_run_mode("auto_calibration")
        assert page.analyzer_version_var.get() == display_analyzer_software_version("pre_v5")
        assert page.device_id_assignment_var.get() == display_device_id_assignment("manual")
        assert page.manual_device_ids_var.get() == "021"
        assert page.gas_group_var.get() == "D"
        assert page.gas_cylinder_var.get() == "701"

        page.selected_temps_var.set("bad-input")
        page._compile_preview()
        assert "计划选项无效" in page.status_var.get()

        page.selected_temps_var.set("")
        page.profile_version_var.set("bad version")
        page._save_profile()
        assert "档案版本不能包含空白字符" in page.status_var.get()
        page.profile_version_var.set("4.0")
        page._refresh_gas_tree(select_index=0)
        page.gas_value_var.set("-1")
        page._update_gas()
        assert "CO2 ppm 必须大于等于 0" in page.status_var.get()
    finally:
        root.destroy()


def test_plan_gateway_supports_validation_diff_library_and_runtime_snapshot(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)
    gateway = facade.plan_gateway

    baseline = gateway.create_empty_profile(name="baseline_profile", description="simulation baseline")
    baseline["temperatures"] = [{"temperature_c": 25.0, "enabled": True}]
    baseline["gas_points"] = [{"co2_ppm": 400.0, "co2_group": "A", "cylinder_nominal_ppm": 405.0, "enabled": True}]
    baseline["pressures"] = [{"pressure_hpa": 1000.0, "enabled": True}]
    saved = gateway.save_profile(baseline, set_default=True)

    validation = gateway.validate_profile(saved)
    invalid_payload = dict(saved)
    invalid_payload["profile_version"] = "bad version"
    invalid_payload["temperatures"] = []
    invalid_validation = gateway.validate_profile(invalid_payload)
    candidate = dict(saved)
    candidate["name"] = "candidate_profile"
    candidate["run_mode"] = "co2_measurement"
    candidate["gas_points"] = [{"co2_ppm": 800.0, "co2_group": "B", "cylinder_nominal_ppm": 810.0, "enabled": True}]
    diff_payload = gateway.diff_profiles(saved, candidate)
    library = gateway.list_simulation_profile_library()
    operator_safe = gateway.build_operator_safe_default_profile()
    runtime_payload = gateway.build_runtime_points_file(saved, destination=tmp_path / "compiled_plan.json")
    runtime_snapshot = gateway.build_runtime_snapshot(saved)

    assert validation["ok"] is True
    assert saved["config_safety"]["classification"] == "simulation_real_port_inventory_risk"
    assert "real_com_risk" in saved["config_safety"]["badge_ids"]
    assert invalid_validation["ok"] is False
    assert any("配置版本不能包含空白字符" in item for item in invalid_validation["errors"])
    assert any("至少需要一个启用的温度点" in item for item in invalid_validation["errors"])
    assert diff_payload["change_count"] >= 2
    assert any(str(item.get("field") or "").startswith("gas_points") for item in diff_payload["changes"])
    assert library
    assert library[0]["simulation_only"] is True
    assert operator_safe["name"] == "simulation_operator_safe"
    assert runtime_snapshot["validation"]["ok"] is True
    assert runtime_snapshot["counts"]["runtime_rows"] >= 1
    assert runtime_snapshot["config_safety_review"]["classification"] == "simulation_real_port_inventory_risk"
    assert "real_com_risk" in runtime_snapshot["config_safety_review"]["badge_ids"]
    assert runtime_snapshot["config_safety_review"]["execution_gate"]["status"] == "blocked"
    assert runtime_snapshot["config_safety_review"]["warnings"]
    assert runtime_snapshot["config_governance_handoff"]["blocked_reason_details"]
    assert Path(runtime_payload["path"]).exists()
    assert Path(runtime_payload["runtime_snapshot_path"]).exists()
    assert Path(runtime_payload["audit_trail_path"]).exists()
    assert runtime_payload["runtime_snapshot"]["validation"]["ok"] is True
    assert runtime_payload["runtime_snapshot"]["config_governance_handoff"]["execution_gate"]["status"] == "blocked"
    assert runtime_payload["audit_trail"][1]["classification"] == "simulation_real_port_inventory_risk"
    assert runtime_payload["audit_trail"][1]["inventory_summary"]
    assert runtime_payload["audit_trail"][1]["blocked_reasons"]
    assert runtime_payload["audit_trail"][1]["execution_gate"]["status"] == "blocked"
    assert runtime_payload["audit_trail"][1]["devices_with_real_ports"]


def test_plan_gateway_import_profile_exposes_imported_governance_handoff(tmp_path: Path) -> None:
    gateway = PlanGateway(
        profile_store=ProfileStore(tmp_path / "profiles"),
        config_provider=lambda: AppConfig.from_dict(
            {
                "features": {"simulation_mode": True},
                "devices": {
                    "pressure_controller": {"port": "COM7", "enabled": True},
                    "gas_analyzers": [{"port": "SIM-GA", "enabled": True}],
                },
                "workflow": {
                    "pressure": {
                        "capture_then_hold_enabled": True,
                        "adaptive_pressure_sampling_enabled": False,
                        "soft_control_enabled": False,
                    }
                },
            }
        ),
        compiled_points_dir=tmp_path / "compiled",
    )
    import_path = tmp_path / "with_governance_profile.json"
    import_path.write_text(
        json.dumps(
            {
                "name": "imported_with_governance",
                "profile_version": "2.0",
                "temperatures": [{"temperature_c": 25.0, "enabled": True}],
                "gas_points": [{"co2_ppm": 400.0, "co2_group": "A", "enabled": True}],
                "pressures": [{"pressure_hpa": 1000.0, "enabled": True}],
                "step2_config_governance": {
                    "config_governance_handoff": {
                        "classification": "simulation_real_port_inventory_risk",
                        "execution_gate": {"status": "blocked"},
                        "devices_with_real_ports": [{"device": "pressure_controller", "port": "COM7"}],
                    }
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    imported = gateway.import_profile(import_path)

    assert imported["config_governance_handoff"]["execution_gate"]["status"] == "blocked"
    assert imported["import_governance_handoff"]["has_imported_governance"] is True
    assert imported["import_governance_handoff"]["imported_file_governance"]["execution_gate"]["status"] == "blocked"
    assert imported["import_governance_handoff"]["current_runtime_governance"]["enabled_engineering_flags"]


def test_plan_gateway_marks_real_port_and_non_simulation_config_risks(tmp_path: Path) -> None:
    gateway = PlanGateway(
        profile_store=ProfileStore(tmp_path / "profiles"),
        config_provider=lambda: AppConfig.from_dict(
            {
                "features": {"simulation_mode": False},
                "devices": {
                    "pressure_controller": {"port": "COM3", "enabled": True},
                    "pressure_meter": {"port": "SIM-PARO", "enabled": True},
                    "gas_analyzers": [{"port": "COM8", "enabled": True}],
                },
            }
        ),
        compiled_points_dir=tmp_path / "compiled",
    )
    payload = gateway.create_empty_profile(name="risk_profile")
    payload["temperatures"] = [{"temperature_c": 25.0, "enabled": True}]
    payload["gas_points"] = [{"co2_ppm": 400.0, "co2_group": "A", "enabled": True}]
    payload["pressures"] = [{"pressure_hpa": 1000.0, "enabled": True}]

    validation = gateway.validate_profile(payload)
    preview = gateway.compile_profile_preview(payload)
    runtime_snapshot = gateway.build_runtime_snapshot(payload)

    assert validation["ok"] is True
    assert validation["config_safety"]["status"] == "warn"
    assert validation["config_safety"]["real_port_device_count"] == 2
    assert validation["config_safety"]["operator_safe"] is False
    assert "simulation_mode_disabled" in validation["config_safety"]["risk_markers"]
    assert "real_ports_detected" in validation["config_safety"]["risk_markers"]
    assert any("未启用 simulation_mode" in item for item in validation["warnings"])
    assert any("非仿真设备端口" in item for item in validation["warnings"])
    assert preview["config_safety"]["status"] == "warn"
    assert preview["config_safety_review"]["status"] == "blocked"
    assert preview["config_safety"]["classification"] == "non_simulation_real_port_risk"
    assert "simulation_disabled" in preview["config_safety"]["badge_ids"]
    assert "real_com_risk" in preview["config_safety"]["badge_ids"]
    assert preview["config_safety_review"]["blocked_reason_details"][0]["code"] == "simulation_mode_disabled"
    assert "Step 2 默认工作流已拦截当前配置" in preview["config_safety_review"]["summary"]
    assert preview["config_safety_review"]["execution_gate"]["status"] == "blocked"
    assert preview["config_safety_review"]["warnings"]
    assert runtime_snapshot["config_safety"]["real_port_device_count"] == 2
    assert validation["config_safety"]["requires_explicit_unlock"] is True
    assert validation["config_safety"]["step2_default_workflow_allowed"] is False
    assert validation["config_safety"]["execution_gate"]["status"] == "blocked"
    assert runtime_snapshot["audit_trail"][1]["step"] == "config_safety_review"
    assert runtime_snapshot["audit_trail"][1]["status"] == "blocked"
    assert runtime_snapshot["audit_trail"][1]["classification"] == "non_simulation_real_port_risk"
    assert runtime_snapshot["audit_trail"][1]["blocked_reason_details"]
    assert runtime_snapshot["audit_trail"][1]["blocked_reasons"] == [
        "simulation_mode_disabled",
        "real_ports_detected",
    ]
    assert runtime_snapshot["audit_trail"][1]["execution_gate"]["status"] == "blocked"


def test_plan_gateway_marks_engineering_only_pressure_flags_as_config_safety_risk(tmp_path: Path) -> None:
    gateway = PlanGateway(
        profile_store=ProfileStore(tmp_path / "profiles"),
        config_provider=lambda: AppConfig.from_dict(
            {
                "features": {"simulation_mode": True},
                "devices": {
                    "pressure_controller": {"port": "SIM-PC", "enabled": True},
                    "gas_analyzers": [{"port": "SIM-GA", "enabled": True}],
                },
                "workflow": {
                    "pressure": {
                        "capture_then_hold_enabled": True,
                        "adaptive_pressure_sampling_enabled": True,
                        "soft_control_enabled": True,
                    }
                },
            }
        ),
        compiled_points_dir=tmp_path / "compiled",
    )
    payload = gateway.create_empty_profile(name="engineering_profile")
    payload["temperatures"] = [{"temperature_c": 25.0, "enabled": True}]
    payload["gas_points"] = [{"co2_ppm": 400.0, "co2_group": "A", "enabled": True}]
    payload["pressures"] = [{"pressure_hpa": 1000.0, "enabled": True}]

    validation = gateway.validate_profile(payload)
    preview = gateway.compile_profile_preview(payload)
    runtime_snapshot = gateway.build_runtime_snapshot(payload)

    enabled_paths = {
        item["config_path"] for item in validation["config_safety"]["enabled_engineering_flags"]
    }

    assert validation["ok"] is True
    assert validation["config_safety"]["status"] == "warn"
    assert validation["config_safety"]["simulation_only"] is True
    assert validation["config_safety"]["real_port_device_count"] == 0
    assert validation["config_safety"]["engineering_only_flag_count"] == 3
    assert "engineering_only_flags_enabled" in validation["config_safety"]["risk_markers"]
    assert enabled_paths == {
        "workflow.pressure.capture_then_hold_enabled",
        "workflow.pressure.adaptive_pressure_sampling_enabled",
        "workflow.pressure.soft_control_enabled",
    }
    assert any("workflow.pressure.capture_then_hold_enabled" in item for item in validation["warnings"])
    assert any("workflow.pressure.adaptive_pressure_sampling_enabled" in item for item in validation["warnings"])
    assert any("workflow.pressure.soft_control_enabled" in item for item in validation["warnings"])
    assert preview["config_safety"]["engineering_only_flag_count"] == 3
    assert preview["config_safety_review"]["status"] == "blocked"
    assert preview["config_safety"]["classification"] == "simulation_engineering_only_risk"
    assert "engineering_only" in preview["config_safety"]["badge_ids"]
    assert preview["config_safety"]["inventory"]["engineering_only_flag_count"] == 3
    assert runtime_snapshot["config_safety"]["engineering_only_flag_count"] == 3
    assert validation["config_safety"]["requires_explicit_unlock"] is True
    assert validation["config_safety"]["execution_gate"]["status"] == "blocked"
    assert runtime_snapshot["audit_trail"][1]["status"] == "blocked"
    assert runtime_snapshot["audit_trail"][1]["classification"] == "simulation_engineering_only_risk"


def test_plan_gateway_keeps_operator_safe_status_when_simulation_only_defaults_are_clean(tmp_path: Path) -> None:
    gateway = PlanGateway(
        profile_store=ProfileStore(tmp_path / "profiles"),
        config_provider=lambda: AppConfig.from_dict(
            {
                "features": {"simulation_mode": True},
                "devices": {
                    "pressure_controller": {"port": "SIM-PC", "enabled": True},
                    "pressure_meter": {"port": "SIM-DP", "enabled": True},
                    "gas_analyzers": [{"port": "SIM-GA", "enabled": True}],
                },
                "workflow": {
                    "pressure": {
                        "capture_then_hold_enabled": False,
                        "adaptive_pressure_sampling_enabled": False,
                        "soft_control_enabled": False,
                    }
                },
            }
        ),
        compiled_points_dir=tmp_path / "compiled",
    )
    payload = gateway.create_empty_profile(name="operator_safe_profile")
    payload["temperatures"] = [{"temperature_c": 25.0, "enabled": True}]
    payload["gas_points"] = [{"co2_ppm": 400.0, "co2_group": "A", "enabled": True}]
    payload["pressures"] = [{"pressure_hpa": 1000.0, "enabled": True}]

    validation = gateway.validate_profile(payload)

    assert validation["ok"] is True
    assert validation["config_safety"]["status"] == "ok"
    assert validation["config_safety"]["operator_safe"] is True
    assert validation["config_safety"]["risk_markers"] == []
    assert validation["config_safety"]["real_port_device_count"] == 0
    assert validation["config_safety"]["engineering_only_flag_count"] == 0
    assert validation["config_safety"]["enabled_engineering_flags"] == []
    assert validation["config_safety"]["requires_explicit_unlock"] is False
    assert validation["config_safety"]["step2_default_workflow_allowed"] is True
    assert validation["config_safety"]["execution_gate"]["status"] == "open"
