from __future__ import annotations

from datetime import datetime, timedelta
import inspect
import json
from pathlib import Path
import tkinter as tk
import types
import csv

import pytest

from gas_calibrator.ui import app as app_module
from gas_calibrator.data.points import CalibrationPoint
from gas_calibrator.logging_utils import RunLogger


def _basic_cfg(output_dir: str = "out") -> dict:
    return {
        "paths": {"points_excel": "demo.xlsx", "output_dir": output_dir},
        "valves": {
            "co2_map": {"0": 1, "200": 2, "400": 3},
            "co2_map_group2": {"100": 4, "300": 5},
        },
        "workflow": {"skip_co2_ppm": [200]},
    }


def _cfg_with_devices(output_dir: str = "out") -> dict:
    cfg = _basic_cfg(output_dir)
    cfg["devices"] = {
        "pressure_controller": {"enabled": True, "port": "COM31", "baud": 9600},
        "pressure_gauge": {"enabled": True, "port": "COM30", "baud": 9600, "dest_id": "01"},
        "dewpoint_meter": {"enabled": True, "port": "COM25", "baud": 9600, "station": "001"},
        "humidity_generator": {"enabled": True, "port": "COM24", "baud": 9600},
        "temperature_chamber": {"enabled": True, "port": "COM27", "baud": 9600, "addr": 1},
        "thermometer": {"enabled": False, "port": "COM26", "baud": 2400},
        "relay": {"enabled": True, "port": "COM28", "baud": 38400, "addr": 1},
        "relay_8": {"enabled": True, "port": "COM29", "baud": 38400, "addr": 1},
        "gas_analyzer": {"enabled": True, "port": "COM16", "baud": 115200, "device_id": "000"},
        "gas_analyzers": [
            {"name": f"ga{idx:02d}", "enabled": True, "port": f"COM{15 + idx}", "baud": 115200, "device_id": f"{idx:03d}"}
            for idx in range(1, 9)
        ],
    }
    return cfg


def _points(*temps: float):
    return [type("P", (), {"temp_chamber_c": temp})() for temp in temps]


def _points_with_co2(*pairs: tuple[float, float | None]):
    return [type("P", (), {"temp_chamber_c": temp, "co2_ppm": ppm})() for temp, ppm in pairs]


def _preview_points():
    return [
        types.SimpleNamespace(
            index=3,
            temp_chamber_c=-10.0,
            co2_ppm=0.0,
            hgen_temp_c=None,
            hgen_rh_pct=None,
            target_pressure_hpa=1100.0,
            co2_group=None,
            is_h2o_point=False,
        ),
        types.SimpleNamespace(
            index=9,
            temp_chamber_c=0.0,
            co2_ppm=0.0,
            hgen_temp_c=0.0,
            hgen_rh_pct=50.0,
            target_pressure_hpa=1100.0,
            co2_group=None,
            is_h2o_point=True,
        ),
        types.SimpleNamespace(
            index=10,
            temp_chamber_c=0.0,
            co2_ppm=400.0,
            hgen_temp_c=None,
            hgen_rh_pct=None,
            target_pressure_hpa=800.0,
            co2_group=None,
            is_h2o_point=False,
        ),
    ]


def _preview_points_ordered():
    return [
        types.SimpleNamespace(
            index=4,
            temp_chamber_c=-10.0,
            co2_ppm=400.0,
            hgen_temp_c=None,
            hgen_rh_pct=None,
            target_pressure_hpa=800.0,
            co2_group=None,
            is_h2o_point=False,
        ),
        types.SimpleNamespace(
            index=3,
            temp_chamber_c=-10.0,
            co2_ppm=0.0,
            hgen_temp_c=None,
            hgen_rh_pct=None,
            target_pressure_hpa=800.0,
            co2_group=None,
            is_h2o_point=False,
        ),
        types.SimpleNamespace(
            index=2,
            temp_chamber_c=-10.0,
            co2_ppm=0.0,
            hgen_temp_c=None,
            hgen_rh_pct=None,
            target_pressure_hpa=1100.0,
            co2_group=None,
            is_h2o_point=False,
        ),
        types.SimpleNamespace(
            index=5,
            temp_chamber_c=-10.0,
            co2_ppm=400.0,
            hgen_temp_c=None,
            hgen_rh_pct=None,
            target_pressure_hpa=500.0,
            co2_group=None,
            is_h2o_point=False,
        ),
    ]


def _require_tk_display() -> None:
    probe = None
    try:
        probe = tk.Tk()
        probe.withdraw()
    except tk.TclError as exc:
        pytest.skip(f"tk display unavailable in headless environment: {exc}")
    finally:
        if probe is not None:
            probe.destroy()


def test_refresh_temperature_and_co2_options_build_selection_lists(monkeypatch) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0, 30.0, 20.0))

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        assert list(ui.temp_check_vars.keys()) == [20.0, 30.0]
        assert list(ui.co2_check_vars.keys()) == [0, 100, 200, 300, 400]
        assert ui.co2_check_vars[200].get() is False
        assert ui.temp_listbox is not None
        assert ui.co2_listbox is not None
        assert ui.pressure_listbox is not None
        assert ui.temp_listbox.size() == 2
        assert ui.co2_listbox.size() == 5
        assert ui.pressure_listbox.size() == 7
    finally:
        root.destroy()


def test_temp_scope_and_control_lock_update_listbox_state(monkeypatch) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0, 30.0))

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        ui.temp_scope_var.set("全部温度点")
        assert ui.temp_listbox is not None
        assert ui.co2_listbox is not None
        assert ui.pressure_listbox is not None
        assert str(ui.temp_listbox.cget("state")) == "normal"
        ui.temp_scope_var.set("指定温度点")
        ui._apply_control_lock()
        assert str(ui.temp_listbox.cget("state")) == "normal"
        ui.worker = types.SimpleNamespace(is_alive=lambda: True)
        ui._apply_control_lock()
        assert str(ui.temp_listbox.cget("state")) == "disabled"
        assert str(ui.co2_listbox.cget("state")) == "disabled"
        assert str(ui.pressure_listbox.cget("state")) == "disabled"
    finally:
        root.destroy()


def test_refresh_co2_options_prefers_points_table_ppm(monkeypatch) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(
        app_module,
        "load_points_from_excel",
        lambda *_args, **_kwargs: _points_with_co2((20.0, 0.0), (20.0, 200.0), (30.0, 400.0), (30.0, None)),
    )

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        assert list(ui.co2_check_vars.keys()) == [0, 200, 400]
        assert "来源：点表" in ui.co2_hint_var.get()
    finally:
        root.destroy()


def test_refresh_co2_options_uses_real_execution_sources_for_10c_full_sweep(monkeypatch) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(
        app_module,
        "load_points_from_excel",
        lambda *_args, **_kwargs: [
            CalibrationPoint(
                index=12,
                temp_chamber_c=10.0,
                co2_ppm=0.0,
                hgen_temp_c=None,
                hgen_rh_pct=None,
                target_pressure_hpa=1100.0,
                dewpoint_c=None,
                h2o_mmol=None,
                raw_h2o=None,
            ),
            CalibrationPoint(
                index=16,
                temp_chamber_c=10.0,
                co2_ppm=400.0,
                hgen_temp_c=None,
                hgen_rh_pct=None,
                target_pressure_hpa=900.0,
                dewpoint_c=None,
                h2o_mmol=None,
                raw_h2o=None,
            ),
            CalibrationPoint(
                index=22,
                temp_chamber_c=10.0,
                co2_ppm=1000.0,
                hgen_temp_c=None,
                hgen_rh_pct=None,
                target_pressure_hpa=600.0,
                dewpoint_c=None,
                h2o_mmol=None,
                raw_h2o=None,
            ),
        ],
    )

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        assert list(ui.co2_check_vars.keys()) == [0, 100, 300, 400, 500, 600, 700, 800, 900, 1000]
        assert 200 not in ui.co2_check_vars
        assert ui.co2_check_vars[600].get() is True
    finally:
        root.destroy()


def test_temp_list_click_switches_scope_to_specific(monkeypatch) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0, 30.0))

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        assert ui.temp_listbox is not None
        ui.temp_scope_var.set("全部温度点")
        ui.temp_listbox.selection_clear(0, "end")
        event = types.SimpleNamespace(y=0)
        ui._on_temp_listbox_click(event)
        assert ui.temp_scope_var.get() == "指定温度点"
    finally:
        root.destroy()


def test_build_runtime_cfg_uses_checked_temperatures(monkeypatch) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0, 30.0, 40.0))

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        ui.temp_scope_var.set("指定温度点")
        ui.temp_check_vars[20.0].set(False)
        ui.temp_check_vars[30.0].set(True)
        ui.temp_check_vars[40.0].set(True)
        runtime_cfg = ui._build_runtime_cfg()
        assert runtime_cfg["workflow"]["selected_temps_c"] == [30.0, 40.0]
    finally:
        root.destroy()


def test_build_runtime_cfg_uses_temperature_order_selection(monkeypatch) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0, 30.0, 40.0))

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        ui.temperature_order_var.set("从低到高")
        runtime_cfg = ui._build_runtime_cfg()
        assert runtime_cfg["workflow"]["temperature_descending"] is False
    finally:
        root.destroy()


def test_build_runtime_cfg_uses_checked_co2_points(monkeypatch) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        ui.co2_check_vars[0].set(True)
        ui.co2_check_vars[100].set(False)
        ui.co2_check_vars[200].set(False)
        ui.co2_check_vars[300].set(True)
        ui.co2_check_vars[400].set(True)
        runtime_cfg = ui._build_runtime_cfg()
        assert runtime_cfg["workflow"]["skip_co2_ppm"] == [100, 200]
    finally:
        root.destroy()


def test_build_runtime_cfg_uses_checked_pressure_points(monkeypatch) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        for var in ui.pressure_check_vars.values():
            var.set(False)
        ui.pressure_check_vars[1100].set(True)
        ui.pressure_check_vars[900].set(True)
        ui.pressure_check_vars[700].set(True)
        runtime_cfg = ui._build_runtime_cfg()
        assert runtime_cfg["workflow"]["selected_pressure_points"] == [1100, 900, 700]
    finally:
        root.destroy()


def test_build_runtime_cfg_supports_ambient_pressure_only(monkeypatch) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        for var in ui.pressure_check_vars.values():
            var.set(False)
        ui.ambient_pressure_var.set(True)
        runtime_cfg = ui._build_runtime_cfg()
        assert runtime_cfg["workflow"]["selected_pressure_points"] == ["ambient"]
    finally:
        root.destroy()


def test_build_runtime_cfg_omits_pressure_selection_when_all_selected(monkeypatch) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        runtime_cfg = ui._build_runtime_cfg()
        assert "selected_pressure_points" not in runtime_cfg["workflow"]
    finally:
        root.destroy()


def test_load_config_reads_selected_pressure_points(monkeypatch) -> None:
    cfg = _basic_cfg()
    cfg["workflow"]["selected_pressure_points"] = [1100, 900, 700]
    monkeypatch.setattr(app_module, "load_config", lambda _path: cfg)
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        assert ui.pressure_check_vars[1100].get() is True
        assert ui.pressure_check_vars[900].get() is True
        assert ui.pressure_check_vars[700].get() is True
        assert ui.pressure_check_vars[1000].get() is False
        assert ui.pressure_check_vars[800].get() is False
    finally:
        root.destroy()


def test_load_config_reads_ambient_selected_pressure_point(monkeypatch) -> None:
    cfg = _basic_cfg()
    cfg["workflow"]["selected_pressure_points"] = ["ambient", 1100, 900]
    monkeypatch.setattr(app_module, "load_config", lambda _path: cfg)
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        assert ui.ambient_pressure_var.get() is True
        assert ui.pressure_check_vars[1100].get() is True
        assert ui.pressure_check_vars[900].get() is True
        assert ui.pressure_check_vars[800].get() is False
    finally:
        root.destroy()


def test_load_config_with_invalid_pressure_selection_surfaces_hint(monkeypatch) -> None:
    cfg = _basic_cfg()
    cfg["workflow"]["selected_pressure_points"] = [950]
    monkeypatch.setattr(app_module, "load_config", lambda _path: cfg)
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        assert "配置中的压力点非法" in ui.pressure_hint_var.get()
        assert "950hPa" in ui.pressure_hint_var.get()
    finally:
        root.destroy()


def test_build_runtime_cfg_enables_calibration_fit(monkeypatch) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        ui.fit_enabled_var.set(True)
        runtime_cfg = ui._build_runtime_cfg()
        assert runtime_cfg["workflow"]["collect_only"] is False
        assert runtime_cfg["coefficients"]["enabled"] is True
        assert runtime_cfg["coefficients"]["auto_fit"] is True
        assert runtime_cfg["coefficients"]["fit_h2o"] is True
    finally:
        root.destroy()


def test_build_runtime_cfg_disables_calibration_fit(monkeypatch) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        ui.fit_enabled_var.set(False)
        runtime_cfg = ui._build_runtime_cfg()
        assert runtime_cfg["workflow"]["collect_only"] is True
    finally:
        root.destroy()


def test_build_runtime_cfg_strips_postrun_corrected_delivery_from_v1_ui(monkeypatch) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        ui.postrun_delivery_var.set(True)
        runtime_cfg = ui._build_runtime_cfg()
        assert "postrun_corrected_delivery" not in runtime_cfg["workflow"]
    finally:
        root.destroy()


def test_load_config_merges_user_tuning_override(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0, 30.0))

    cfg_dir = tmp_path / "configs"
    cfg_dir.mkdir()
    base_cfg_path = cfg_dir / "default_config.json"
    base_cfg_path.write_text(
        json.dumps(
            {
                "paths": {"points_excel": "demo.xlsx", "output_dir": "out"},
                "valves": {"co2_map": {"0": 1}, "co2_map_group2": {"100": 4}},
                "workflow": {"pressure": {"stabilize_timeout_s": 120}},
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    (cfg_dir / "user_tuning.json").write_text(
        json.dumps(
            {"workflow": {"pressure": {"stabilize_timeout_s": 321}, "sampling": {"count": 7}}},
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8-sig",
    )

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        ui.config_path.set(str(base_cfg_path))
        ui.load_config()
        assert ui.cfg["workflow"]["pressure"]["stabilize_timeout_s"] == 321
        assert ui.cfg["workflow"]["sampling"]["count"] == 7
    finally:
        root.destroy()


def test_load_config_reads_calibration_fit_switch(monkeypatch) -> None:
    cfg = _basic_cfg()
    cfg["workflow"]["collect_only"] = True
    cfg["coefficients"] = {"enabled": True, "auto_fit": True, "fit_h2o": False}
    monkeypatch.setattr(app_module, "load_config", lambda _path: cfg)
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        assert ui.fit_enabled_var.get() is False
        assert ui.fit_mode_brief_var.get() == "拟合：关闭，仅采集"
    finally:
        root.destroy()


def test_load_config_ignores_postrun_corrected_delivery_switch(monkeypatch) -> None:
    cfg = _basic_cfg()
    cfg["workflow"]["postrun_corrected_delivery"] = {"enabled": True}
    monkeypatch.setattr(app_module, "load_config", lambda _path: cfg)
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        assert ui.postrun_delivery_var.get() is False
        assert str(ui.postrun_delivery_check.cget("state")) == "disabled"
        assert "自动交付" not in ui.summary_var.get()
    finally:
        root.destroy()


def test_load_config_merges_user_tuning_override_with_bom_for_fast_flags(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    cfg_dir = tmp_path / "configs"
    cfg_dir.mkdir()
    base_cfg_path = cfg_dir / "default_config.json"
    base_cfg = _basic_cfg(str(tmp_path / "logs"))
    base_cfg["workflow"] = {
        "collect_only": True,
        "stability": {
            "temperature": {
                "wait_for_target_before_continue": True,
                "analyzer_chamber_temp_enabled": True,
            }
        },
    }
    base_cfg_path.write_text(json.dumps(base_cfg, ensure_ascii=False, indent=2), encoding="utf-8")
    (cfg_dir / "user_tuning.json").write_text(
        json.dumps(
            {
                "workflow": {
                    "stability": {
                        "temperature": {
                            "wait_for_target_before_continue": False,
                            "analyzer_chamber_temp_enabled": False,
                        }
                    }
                }
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8-sig",
    )

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        ui.config_path.set(str(base_cfg_path))
        ui.load_config()
        temp_cfg = ui.cfg["workflow"]["stability"]["temperature"]
        assert temp_cfg["wait_for_target_before_continue"] is False
        assert temp_cfg["analyzer_chamber_temp_enabled"] is False
    finally:
        root.destroy()


def test_load_config_applies_route_mode_and_selected_temps_to_ui(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(0.0, 10.0, 20.0))

    cfg_dir = tmp_path / "configs"
    cfg_dir.mkdir()
    cfg_path = cfg_dir / "default_config.json"
    points_path = tmp_path / "demo.xlsx"
    points_path.write_text("placeholder", encoding="utf-8")
    cfg_path.write_text(
        json.dumps(
            {
                "paths": {"points_excel": str(points_path), "output_dir": "out"},
                "valves": {"co2_map": {"0": 1}, "co2_map_group2": {"100": 4}},
                "workflow": {
                    "route_mode": "h2o_only",
                    "selected_temps_c": [0.0, 10.0],
                    "temperature_descending": True,
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        ui.config_path.set(str(cfg_path))
        ui.load_config()
        assert ui.route_mode_var.get() == "只测水路"
        assert ui.temp_scope_var.get() == "指定温度点"
        assert ui.temperature_order_var.get() == "从高到低"
        assert ui._selected_temp_values() == [0.0, 10.0]
    finally:
        root.destroy()


def test_device_port_editor_loads_ports_from_config(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    cfg_dir = tmp_path / "configs"
    cfg_dir.mkdir()
    cfg_path = cfg_dir / "default_config.json"
    cfg_path.write_text(json.dumps(_cfg_with_devices(), ensure_ascii=False, indent=2), encoding="utf-8")

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        ui.config_path.set(str(cfg_path))
        ui.load_config()
        assert ui.device_port_vars["pressure_controller"].get() == "COM31"
        assert ui.device_port_vars["gas_analyzers.0.port"].get() == "COM16"
        assert ui.device_port_vars["gas_analyzers.7.port"].get() == "COM23"
        assert "兼容单分析仪端口：COM16" == ui.device_port_compat_var.get()
    finally:
        root.destroy()


def test_save_device_port_config_updates_current_file(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))
    monkeypatch.setattr(app_module.messagebox, "showinfo", lambda *_args, **_kwargs: None)

    cfg_dir = tmp_path / "configs"
    cfg_dir.mkdir()
    cfg_path = cfg_dir / "default_config.json"
    cfg_path.write_text(json.dumps(_cfg_with_devices(), ensure_ascii=False, indent=2), encoding="utf-8")

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        ui.config_path.set(str(cfg_path))
        ui.load_config()
        ui.device_port_vars["pressure_controller"].set("COM55")
        ui.device_port_vars["gas_analyzers.0.port"].set("COM35")
        ui.device_port_vars["gas_analyzers.7.port"].set("COM42")
        ui._save_device_port_config()

        saved = json.loads(cfg_path.read_text(encoding="utf-8"))
        assert saved["devices"]["pressure_controller"]["port"] == "COM55"
        assert saved["devices"]["gas_analyzers"][0]["port"] == "COM35"
        assert saved["devices"]["gas_analyzers"][7]["port"] == "COM42"
        assert saved["devices"]["gas_analyzer"]["port"] == "COM35"
    finally:
        root.destroy()


def test_execution_summary_reflects_selection(monkeypatch) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0, 30.0))

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        ui.route_mode_var.set("只测气路")
        ui.temp_scope_var.set("指定温度点")
        ui.temp_check_vars[20.0].set(True)
        ui.temp_check_vars[30.0].set(False)
        ui.co2_check_vars[0].set(True)
        ui.co2_check_vars[100].set(True)
        ui.co2_check_vars[200].set(False)
        ui.co2_check_vars[400].set(False)
        ui.pressure_check_vars[1100].set(True)
        ui.pressure_check_vars[1000].set(False)
        ui.pressure_check_vars[900].set(True)
        ui.pressure_check_vars[800].set(False)
        ui.pressure_check_vars[700].set(False)
        ui.pressure_check_vars[600].set(False)
        ui.pressure_check_vars[500].set(False)
        ui._refresh_execution_summary()
        summary = ui.summary_var.get()
        assert "只测气路" in summary
        assert "从高到低" in summary
        assert "20°C" in summary
        assert "0ppm" in summary
        assert "100ppm" in summary
        assert "1100hPa" in summary
        assert "只测气路" in ui.startup_summary_var.get()
        assert "从高到低" in ui.startup_summary_var.get()
        assert "只测气路" in ui.current_selection_var.get()
        assert "20°C" in ui.current_selection_var.get()
        assert "1100hPa" in ui.current_selection_var.get()
        assert ui.start_readiness_var.get() == "启动校验：就绪"
    finally:
        root.destroy()


def test_load_config_reflects_temperature_order_from_cfg(monkeypatch) -> None:
    cfg = _basic_cfg()
    cfg["workflow"]["temperature_descending"] = False
    monkeypatch.setattr(app_module, "load_config", lambda _path: cfg)
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0, 30.0))

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        assert ui.temperature_order_var.get() == "从低到高"
        assert "从低到高" in ui.summary_var.get()
    finally:
        root.destroy()


def test_points_preview_shows_loaded_points(monkeypatch) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _preview_points())

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        assert ui.points_tree is not None
        items = ui.points_tree.get_children()
        assert len(items) == 5
        first = ui.points_tree.item(items[0], "values")
        assert first[0] == "1"
        assert first[1] == "9"
        assert first[2] == "0°C"
        assert first[3] == "水路"
        assert "执行" in first[8]
        h2o_row = next(
            ui.points_tree.item(item, "values")
            for item in items
            if ui.points_tree.item(item, "values")[1] == "9" and ui.points_tree.item(item, "values")[3] == "水路"
        )
        assert h2o_row[5] == "--"
        assert h2o_row[7] == "--"
    finally:
        root.destroy()


def test_points_preview_shows_ambient_rows_when_selected(monkeypatch) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _preview_points())

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        for var in ui.pressure_check_vars.values():
            var.set(False)
        ui.ambient_pressure_var.set(True)
        ui._refresh_points_preview()
        assert ui.points_tree is not None
        rows = [ui.points_tree.item(item, "values") for item in ui.points_tree.get_children()]
        assert any(row[1] == "9" and row[6] == "当前大气压" and "执行" in row[8] for row in rows)
        assert any(row[1] == "10" and row[6] == "当前大气压" and "执行" in row[8] for row in rows)
    finally:
        root.destroy()


def test_points_preview_marks_skipped_reason_from_current_selection(monkeypatch) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _preview_points())

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        ui.route_mode_var.set("只测气路")
        ui.co2_check_vars[400].set(False)
        ui.pressure_check_vars[1100].set(True)
        ui.pressure_check_vars[1000].set(False)
        ui.pressure_check_vars[900].set(False)
        ui.pressure_check_vars[800].set(False)
        ui.pressure_check_vars[700].set(False)
        ui.pressure_check_vars[600].set(False)
        ui.pressure_check_vars[500].set(False)
        ui._refresh_execution_summary()
        assert ui.points_tree is not None
        rows = [ui.points_tree.item(item, "values") for item in ui.points_tree.get_children()]
        co2_row = next(row for row in rows if row[3] == "气路" and row[5] == "400ppm")
        assert co2_row[8] == "跳过：气点未选"
    finally:
        root.destroy()


def test_points_preview_h2o_route_respects_selected_pressure_points(monkeypatch) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _preview_points())

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        ui.route_mode_var.set("只测水路")
        ui.pressure_check_vars[1100].set(False)
        ui.pressure_check_vars[1000].set(False)
        ui.pressure_check_vars[900].set(False)
        ui.pressure_check_vars[800].set(False)
        ui.pressure_check_vars[700].set(False)
        ui.pressure_check_vars[600].set(False)
        ui.pressure_check_vars[500].set(False)
        ui.pressure_check_vars[900].set(True)
        ui._refresh_execution_summary()
        assert ui.points_tree is not None
        rows = [ui.points_tree.item(item, "values") for item in ui.points_tree.get_children()]
        h2o_row = next(row for row in rows if row[1] == "9")
        assert h2o_row[8] == "跳过：压力点未选"
    finally:
        root.destroy()


def test_points_preview_uses_execution_order_within_same_temperature(monkeypatch) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _preview_points_ordered())

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        assert ui.points_tree is not None
        rows = [ui.points_tree.item(item, "values") for item in ui.points_tree.get_children()]
        assert [(row[1], row[5], row[6]) for row in rows] == [
            ("2", "0ppm", "1100hPa"),
            ("4", "0ppm", "800hPa"),
            ("5", "0ppm", "500hPa"),
            ("2", "400ppm", "1100hPa"),
            ("4", "400ppm", "800hPa"),
            ("5", "400ppm", "500hPa"),
        ]
    finally:
        root.destroy()


def test_extract_key_events_from_io_uses_new_gas_baseline_semantics() -> None:
    rows = [
        {"port": "COM29", "direction": "TX", "command": "write_coil(0,False,addr=1)"},
        {"port": "COM29", "direction": "TX", "command": "write_coil(1,False,addr=1)"},
        {"port": "COM29", "direction": "TX", "command": "write_coil(2,False,addr=1)"},
        {"port": "COM29", "direction": "TX", "command": "write_coil(7,False,addr=1)"},
    ]

    events = app_module.App._extract_key_events_from_io(rows, count=6)

    assert any("总气路阀关" in item for item in events)


def test_extract_key_events_from_io_uses_runtime_cfg_relay_mapping() -> None:
    runtime_cfg = {
        "devices": {"relay_8": {"port": "COM41"}},
        "valves": {
            "h2o_path": 8,
            "hold": 9,
            "flow_switch": 10,
            "gas_main": 11,
            "relay_map": {
                "8": {"device": "relay_8", "channel": 8},
                "9": {"device": "relay_8", "channel": 1},
                "10": {"device": "relay_8", "channel": 2},
                "11": {"device": "relay_8", "channel": 3},
            },
        },
    }
    rows = [
        {"port": "COM41", "direction": "TX", "command": "write_coil(0,False,addr=1)"},
        {"port": "COM41", "direction": "TX", "command": "write_coil(1,False,addr=1)"},
        {"port": "COM41", "direction": "TX", "command": "write_coil(2,False,addr=1)"},
        {"port": "COM41", "direction": "TX", "command": "write_coil(7,False,addr=1)"},
    ]

    events = app_module.App._extract_key_events_from_io(rows, count=6, runtime_cfg=runtime_cfg)

    assert any("总气路阀关" in item for item in events)


def test_start_readiness_requires_selected_temp_when_scope_is_specific(monkeypatch) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0, 30.0))

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        ui.temp_scope_var.set("指定温度点")
        ui._clear_all_temps()
        assert ui.start_readiness_var.get() == "启动校验：请至少勾选一个温度点"
        assert str(ui.start_button["state"]) == "disabled"
    finally:
        root.destroy()


def test_start_readiness_requires_selected_gas_points_for_co2_modes(monkeypatch) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        ui.route_mode_var.set("只测气路")
        ui._clear_all_co2()
        assert ui.start_readiness_var.get() == "启动校验：请至少勾选一个气点"
        assert str(ui.start_button["state"]) == "disabled"
    finally:
        root.destroy()


def test_manual_refresh_calls_refresh_helpers(monkeypatch) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        calls: list[str] = []
        ui._refresh_execution_summary = lambda: calls.append("summary")
        ui._refresh_progress_status = lambda: calls.append("progress")
        ui._refresh_key_events = lambda: calls.append("events")
        ui._refresh_live_device_values = lambda **_kwargs: calls.append("devices")
        ui._apply_banner_states = lambda: calls.append("banners")
        ui.log = lambda msg: calls.append(msg)
        ui._manual_refresh()
        assert calls[:5] == ["summary", "progress", "events", "devices", "banners"]
        assert "界面已手动刷新" in calls[-1]
    finally:
        root.destroy()


def test_apply_responsive_layout_does_not_raise_before_device_view_check(monkeypatch) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        ui._apply_responsive_layout()
    finally:
        root.destroy()


def test_build_device_trends_extracts_recent_delta() -> None:
    now = datetime(2026, 3, 10, 10, 0, 30)
    rows = [
        {
            "ts": (now - timedelta(seconds=25)).isoformat(),
            "port": "COM31",
            "direction": "RX",
            "response": ":SENS:PRES:INL 1000.0, 0",
        },
        {
            "ts": (now - timedelta(seconds=5)).isoformat(),
            "port": "COM31",
            "direction": "RX",
            "response": ":SENS:PRES:INL 1001.2, 1",
        },
        {
            "ts": (now - timedelta(seconds=20)).isoformat(),
            "port": "COM24",
            "direction": "RX",
            "response": "Uw= 30.0 Tc= 20.00 Td= 1.00 Flux= 1.0",
        },
        {
            "ts": now.isoformat(),
            "port": "COM24",
            "direction": "RX",
            "response": "Uw= 31.5 Tc= 20.10 Td= 1.10 Flux= 1.0",
        },
    ]
    trends = app_module.App._build_device_trends(rows, seconds=30)
    assert abs(trends["pace"]["delta"] - 1.2) < 1e-6
    assert trends["pace"]["level"] == "warn"
    assert abs(trends["hgen"]["delta"] - 1.5) < 1e-6
    assert "30秒变化：" in trends["hgen"]["text"]


def test_classify_event_level_uses_expected_severity() -> None:
    assert app_module.App._classify_event_level("Point 21 samples saved") == "ok"
    assert app_module.App._classify_event_level("CO2 300.0 ppm @ 550.0 hPa skipped: pressure did not stabilize") == "warn"
    assert app_module.App._classify_event_level("[gas_analyzer:ga04] FAIL INVALID_RESPONSE") == "error"
    assert app_module.App._classify_event_level("启动告警：STARTUP_NO_ACK") == "info"
    assert app_module.App._classify_event_level("分析仪运行告警：GA08 SETCOMWAY active（功能验证已通过）") == "warn"
    assert app_module.App._classify_event_level("分析仪恢复：GA08") == "ok"
    assert app_module.App._classify_event_level("CO2 route opened; wait 120s before pressure sealing") == "info"


def test_event_filter_matches_expected_groups() -> None:
    assert app_module.App._event_matches_filter("warn", "只看异常") is True
    assert app_module.App._event_matches_filter("error", "只看异常") is True
    assert app_module.App._event_matches_filter("ok", "只看异常") is False
    assert app_module.App._event_matches_filter("ok", "只看保存成功") is True
    assert app_module.App._event_matches_filter("warn", "只看保存成功") is False


def test_format_run_event_formats_analyzer_state_changes() -> None:
    warning = app_module.App._format_run_event(
        {
            "command": "analyzer-config-warning",
            "payload": {
                "label": "ga08",
                "phase": "runtime",
                "warnings": ["SETCOMWAY active", "AVERAGE_FILTER window=49"],
            },
        }
    )
    disabled = app_module.App._format_run_event(
        {
            "command": "analyzers-disabled",
            "payload": {"labels": ["ga03", "ga04"], "reason": "startup_mode2_verify_failed"},
        }
    )
    restored = app_module.App._format_run_event(
        {
            "command": "analyzers-restored",
            "payload": {"labels": ["ga03"]},
        }
    )

    assert warning == "分析仪运行告警：GA08 SETCOMWAY active、AVERAGE_FILTER window=49（功能验证已通过）"
    assert disabled == "分析仪告警：已禁用 GA03、GA04（startup_mode2_verify_failed）"
    assert restored == "分析仪恢复：GA03"


def test_build_device_trends_includes_detail_string() -> None:
    now = datetime(2026, 3, 10, 10, 0, 30)
    rows = [
        {"ts": (now - timedelta(seconds=10)).isoformat(), "port": "COM30", "direction": "RX", "response": "*0001100.0,"},
        {"ts": now.isoformat(), "port": "COM30", "direction": "RX", "response": "*0001102.0,"},
    ]
    trends = app_module.App._build_device_trends(rows, seconds=30)
    assert "起始=" in trends["gauge"]["detail"]
    assert "结束=" in trends["gauge"]["detail"]


def test_flow_help_tracks_route_mode(monkeypatch) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0, 30.0))

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        assert ui.flow_help_summary_var.get() == "先水后气：同温度组先水后气。"
        assert ui.flow_help_var.get() == ""
        assert ui.flow_help_toggle_button.cget("text") == "展开详细版"
        assert ui.flow_help_tooltip.widget is ui.flow_help_summary_label

        ui._toggle_flow_help()
        assert "开路等待 4 分钟" in ui.flow_help_var.get()
        assert "通气等待 120 秒" in ui.flow_help_var.get()

        ui.route_mode_var.set("只测水路")
        assert ui.flow_help_summary_var.get() == "水路：温箱稳 -> 湿度稳 -> 开水路 4 分钟 -> 封压 -> 控压采样。"
        assert "开路等待 4 分钟" in ui.flow_help_var.get()
        assert "通气等待 120 秒" not in ui.flow_help_var.get()

        ui.route_mode_var.set("只测气路")
        assert ui.flow_help_summary_var.get() == "气路：通标准气 120 秒 -> 判稳 -> 封压 -> 控压采样。"
        assert "通气等待 120 秒" in ui.flow_help_var.get()
        assert "开路等待 5 分钟" not in ui.flow_help_var.get()

        ui._toggle_flow_help()
        assert ui.flow_help_var.get() == ""
        assert ui.flow_help_toggle_button.cget("text") == "展开详细版"
    finally:
        root.destroy()


def test_update_stage_from_status_parses_phase_and_target(monkeypatch) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        ui.set_status("CO2 400ppm 1000hPa")
        assert ui.stage_var.get() == "当前阶段：气路控压/采样"
        assert ui.target_var.get() == "当前点位：CO2 400ppm / 1000hPa"
        assert ui.current_target_ppm_var.get() == "当前标气：400ppm"
        assert ui.current_pressure_point_var.get() == "当前压力点：1000hPa"
        ui.set_status("连接检查中...")
        assert ui.stage_var.get() == "当前阶段：连接检查"
    finally:
        root.destroy()


def test_update_stage_from_status_uses_h2o_row_parameters(monkeypatch) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(
        app_module,
        "load_points_from_excel",
        lambda *_args, **_kwargs: [
            types.SimpleNamespace(
                index=3,
                temp_chamber_c=20.0,
                hgen_temp_c=20.0,
                hgen_rh_pct=30.0,
                target_pressure_hpa=700.0,
                co2_ppm=None,
            )
        ],
    )

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        ui.set_status("H2O row 3")
        assert ui.stage_var.get() == "当前阶段：水路流程"
        assert ui.target_var.get() == "当前点位：水路 温箱20°C / 湿发20°C / 30%RH / 700hPa"
        assert ui.current_target_ppm_var.get() == "湿发设定：20°C / 30%RH"
        assert ui.current_pressure_point_var.get() == "当前压力点：700hPa"
    finally:
        root.destroy()


def test_update_stage_from_status_uses_co2_row_parameters(monkeypatch) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(
        app_module,
        "load_points_from_excel",
        lambda *_args, **_kwargs: [
            types.SimpleNamespace(
                index=8,
                temp_chamber_c=30.0,
                hgen_temp_c=None,
                hgen_rh_pct=None,
                target_pressure_hpa=900.0,
                co2_ppm=400.0,
            )
        ],
    )

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        ui.set_status("CO2 row 8")
        assert ui.stage_var.get() == "当前阶段：气路流程"
        assert ui.target_var.get() == "当前点位：气路 温箱30°C / 400ppm"
        assert ui.current_target_ppm_var.get() == "当前标气：400ppm"
        assert ui.current_pressure_point_var.get() == "当前压力点：--"
    finally:
        root.destroy()


def test_build_start_confirmation_text_contains_selection(monkeypatch) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        ui.pressure_check_vars[1100].set(True)
        ui.pressure_check_vars[1000].set(False)
        ui.pressure_check_vars[900].set(True)
        ui.pressure_check_vars[800].set(False)
        ui.pressure_check_vars[700].set(False)
        ui.pressure_check_vars[600].set(False)
        ui.pressure_check_vars[500].set(False)
        runtime_cfg = ui._build_runtime_cfg()
        text = ui._build_start_confirmation_text(runtime_cfg)
        assert "流程模式" in text
        assert "校准拟合：开启" in text
        assert "温度点" in text
        assert "气点" in text
        assert "压力点" in text
        assert "0ppm" in text
        assert "200ppm" in text
        assert "1100hPa、900hPa" in text
    finally:
        root.destroy()


def test_start_cancelled_by_confirmation_does_not_launch(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg(str(tmp_path)))
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))
    monkeypatch.setattr(app_module.messagebox, "askokcancel", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(app_module, "RunLogger", lambda *_args, **_kwargs: object())

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        built = {"called": False}
        ui._startup_connectivity_check = types.MethodType(lambda self, _logger: True, ui)
        ui._build_devices = types.MethodType(lambda self, io_logger=None: built.__setitem__("called", True), ui)
        ui.start()
        assert built["called"] is False
        assert ui.worker is None
    finally:
        root.destroy()


def test_start_launches_background_startup_thread(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg(str(tmp_path)))
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))
    monkeypatch.setattr(app_module.messagebox, "askokcancel", lambda *_args, **_kwargs: True)

    created: dict[str, object] = {}

    class DummyThread:
        def __init__(self, *, target=None, args=(), daemon=None):
            created["target"] = target
            created["args"] = args
            created["daemon"] = daemon
            created["started"] = False

        def start(self):
            created["started"] = True

    monkeypatch.setattr(app_module.threading, "Thread", DummyThread)

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        ui.start()
        assert created["target"] == ui._start_run_background
        assert created["daemon"] is False
        assert created["started"] is True
        assert ui.status_var.get() == "启动中..."
    finally:
        root.destroy()


def test_start_ignores_duplicate_when_startup_thread_alive(monkeypatch) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    class AliveThread:
        def is_alive(self):
            return True

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        ui.startup_thread = AliveThread()
        logs: list[str] = []
        ui.log = lambda msg: logs.append(msg)
        ui.start()
        assert any("启动中" in item for item in logs)
    finally:
        root.destroy()


def test_start_run_background_logs_error_and_sets_error(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg(str(tmp_path)))
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        logs: list[str] = []
        ui.log = lambda msg: logs.append(msg)
        ui._startup_connectivity_check = types.MethodType(lambda self, _logger: True, ui)
        ui._build_devices = types.MethodType(lambda self, io_logger=None: (_ for _ in ()).throw(RuntimeError("boom")), ui)
        ui._start_run_background(ui._build_runtime_cfg())
        assert ui.status_var.get() == "ERROR"
        assert any("启动失败：boom" in item for item in logs)
    finally:
        root.destroy()


def test_parse_live_device_values_extracts_core_devices() -> None:
    rows = [
        {"port": "COM31", "direction": "RX", "response": ":SENS:PRES:INL 1099.9562988, 1", "ts": "2026-03-09 11:00:01"},
        {"port": "COM30", "direction": "RX", "response": "*0001100.125,", "ts": "2026-03-09 11:00:02"},
        {"port": "COM27", "direction": "RX", "response": "temp_c=20.0", "ts": "2026-03-09 11:00:03"},
        {"port": "COM27", "direction": "RX", "response": "rh_pct=55.0", "ts": "2026-03-09 11:00:04"},
        {
            "port": "COM24",
            "direction": "RX",
            "response": "Uw= 29.8,Ui= 0.0,Tc= 20.087,Ts= 20.100,Td= 1.95,Tf= 1.96,Pc= 1028.90,Ps= 1030.04,Flux= 0.8",
            "ts": "2026-03-09 11:00:05",
        },
        {
            "port": "COM25",
            "direction": "RX",
            "response": "001_GetCurData_-14.46_30.26_0.21_63_2.0_0_1709.44_4.01_True_False_False_False_END",
            "ts": "2026-03-09 11:00:06",
        },
    ]

    values = app_module.App._parse_live_device_values(rows)
    assert "1099.96" in values["pace"]["text"]
    assert "稳定标志=1" in values["pace"]["text"]
    assert values["pace"]["timestamp"] == "2026-03-09 11:00:01"
    assert "1100.125" in values["gauge"]["text"]
    assert "20.0" in values["chamber"]["text"]
    assert "55.0" in values["chamber"]["text"]
    assert values["chamber"]["timestamp"] == "2026-03-09 11:00:04"
    assert "Tc=20.09" in values["hgen"]["text"]
    assert "Uw=29.8" in values["hgen"]["text"]
    assert "露点=-14.46" in values["dewpoint"]["text"]
    assert "湿度=4.01" in values["dewpoint"]["text"]


def test_parse_live_device_values_ignores_non_rx_or_empty() -> None:
    rows = [
        {"port": "COM31", "direction": "TX", "response": ":SENS:PRES:INL?"},
        {"port": "COM30", "direction": "RX", "response": ""},
    ]

    values = app_module.App._parse_live_device_values(rows)
    assert values["pace"]["text"] == "压力控制器：--"
    assert values["gauge"]["text"] == "数字气压计：--"


def test_parse_live_device_values_keeps_latest_parseable_pressure_response() -> None:
    rows = [
        {"port": "COM31", "direction": "RX", "response": ":SENS:PRES:INL 987.654, 0", "ts": "2026-03-09 11:00:01"},
        {"port": "COM31", "direction": "RX", "response": ":OUTP:STAT 0", "ts": "2026-03-09 11:00:02"},
    ]

    values = app_module.App._parse_live_device_values(rows)

    assert values["pace"]["text"] == "压力控制器：987.65 hPa，稳定标志=0"
    assert values["pace"]["timestamp"] == "2026-03-09 11:00:01"


def test_parse_live_analyzer_values_extracts_mode2_fields() -> None:
    rows = [
        {
            "port": "COM16",
            "direction": "RX",
            "response": "YGAS,001,400,1.234,2.1,3.2,0.456,0.457,0.111,0.112,9.9,8.8,7.7,20.12,21.34,101.56,T",
            "ts": "2026-03-09 11:00:07",
        }
    ]

    values = app_module.App._parse_live_analyzer_values(rows, _cfg_with_devices())

    assert values["ga01"]["port"] == "COM16"
    assert values["ga01"]["co2_ppm"] == "400"
    assert values["ga01"]["h2o_mmol"] == "1.234"
    assert values["ga01"]["co2_ratio_f"] == "0.456"
    assert values["ga01"]["chamber_temp_c"] == "20.12"
    assert values["ga01"]["status"] == "T"


def test_merge_live_device_values_preserves_cached_unused_device(monkeypatch) -> None:
    _require_tk_display()
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        ui._live_device_cache = {
            "gauge": {"text": "数字气压计：1000.123 hPa", "timestamp": "2026-03-09 11:00:02"}
        }
        merged = ui._merge_live_device_values(
            {
                "pace": {"text": "压力控制器：900.00 hPa，稳定标志=1", "timestamp": "2026-03-09 11:10:00"},
                "gauge": {"text": "数字气压计：--", "timestamp": "--"},
            }
        )

        assert merged["gauge"]["text"] == "数字气压计：1000.123 hPa"
        assert merged["gauge"]["timestamp"] == "2026-03-09 11:00:02"
        assert merged["pace"]["text"] == "压力控制器：900.00 hPa，稳定标志=1"
    finally:
        root.destroy()


def test_enabled_failures_expands_multi_gas_analyzer_items(monkeypatch) -> None:
    _require_tk_display()
    monkeypatch.setattr(app_module, "load_config", lambda _path: _cfg_with_devices())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        failures = ui._enabled_failures(
            ui.cfg,
            {
                "gas_analyzer": {
                    "ok": False,
                    "items": [
                        {"name": "ga01", "ok": True},
                        {"name": "ga02", "ok": False, "err": "TIMEOUT"},
                        {"name": "ga03", "ok": False, "err": "INVALID_RESPONSE"},
                    ],
                }
            },
        )
        assert failures == [("ga02", "TIMEOUT"), ("ga03", "INVALID_RESPONSE")]
    finally:
        root.destroy()


def test_startup_connectivity_check_can_skip_failed_gas_analyzers(monkeypatch) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _cfg_with_devices())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        ui.cfg.setdefault("workflow", {})["startup_connect_check"] = {"enabled": True}
        ui._call_on_ui_thread = lambda func, *args, **kwargs: func(*args, **kwargs)
        monkeypatch.setattr(
            app_module,
            "run_self_test",
            lambda *_args, **_kwargs: {
                "gas_analyzer": {
                    "ok": False,
                    "items": [
                        {"name": "ga01", "ok": True},
                        {"name": "ga02", "ok": False, "err": "TIMEOUT"},
                    ],
                }
            },
        )
        monkeypatch.setattr(app_module.messagebox, "askyesnocancel", lambda *_args, **_kwargs: False)
        assert ui._startup_connectivity_check(io_logger=object()) is True
        gas_cfg = ui.cfg["devices"]["gas_analyzers"]
        assert gas_cfg[0]["enabled"] is True
        assert gas_cfg[1]["enabled"] is False
    finally:
        root.destroy()


def test_startup_connectivity_check_can_continue_without_any_gas_analyzers(monkeypatch) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _cfg_with_devices())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        ui.cfg.setdefault("workflow", {})["startup_connect_check"] = {"enabled": True}
        ui._call_on_ui_thread = lambda func, *args, **kwargs: func(*args, **kwargs)

        captured = {"title": "", "message": ""}

        def _askyesnocancel(title, message, **_kwargs):
            captured["title"] = title
            captured["message"] = message
            return False

        monkeypatch.setattr(
            app_module,
            "run_self_test",
            lambda *_args, **_kwargs: {
                "gas_analyzer": {
                    "ok": False,
                    "items": [
                        {"name": f"ga{idx:02d}", "ok": False, "err": "INVALID_RESPONSE"}
                        for idx in range(1, 9)
                    ],
                }
            },
        )
        monkeypatch.setattr(app_module.messagebox, "askyesnocancel", _askyesnocancel)

        assert ui._startup_connectivity_check(io_logger=object()) is True
        assert "当前没有可用分析仪" in captured["message"]
        assert "不会采集分析仪数据" in captured["message"]
        gas_cfg = ui.cfg["devices"]["gas_analyzers"]
        assert all(item["enabled"] is False for item in gas_cfg)
        assert ui.cfg["devices"]["gas_analyzer"]["enabled"] is False
    finally:
        root.destroy()


def test_startup_connectivity_check_can_continue_when_analyzers_and_thermometer_fail(monkeypatch) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _cfg_with_devices())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        ui.cfg.setdefault("workflow", {})["startup_connect_check"] = {"enabled": True}
        ui.cfg["devices"]["thermometer"]["enabled"] = True
        ui._call_on_ui_thread = lambda func, *args, **kwargs: func(*args, **kwargs)

        captured = {"message": ""}

        def _askyesnocancel(_title, message, **_kwargs):
            captured["message"] = message
            return False

        monkeypatch.setattr(
            app_module,
            "run_self_test",
            lambda *_args, **_kwargs: {
                "gas_analyzer": {
                    "ok": False,
                    "items": [
                        {"name": "ga01", "ok": True},
                        {"name": "ga02", "ok": False, "err": "INVALID_RESPONSE"},
                    ],
                },
                "thermometer": {"ok": False, "err": "NO_VALID_FRAME"},
            },
        )
        monkeypatch.setattr(app_module.messagebox, "askyesnocancel", _askyesnocancel)

        assert ui._startup_connectivity_check(io_logger=object()) is True
        assert "温度计也将被跳过" in captured["message"]
        assert ui.cfg["devices"]["gas_analyzers"][0]["enabled"] is True
        assert ui.cfg["devices"]["gas_analyzers"][1]["enabled"] is False
        assert ui.cfg["devices"]["thermometer"]["enabled"] is False
    finally:
        root.destroy()


def test_refresh_live_device_values_updates_analyzer_table(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _cfg_with_devices())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        cfg = _cfg_with_devices(str(tmp_path))
        run_dir = tmp_path / "run_demo"
        run_dir.mkdir()
        rows = [
            {
                "port": "COM16",
                "direction": "RX",
                "response": "YGAS,001,400,1.234,2.1,3.2,0.456,0.457,0.111,0.112,9.9,8.8,7.7,20.12,21.34,101.56,T",
                "ts": "2026-03-09 11:00:07",
            }
        ]
        monkeypatch.setattr(ui, "_tail_csv_rows", lambda *_args, **_kwargs: rows)
        monkeypatch.setattr(ui, "_load_runtime_config_snapshot", lambda *_args, **_kwargs: cfg)
        io_path = tmp_path / "io_demo.csv"
        io_path.write_text("x", encoding="utf-8")
        ui.current_io_path = io_path
        ui.current_run_dir = run_dir

        ui._refresh_live_device_values()

        assert ui.analyzer_table is not None
        row = ui.analyzer_table.item(ui.analyzer_table_items["ga01"], "values")
        columns = [key for key, _label in app_module.ANALYZER_MODE2_COLUMNS]
        row_map = {columns[idx]: row[idx] for idx in range(min(len(columns), len(row)))}
        assert row[0] == "GA01"
        assert row[1] == "COM16"
        assert row_map["co2_ppm"] == "400"
        assert row_map["h2o_mmol"] == "1.234"
        assert row_map["chamber_temp_c"] == "20.12"
        assert row_map["status"] == "T"
    finally:
        root.destroy()


def test_refresh_live_device_values_deep_scans_initial_analyzer_rows(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _cfg_with_devices())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        cfg = _cfg_with_devices(str(tmp_path))
        run_dir = tmp_path / "run_demo"
        run_dir.mkdir()
        deep_rows = [
            {
                "port": "COM16",
                "direction": "RX",
                "response": "YGAS,001,400,1.234,2.1,3.2,0.456,0.457,0.111,0.112,9.9,8.8,7.7,20.12,21.34,101.56,T",
                "ts": "2026-03-09 11:00:07",
            }
        ]

        def _fake_tail(_path, count=120):
            if count >= 30000:
                return deep_rows
            return []

        monkeypatch.setattr(ui, "_tail_csv_rows", _fake_tail)
        monkeypatch.setattr(ui, "_load_runtime_config_snapshot", lambda *_args, **_kwargs: cfg)
        io_path = tmp_path / "io_demo.csv"
        io_path.write_text("x", encoding="utf-8")
        ui.current_io_path = io_path
        ui.current_run_dir = run_dir

        ui._refresh_live_device_values()

        assert ui.analyzer_table is not None
        row = ui.analyzer_table.item(ui.analyzer_table_items["ga01"], "values")
        columns = [key for key, _label in app_module.ANALYZER_MODE2_COLUMNS]
        row_map = {columns[idx]: row[idx] for idx in range(min(len(columns), len(row)))}
        assert row[0] == "GA01"
        assert row_map["co2_ppm"] == "400"
        assert row_map["chamber_temp_c"] == "20.12"
    finally:
        root.destroy()


def test_refresh_live_device_values_throttles_analyzer_updates_until_forced(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _cfg_with_devices())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        cfg = _cfg_with_devices(str(tmp_path))
        run_dir = tmp_path / "run_demo"
        run_dir.mkdir()
        state = {
            "response": "YGAS,001,400,1.234,2.1,3.2,0.456,0.457,0.111,0.112,9.9,8.8,7.7,20.12,21.34,101.56,T"
        }

        def _fake_tail(_path, count=120):
            return [
                {
                    "port": "COM16",
                    "direction": "RX",
                    "response": state["response"],
                    "ts": "2026-03-09 11:00:07",
                }
            ]

        monkeypatch.setattr(ui, "_tail_csv_rows", _fake_tail)
        monkeypatch.setattr(ui, "_load_runtime_config_snapshot", lambda *_args, **_kwargs: cfg)
        io_path = tmp_path / "io_demo.csv"
        io_path.write_text("x", encoding="utf-8")
        ui.current_io_path = io_path
        ui.current_run_dir = run_dir

        ui._refresh_live_device_values()

        columns = [key for key, _label in app_module.ANALYZER_MODE2_COLUMNS]
        row = ui.analyzer_table.item(ui.analyzer_table_items["ga01"], "values")
        row_map = {columns[idx]: row[idx] for idx in range(min(len(columns), len(row)))}
        assert row_map["co2_ppm"] == "400"

        state["response"] = "YGAS,001,500,1.234,2.1,3.2,0.456,0.457,0.111,0.112,9.9,8.8,7.7,20.52,21.34,101.56,T"

        ui._refresh_live_device_values()

        row = ui.analyzer_table.item(ui.analyzer_table_items["ga01"], "values")
        row_map = {columns[idx]: row[idx] for idx in range(min(len(columns), len(row)))}
        assert row_map["co2_ppm"] == "400"

        ui._refresh_live_device_values(force=True)

        row = ui.analyzer_table.item(ui.analyzer_table_items["ga01"], "values")
        row_map = {columns[idx]: row[idx] for idx in range(min(len(columns), len(row)))}
        assert row_map["co2_ppm"] == "500"
        assert row_map["chamber_temp_c"] == "20.52"
    finally:
        root.destroy()


def test_runtime_config_diff_text_reports_changes(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    run_dir = tmp_path / "run_demo"
    run_dir.mkdir()
    snapshot_cfg = _basic_cfg()
    snapshot_cfg.setdefault("workflow", {})["stability"] = {
        "h2o_route": {"preseal_soak_s": 300},
        "co2_route": {"preseal_soak_s": 120},
    }
    snapshot_cfg["workflow"]["sampling"] = {"count": 10, "h2o_interval_s": 10, "co2_interval_s": 10}
    app_module.App._write_runtime_config_snapshot(run_dir, snapshot_cfg)

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        ui.last_runtime_cfg = _basic_cfg()
        ui.last_runtime_cfg.setdefault("workflow", {})["stability"] = {
            "h2o_route": {"preseal_soak_s": 180},
            "co2_route": {"preseal_soak_s": 120},
        }
        ui.last_runtime_cfg["workflow"]["sampling"] = {"count": 10, "h2o_interval_s": 1, "co2_interval_s": 1}
        text = ui._build_runtime_config_diff_text(run_dir)
        assert "配置差异：" in text
        assert "水路开路等待" in text
    finally:
        root.destroy()


def test_load_runtime_config_snapshot_uses_cache_until_file_changes(tmp_path, monkeypatch) -> None:
    run_dir = tmp_path / "run_demo"
    run_dir.mkdir()
    app_module.App._runtime_snapshot_cache = {}
    app_module.App._write_runtime_config_snapshot(run_dir, {"workflow": {"sampling": {"count": 10}}})
    snapshot_path = app_module.App._runtime_config_snapshot_path(run_dir)
    original_read_text = Path.read_text
    calls = {"count": 0}

    def _counting_read_text(self, *args, **kwargs):
        if self == snapshot_path:
            calls["count"] += 1
        return original_read_text(self, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", _counting_read_text)

    first = app_module.App._load_runtime_config_snapshot(run_dir)
    second = app_module.App._load_runtime_config_snapshot(run_dir)

    assert first == second
    assert calls["count"] == 1


def test_infer_route_state_detects_open_seal_and_control() -> None:
    open_rows = [
        {"port": "COM29", "direction": "TX", "command": "write_coil(0,True,addr=1)"},
        {"port": "COM29", "direction": "TX", "command": "write_coil(1,True,addr=1)"},
        {"port": "COM29", "direction": "TX", "command": "write_coil(7,True,addr=1)"},
        {"port": "COM31", "direction": "TX", "command": ":SOUR:PRES:LEV:IMM:AMPL:VENT 1"},
    ]
    assert app_module.App._infer_route_state(open_rows) == "开路"

    seal_rows = [
        {"port": "COM29", "direction": "TX", "command": "write_coil(0,False,addr=1)"},
        {"port": "COM29", "direction": "TX", "command": "write_coil(1,False,addr=1)"},
        {"port": "COM29", "direction": "TX", "command": "write_coil(7,False,addr=1)"},
        {"port": "COM31", "direction": "TX", "command": ":SOUR:PRES:LEV:IMM:AMPL:VENT 0"},
    ]
    assert app_module.App._infer_route_state(seal_rows) == "封压中"

    control_rows = seal_rows + [
        {"port": "COM31", "direction": "TX", "command": ":OUTP 1"},
    ]
    assert app_module.App._infer_route_state(control_rows) == "控压中"


def test_refresh_live_device_values_appends_route_state(monkeypatch) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        rows = [
            {"port": "COM31", "direction": "TX", "command": ":SOUR:PRES:LEV:IMM:AMPL:VENT 0"},
            {"port": "COM31", "direction": "TX", "command": ":OUTP 1"},
            {"port": "COM31", "direction": "RX", "response": ":SENS:PRES:INL 1099.9562988, 1", "ts": "2026-03-09 11:00:01"},
            {"port": "COM30", "direction": "RX", "response": "*0001100.125,", "ts": "2026-03-09 11:00:02"},
        ]
        monkeypatch.setattr(ui, "_tail_csv_rows", lambda *_args, **_kwargs: rows)
        io_path = Path(root.tk.eval('info nameofexecutable')).resolve()
        # any existing file path works because _tail_csv_rows is stubbed
        ui.current_io_path = io_path
        ui._refresh_live_device_values()
        assert "工艺：控压中" in ui.device_state_vars["pace"].get()
        assert "工艺：控压中" in ui.device_state_vars["gauge"].get()
    finally:
        root.destroy()


def test_compute_progress_status_from_run_dir(tmp_path) -> None:
    run_dir = tmp_path / "run_demo"
    run_dir.mkdir()
    stdout = run_dir / "run_demo_stdout.log"
    stdout.write_text(
        "\n".join(
            [
                "Temperature group 20.0C CO2 sweep: sources=[0, 400, 600] pressures=[1100, 1000]",
                "Point 21 samples saved: demo1.csv",
                "CO2 400.0 ppm @ 1000.0 hPa skipped: pressure did not stabilize",
                "CO2 600ppm 1100hPa",
            ]
        ),
        encoding="utf-8",
    )
    (run_dir / "point_0021_co2_demo_samples.csv").write_text("x", encoding="utf-8")

    progress = app_module.App._compute_progress_status(run_dir)
    assert progress["total"] == 6
    assert progress["completed"] == 1
    assert progress["skipped"] == 1
    assert progress["current"] == "CO2 600ppm 1100hPa"
    assert round(progress["percent"], 1) == round((2 / 6) * 100.0, 1)
    assert progress["route_group"] == "第一组气路"
    assert "skipped" in progress["last_issue"]
    assert progress["failed"] == 0
    assert any("Point 21 samples saved" in item for item in progress["recent_points"])


def test_extract_key_events_filters_relevant_lines() -> None:
    lines = [
        "random line",
        "CO2 route opened; wait 120s before pressure sealing (row 21)",
        "Pressure in-limits at target 1100.0 hPa",
        "Point 21 samples saved: demo.csv",
    ]
    events = app_module.App._extract_key_events(lines, count=10)
    assert len(events) == 3
    assert "CO2 route opened" in events[0]
    assert "Pressure in-limits" in events[1]
    assert "Point 21 samples saved" in events[2]


def test_refresh_progress_status_updates_ui(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    run_dir = tmp_path / "run_demo"
    run_dir.mkdir()
    (run_dir / "run_demo_stdout.log").write_text(
        "Temperature group 20.0C CO2 sweep: sources=[100, 300] pressures=[1100]\n"
        "CO2 100.0 ppm @ 550.0 hPa skipped: pressure did not stabilize\n"
        "CO2 300ppm 1100hPa\n",
        encoding="utf-8",
    )

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        ui.current_run_dir = run_dir
        ui._refresh_progress_status()
        assert "CO2 300ppm 1100hPa" in ui.progress_summary_var.get()
        assert "总点数：2" in ui.progress_detail_var.get()
        assert float(ui.progress_var.get()) == 50.0
        assert ui.route_group_var.get() == "当前气路组别：第二组气路"
        assert ui.current_route_group_detail_var.get() == "当前气路组：第二组气路"
        assert "100.0 ppm @ 550.0 hPa skipped" in ui.last_issue_var.get()
        assert ui.stat_completed_var.get() == "成功 0"
        assert ui.stat_skipped_var.get() == "跳过 1"
        assert ui.stat_failed_var.get() == "失败 0"
        assert ui.history_list.size() >= 2
    finally:
        root.destroy()


def test_refresh_progress_status_maps_history_file_and_workbook(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    run_dir = tmp_path / "run_demo"
    run_dir.mkdir()
    sample_file = run_dir / "point_0021_co2_co2_groupa_0ppm_1100hpa_samples.csv"
    sample_file.write_text("x", encoding="utf-8")
    workbook = run_dir / "co2_analyzer_sheets_20260309_111524.xlsx"
    workbook.write_text("x", encoding="utf-8")
    (run_dir / "run_demo_stdout.log").write_text(
        "Point 21 samples saved: point_0021_co2_co2_groupa_0ppm_1100hpa_samples.csv\n"
        "CO2 0ppm 1100hPa\n",
        encoding="utf-8",
    )

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        ui.current_run_dir = run_dir
        ui._refresh_progress_status()
        assert ui.current_workbook_path == workbook
        assert ui.current_latest_point_path == sample_file
        assert ui.current_workbook_name_var.get() == f"Workbook：{workbook.name}"
        assert ui.current_latest_point_name_var.get() == f"最新点文件：{sample_file.name}"
        assert ui.history_item_paths["Point 21 samples saved: point_0021_co2_co2_groupa_0ppm_1100hpa_samples.csv"] == sample_file
        assert ui.history_item_paths["CO2 0ppm 1100hPa"] == sample_file
        assert str(ui.open_latest_point_button.cget("state")) == "normal"
        assert str(ui.open_workbook_button.cget("state")) == "normal"
        assert str(ui.open_run_dir_button.cget("state")) == "normal"
    finally:
        root.destroy()


def test_refresh_progress_status_loads_coefficient_report_preview(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    run_dir = tmp_path / "run_demo"
    run_dir.mkdir()
    report = run_dir / "co2_GA07_ratio_poly_fit_20260316_140000.json"
    residuals = run_dir / "co2_GA07_ratio_poly_fit_20260316_140000_residuals.csv"
    report.write_text(
        json.dumps(
            {
                "model": "ratio_poly_rt_p",
                "gas": "co2",
                "n": 15,
                "feature_terms": {"a0": "1", "a1": "R", "a2": "R^2"},
                "original_coefficients": {"a0": 42.500123, "a1": 1.234891, "a2": -0.0004558},
                "simplified_coefficients": {"a0": 42.5, "a1": 1.234567, "a2": -0.000456},
                "stats": {
                    "rmse_original": 0.0101,
                    "rmse_simplified": 0.0123,
                    "mae_simplified": 0.0088,
                    "max_abs_simplified": 0.0456,
                    "rmse_change": 0.0022,
                    "dataset_split": {
                        "fit_count": 15,
                        "train_count": 10,
                        "validation_count": 3,
                        "test_count": 2,
                        "fit_scope": "full_dataset",
                    },
                    "fit_settings": {
                        "fitting_method": "least_squares",
                        "simplification_method": "column_norm",
                    },
                    "simplification_summary": {"selected_digits": 6},
                    "model_features": ["intercept", "R", "R2"],
                    "train_metrics": {
                        "sample_count": 10,
                        "simplified": {"RMSE": 0.011, "R2": 0.998, "Bias": 0.001, "MaxError": 0.03},
                    },
                    "validation_metrics": {
                        "sample_count": 3,
                        "simplified": {"RMSE": 0.013, "R2": 0.997, "Bias": -0.002, "MaxError": 0.04},
                    },
                    "test_metrics": {
                        "sample_count": 2,
                        "simplified": {"RMSE": 0.014, "R2": 0.996, "Bias": 0.0005, "MaxError": 0.045},
                        "range_simplified": [
                            {"RangeLabel": "0-200", "Count": 1, "RMSE": 0.012, "Bias": 0.001, "MaxError": 0.012},
                            {"RangeLabel": "200-400", "Count": 1, "RMSE": 0.016, "Bias": -0.001, "MaxError": 0.016},
                        ],
                    },
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    residuals.write_text("dataset_split,Analyzer\ntrain,GA07\n", encoding="utf-8")
    (run_dir / "run_demo_stdout.log").write_text("CO2 0ppm 1100hPa\n", encoding="utf-8")

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        ui.current_run_dir = run_dir
        ui._refresh_progress_status()
        assert ui.current_coefficient_report_path == report
        assert report.name in ui.current_coefficient_report_name_var.get()
        preview_text = ui.coefficient_text.get("1.0", "end-1c")
        status_preview_text = ui.status_coefficient_text.get("1.0", "end-1c")
        page_preview_text = ui.coefficient_page_text.get("1.0", "end-1c")
        assert "GA07" in preview_text
        assert "最终系数(简化):" in preview_text
        assert "a1 (R)" in preview_text
        assert "原始系数:" in preview_text
        assert "RMSE(原始): 0.0101" in preview_text
        assert "RMSE(简化): 0.0123" in preview_text
        assert "数据划分: 拟合=15 | 训练=10 | 验证=3 | 测试=2 | 范围=full_dataset" in preview_text
        assert "训练集(简化) | n=10 | RMSE=0.011" in preview_text
        assert "测试集分段表现(简化):" in preview_text
        assert "0-200 | n=1 | RMSE=0.012 | Bias=0.001 | MaxErr=0.012" in preview_text
        assert preview_text == status_preview_text
        assert preview_text == page_preview_text
        assert str(ui.open_coefficient_tab_button.cget("state")) == "normal"
        assert str(ui.open_coefficient_report_button.cget("state")) == "normal"
    finally:
        root.destroy()


def test_refresh_progress_status_loads_temperature_compensation_preview(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    run_dir = tmp_path / "run_demo"
    run_dir.mkdir()
    (run_dir / "temperature_compensation.xlsx").write_text("x", encoding="utf-8")
    (run_dir / "temperature_compensation_commands.txt").write_text(
        "SENCO7,YGAS,FFF,0.00000e00,1.00000e00,0.00000e00,0.00000e00\n",
        encoding="utf-8",
    )
    (run_dir / "temperature_compensation_coefficients.csv").write_text(
        "analyzer_id,fit_type,senco_channel,ref_temp_source,n_points,fit_ok,availability,polynomial_degree_used,rmse,max_abs_error,A,B,C,D,command_string\n"
        "GA01,cell,SENCO7,env,7,True,available,3,0.01,0.02,0.0,1.0,0.0,0.0,\"SENCO7,YGAS,FFF,0.00000e00,1.00000e00,0.00000e00,0.00000e00\"\n"
        "GA01,shell,SENCO8,env,7,True,available,3,0.03,0.04,0.0,1.0,0.0,0.0,\"SENCO8,YGAS,FFF,0.00000e00,1.00000e00,0.00000e00,0.00000e00\"\n",
        encoding="utf-8",
    )

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        ui.current_run_dir = run_dir
        ui._refresh_progress_status()
        preview_text = ui.coefficient_page_text.get("1.0", "end-1c")
        assert "温度补偿结果:" in preview_text
        assert "GA01" in preview_text
        assert "SENCO7" in preview_text
        assert "SENCO8" in preview_text
        assert "SENCO7,YGAS,FFF" in preview_text
        assert ui.current_temperature_compensation_report_path == run_dir / "temperature_compensation.xlsx"
        assert "temperature_compensation.xlsx" in ui.current_temperature_compensation_name_var.get()
        assert str(ui.open_temperature_compensation_button.cget("state")) == "normal"
        assert str(ui.apply_temperature_compensation_button.cget("state")) == "normal"
    finally:
        root.destroy()


def test_v1_ui_does_not_expose_merged_sidecar_entry(monkeypatch) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))
    _require_tk_display()

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        assert not hasattr(ui, "run_merged_calibration_sidecar_button")
        assert not hasattr(ui, "_open_merged_calibration_sidecar_dialog")
        assert not hasattr(ui, "_launch_merged_calibration_sidecar")
    finally:
        root.destroy()


def test_v1_ui_headless_contract_does_not_expose_merged_sidecar_entry() -> None:
    class_source = inspect.getsource(app_module.App)

    assert "merged_calibration_sidecar" not in class_source
    assert not hasattr(app_module.App, "_open_merged_calibration_sidecar_dialog")
    assert not hasattr(app_module.App, "_launch_merged_calibration_sidecar")


def test_open_current_run_dir_and_workbook(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        workbook = tmp_path / "co2_analyzer_sheets_test.xlsx"
        workbook.write_text("x", encoding="utf-8")
        ui.current_run_dir = tmp_path
        ui.current_workbook_path = workbook
        ui.current_latest_point_path = tmp_path / "point_0021.csv"
        ui.current_latest_point_path.write_text("x", encoding="utf-8")
        ui.current_workbook_name_var.set(f"Workbook：{workbook.name}")
        opened: list[str] = []
        monkeypatch.setattr(app_module.os, "startfile", lambda path: opened.append(path))
        ui._open_latest_point_file()
        ui._open_current_workbook()
        ui._open_current_run_dir()
        assert opened == [str(ui.current_latest_point_path), str(workbook), str(tmp_path)]
    finally:
        root.destroy()


def test_open_current_coefficient_report(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        report = tmp_path / "co2_GA07_ratio_poly_fit_20260316_140000.json"
        report.write_text("{}", encoding="utf-8")
        ui.current_coefficient_report_path = report
        opened: list[str] = []
        monkeypatch.setattr(app_module.os, "startfile", lambda path: opened.append(path))
        ui._open_current_coefficient_report()
        assert opened == [str(report)]
    finally:
        root.destroy()


def test_run_temperature_compensation_apply_writes_senco(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg(str(tmp_path)))
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)

        calls: list[tuple[str, object]] = []

        class _FakeAnalyzer:
            def set_mode(self, mode: int) -> bool:
                calls.append(("mode", int(mode)))
                return True

            def set_senco(self, index: int, *coeffs: float) -> bool:
                calls.append((f"senco{index}", tuple(coeffs)))
                return True

            def close(self) -> None:
                return None

        monkeypatch.setattr(
            app_module.App,
            "_build_gas_analyzers_for_temperature_compensation",
            staticmethod(lambda cfg, io_logger=None: {"GA01": _FakeAnalyzer()}),
        )
        monkeypatch.setattr(ui, "_call_on_ui_thread", lambda func, *args, **kwargs: func(*args, **kwargs))
        shown: list[tuple[str, str]] = []
        monkeypatch.setattr(app_module.messagebox, "showinfo", lambda title, msg, parent=None: shown.append((title, msg)))
        monkeypatch.setattr(app_module.messagebox, "showwarning", lambda title, msg, parent=None: shown.append((title, msg)))
        monkeypatch.setattr(app_module.messagebox, "showerror", lambda title, msg, parent=None: shown.append((title, msg)))

        plan = {
            "GA01": {
                "cell": {"A": 0.1, "B": 1.0, "C": 0.0, "D": 0.0},
                "shell": {"A": 0.2, "B": 1.1, "C": 0.0, "D": 0.0},
            }
        }
        ui._run_temperature_compensation_apply({"paths": {"output_dir": str(tmp_path)}, "devices": {}}, plan)
        assert calls == [
            ("mode", 2),
            ("senco7", (0.1, 1.0, 0.0, 0.0)),
            ("senco8", (0.2, 1.1, 0.0, 0.0)),
            ("mode", 1),
        ]
        assert any(title == "温度补偿下发完成" for title, _ in shown)
        assert "成功 2 项" in ui.temperature_compensation_apply_status_var.get()
    finally:
        root.destroy()


def test_run_temperature_compensation_apply_attempts_mode_exit_after_failure(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg(str(tmp_path)))
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)

        calls: list[tuple[str, object]] = []

        class _FakeAnalyzer:
            def set_mode(self, mode: int) -> bool:
                calls.append(("mode", int(mode)))
                return True

            def set_senco(self, index: int, *coeffs: float) -> bool:
                calls.append((f"senco{index}", tuple(coeffs)))
                raise RuntimeError("write failed")

            def close(self) -> None:
                return None

        monkeypatch.setattr(
            app_module.App,
            "_build_gas_analyzers_for_temperature_compensation",
            staticmethod(lambda cfg, io_logger=None: {"GA01": _FakeAnalyzer()}),
        )
        monkeypatch.setattr(ui, "_call_on_ui_thread", lambda func, *args, **kwargs: func(*args, **kwargs))
        shown: list[tuple[str, str]] = []
        monkeypatch.setattr(app_module.messagebox, "showinfo", lambda title, msg, parent=None: shown.append((title, msg)))
        monkeypatch.setattr(app_module.messagebox, "showwarning", lambda title, msg, parent=None: shown.append((title, msg)))
        monkeypatch.setattr(app_module.messagebox, "showerror", lambda title, msg, parent=None: shown.append((title, msg)))

        plan = {"GA01": {"cell": {"A": 0.1, "B": 1.0, "C": 0.0, "D": 0.0}}}
        ui._run_temperature_compensation_apply({"paths": {"output_dir": str(tmp_path)}, "devices": {}}, plan)

        assert calls == [("mode", 2), ("senco7", (0.1, 1.0, 0.0, 0.0)), ("mode", 1)]
        assert any(title == "温度补偿下发失败" for title, _ in shown)
        assert "失败" in ui.temperature_compensation_apply_status_var.get()
    finally:
        root.destroy()


def test_double_click_history_opens_mapped_file(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        sample_file = tmp_path / "point_0021_co2_co2_groupa_0ppm_1100hpa_samples.csv"
        sample_file.write_text("x", encoding="utf-8")
        item = "CO2 0ppm 1100hPa"
        ui.history_item_paths[item] = sample_file
        ui.history_list.insert("end", item)
        ui.history_list.selection_set(0)
        opened: list[str] = []
        monkeypatch.setattr(app_module.os, "startfile", lambda path: opened.append(path))
        ui._open_selected_history_item()
        assert opened == [str(sample_file)]
    finally:
        root.destroy()


def test_open_selected_history_parent(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        sample_file = tmp_path / "point_0021_co2_co2_groupa_0ppm_1100hpa_samples.csv"
        sample_file.write_text("x", encoding="utf-8")
        item = "CO2 0ppm 1100hPa"
        ui.history_item_paths[item] = sample_file
        ui.history_list.insert("end", item)
        ui.history_list.selection_set(0)
        opened: list[str] = []
        monkeypatch.setattr(app_module.os, "startfile", lambda path: opened.append(path))
        ui._open_selected_history_parent()
        assert opened == [str(tmp_path)]
    finally:
        root.destroy()


def test_copy_selected_history_item(monkeypatch) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        ui.history_list.insert("end", "CO2 0ppm 1100hPa")
        ui.history_list.selection_set(0)
        copied: list[str] = []
        monkeypatch.setattr(ui.root, "clipboard_clear", lambda: None)
        monkeypatch.setattr(ui.root, "clipboard_append", lambda text: copied.append(text))
        ui._copy_selected_history_item()
        assert copied == ["CO2 0ppm 1100hPa"]
    finally:
        root.destroy()


def test_copy_selected_history_path(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        sample_file = tmp_path / "point_0021_co2_co2_groupa_0ppm_1100hpa_samples.csv"
        sample_file.write_text("x", encoding="utf-8")
        item = "CO2 0ppm 1100hPa"
        ui.history_item_paths[item] = sample_file
        ui.history_list.insert("end", item)
        ui.history_list.selection_set(0)
        copied: list[str] = []
        monkeypatch.setattr(ui.root, "clipboard_clear", lambda: None)
        monkeypatch.setattr(ui.root, "clipboard_append", lambda text: copied.append(text))
        ui._copy_selected_history_path()
        assert copied == [str(sample_file)]
    finally:
        root.destroy()


def test_show_history_context_menu_selects_nearest_item(monkeypatch) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        ui.history_list.insert("end", "第一项")
        ui.history_list.insert("end", "第二项")
        popup_calls: list[tuple[int, int]] = []
        monkeypatch.setattr(ui.history_menu, "tk_popup", lambda x, y: popup_calls.append((x, y)))
        event = types.SimpleNamespace(x_root=10, y_root=20, y=5)
        ui._show_history_context_menu(event)
        assert popup_calls == [(10, 20)]
        assert ui.history_list.get(ui.history_list.curselection()[0]) == "第一项"
    finally:
        root.destroy()


def test_refresh_progress_status_updates_workbook_name(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    run_dir = tmp_path / "run_demo"
    run_dir.mkdir()
    workbook = run_dir / "co2_analyzer_sheets_20260309_111524.xlsx"
    workbook.write_text("x", encoding="utf-8")
    (run_dir / "run_demo_stdout.log").write_text("CO2 0ppm 1100hPa\n", encoding="utf-8")

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        ui.current_run_dir = run_dir
        ui._refresh_progress_status()
        assert ui.current_workbook_name_var.get() == f"Workbook：{workbook.name}"
    finally:
        root.destroy()


def test_history_item_level_maps_success_skip_current() -> None:
    assert app_module.App._history_item_level("Point 21 samples saved: demo.csv", "CO2 0ppm 1100hPa") == "ok"
    assert app_module.App._history_item_level("CO2 400.0 ppm @ 550.0 hPa skipped: pressure did not stabilize", "CO2 0ppm 1100hPa") == "warn"
    assert app_module.App._history_item_level("CO2 0ppm 1100hPa", "CO2 0ppm 1100hPa") == "info"


def test_history_item_matches_filter() -> None:
    assert app_module.App._history_item_matches_filter("ok", "全部") is True
    assert app_module.App._history_item_matches_filter("ok", "成功") is True
    assert app_module.App._history_item_matches_filter("warn", "成功") is False
    assert app_module.App._history_item_matches_filter("warn", "跳过") is True
    assert app_module.App._history_item_matches_filter("info", "当前") is True


def test_refresh_history_list_applies_filter(monkeypatch) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        ui.history_items_cache = [
            "Point 21 samples saved: demo.csv",
            "CO2 400.0 ppm @ 550.0 hPa skipped: pressure did not stabilize",
            "CO2 0ppm 1100hPa",
        ]
        ui.history_filter_var.set("成功")
        ui._refresh_history_list("CO2 0ppm 1100hPa")
        assert ui.history_list.size() == 1
        assert "samples saved" in ui.history_list.get(0)

        ui.history_filter_var.set("跳过")
        ui._refresh_history_list("CO2 0ppm 1100hPa")
        assert ui.history_list.size() == 1
        assert "skipped" in ui.history_list.get(0)

        ui.history_filter_var.set("当前")
        ui._refresh_history_list("CO2 0ppm 1100hPa")
        assert ui.history_list.size() == 1
        assert ui.history_list.get(0) == "CO2 0ppm 1100hPa"
    finally:
        root.destroy()


def test_refresh_progress_status_sets_empty_file_hints(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    run_dir = tmp_path / "run_demo"
    run_dir.mkdir()
    (run_dir / "run_demo_stdout.log").write_text("CO2 0ppm 1100hPa\n", encoding="utf-8")

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        ui.current_run_dir = run_dir
        ui._refresh_progress_status()
        assert ui.current_workbook_name_var.get() == "Workbook：当前无文件"
        assert ui.current_latest_point_name_var.get() == "最新点文件：当前无文件"
        assert str(ui.open_workbook_button.cget("state")) == "disabled"
        assert str(ui.open_latest_point_button.cget("state")) == "disabled"
    finally:
        root.destroy()


def test_compute_progress_status_falls_back_to_io_when_stdout_missing(tmp_path) -> None:
    run_dir = tmp_path / "run_demo"
    run_dir.mkdir()
    (run_dir / "point_0001_h2o_samples.csv").write_text("x", encoding="utf-8")
    (run_dir / "io_20260309_191507.csv").write_text(
        "timestamp,port,device,direction,command,response,error\n"
        "2026-03-09T19:15:45,COM29,relay_controller,TX,\"write_coil(0,False,addr=1)\",,\n"
        "2026-03-09T19:15:45,COM29,relay_controller,TX,\"write_coil(1,True,addr=1)\",,\n"
        "2026-03-09T19:15:45,COM29,relay_controller,TX,\"write_coil(7,False,addr=1)\",,\n"
        "2026-03-09T19:15:53,COM24,humidity_generator,RX,,\"Uw= 26.4,Ui= 0.0,Tc= 22.052,Ts= 19.958,Td= 1.90,Tf= -1001.00,Pc= 1027.50,Ps= 3419.53,Flux= 5.3,PST= 00:12:13,TST= 00:04:29\",\n"
        "2026-03-09T19:15:54,COM31,pace5000,TX,\":SOUR:PRES:LEV:IMM:AMPL:VENT 1\\n\",,\n",
        encoding="utf-8",
    )

    progress = app_module.App._compute_progress_status(run_dir)
    assert progress["completed"] == 1
    assert progress["route_group"] == "水路"
    assert "H2O 前置" in progress["current"]
    assert any("Point 1 samples saved" in item for item in progress["recent_points"])
    assert progress["sample_progress"].endswith("--")
    assert "s" in progress["freshness_text"] or progress["freshness_text"].endswith("--")


def test_tail_text_lines_returns_last_lines(tmp_path) -> None:
    path = tmp_path / "demo.log"
    path.write_text("line1\nline2\nline3\nline4\n", encoding="utf-8")

    assert app_module.App._tail_text_lines(path, count=2) == ["line3", "line4"]


def test_tail_csv_rows_returns_last_rows_with_quoted_commas(tmp_path) -> None:
    path = tmp_path / "demo.csv"
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["timestamp", "port", "response"])
        writer.writeheader()
        writer.writerow({"timestamp": "1", "port": "COM1", "response": "alpha,one"})
        writer.writerow({"timestamp": "2", "port": "COM2", "response": "beta,two"})
        writer.writerow({"timestamp": "3", "port": "COM3", "response": "gamma,three"})

    rows = app_module.App._tail_csv_rows(path, count=2)

    assert [row["timestamp"] for row in rows] == ["2", "3"]
    assert rows[-1]["response"] == "gamma,three"


def test_compute_progress_status_prefers_h2o_open_route_over_stale_pressure_target(tmp_path) -> None:
    run_dir = tmp_path / "run_demo"
    run_dir.mkdir()
    (run_dir / "io_20260309_191507.csv").write_text(
        "timestamp,port,device,direction,command,response,error\n"
        "2026-03-09T19:55:00,COM31,pace5000,TX,\":SOUR:PRES:LEV:IMM:AMPL 600\\n\",,\n"
        "2026-03-09T20:22:42,COM29,relay_controller,TX,\"write_coil(0,True,addr=1)\",,\n"
        "2026-03-09T20:22:42,COM29,relay_controller,TX,\"write_coil(1,True,addr=1)\",,\n"
        "2026-03-09T20:22:42,COM29,relay_controller,TX,\"write_coil(7,True,addr=1)\",,\n"
        "2026-03-09T20:22:43,COM31,pace5000,TX,\":SOUR:PRES:LEV:IMM:AMPL:VENT 1\\n\",,\n"
        "2026-03-09T20:22:44,COM24,humidity_generator,RX,,\"Uw= 69.3,Ui= 0.0,Tc= 20.160,Ts= 20.007,Td= 14.37,Tf= -1001.00,Pc= 1027.18,Ps= 1468.03,Flux= 1.8\",\n",
        encoding="utf-8",
    )

    progress = app_module.App._compute_progress_status(run_dir)
    assert progress["route_group"] == "水路"
    assert progress["current"].startswith("H2O 开路等待")


def test_infer_route_state_uses_runtime_cfg_mapping() -> None:
    runtime_cfg = {
        "devices": {"relay": {"port": "COM40"}, "relay_8": {"port": "COM41"}},
        "valves": {
            "co2_path": 7,
            "co2_path_group2": 16,
            "gas_main": 11,
            "h2o_path": 8,
            "flow_switch": 10,
            "hold": 9,
            "relay_map": {
                "7": {"device": "relay", "channel": 15},
                "8": {"device": "relay_8", "channel": 8},
                "9": {"device": "relay_8", "channel": 1},
                "10": {"device": "relay_8", "channel": 2},
                "11": {"device": "relay_8", "channel": 3},
                "16": {"device": "relay", "channel": 16},
            },
        },
    }
    rows = [
        {"port": "COM41", "direction": "TX", "command": "write_coil(0,False,addr=1)"},
        {"port": "COM41", "direction": "TX", "command": "write_coil(1,False,addr=1)"},
        {"port": "COM41", "direction": "TX", "command": "write_coil(2,True,addr=1)"},
        {"port": "COM41", "direction": "TX", "command": "write_coil(7,True,addr=1)"},
        {"port": "COM40", "direction": "TX", "command": "write_coil(14,True,addr=1)"},
        {"port": "COM31", "direction": "TX", "command": ":SOUR:PRES:LEV:IMM:AMPL:VENT 1"},
    ]

    assert app_module.App._infer_route_state(rows, runtime_cfg) == "开路"


def test_compute_progress_status_from_io_uses_runtime_snapshot_relay_mapping(tmp_path) -> None:
    run_dir = tmp_path / "run_demo"
    run_dir.mkdir()
    app_module.App._write_runtime_config_snapshot(
        run_dir,
        {
            "devices": {"relay": {"port": "COM40"}, "relay_8": {"port": "COM41"}},
            "valves": {
                "co2_path": 7,
                "co2_path_group2": 16,
                "gas_main": 11,
                "h2o_path": 8,
                "flow_switch": 10,
                "hold": 9,
                "co2_map": {"0": 1},
                "co2_map_group2": {"300": 23},
                "relay_map": {
                    "7": {"device": "relay", "channel": 15},
                    "8": {"device": "relay_8", "channel": 8},
                    "9": {"device": "relay_8", "channel": 1},
                    "10": {"device": "relay_8", "channel": 2},
                    "11": {"device": "relay_8", "channel": 3},
                    "16": {"device": "relay", "channel": 16},
                    "23": {"device": "relay", "channel": 4},
                },
                "paths": {"output_dir": str(run_dir)},
            },
            "workflow": {"sampling": {"count": 10}},
        },
    )
    (run_dir / "io_20260309_191507.csv").write_text(
        "timestamp,port,device,direction,command,response,error\n"
        "2026-03-09T20:22:42,COM41,relay_controller,TX,\"write_coil(0,False,addr=1)\",,\n"
        "2026-03-09T20:22:42,COM41,relay_controller,TX,\"write_coil(1,False,addr=1)\",,\n"
        "2026-03-09T20:22:42,COM41,relay_controller,TX,\"write_coil(2,True,addr=1)\",,\n"
        "2026-03-09T20:22:42,COM41,relay_controller,TX,\"write_coil(7,True,addr=1)\",,\n"
        "2026-03-09T20:22:42,COM40,relay_controller,TX,\"write_coil(15,True,addr=1)\",,\n"
        "2026-03-09T20:22:42,COM40,relay_controller,TX,\"write_coil(3,True,addr=1)\",,\n"
        "2026-03-09T20:22:43,COM31,pace5000,TX,\":SOUR:PRES:LEV:IMM:AMPL:VENT 1\\n\",,\n",
        encoding="utf-8",
    )

    progress = app_module.App._compute_progress_status(run_dir)
    assert progress["route_group"] == "第二组气路"
    assert progress["current"] == "CO2 300ppm 通气等待"


def test_compute_progress_status_from_io_prefers_runner_stage_and_sample_events(monkeypatch, tmp_path) -> None:
    run_dir = tmp_path / "run_demo"
    run_dir.mkdir()
    (run_dir / "point_0001_co2_demo_samples.csv").write_text("x", encoding="utf-8")
    app_module.App._write_runtime_config_snapshot(
        run_dir,
        {
            "workflow": {"sampling": {"count": 10}},
        },
    )
    monkeypatch.setattr(app_module.App, "_planned_run_points", staticmethod(lambda _cfg: [object()] * 6))
    (run_dir / "io_20260314_101010.csv").write_text(
        "timestamp,port,device,direction,command,response,error\n"
        "2026-03-14T10:10:00,RUN,runner,EVENT,stage,\"{\"\"current\"\":\"\"CO2 400ppm 1100hPa\"\",\"\"route_group\"\":\"\"第一组气路\"\",\"\"wait_reason\"\":\"\"控压中\"\"}\",\n"
        "2026-03-14T10:10:01,RUN,runner,EVENT,sample-progress,\"{\"\"current\"\":0,\"\"total\"\":10,\"\"text\"\":\"\"采样进度：0/10\"\"}\",\n"
        "2026-03-14T10:10:02,COM40,gas_analyzer_05,RX,,,NO_ACK\n",
        encoding="utf-8",
    )

    progress = app_module.App._compute_progress_status(run_dir)

    assert progress["current"] == "CO2 400ppm 1100hPa（控压中）"
    assert progress["route_group"] == "第一组气路"
    assert progress["sample_progress"] == "采样进度：0/10"
    assert progress["total"] == 6
    assert round(progress["percent"], 1) == round((1 / 6) * 100.0, 1)
    assert progress["last_issue"] == "--"


def test_compute_progress_status_from_io_resets_sample_progress_after_stage_change(monkeypatch, tmp_path) -> None:
    run_dir = tmp_path / "run_demo"
    run_dir.mkdir()
    app_module.App._write_runtime_config_snapshot(run_dir, {"workflow": {"sampling": {"count": 10}}})
    monkeypatch.setattr(app_module.App, "_planned_run_points", staticmethod(lambda _cfg: [object()] * 2))
    (run_dir / "io_20260314_111111.csv").write_text(
        "timestamp,port,device,direction,command,response,error\n"
        "2026-03-14T11:11:00,RUN,runner,EVENT,sample-progress,\"{\"\"current\"\":10,\"\"total\"\":10,\"\"text\"\":\"\"采样进度：10/10\"\"}\",\n"
        "2026-03-14T11:11:05,RUN,runner,EVENT,stage,\"{\"\"current\"\":\"\"CO2 400ppm 500hPa\"\",\"\"route_group\"\":\"\"第一组气路\"\",\"\"wait_reason\"\":\"\"控压中\"\"}\",\n"
        "2026-03-14T11:11:06,COM35,gas_analyzer,RX,,\"YGAS,097,401.0,1.0,1,1,1,1,1,1,1,1,20.0,20.0,101.3,OK\",\n",
        encoding="utf-8",
    )

    progress = app_module.App._compute_progress_status(run_dir)

    assert progress["current"] == "CO2 400ppm 500hPa（控压中）"
    assert progress["sample_progress"] == "采样进度：等待开始"


def test_compute_progress_status_from_io_does_not_infer_progress_during_non_sampling_stage(monkeypatch, tmp_path) -> None:
    run_dir = tmp_path / "run_demo"
    run_dir.mkdir()
    app_module.App._write_runtime_config_snapshot(run_dir, {"workflow": {"sampling": {"count": 10}}})
    monkeypatch.setattr(app_module.App, "_planned_run_points", staticmethod(lambda _cfg: [object()] * 2))
    (run_dir / "io_20260314_121212.csv").write_text(
        "timestamp,port,device,direction,command,response,error\n"
        "2026-03-14T12:12:00,RUN,runner,EVENT,stage,\"{\"\"current\"\":\"\"CO2 0ppm 腔温判稳 ga01=-15.36°C\"\",\"\"route_group\"\":\"\"第一组气路\"\",\"\"wait_reason\"\":\"\"分析仪腔温判稳\"\",\"\"countdown_s\"\":42}\",\n"
        "2026-03-14T12:12:01,COM35,gas_analyzer,RX,,\"YGAS,097,0.5,0.8,1,1,1,1,1,1,1,1,-15.30,-15.40,101.3,OK\",\n"
        "2026-03-14T12:12:02,COM35,gas_analyzer,RX,,\"YGAS,097,0.5,0.8,1,1,1,1,1,1,1,1,-15.31,-15.41,101.3,OK\",\n",
        encoding="utf-8",
    )

    progress = app_module.App._compute_progress_status(run_dir)

    assert "腔温判稳" in progress["current"]
    assert progress["sample_progress"] == "采样进度：等待开始"


def test_extract_key_events_from_io_includes_runner_and_logger_events() -> None:
    rows = [
        {
            "timestamp": "2026-03-14T10:08:06",
            "port": "RUN",
            "device": "runner",
            "direction": "EVENT",
            "command": "stage",
            "response": "{\"current\":\"H2O 开路等待 Tc=0.6°C Uw=48.1%\",\"wait_reason\":\"露点仪对齐\",\"route_group\":\"水路\"}",
            "error": "",
        },
        {
            "timestamp": "2026-03-14T10:08:07",
            "port": "LOG",
            "device": "run_logger",
            "direction": "WARN",
            "command": "readable-point-workbook",
            "response": "csv-only fallback",
            "error": "Readable points workbook header mismatch",
        },
    ]

    events = app_module.App._extract_key_events_from_io(rows)

    assert any("H2O 开路等待" in item and "露点仪对齐" in item for item in events)
    assert any("报表告警" in item and "readable-point-workbook" in item for item in events)


def test_last_issue_from_progress_rows_ignores_startup_no_ack() -> None:
    rows = [
        {
            "timestamp": "2026-03-14T10:50:19",
            "port": "COM36",
            "device": "gas_analyzer",
            "direction": "WARN",
            "command": "SETCOMWAY,YGAS,FFF,0",
            "response": "STARTUP_NO_ACK_RETRY 1/4",
            "error": "",
        },
        {
            "timestamp": "2026-03-14T10:50:21",
            "port": "COM36",
            "device": "gas_analyzer",
            "direction": "WARN",
            "command": "SETCOMWAY,YGAS,FFF,0",
            "response": "STARTUP_NO_ACK",
            "error": "",
        },
    ]

    assert app_module.App._last_issue_from_progress_rows(rows) == "--"


def test_summarize_analyzer_health_issue_ignores_soft_marked_extreme_frames() -> None:
    runtime_cfg = {
        "devices": {
            "gas_analyzers": [
                {"name": "ga01", "port": "COM16", "enabled": True},
            ]
        }
    }
    rows = [
        {
            "timestamp": f"2026-03-14T10:50:2{idx}",
            "port": "COM16",
            "direction": "RX",
            "response": "YGAS,001,3000,72,2.1,3.2,0.456,0.457,0.111,0.112,9.9,8.8,7.7,20.12,21.34,101.56,T",
            "error": "",
        }
        for idx in range(3)
    ]

    assert app_module.App._summarize_analyzer_health_issue(rows, runtime_cfg=runtime_cfg) == "--"


def test_summarize_analyzer_health_issue_ignores_soft_marked_extreme_legacy_frames() -> None:
    runtime_cfg = {
        "devices": {
            "gas_analyzers": [
                {"name": "ga01", "port": "COM16", "enabled": True},
            ]
        }
    }
    rows = [
        {
            "timestamp": f"2026-03-14T10:51:2{idx}",
            "port": "COM16",
            "direction": "RX",
            "response": "YGAS,027,3000.000,72.000,0.71,0.71,027.74,101.31,0003,2726",
            "error": "",
        }
        for idx in range(3)
    ]

    assert app_module.App._summarize_analyzer_health_issue(rows, runtime_cfg=runtime_cfg) == "--"


def test_infer_sample_progress_counts_com16_samples_after_in_limits() -> None:
    rows = [
        {"ts": "2026-03-09T11:00:00", "port": "COM31", "direction": "RX", "response": ":SENS:PRES:INL 1000.0, 1"},
        {"ts": "2026-03-09T11:00:01", "port": "COM16", "direction": "RX", "response": "YGAS,097,1,2,3"},
        {"ts": "2026-03-09T11:00:02", "port": "COM16", "direction": "RX", "response": "YGAS,097,1,2,3"},
        {"ts": "2026-03-09T11:00:03", "port": "COM17", "direction": "RX", "response": "YGAS,102,1,2,3"},
    ]
    assert app_module.App._infer_sample_progress(rows).endswith("2/10")


def test_infer_sample_progress_uses_most_active_analyzer_port() -> None:
    rows = [
        {"ts": "2026-03-09T11:00:00", "port": "COM31", "direction": "RX", "response": ":SENS:PRES:INL 1000.0, 1"},
        {"ts": "2026-03-09T11:00:01", "port": "COM17", "direction": "RX", "response": "YGAS,102,1,2,3"},
        {"ts": "2026-03-09T11:00:02", "port": "COM17", "direction": "RX", "response": "YGAS,102,1,2,3"},
        {"ts": "2026-03-09T11:00:03", "port": "COM18", "direction": "RX", "response": "YGAS,103,1,2,3"},
    ]
    assert app_module.App._infer_sample_progress(rows, expected_count=5).endswith("2/5")


def test_infer_sample_progress_ignores_repeated_in_limits_updates_during_sampling() -> None:
    rows = [
        {"ts": "2026-03-09T11:00:00", "port": "COM31", "direction": "TX", "command": ":OUTP 1", "response": ""},
        {"ts": "2026-03-09T11:00:01", "port": "COM17", "direction": "RX", "response": "YGAS,102,1,2,3"},
        {"ts": "2026-03-09T11:00:02", "port": "COM31", "direction": "RX", "response": ":SENS:PRES:INL 1000.0, 1"},
        {"ts": "2026-03-09T11:00:03", "port": "COM17", "direction": "RX", "response": "YGAS,102,1,2,3"},
        {"ts": "2026-03-09T11:00:04", "port": "COM31", "direction": "RX", "response": ":SENS:PRES:INL 1000.0, 1"},
        {"ts": "2026-03-09T11:00:05", "port": "COM17", "direction": "RX", "response": "YGAS,102,1,2,3"},
    ]
    assert app_module.App._infer_sample_progress(rows).endswith("3/10")


def test_compute_progress_status_uses_runtime_sampling_count_for_progress(tmp_path) -> None:
    run_dir = tmp_path / "run_demo"
    run_dir.mkdir()
    app_module.App._write_runtime_config_snapshot(run_dir, {"workflow": {"sampling": {"count": 6}}})
    (run_dir / "io_20260309_191507.csv").write_text(
        "ts,port,direction,response\n"
        "\"2026-03-09T11:00:00\",COM31,RX,\":SENS:PRES:INL 1000.0, 1\"\n"
        "\"2026-03-09T11:00:01\",COM17,RX,\"YGAS,102,1,2,3\"\n"
        "\"2026-03-09T11:00:02\",COM17,RX,\"YGAS,102,1,2,3\"\n",
        encoding="utf-8",
    )
    progress = app_module.App._compute_progress_status(run_dir)
    assert progress["sample_progress"] == "采样进度：2/6"


def test_compute_data_freshness_marks_stale_rows() -> None:
    stale_ts = (datetime.now() - timedelta(seconds=30)).strftime("%Y-%m-%dT%H:%M:%S")
    rows = [{"ts": stale_ts, "port": "COM31", "direction": "RX", "response": ":SENS:PRES:INL 1000.0, 1"}]
    text_value, level = app_module.App._compute_data_freshness(rows)
    assert "30s" in text_value
    assert level == "error"


def test_refresh_key_events_falls_back_to_io_when_stdout_missing(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    run_dir = tmp_path / "run_demo"
    run_dir.mkdir()
    (run_dir / "io_20260309_191507.csv").write_text(
        "timestamp,port,device,direction,command,response,error\n"
        "2026-03-09T19:15:45,COM29,relay_controller,TX,\"write_coil(0,False,addr=1)\",,\n"
        "2026-03-09T19:15:45,COM29,relay_controller,TX,\"write_coil(1,True,addr=1)\",,\n"
        "2026-03-09T19:15:45,COM29,relay_controller,TX,\"write_coil(7,False,addr=1)\",,\n"
        "2026-03-09T19:15:53,COM24,humidity_generator,RX,,\"Uw= 26.4,Ui= 0.0,Tc= 22.052,Ts= 19.958,Td= 1.90,Tf= -1001.00,Pc= 1027.50,Ps= 3419.53,Flux= 5.3,PST= 00:12:13,TST= 00:04:29\",\n"
        "2026-03-09T19:15:54,COM31,pace5000,TX,\":SOUR:PRES:LEV:IMM:AMPL:VENT 1\\n\",,\n",
        encoding="utf-8",
    )

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        ui.current_run_dir = run_dir
        ui._refresh_key_events()
        text = ui.event_text.get("1.0", "end")
        assert "湿度发生器前置" in text
        assert "压力控制器：通大气保持中" in text
    finally:
        root.destroy()


def test_refresh_key_events_reuses_cached_source_for_filter_change(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    run_dir = tmp_path / "run_demo"
    run_dir.mkdir()
    stdout_path = run_dir / "runner_stdout.log"
    stdout_path.write_text("2026-03-09 Point 1 samples saved: demo.csv\n", encoding="utf-8")

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        ui.current_run_dir = run_dir
        calls = {"text": 0, "csv": 0}
        ui._tail_text_lines = lambda *_args, **_kwargs: calls.__setitem__("text", calls["text"] + 1) or ["Point 1 samples saved: demo.csv"]
        ui._tail_csv_rows = lambda *_args, **_kwargs: calls.__setitem__("csv", calls["csv"] + 1) or []

        ui._refresh_key_events()
        assert calls == {"text": 1, "csv": 0}

        ui.event_filter_var.set("只看保存成功")
        ui._refresh_key_events()
        assert calls == {"text": 1, "csv": 0}
    finally:
        root.destroy()


def test_export_run_summary_writes_text(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    target = tmp_path / "summary.txt"
    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        ui.current_run_dir = tmp_path
        ui.startup_summary_var.set("测量模式：只测气路")
        ui.summary_var.set("执行摘要：只测气路 | 温度：20°C | 气点：0ppm")
        ui.stage_var.set("当前阶段：气路控压/采样")
        ui.target_var.set("当前点位：CO2 0ppm / 1100hPa")
        ui.current_target_ppm_var.set("当前标气：0ppm")
        ui.current_pressure_point_var.set("当前压力点：1100hPa")
        ui.current_route_group_detail_var.set("当前气路组：第一组气路")
        ui.progress_summary_var.set("进度：当前=CO2 0ppm 1100hPa")
        ui.progress_detail_var.set("已完成：1 | 已跳过：0 | 总点数：7")
        ui.route_group_var.set("当前气路组别：第一组气路")
        ui.last_issue_var.set("最近一次异常：--")
        ui.current_workbook_name_var.set("Workbook：demo.xlsx")
        ui.current_latest_point_name_var.set("最新点文件：point_0021.csv")
        ui.device_vars["pace"].set("压力控制器：1100.00 hPa，稳定标志=1")
        ui.device_state_vars["pace"].set("状态：稳定")
        ui.device_update_vars["pace"].set("更新：2026-03-09 11:00:01")
        ui.event_text.configure(state="normal")
        ui.event_text.delete("1.0", "end")
        ui.event_text.insert("end", "Point 21 samples saved: demo.csv")
        ui.event_text.configure(state="disabled")
        ui.history_items_cache = ["Point 21 samples saved: demo.csv"]
        ui._refresh_history_list("CO2 0ppm 1100hPa")
        logs: list[str] = []
        monkeypatch.setattr(app_module.filedialog, "asksaveasfilename", lambda **_kwargs: str(target))
        ui.log = lambda msg: logs.append(msg)
        ui._export_run_summary()
        text = target.read_text(encoding="utf-8")
        assert "本次运行摘要" in text
        assert "测量模式：只测气路" in text
        assert "压力控制器：1100.00 hPa，稳定标志=1" in text
        assert "Point 21 samples saved: demo.csv" in text
        assert any("运行摘要已导出" in item for item in logs)
    finally:
        root.destroy()


def test_refresh_live_device_values_updates_timestamps(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    io_path = tmp_path / "io.csv"
    io_path.write_text(
        "ts,port,direction,response\n"
        "\"2026-03-09 11:00:01\",COM31,RX,\":SENS:PRES:INL 1099.95, 1\"\n"
        "\"2026-03-09 11:00:02\",COM30,RX,\"*0001100.125,\"\n",
        encoding="utf-8",
    )

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        ui.current_io_path = io_path
        ui._refresh_live_device_values()
        assert ui.device_update_vars["pace"].get() == "更新：2026-03-09 11:00:01"
        assert ui.device_update_vars["gauge"].get() == "更新：2026-03-09 11:00:02"
        assert ui.current_pressure_live_var.get() == "当前实压：1099.95hPa"
        assert ui.current_pressure_stability_var.get() == "稳定标志：1"
    finally:
        root.destroy()


def test_device_state_from_text_maps_levels() -> None:
    assert app_module.App._device_state_from_text("pace", "压力控制器：550.00 hPa，稳定标志=1") == ("状态：稳定", "ok")
    assert app_module.App._device_state_from_text("pace", "压力控制器：550.00 hPa，稳定标志=0") == ("状态：未稳定", "warn")
    assert app_module.App._device_state_from_text("hgen", "湿度发生器：Tc=20.00°C，Uw=30.0%，Td=1.20°C，流量=0.0") == ("状态：已停机", "idle")
    assert app_module.App._device_state_from_text("chamber", "温度箱：温度=20.0°C，湿度=50.0%") == ("状态：温度到位", "ok")


def test_safe_stop_without_cfg_shows_error(monkeypatch) -> None:
    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App.__new__(app_module.App)
        ui.root = root
        ui.worker = None
        ui.runner = None
        ui.last_runtime_cfg = None
        called = {"err": False}
        monkeypatch.setattr(app_module.messagebox, "showerror", lambda *_args, **_kwargs: called.__setitem__("err", True))
        app_module.App.safe_stop(ui)
        assert called["err"] is True
    finally:
        root.destroy()


def test_safe_stop_waits_for_worker_exit_before_reopening_devices(monkeypatch, tmp_path: Path) -> None:
    class _InlineThread:
        def __init__(self, target=None, args=(), kwargs=None, daemon=None):
            self._target = target
            self._args = args
            self._kwargs = kwargs or {}

        def start(self) -> None:
            if self._target:
                self._target(*self._args, **self._kwargs)

    class _StoppingWorker:
        def __init__(self, events: list[str]) -> None:
            self._alive = True
            self._events = events

        def is_alive(self) -> bool:
            return self._alive

        def join(self, timeout=None) -> None:
            self._events.append("worker-join")
            self._alive = False

    class _FakeLogger:
        def __init__(self, out_dir, run_id=None, cfg=None):
            base = Path(out_dir)
            base.mkdir(parents=True, exist_ok=True)
            self.run_dir = str(base / (run_id or "run"))
            self.io_path = str(base / "io.csv")

        def close(self) -> None:
            return None

    events: list[str] = []
    ui = app_module.App.__new__(app_module.App)
    ui.worker = _StoppingWorker(events)
    ui.runner = types.SimpleNamespace(stop=lambda: events.append("runner-stop"))
    ui.safe_stop_in_progress = False
    ui.last_runtime_cfg = _basic_cfg(str(tmp_path))
    ui.cfg = ui.last_runtime_cfg
    ui.current_io_path = None
    ui.current_run_dir = None
    ui.log = lambda msg: events.append(str(msg))
    ui.set_status = lambda status: events.append(f"status:{status}")
    ui._log_app_event = lambda *_args, **_kwargs: None
    ui._start_safe_stop_countdown = lambda total_s: events.append(f"countdown:{total_s}")
    ui._set_safe_stop_ui_state = lambda *_args, **_kwargs: None
    ui._close_devices = lambda devices: events.append("devices-close")

    monkeypatch.setattr(app_module.messagebox, "askokcancel", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(app_module.threading, "Thread", _InlineThread)
    monkeypatch.setattr(app_module, "RunLogger", _FakeLogger)
    monkeypatch.setattr(
        ui,
        "_build_devices_for_maintenance",
        lambda cfg, io_logger=None: events.append("build-devices") or {"pace": object()},
    )
    monkeypatch.setattr(
        app_module,
        "perform_safe_stop_with_retries",
        lambda devices, log_fn=None, cfg=None, attempts=3, retry_delay_s=1.5: events.append("perform-safe-stop") or {"ok": True, "safe_stop_verified": True},
    )

    app_module.App.safe_stop(ui)

    assert events.index("runner-stop") < events.index("worker-join")
    assert events.index("worker-join") < events.index("build-devices")
    assert events.index("build-devices") < events.index("perform-safe-stop")


def test_safe_stop_retries_device_reopen_when_verification_fails(monkeypatch, tmp_path: Path) -> None:
    class _InlineThread:
        def __init__(self, target=None, args=(), kwargs=None, daemon=None):
            self._target = target
            self._args = args
            self._kwargs = kwargs or {}

        def start(self) -> None:
            if self._target:
                self._target(*self._args, **self._kwargs)

    class _FakeLogger:
        def __init__(self, out_dir, run_id=None, cfg=None):
            base = Path(out_dir)
            base.mkdir(parents=True, exist_ok=True)
            self.run_dir = str(base / (run_id or "run"))
            self.io_path = str(base / "io.csv")

        def close(self) -> None:
            return None

    events: list[str] = []
    ui = app_module.App.__new__(app_module.App)
    ui.worker = None
    ui.runner = None
    ui.safe_stop_in_progress = False
    ui.last_runtime_cfg = _basic_cfg(str(tmp_path))
    ui.last_runtime_cfg.setdefault("workflow", {})["safe_stop"] = {
        "perform_attempts": 2,
        "reopen_attempts": 2,
        "retry_delay_s": 0.0,
        "reopen_retry_delay_s": 0.0,
    }
    ui.cfg = ui.last_runtime_cfg
    ui.current_io_path = None
    ui.current_run_dir = None
    ui.log = lambda msg: events.append(str(msg))
    ui.set_status = lambda status: events.append(f"status:{status}")
    ui._log_app_event = lambda *_args, **_kwargs: None
    ui._start_safe_stop_countdown = lambda total_s: events.append(f"countdown:{total_s}")
    ui._set_safe_stop_ui_state = lambda *_args, **_kwargs: None
    ui._close_devices = lambda devices: events.append(f"devices-close:{len(devices)}")

    monkeypatch.setattr(app_module.threading, "Thread", _InlineThread)
    monkeypatch.setattr(app_module, "RunLogger", _FakeLogger)
    monkeypatch.setattr(
        ui,
        "_build_devices_for_maintenance",
        lambda cfg, io_logger=None: events.append("build-devices") or {"pace": object()},
    )

    results = iter(
        [
            {"safe_stop_verified": False, "safe_stop_issues": ["relay state mismatch"]},
            {"safe_stop_verified": True, "safe_stop_issues": []},
        ]
    )
    monkeypatch.setattr(
        app_module,
        "perform_safe_stop_with_retries",
        lambda devices, log_fn=None, cfg=None, attempts=3, retry_delay_s=1.5: events.append(f"perform-safe-stop:{attempts}") or next(results),
    )

    app_module.App.safe_stop(ui)

    assert events.count("build-devices") == 2
    assert events.count("perform-safe-stop:2") == 2
    assert "status:恢复基线完成" in events


def test_stop_logs_ui_stop_request_to_io(tmp_path) -> None:
    ui = app_module.App.__new__(app_module.App)
    ui.logger = RunLogger(tmp_path)
    calls = {"stop": 0}
    ui.runner = types.SimpleNamespace(stop=lambda: calls.__setitem__("stop", calls["stop"] + 1))
    ui.log = lambda *_args, **_kwargs: None

    app_module.App.stop(ui)
    ui.logger.close()

    with ui.logger.io_path.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))

    assert calls["stop"] == 1
    ui_events = [row for row in rows if row["port"] == "UI" and row["device"] == "app"]
    assert any(row["command"] == "stop-request" and row["response"] == "ui-stop-button" for row in ui_events)


def test_set_safe_stop_ui_state_updates_button_and_text(monkeypatch) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        ui._set_safe_stop_ui_state(True, 12)
        assert str(ui.safe_stop_button.cget("state")) == "disabled"
        assert "12s" in ui.safe_stop_countdown_var.get()
        ui._set_safe_stop_ui_state(False)
        assert str(ui.safe_stop_button.cget("state")) == "normal"
        assert ui.safe_stop_countdown_var.get() == "恢复基线：待命"
    finally:
        root.destroy()


def test_apply_control_lock_disables_startup_controls_when_worker_alive(monkeypatch) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    class AliveThread:
        def is_alive(self):
            return True

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        ui.worker = AliveThread()
        ui._apply_control_lock()
        assert str(ui.start_button.cget("state")) == "disabled"
        assert str(ui.self_test_button.cget("state")) == "disabled"
        assert str(ui.route_mode_combo.cget("state")) == "disabled"
        assert str(ui.temp_scope_combo.cget("state")) == "disabled"
        assert str(ui.temperature_order_combo.cget("state")) == "disabled"
        assert str(ui.postrun_delivery_check.cget("state")) == "disabled"
        assert str(ui.config_entry.cget("state")) == "disabled"
        assert str(ui.stop_button.cget("state")) == "normal"
    finally:
        root.destroy()


def test_copy_buttons_put_text_on_clipboard(monkeypatch) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        logs: list[str] = []
        ui.log = lambda msg: logs.append(msg)
        ui.target_var.set("当前点位：CO2 400ppm / 1000hPa")
        ui.current_target_ppm_var.set("当前标气：400ppm")
        ui.current_pressure_point_var.set("当前压力点：1000hPa")
        ui.current_route_group_detail_var.set("当前气路组：第一组气路")
        ui.last_issue_var.set("最近一次异常：CO2 400.0 ppm @ 550.0 hPa skipped: pressure did not stabilize")

        ui._copy_current_point()
        assert "CO2 400ppm / 1000hPa" in root.clipboard_get()
        ui._copy_last_issue()
        assert "550.0 hPa skipped" in root.clipboard_get()
        assert any("当前点位已复制" in item for item in logs)
        assert any("最近异常已复制" in item for item in logs)
    finally:
        root.destroy()


def test_find_latest_active_run_dir_picks_recent_io(tmp_path) -> None:
    old_run = tmp_path / "run_old"
    old_run.mkdir()
    old_io = old_run / "io_old.csv"
    old_io.write_text("timestamp,port,direction,response\n", encoding="utf-8")

    new_run = tmp_path / "run_new"
    new_run.mkdir()
    new_io = new_run / "io_new.csv"
    new_io.write_text("timestamp,port,direction,response\n", encoding="utf-8")

    stale_time = (datetime.now() - timedelta(seconds=500)).timestamp()
    fresh_time = (datetime.now() - timedelta(seconds=5)).timestamp()
    old_io.touch()
    new_io.touch()
    import os

    os.utime(old_io, (stale_time, stale_time))
    os.utime(new_io, (fresh_time, fresh_time))

    run_dir, io_path = app_module.App._find_latest_active_run_dir(tmp_path, freshness_s=180)
    assert run_dir == new_run
    assert io_path == new_io


def test_attach_latest_active_run_sets_current_paths(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg(str(tmp_path)))
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    run_dir = tmp_path / "run_live"
    run_dir.mkdir()
    io_path = run_dir / "io_20260309_000001.csv"
    io_path.write_text("timestamp,port,direction,response\n", encoding="utf-8")

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        ui.current_run_dir = None
        ui.current_io_path = None
        logs: list[str] = []
        ui.log = lambda msg: logs.append(msg)
        ui._attach_latest_active_run()
        assert ui.current_run_dir == run_dir
        assert ui.current_io_path == io_path
        assert ui.current_run_dir_name_var.get() == f"Run目录：{run_dir.name}"
        assert ui.current_io_name_var.get() == f"IO文件：{io_path.name}"
        assert ui.status_var.get() == "已附加到运行中流程"
        assert any("已附加到运行目录" in item for item in logs)
    finally:
        root.destroy()


def test_poll_log_reattaches_latest_active_run_before_refresh(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg(str(tmp_path)))
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    run_dir = tmp_path / "run_live"
    run_dir.mkdir()
    io_path = run_dir / "io_20260309_000001.csv"
    io_path.write_text("timestamp,port,direction,response\n", encoding="utf-8")

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        ui.current_run_dir = None
        ui.current_io_path = None
        calls: list[str] = []
        ui._refresh_progress_status = lambda: calls.append(f"progress:{ui.current_run_dir.name if ui.current_run_dir else '--'}")
        ui._refresh_live_device_values = lambda: calls.append("devices")
        ui._refresh_key_events = lambda: calls.append("events")
        ui._apply_control_lock = lambda: calls.append("lock")
        ui.root.after = lambda *_args, **_kwargs: None
        ui._poll_log()
        assert ui.current_run_dir == run_dir
        assert ui.current_io_path == io_path
        assert calls[0] == f"progress:{run_dir.name}"
    finally:
        root.destroy()


def test_refresh_progress_status_without_run_sets_run_and_io_empty(monkeypatch) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        ui.current_run_dir = None
        ui.current_io_path = None
        ui._refresh_progress_status()
        assert ui.current_run_dir_name_var.get() == "Run目录：当前无目录"
        assert ui.current_io_name_var.get() == "IO文件：当前无文件"
    finally:
        root.destroy()


def test_parse_pressure_reapply_info_counts_latest_block() -> None:
    rows = [
        {"port": "COM31", "direction": "TX", "command": ":SOUR:PRES:LEV:IMM:AMPL 1100.0"},
        {"port": "COM31", "direction": "TX", "command": ":SOUR:PRES:LEV:IMM:AMPL 1000.0"},
        {"port": "COM31", "direction": "TX", "command": ":SOUR:PRES:LEV:IMM:AMPL 1000.0"},
        {"port": "COM31", "direction": "TX", "command": ":SOUR:PRES:LEV:IMM:AMPL 1000.0"},
    ]
    target, reapply_count = app_module.App._parse_pressure_reapply_info(rows)
    assert target == 1000
    assert reapply_count == 2


def test_find_latest_active_run_dir_supports_rerun_prefix(monkeypatch, tmp_path) -> None:
    old_run = tmp_path / "run_old"
    old_run.mkdir()
    old_io = old_run / "io_old.csv"
    old_io.write_text("timestamp,port,direction,response\n", encoding="utf-8")

    rerun_dir = tmp_path / "rerun_new"
    rerun_dir.mkdir()
    rerun_io = rerun_dir / "io_new.csv"
    rerun_io.write_text("timestamp,port,direction,response\n", encoding="utf-8")

    stale_time = (datetime.now() - timedelta(seconds=120)).timestamp()
    fresh_time = (datetime.now() - timedelta(seconds=3)).timestamp()
    import os

    os.utime(old_io, (stale_time, stale_time))
    os.utime(rerun_io, (fresh_time, fresh_time))

    run_dir, io_path = app_module.App._find_latest_active_run_dir(tmp_path, freshness_s=180)
    assert run_dir == rerun_dir
    assert io_path == rerun_io


def test_latest_io_path_prefers_newest_mtime(tmp_path) -> None:
    run_dir = tmp_path / "run_demo"
    run_dir.mkdir()
    older = run_dir / "io_20260309_999999.csv"
    newer = run_dir / "io_20260309_000001.csv"
    older.write_text("timestamp,port,direction,response\n", encoding="utf-8")
    newer.write_text("timestamp,port,direction,response\n", encoding="utf-8")

    import os

    older_time = (datetime.now() - timedelta(seconds=60)).timestamp()
    newer_time = (datetime.now() - timedelta(seconds=5)).timestamp()
    os.utime(older, (older_time, older_time))
    os.utime(newer, (newer_time, newer_time))

    assert app_module.App._latest_io_path(run_dir) == newer


def test_compute_progress_status_uses_latest_stdout_log(tmp_path) -> None:
    run_dir = tmp_path / "run_demo"
    run_dir.mkdir()
    older_stdout = run_dir / "old_stdout.log"
    newer_stdout = run_dir / "new_stdout.log"
    older_stdout.write_text("CO2 0ppm 1100hPa\n", encoding="utf-8")
    newer_stdout.write_text("CO2 400ppm 900hPa\n", encoding="utf-8")

    import os

    older_time = (datetime.now() - timedelta(seconds=60)).timestamp()
    newer_time = (datetime.now() - timedelta(seconds=5)).timestamp()
    os.utime(older_stdout, (older_time, older_time))
    os.utime(newer_stdout, (newer_time, newer_time))

    progress = app_module.App._compute_progress_status(run_dir)
    assert progress["current"] == "CO2 400ppm 900hPa"


def test_compute_online_state_marks_recent_timestamp_online() -> None:
    recent = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    text, level = app_module.App._compute_online_state(recent, "压力控制器：1000.00 hPa，稳定标志=1")
    assert text.endswith("在线")
    assert level in {"ok", "warn"}


def test_compute_online_state_marks_stale_payload_as_not_polled_instead_of_offline() -> None:
    stale = (datetime.now() - timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%S")
    text, level = app_module.App._compute_online_state(stale, "数字气压计：1000.123 hPa")

    assert text == "◑ 本阶段未轮询"
    assert level == "idle"


def test_compute_online_state_uses_recent_activity_when_payload_missing() -> None:
    recent = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    text, level = app_module.App._compute_online_state("--", "湿度发生器：--", recent)

    assert text == "◔ 在线"
    assert level == "warn"


def test_copy_selected_event_uses_text_selection(monkeypatch) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        ui.event_text.configure(state="normal")
        ui.event_text.delete("1.0", "end")
        ui.event_text.insert("1.0", "Point 21 samples saved")
        ui.event_text.tag_add("sel", "1.0", "1.end")
        ui.event_text.configure(state="disabled")
        logs: list[str] = []
        ui.log = lambda msg: logs.append(msg)
        ui._copy_selected_event()
        assert "Point 21 samples saved" in root.clipboard_get()
        assert any("关键事件已复制" in item for item in logs)
    finally:
        root.destroy()


def test_export_event_list_writes_current_events(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        ui.current_events_cache = ["第一条事件", "第二条事件"]
        target = tmp_path / "events.txt"
        monkeypatch.setattr(app_module.filedialog, "asksaveasfilename", lambda **_kwargs: str(target))
        logs: list[str] = []
        ui.log = lambda msg: logs.append(msg)
        ui._export_event_list()
        assert target.read_text(encoding="utf-8") == "第一条事件\n第二条事件"
        assert any("关键事件已导出" in item for item in logs)
    finally:
        root.destroy()


def test_double_click_event_copies_clicked_line(monkeypatch) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        ui.event_text.configure(state="normal")
        ui.event_text.delete("1.0", "end")
        ui.event_text.insert("1.0", "● 第一条事件\n│\n● 第二条事件")
        ui.event_text.configure(state="disabled")
        logs: list[str] = []
        ui.log = lambda msg: logs.append(msg)
        event = types.SimpleNamespace(x=5, y=2)
        ui._copy_event_from_double_click(event)
        assert "第一条事件" in root.clipboard_get()
        assert any("关键事件已复制" in item for item in logs)
    finally:
        root.destroy()


def test_build_device_issue_summaries_picks_recent_error() -> None:
    now = datetime.now()
    rows = [
        {
            "ts": (now - timedelta(seconds=5)).strftime("%Y-%m-%dT%H:%M:%S"),
            "port": "COM31",
            "direction": "RX",
            "response": "",
            "error": "NO_RESPONSE",
        }
    ]
    issues = app_module.App._build_device_issue_summaries(rows, seconds=60)
    assert "NO_RESPONSE" in issues["pace"]["text"]
    assert issues["pace"]["level"] == "error"
    assert issues["pace"]["timestamp"] != "--"


def test_event_to_point_path_resolves_saved_event(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        run_dir = tmp_path / "run_demo"
        run_dir.mkdir()
        sample = run_dir / "point_0021_co2_co2_groupa_0ppm_1100hpa_samples.csv"
        sample.write_text("x", encoding="utf-8")
        ui.current_run_dir = run_dir
        path = ui._event_to_point_path("Point 21 samples saved: point_0021_co2_co2_groupa_0ppm_1100hpa_samples.csv")
        assert path == sample
    finally:
        root.destroy()


def test_save_modeling_input_selection_persists_and_recovers(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _basic_cfg())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: _points(20.0))

    project_root = tmp_path / "proj"
    configs_dir = project_root / "configs"
    configs_dir.mkdir(parents=True)
    default_config = {
        "paths": {"points_excel": "demo.xlsx", "output_dir": "logs"},
        "modeling": {"enabled": False},
    }
    default_path = configs_dir / "default_config.json"
    default_path.write_text(json.dumps(default_config, ensure_ascii=False), encoding="utf-8")
    source_path = project_root / "summary.csv"
    source_path.parent.mkdir(parents=True, exist_ok=True)
    source_path.write_text("ppm_CO2_Tank,R_CO2,T1,BAR\n1,2,20,100\n", encoding="utf-8")

    root = tk.Tk()
    root.withdraw()
    try:
        ui = app_module.App(root)
        ui.config_path.set(str(default_path))
        errors: list[tuple] = []
        monkeypatch.setattr(app_module.messagebox, "showerror", lambda *args, **_kwargs: errors.append(args))
        logs: list[str] = []
        ui.log = lambda msg: logs.append(msg)

        ui.modeling_input_path_var.set(str(source_path))
        ui.modeling_input_file_type_var.set("auto")
        ui.modeling_input_sheet_var.set("Data")
        ui._save_modeling_input_selection()

        modeling_path = configs_dir / "modeling_offline.json"
        payload = json.loads(modeling_path.read_text(encoding="utf-8"))
        data_source = payload["modeling"]["data_source"]
        assert data_source["file_type"] == "csv"
        assert data_source["sheet_name"] == 0
        assert not errors
        assert "已保存" in ui.modeling_save_status_var.get()
        assert any("离线建模输入文件已保存" in item for item in logs)

        ui.modeling_input_path_var.set("")
        ui.modeling_input_file_type_var.set("auto")
        ui.modeling_input_sheet_var.set("0")
        ui._refresh_modeling_panel()

        assert ui.modeling_input_path_var.get() == str(source_path.resolve())
        assert ui.modeling_input_file_type_var.get() == "csv"
        assert ui.modeling_input_sheet_var.get() == "0"
    finally:
        root.destroy()
