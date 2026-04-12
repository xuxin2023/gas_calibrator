from __future__ import annotations

import tkinter as tk
import types

import pytest

from gas_calibrator.ui import app as app_module
from gas_calibrator.ui import dewpoint_page as dewpoint_module
from gas_calibrator.ui import humidity_page as humidity_module
from gas_calibrator.ui import thermometer_page as thermometer_module
from gas_calibrator.ui import valve_page as valve_module


def _root() -> tk.Tk:
    root = tk.Tk()
    root.withdraw()
    return root


def _app_cfg() -> dict:
    return {
        "paths": {"points_excel": "demo.xlsx", "output_dir": "out"},
        "devices": {
            "humidity_generator": {"port": "COM8", "baud": 9600},
            "dewpoint_meter": {"port": "COM13", "baud": 9600, "station": "001"},
            "thermometer": {"port": "COM9", "baud": 2400},
            "relay": {"port": "COM28", "baud": 38400, "addr": 1},
            "relay_8": {"port": "COM29", "baud": 38400, "addr": 1},
        },
        "valves": {
            "co2_map": {"0": 1, "200": 2, "400": 3},
            "co2_map_group2": {"100": 4, "300": 5},
            "co2_path": 11,
            "co2_path_group2": 12,
            "gas_main": 16,
            "h2o_path": 13,
            "hold": 14,
            "flow_switch": 15,
        },
        "workflow": {"skip_co2_ppm": []},
    }


class _FakeHumidity:
    def __init__(self, port: str, baud: int):
        self.port = port
        self.baud = baud
        self.fetch_calls = 0
        self.commands = []

    def open(self) -> None:
        return None

    def close(self) -> None:
        return None

    def set_target_temp(self, _value: float) -> None:
        self.commands.append(("set_target_temp", float(_value)))
        return None

    def set_target_rh(self, _value: float) -> None:
        self.commands.append(("set_target_rh", float(_value)))
        return None

    def set_flow_target(self, _value: float) -> None:
        return None

    def set_target_dewpoint(self, value: float) -> dict:
        dewpoint = float(value)
        self.commands.append(("set_target_dewpoint", dewpoint))
        return {
            "target_dewpoint_c": dewpoint,
            "target_temp_c": 20.0,
            "target_rh_pct": 30.19,
        }

    def enable_control(self, _value: bool) -> None:
        return None

    def cool_on(self) -> None:
        return None

    def cool_off(self) -> None:
        return None

    def heat_on(self) -> None:
        return None

    def heat_off(self) -> None:
        return None

    def ensure_run(self) -> str:
        return "ok"

    def safe_stop(self) -> None:
        return None

    def fetch_all(self) -> dict:
        self.fetch_calls += 1
        return {"raw": "ok", "data": {"Tc": 20.0, "Uw": 50.0, "Td": 1.0, "Fl": 1.2}}

    def fetch_tag_value(self, _tag: str) -> dict:
        mapping = {
            "Ta": 20.0,
            "TA": 20.0,
            "UwA": 50.0,
            "UiA": 40.0,
            "Fa": 1.2,
            "Br": 9600.0,
            "Ver": 1.0,
        }
        return {"raw_pick": "tag", "value": mapping.get(_tag, 20.0)}


class _FakeDewpoint:
    def __init__(self, port: str, baudrate: int, station: str, timeout: float):
        self.port = port
        self.baudrate = baudrate
        self.station = station
        self.timeout = timeout

    def open(self) -> None:
        return None

    def close(self) -> None:
        return None

    def get_current(self, timeout_s: float, attempts: int) -> dict:
        return {
            "ok": True,
            "cmd": "READ",
            "raw": "RAW",
            "lines": ["RAW"],
            "payload": [1.1, 20.0, 50.0],
            "dewpoint_c": 1.1,
            "temp_c": 20.0,
            "rh_pct": 50.0,
            "flags": [0, 1],
        }


class _FakeThermometer:
    def __init__(self, *_args, **_kwargs):
        return None

    def open(self) -> None:
        return None

    def close(self) -> None:
        return None

    def flush_input(self) -> None:
        return None

    def read_current(self) -> dict:
        return {"ok": True, "raw": "*000120.0,", "temp_c": 20.0}


class _FakeRelay:
    def __init__(self, port: str, baudrate: int, addr: int):
        self.port = port
        self.baudrate = baudrate
        self.addr = addr
        self.states = {}

    def open(self) -> None:
        return None

    def close(self) -> None:
        return None

    def set_valve(self, _channel: int, _is_open: bool) -> None:
        self.states[int(_channel)] = bool(_is_open)
        return None


def test_app_disables_child_pages_when_controls_locked(monkeypatch) -> None:
    monkeypatch.setattr(app_module, "load_config", lambda _path: _app_cfg())
    monkeypatch.setattr(app_module, "load_points_from_excel", lambda *_args, **_kwargs: [type("P", (), {"temp_chamber_c": 20.0})()])

    root = _root()
    try:
        ui = app_module.App(root)
        assert str(ui.open_humidity_button.cget("state")) == "normal"
        assert str(ui.export_summary_button.cget("state")) == "disabled"

        ui.worker = types.SimpleNamespace(is_alive=lambda: True)
        ui._apply_control_lock()
        assert str(ui.open_humidity_button.cget("state")) == "disabled"
        assert str(ui.open_dewpoint_button.cget("state")) == "disabled"
        assert str(ui.open_thermometer_button.cget("state")) == "disabled"
        assert str(ui.open_valve_button.cget("state")) == "disabled"
    finally:
        root.destroy()


def test_humidity_page_buttons_follow_connection_state(monkeypatch) -> None:
    monkeypatch.setattr(humidity_module, "HumidityGenerator", _FakeHumidity)

    root = _root()
    try:
        page = humidity_module.HumidityPage(root, {"port": "COM8", "baud": 9600})
        assert str(page.connect_button.cget("state")) == "normal"
        assert str(page.disconnect_button.cget("state")) == "disabled"
        assert str(page.set_temp_button.cget("state")) == "disabled"
        assert str(page.set_dewpoint_button.cget("state")) == "disabled"
        assert str(page.tag_combo.cget("state")) == "disabled"

        page.connect()
        assert "已连接" in page.status_var.get()
        assert str(page.connect_button.cget("state")) == "disabled"
        assert str(page.disconnect_button.cget("state")) == "normal"
        assert str(page.set_temp_button.cget("state")) == "normal"
        assert str(page.set_dewpoint_button.cget("state")) == "normal"
        assert str(page.read_all_button.cget("state")) == "normal"
        assert str(page.tag_combo.cget("state")) == "readonly"

        page.disconnect()
        assert page.status_var.get() == "未连接"
        assert str(page.read_all_button.cget("state")) == "disabled"
    finally:
        root.destroy()

def test_humidity_page_read_all_updates_snapshot(monkeypatch) -> None:
    monkeypatch.setattr(humidity_module, "HumidityGenerator", _FakeHumidity)

    root = _root()
    try:
        page = humidity_module.HumidityPage(root, {"port": "COM8", "baud": 9600})
        page.connect()
        page.read_all()
        assert page.status_var.get() == "读取完成"
        assert "Tc=20.00 C" in page.snapshot_var.get()
        assert page.summary_vars["water_rh"].get() == "实测 50.00 / 目标 -- %RH"
    finally:
        root.destroy()


def test_humidity_page_connect_starts_live_refresh(monkeypatch) -> None:
    monkeypatch.setattr(humidity_module, "HumidityGenerator", _FakeHumidity)

    root = _root()
    try:
        page = humidity_module.HumidityPage(root, {"port": "COM8", "baud": 9600, "poll_interval_ms": 3000})
        page.connect()
        assert "Tc=20.00 C" in page.snapshot_var.get()
        assert page.summary_vars["flow"].get() == "实测 1.20 / 目标 -- L/min"
        assert page.dev is not None
        assert page.dev.fetch_calls >= 1
        assert page._poll_job is not None
    finally:
        root.destroy()


def test_humidity_page_action_refreshes_live_snapshot(monkeypatch) -> None:
    monkeypatch.setattr(humidity_module, "HumidityGenerator", _FakeHumidity)

    root = _root()
    try:
        page = humidity_module.HumidityPage(root, {"port": "COM8", "baud": 9600})
        page.connect()
        assert page.dev is not None
        baseline = page.dev.fetch_calls
        page.ctrl(True)
        assert page.dev.fetch_calls > baseline
        assert "Tc=20.00 C" in page.snapshot_var.get()
    finally:
        root.destroy()


def test_humidity_page_set_dewpoint_derives_target_temp_and_rh(monkeypatch) -> None:
    monkeypatch.setattr(humidity_module, "HumidityGenerator", _FakeHumidity)

    root = _root()
    try:
        page = humidity_module.HumidityPage(root, {"port": "COM8", "baud": 9600})
        page.connect()
        assert page.dev is not None

        page.dewpoint_var.set("2.0")
        page.set_dewpoint()

        assert float(page.temp_var.get()) == 20.0
        assert float(page.rh_var.get()) == pytest.approx(30.19, abs=1e-3)
        assert page.last_data["Tda"] == 2.0
        assert page.last_data["Ta"] == 20.0
        assert page.last_data["UwA"] == pytest.approx(30.19, abs=1e-3)
        assert ("set_target_dewpoint", 2.0) in page.dev.commands
        assert "按露点设定 2C -> 温度 20C / 湿度 30.19%RH" in page.log_text.get("1.0", "end")
    finally:
        root.destroy()


def test_humidity_page_hides_invalid_measurement_sentinels(monkeypatch) -> None:
    class _InvalidHumidity(_FakeHumidity):
        def fetch_all(self) -> dict:
            self.fetch_calls += 1
            return {
                "raw": "ok",
                "data": {"Tc": 20.0, "Uw": -1001.0, "Td": -1001.0, "Fl": 0.0},
            }

    monkeypatch.setattr(humidity_module, "HumidityGenerator", _InvalidHumidity)

    root = _root()
    try:
        page = humidity_module.HumidityPage(root, {"port": "COM8", "baud": 9600})
        page.connect()
        page.read_all()
        assert page.summary_vars["water_rh"].get() == "实测 -- / 目标 -- %RH"
        assert page.summary_vars["dewpoint"].get() == "实测 -- / 目标 -- C"
        assert "Uw=--%RH" in page.snapshot_var.get()
    finally:
        root.destroy()


def test_dewpoint_page_polling_constraints(monkeypatch) -> None:
    monkeypatch.setattr(dewpoint_module, "DewpointMeter", _FakeDewpoint)

    root = _root()
    try:
        page = dewpoint_module.DewpointPage(root, {"port": "COM13", "baud": 9600, "station": "001"})
        assert str(page.read_once_button.cget("state")) == "disabled"
        page.connect()
        assert str(page.read_once_button.cget("state")) == "normal"
        assert str(page.start_poll_button.cget("state")) == "normal"
        page._poll_once = lambda: setattr(page, "_poll_job", "job")
        page.win.after_cancel = lambda _job: None
        page.start_poll()
        assert page.status_var.get() == "轮询中"
        assert str(page.read_once_button.cget("state")) == "disabled"
        assert str(page.start_poll_button.cget("state")) == "disabled"
        assert str(page.stop_poll_button.cget("state")) == "normal"
        assert str(page.interval_entry.cget("state")) == "disabled"

        page.stop_poll()
        assert page.status_var.get() == "轮询停止"
        assert str(page.read_once_button.cget("state")) == "normal"
        assert str(page.stop_poll_button.cget("state")) == "disabled"
    finally:
        root.destroy()


def test_dewpoint_page_read_once_updates_summary(monkeypatch) -> None:
    monkeypatch.setattr(dewpoint_module, "DewpointMeter", _FakeDewpoint)

    root = _root()
    try:
        page = dewpoint_module.DewpointPage(root, {"port": "COM13", "baud": 9600, "station": "001"})
        page.connect()
        page.read_once()
        assert page.status_var.get() == "读取完成"
        assert page.summary_vars["dewpoint"].get() == "1.10 C"
        assert "Flags: 0, 1" in page.snapshot_var.get()
    finally:
        root.destroy()


def test_thermometer_page_polling_constraints(monkeypatch) -> None:
    monkeypatch.setattr(thermometer_module, "Thermometer", _FakeThermometer)

    root = _root()
    try:
        page = thermometer_module.ThermometerPage(root, {"port": "COM9", "baud": 2400})
        assert page.summary_vars["status"].get() == "未连接"
        assert str(page.read_once_button.cget("state")) == "disabled"

        page.connect()
        assert page.summary_vars["status"].get() == "已连接"
        assert str(page.connect_button.cget("state")) == "disabled"
        assert str(page.read_once_button.cget("state")) == "normal"

        page._poll_once = lambda: setattr(page, "_poll_job", "job")
        page.win.after_cancel = lambda _job: None
        page.start_poll()
        assert page.summary_vars["status"].get() == "轮询中"
        assert str(page.read_once_button.cget("state")) == "disabled"
        assert str(page.start_poll_button.cget("state")) == "disabled"
        assert str(page.stop_poll_button.cget("state")) == "normal"
        assert str(page.corr_entry.cget("state")) == "disabled"

        page.stop_poll()
        assert page.summary_vars["status"].get() == "轮询停止"
        assert str(page.read_once_button.cget("state")) == "normal"
        assert str(page.clear_stats_button.cget("state")) == "normal"
    finally:
        root.destroy()


def test_thermometer_page_clear_stats_resets_summary(monkeypatch) -> None:
    monkeypatch.setattr(thermometer_module, "Thermometer", _FakeThermometer)

    root = _root()
    try:
        page = thermometer_module.ThermometerPage(root, {"port": "COM9", "baud": 2400})
        page.connect()
        page.read_once()
        assert page.summary_vars["count"].get() == "1"
        page.clear_stats()
        assert page.summary_vars["count"].get() == "0"
        assert page.summary_vars["raw_temp"].get() == "--"
        assert page.summary_vars["actual_temp"].get() == "--"
    finally:
        root.destroy()


def test_valve_page_route_buttons_require_connection(monkeypatch) -> None:
    monkeypatch.setattr(valve_module, "RelayController", _FakeRelay)

    root = _root()
    try:
        page = valve_module.ValvePage(root, _app_cfg())
        assert str(page.connect_button.cget("state")) == "normal"
        assert str(page.disconnect_button.cget("state")) == "disabled"
        assert all(str(button.cget("state")) == "disabled" for button in page.group_a_buttons)
        assert str(page.h2o_on_button.cget("state")) == "disabled"

        page.connect()
        assert "已连接" in page.status_var.get()
        assert str(page.connect_button.cget("state")) == "disabled"
        assert str(page.disconnect_button.cget("state")) == "normal"
        assert all(str(button.cget("state")) == "normal" for button in page.group_a_buttons)
        assert str(page.all_close_button.cget("state")) == "normal"

        page.disconnect()
        assert page.status_var.get() == "未连接"
        assert all(str(button.cget("state")) == "disabled" for button in page.group_b_buttons)
    finally:
        root.destroy()


def test_valve_page_route_hint_updates_after_switch(monkeypatch) -> None:
    monkeypatch.setattr(valve_module, "RelayController", _FakeRelay)

    root = _root()
    try:
        page = valve_module.ValvePage(root, _app_cfg())
        page.connect()
        page.set_co2_group("A", 200)
        assert page.status_var.get() == "组 A / 200 ppm"
        assert "目标浓度: 200 ppm" in page.route_hint_var.get()
    finally:
        root.destroy()

def test_valve_page_all_close_restores_bypass_baseline(monkeypatch) -> None:
    monkeypatch.setattr(valve_module, "RelayController", _FakeRelay)

    root = _root()
    try:
        page = valve_module.ValvePage(root, _app_cfg())
        page.connect()
        page.all_close()
        relay = page.relays["relay"]
        assert relay.states.get(15) is False
        assert relay.states.get(16) is False
    finally:
        root.destroy()
