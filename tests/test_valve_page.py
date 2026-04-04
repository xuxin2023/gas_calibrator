from __future__ import annotations

import tkinter as tk

from gas_calibrator.ui.valve_page import ValvePage


def _valve_cfg() -> dict:
    return {
        "devices": {
            "relay": {"port": "COM28", "baud": 38400, "addr": 1},
            "relay_8": {"port": "COM29", "baud": 38400, "addr": 1},
        },
        "valves": {
            "co2_path": 7,
            "co2_path_group2": 16,
            "gas_main": 11,
            "h2o_path": 8,
            "flow_switch": 10,
            "hold": 9,
            "relay_map": {
                "1": {"device": "relay", "channel": 7},
                "2": {"device": "relay", "channel": 8},
                "7": {"device": "relay", "channel": 15},
                "8": {"device": "relay_8", "channel": 8},
                "9": {"device": "relay_8", "channel": 1},
                "10": {"device": "relay_8", "channel": 2},
                "11": {"device": "relay_8", "channel": 3},
                "16": {"device": "relay", "channel": 16},
                "21": {"device": "relay", "channel": 6},
            },
            "co2_map": {"0": 1, "200": 2},
            "co2_map_group2": {"100": 21},
        },
    }


class _FakeRelay:
    def __init__(self, channels: int):
        self.states = [False] * channels
        self.commands: list[tuple[int, bool]] = []

    def set_valve(self, channel: int, open_: bool) -> None:
        self.commands.append((channel, bool(open_)))
        self.states[channel - 1] = bool(open_)

    def read_coils(self, start: int = 0, count: int = 1):
        end = start + count
        return list(self.states[start:end])

    def close(self) -> None:
        return None


def _make_page() -> tuple[tk.Tk, ValvePage, _FakeRelay, _FakeRelay]:
    root = tk.Tk()
    root.withdraw()
    page = ValvePage(root, _valve_cfg())
    relay16 = _FakeRelay(16)
    relay8 = _FakeRelay(8)
    page.relays = {"relay": relay16, "relay_8": relay8}
    page._apply_button_states()
    return root, page, relay16, relay8


def test_valve_page_flow_switch_matches_physical_open_state() -> None:
    root, page, _relay16, relay8 = _make_page()
    try:
        flow_switch = int(page.cfg["valves"]["flow_switch"])
        page._apply_open([])
        assert relay8.states[1] is False
        page._apply_open([flow_switch])
        assert relay8.states[1] is True
    finally:
        page._on_close()
        root.destroy()


def test_valve_page_has_named_core_valve_controls() -> None:
    root, page, _relay16, _relay8 = _make_page()
    try:
        assert [entry["label"] for entry in page._core_entries] == [
            "总阀门",
            "旁路阀",
            "水路阀",
            "总气路阀",
            "A组总气路阀",
            "B组总气路阀",
        ]
        for entry in page._core_entries:
            assert len(page._manual_status_labels[entry["valve"]]) >= 2
    finally:
        page._on_close()
        root.destroy()


def test_valve_page_manual_toggle_keeps_other_open_valves() -> None:
    root, page, _relay16, relay8 = _make_page()
    try:
        gas_main = int(page.cfg["valves"]["gas_main"])
        flow_switch = int(page.cfg["valves"]["flow_switch"])

        page.set_manual_valve_state(gas_main, True)
        assert gas_main in page._manual_open_set
        assert relay8.states[2] is True

        page.set_manual_valve_state(flow_switch, False)
        assert gas_main in page._manual_open_set
        assert flow_switch not in page._manual_open_set
        assert relay8.states[1] is False
        assert relay8.states[2] is True

        page.set_manual_valve_state(flow_switch, True)
        assert gas_main in page._manual_open_set
        assert flow_switch in page._manual_open_set
        assert relay8.states[1] is True
        assert relay8.states[2] is True
    finally:
        page._on_close()
        root.destroy()
