from gas_calibrator.validation.v1_5_formal_route_readiness import (
    build_formal_route_readiness_model,
    collect_formal_route_logical_valves,
)


class FakeRelay:
    def __init__(self, count):
        self.bits = [False] * count
        self.writes = []

    def read_coils(self, _start, count):
        return self.bits[:count]

    def set_valve(self, channel, value):
        self.bits[int(channel) - 1] = bool(value)
        self.writes.append((int(channel), bool(value)))


class FakeDewpoint:
    def __init__(self, ok=True):
        self.ok = ok

    def status(self):
        return {"ok": self.ok, "dewpoint_c": -32.5}


def _config(*, include_n2=True):
    valves = {
        "h2o_path": 1,
        "gas_main": 2,
        "co2_path": 3,
        "co2_path_group2": 4,
        "hold": 5,
        "flow_switch": 6,
        "co2_map": {"100ppm": 7, "400ppm": 8},
        "co2_map_group2": {"800ppm": 9},
        "relay_map": {
            "1": {"relay": "relay", "channel": 1},
            "2": {"relay": "relay", "channel": 2},
            "3": {"relay": "relay", "channel": 3},
            "4": {"relay": "relay", "channel": 4},
            "5": {"relay": "relay", "channel": 5},
            "6": {"relay": "relay", "channel": 6},
            "7": {"relay": "relay", "channel": 7},
            "8": {"relay": "relay", "channel": 8},
            "9": {"relay": "relay_8", "channel": 1},
        },
    }
    workflow = {"nitrogen_purge": {"co2_prepurge_s": 0}}
    if include_n2:
        valves["nitrogen_purge_source"] = 27
        valves["relay_map"]["27"] = {"relay": "relay_8", "channel": 2}
        workflow = {"nitrogen_purge": {"co2_prepurge_s": 300, "source_valve": 27}}
    return {
        "devices": {
            "relay": {"enabled": True, "port": "COM20", "addr": 1},
            "relay_8": {"enabled": True, "port": "COM21", "addr": 1},
            "dewpoint_meter": {"enabled": True, "port": "COM18", "station": "A"},
        },
        "workflow": workflow,
        "valves": valves,
    }


def test_collect_formal_route_logical_valves_includes_n2_when_prepurge_enabled():
    logical = collect_formal_route_logical_valves(_config(include_n2=True))

    assert logical["27"] == "n2_prepurge.source"
    assert any(role.startswith("co2_source") for role in logical.values())
    assert "h2o_route.hold" in logical.values()


def test_formal_route_readiness_passes_and_proves_n2_open_close(tmp_path):
    relay = FakeRelay(16)
    relay8 = FakeRelay(8)

    def builder(_cfg):
        return {"relay": relay, "relay_8": relay8, "dewpoint": FakeDewpoint(ok=True)}

    model = build_formal_route_readiness_model(
        _config(include_n2=True),
        output_dir=tmp_path,
        build_devices=builder,
        close_devices=lambda _devices: None,
    )

    assert model["ok"] is True
    checks = {row["check"]: row for row in model["checks"]}
    assert checks["formal_route_relay_map"]["status"] == "pass"
    assert checks["dewpoint_online"]["status"] == "pass"
    assert checks["n2_prepurge_valve_open_close"]["status"] == "pass"
    assert (2, True) in relay8.writes
    assert (2, False) in relay8.writes
    assert model["controls_co2_route"] is False
    assert model["controls_h2o_route"] is False


def test_formal_route_readiness_blocks_missing_n2_relay_map(tmp_path):
    cfg = _config(include_n2=True)
    del cfg["valves"]["relay_map"]["27"]

    model = build_formal_route_readiness_model(cfg, output_dir=tmp_path)

    assert model["ok"] is False
    assert any(issue["code"] == "RELAY_MAP_ENTRY_MISSING" for issue in model["issues"])


def test_formal_route_readiness_blocks_offline_dewpoint(tmp_path):
    relay = FakeRelay(16)
    relay8 = FakeRelay(8)

    def builder(_cfg):
        return {"relay": relay, "relay_8": relay8, "dewpoint": FakeDewpoint(ok=False)}

    model = build_formal_route_readiness_model(
        _config(include_n2=False),
        output_dir=tmp_path,
        build_devices=builder,
        close_devices=lambda _devices: None,
    )

    assert model["ok"] is False
    assert any(issue["code"] == "DEWPOINT_OFFLINE" for issue in model["issues"])
