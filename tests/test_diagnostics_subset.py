from gas_calibrator.diagnostics import run_self_test


def _all_disabled_cfg():
    return {
        "devices": {
            "pressure_controller": {"enabled": False},
            "pressure_gauge": {"enabled": False},
            "dewpoint_meter": {"enabled": False},
            "humidity_generator": {"enabled": False},
            "gas_analyzer": {"enabled": False},
            "temperature_chamber": {"enabled": False},
            "thermometer": {"enabled": False},
            "relay": {"enabled": False},
            "relay_8": {"enabled": False},
        }
    }


def test_run_self_test_subset_only_returns_requested_keys() -> None:
    cfg = _all_disabled_cfg()
    result = run_self_test(cfg, only_devices=["gas_analyzer", "relay_8"])

    assert set(result.keys()) == {"gas_analyzer", "relay_8"}
    assert result["gas_analyzer"]["err"] == "DISABLED"
    assert result["relay_8"]["err"] == "DISABLED"


def test_run_self_test_without_subset_returns_all_keys() -> None:
    cfg = _all_disabled_cfg()
    result = run_self_test(cfg)

    assert set(result.keys()) == {
        "pressure_controller",
        "pressure_gauge",
        "dewpoint_meter",
        "humidity_generator",
        "gas_analyzer",
        "temperature_chamber",
        "thermometer",
        "relay",
        "relay_8",
    }


def test_run_self_test_gas_analyzer_uses_lightweight_probe(monkeypatch) -> None:
    calls = []

    class _FakeGasAnalyzer:
        def __init__(self, *args, **kwargs) -> None:
            calls.append(("init", args, kwargs))

        def open(self) -> None:
            calls.append(("open",))

        def close(self) -> None:
            calls.append(("close",))

        def read_data_active(self, drain_s: float = 0.35, read_timeout_s: float = 0.05) -> str:
            calls.append(("read_active", round(drain_s, 2)))
            return "YGAS,001,400.0,1.0,1,1,1,1,1,1,1,1,20.0,20.0,101.3,OK"

        def read_data_passive(self) -> str:
            calls.append(("read_passive",))
            return ""

        @staticmethod
        def parse_line(line: str):
            return {"co2_ppm": 400.0} if line.startswith("YGAS,") else None

    monkeypatch.setattr("gas_calibrator.diagnostics.GasAnalyzer", _FakeGasAnalyzer)
    cfg = {
        "devices": {
            "pressure_controller": {"enabled": False},
            "pressure_gauge": {"enabled": False},
            "dewpoint_meter": {"enabled": False},
            "humidity_generator": {"enabled": False},
            "temperature_chamber": {"enabled": False},
            "thermometer": {"enabled": False},
            "relay": {"enabled": False},
            "relay_8": {"enabled": False},
            "gas_analyzer": {
                "enabled": True,
                "port": "COM35",
                "baud": 115200,
                "device_id": "001",
                "active_send": True,
                "ftd_hz": 10,
            },
        }
    }

    result = run_self_test(cfg, only_devices=["gas_analyzer"])

    assert result["gas_analyzer"]["ok"] is True
    assert ("read_active", 0.2) in calls
    assert ("read_passive",) not in calls
    assert not any(call[0] in {"set_mode", "set_comm_way", "set_active_freq", "set_average_filter"} for call in calls)


def test_run_self_test_gas_analyzer_keeps_soft_marked_extreme_frame_when_ratio_is_usable(monkeypatch) -> None:
    class _FakeGasAnalyzer:
        def __init__(self, *args, **kwargs) -> None:
            return None

        def open(self) -> None:
            return None

        def close(self) -> None:
            return None

        def read_data_active(self, drain_s: float = 0.35, read_timeout_s: float = 0.05) -> str:
            return "YGAS,001,3000.0,72.0,1,1,1,1,1,1,1,1,25.0,25.0,101.3,OK"

        def read_data_passive(self) -> str:
            return ""

        @staticmethod
        def parse_line(line: str):
            if not line.startswith("YGAS,"):
                return None
            return {"co2_ppm": 3000.0, "h2o_mmol": 72.0, "co2_ratio_f": 1.02}

    monkeypatch.setattr("gas_calibrator.diagnostics.GasAnalyzer", _FakeGasAnalyzer)
    cfg = {
        "devices": {
            "pressure_controller": {"enabled": False},
            "pressure_gauge": {"enabled": False},
            "dewpoint_meter": {"enabled": False},
            "humidity_generator": {"enabled": False},
            "temperature_chamber": {"enabled": False},
            "thermometer": {"enabled": False},
            "relay": {"enabled": False},
            "relay_8": {"enabled": False},
            "gas_analyzer": {
                "enabled": True,
                "port": "COM35",
                "baud": 115200,
                "device_id": "001",
                "active_send": True,
                "ftd_hz": 10,
            },
        }
    }

    result = run_self_test(cfg, only_devices=["gas_analyzer"])

    assert result["gas_analyzer"]["ok"] is True
    assert result["gas_analyzer"]["frame_status"] == "极值已标记"


def test_run_self_test_gas_analyzer_still_rejects_extreme_frame_when_ratio_missing(monkeypatch) -> None:
    class _FakeGasAnalyzer:
        def __init__(self, *args, **kwargs) -> None:
            return None

        def open(self) -> None:
            return None

        def close(self) -> None:
            return None

        def read_data_active(self, drain_s: float = 0.35, read_timeout_s: float = 0.05) -> str:
            return "YGAS,001,3000.0,72.0,1,1,1,1,1,1,1,1,25.0,25.0,101.3,OK"

        def read_data_passive(self) -> str:
            return ""

        @staticmethod
        def parse_line(line: str):
            if not line.startswith("YGAS,"):
                return None
            return {"co2_ppm": 3000.0, "h2o_mmol": 72.0}

    monkeypatch.setattr("gas_calibrator.diagnostics.GasAnalyzer", _FakeGasAnalyzer)
    cfg = {
        "devices": {
            "pressure_controller": {"enabled": False},
            "pressure_gauge": {"enabled": False},
            "dewpoint_meter": {"enabled": False},
            "humidity_generator": {"enabled": False},
            "temperature_chamber": {"enabled": False},
            "thermometer": {"enabled": False},
            "relay": {"enabled": False},
            "relay_8": {"enabled": False},
            "gas_analyzer": {
                "enabled": True,
                "port": "COM35",
                "baud": 115200,
                "device_id": "001",
                "active_send": True,
                "ftd_hz": 10,
            },
        }
    }

    result = run_self_test(cfg, only_devices=["gas_analyzer"])

    assert result["gas_analyzer"]["ok"] is False
    assert result["gas_analyzer"]["err"] == "异常极值"
