from gas_calibrator.tools import device_probe
from gas_calibrator.tools.device_probe import _candidate_thermo_settings, parse_args


def test_candidate_thermo_settings_dedup() -> None:
    cfg = {
        "baud": 2400,
        "parity": "N",
        "bytesize": 8,
        "stopbits": 1,
        "timeout": 1.2,
    }
    rows = _candidate_thermo_settings(cfg, try_all=True)
    assert rows[0] == (2400, "N", 8, 1.0, 1.2)
    assert len(rows) >= 3
    assert len(rows) == len(set(rows))


def test_parse_args_pressure_scan() -> None:
    ns = parse_args(
        [
            "--config",
            "configs/default_config.json",
            "--output-dir",
            "audit/device_probe",
            "pressure",
            "--scan-ids",
        ]
    )
    assert ns.mode == "pressure"
    assert ns.scan_ids is True
    assert ns.exhaustive_controller is False
    assert ns.output_dir == "audit/device_probe"


def test_pressure_controller_probe_is_bounded_query_only_by_default(monkeypatch) -> None:
    class FakePace:
        instances = []

        def __init__(self, *args, **kwargs) -> None:
            self.queries = []
            self.read_pressure_called = False
            self.__class__.instances.append(self)

        def open(self) -> None:
            return None

        def close(self) -> None:
            return None

        def query(self, command: str) -> str:
            self.queries.append(command)
            return ":SENS:PRES 998.25" if command == ":SENS:PRES?" else ""

        def read_pressure(self) -> float:
            self.read_pressure_called = True
            raise AssertionError("production compatibility path must not run")

        @staticmethod
        def _parse_first_float(value: str):
            return 998.25 if value else None

    monkeypatch.setattr(device_probe, "Pace5000", FakePace)

    result = device_probe._probe_pressure_controller(
        {"port": "COM31", "baud": 9600, "timeout": 1.0},
        io_logger=None,
    )

    assert result == {
        "ok": True,
        "pressure_hpa": 998.25,
        "probe_mode": "bounded_query_only",
        "matched_command": ":SENS:PRES?",
    }
    assert FakePace.instances[0].queries == [":SENS:PRES?"]
    assert FakePace.instances[0].read_pressure_called is False


def test_pressure_controller_probe_can_opt_in_to_exhaustive_compatibility(monkeypatch) -> None:
    class FakePace:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def open(self) -> None:
            return None

        def close(self) -> None:
            return None

        def read_pressure(self) -> float:
            return 1001.5

    monkeypatch.setattr(device_probe, "Pace5000", FakePace)

    result = device_probe._probe_pressure_controller(
        {"port": "COM31", "baud": 9600, "timeout": 1.0},
        io_logger=None,
        exhaustive=True,
    )

    assert result == {"ok": True, "pressure_hpa": 1001.5, "probe_mode": "exhaustive"}


def test_parse_args_analyzers_receive_only() -> None:
    ns = parse_args(["analyzers", "--duration-per-port-s", "0.25"])
    assert ns.mode == "analyzers"
    assert ns.duration_per_port_s == 0.25
    assert ns.query_sn is False


def test_analyzer_inventory_is_receive_only_and_blocks_duplicate_observed_ids(monkeypatch) -> None:
    class FakeSerial:
        def __init__(self, port: str) -> None:
            self.port = port
            self.drains = []

        def drain_input_nonblock(self, *, drain_s: float, read_timeout_s: float):
            self.drains.append((drain_s, read_timeout_s))
            return [f"YGAS,001,10.0,0.2,100,200,22.0,100.0,{self.port}"]

        def write(self, *_args, **_kwargs):
            raise AssertionError("receive-only inventory must never transmit")

    class FakeGasAnalyzer:
        instances = []

        def __init__(self, port, baudrate=115200, timeout=1.0, device_id="000", **_kwargs):
            self.port = port
            self.ser = FakeSerial(port)
            self.__class__.instances.append(self)

        def open(self):
            return None

        def close(self):
            return None

        def parse_line(self, line):
            return {
                "id": "001",
                "mode": 1,
                "co2_ppm": 10.0,
                "h2o_mmol": 0.2,
                "temp_c": 22.0,
                "pressure_kpa": 100.0,
            }

    cfg = {
        "devices": {
            "gas_analyzers": [
                {"name": "ga01", "port": "COM35", "baud": 115200, "device_id": "001"},
                {"name": "ga02", "port": "COM36", "baud": 115200, "device_id": "002"},
            ]
        }
    }
    monkeypatch.setattr(device_probe, "GasAnalyzer", FakeGasAnalyzer)
    monkeypatch.setattr(device_probe, "_serial_port_metadata", lambda: {})

    snapshot = device_probe._probe_analyzers_receive_only(
        cfg,
        io_logger=None,
        duration_per_port_s=0.25,
    )

    assert snapshot["status"] == "blocked_duplicate_identity"
    assert snapshot["summary"]["duplicate_observed_ids"] == {
        "001": ["COM35", "COM36"]
    }
    assert snapshot["safety"] == {
        "receive_only": True,
        "sends_device_commands": False,
        "tx_bytes": 0,
        "writes_device_id": False,
        "writes_senco": False,
        "controls_water_or_gas_routes": False,
        "opens_dewpoint_meter": False,
    }
    assert all(item.ser.drains == [(0.25, 0.05)] for item in FakeGasAnalyzer.instances)


def test_analyzer_sn_probe_sends_only_query_and_blocks_missing_sn(monkeypatch) -> None:
    class FakeSerial:
        def __init__(self, port: str) -> None:
            self.port = port
            self.writes = []
            self.pending = False

        def flush_input(self):
            return None

        def write(self, command):
            assert command == "SN,YGAS,FFF\r\n"
            self.writes.append(command)
            self.pending = True

        def readline(self):
            if self.pending and self.port == "COM35":
                self.pending = False
                return "SN,YGAS,032,01260701"
            self.pending = False
            return ""

        def drain_input_nonblock(self, **_kwargs):
            return []

    class FakeGasAnalyzer:
        instances = []

        def __init__(self, port, *_args, **_kwargs):
            self.port = port
            self.ser = FakeSerial(port)
            self.__class__.instances.append(self)

        def open(self):
            return None

        def close(self):
            return None

    cfg = {
        "devices": {
            "gas_analyzers": [
                {"name": "ga01", "port": "COM35", "device_id": "001"},
                {"name": "ga02", "port": "COM36", "device_id": "002"},
            ]
        }
    }
    monkeypatch.setattr(device_probe, "GasAnalyzer", FakeGasAnalyzer)
    monkeypatch.setattr(device_probe, "_serial_port_metadata", lambda: {})
    monkeypatch.setattr(device_probe.time, "sleep", lambda _seconds: None)

    snapshot = device_probe._probe_analyzer_sn_query(cfg, None, timeout_s=0.05)

    assert snapshot["status"] == "blocked_missing_sn"
    assert snapshot["summary"]["missing_sn_ports"] == ["COM36"]
    assert snapshot["analyzers"][0]["sn_code"] == "01260701"
    assert snapshot["analyzers"][0]["sn_bound_valid"] is True
    assert all(item.ser.writes == ["SN,YGAS,FFF\r\n"] for item in FakeGasAnalyzer.instances)
    assert snapshot["safety"]["writes_device_id"] is False
    assert snapshot["safety"]["writes_sn"] is False


def test_analyzer_sn_probe_rejects_uninitialized_zero_sn(monkeypatch) -> None:
    class FakeSerial:
        def flush_input(self):
            return None

        def write(self, command):
            assert command == "SN,YGAS,FFF\r\n"

        def readline(self):
            return "YGAS,001,00000000"

        def drain_input_nonblock(self, **_kwargs):
            return []

    class FakeGasAnalyzer:
        def __init__(self, *_args, **_kwargs):
            self.ser = FakeSerial()

        def open(self):
            return None

        def close(self):
            return None

    monkeypatch.setattr(device_probe, "GasAnalyzer", FakeGasAnalyzer)
    monkeypatch.setattr(device_probe, "_serial_port_metadata", lambda: {})
    snapshot = device_probe._probe_analyzer_sn_query(
        {"devices": {"gas_analyzers": [{"port": "COM42", "device_id": "008"}]}},
        None,
        timeout_s=0.05,
    )

    assert snapshot["status"] == "blocked_invalid_or_uninitialized_sn"
    assert snapshot["summary"]["invalid_or_uninitialized_sn_ports"] == ["COM42"]
    assert snapshot["analyzers"][0]["sn_read_ok"] is True
    assert snapshot["analyzers"][0]["sn_bound_valid"] is False

