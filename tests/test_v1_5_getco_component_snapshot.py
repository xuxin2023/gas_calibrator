import csv
import json

from gas_calibrator.tools import probe_v1_5_getco_component_snapshot as probe


def _write_json(path, payload):
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _read_csv(path):
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _config(tmp_path):
    return {
        "devices": {
            "gas_analyzers": [
                {
                    "name": "ga01",
                    "enabled": True,
                    "port": "COM35",
                    "baud": 115200,
                    "device_id": "023",
                    "mode": 2,
                    "active_send": True,
                    "ftd_hz": 1,
                    "average_filter": 49,
                }
            ],
            "relay": {"enabled": False},
            "relay_8": {"enabled": False},
            "humidity_generator": {"enabled": False},
            "dewpoint_meter": {"enabled": False},
        },
        "paths": {"output_dir": str(tmp_path / "logs")},
    }


class _FakeSerial:
    def __init__(self):
        self.commands = []

    def exchange_readlines(self, command, *, response_timeout_s, read_timeout_s=0.1, clear_input=False):
        stripped = command.strip()
        self.commands.append(("exchange", stripped, response_timeout_s, clear_input))
        if stripped == "GETCO,YGAS,FFF,1":
            return ["YGAS,023,2,900.0,0.0", "<C0:1,C1:2,C2:3,C3:4,C4:0,C5:0>"]
        if stripped == "GETCO,YGAS,FFF,3":
            return ["<C0:5,C1:6,C2:7,C3:8,C4:9,C5:0>"]
        if stripped == "GETCO,YGAS,FFF,5":
            return ["<C0:-20.1,C1:1.003>"]
        if stripped == "GETCO,YGAS,FFF,6":
            return ["<C0:0.2,C1:0.998>"]
        return ["YGAS,023,2,900.0,0.0"]

    def drain_input_nonblock(self, drain_s=0.35, read_timeout_s=0.05):
        self.commands.append(("drain", drain_s, read_timeout_s))
        return ["YGAS,023,2,900.0,0.0"]


class _FakeGasAnalyzer:
    instances = []

    def __init__(self, port, baudrate=115200, timeout=1.0, device_id="000", **_kwargs):
        self.port = port
        self.device_id = device_id
        self.ser = _FakeSerial()
        self.calls = []
        _FakeGasAnalyzer.instances.append(self)

    def open(self):
        self.calls.append(("open",))

    def close(self):
        self.calls.append(("close",))

    def set_senco(self, *_args, **_kwargs):
        raise AssertionError("component GETCO snapshot must not write SENCO")

    def set_device_id(self, *_args, **_kwargs):
        raise AssertionError("component GETCO snapshot must not write device ID")


class _RuntimeRebindSerial(_FakeSerial):
    def exchange_readlines(self, command, *, response_timeout_s, read_timeout_s=0.1, clear_input=False):
        stripped = command.strip()
        self.commands.append(("exchange", stripped, response_timeout_s, clear_input))
        if stripped == "GETCO,YGAS,FFF,1":
            return ["YGAS,090,2,900.0,0.0", "<C0:10,C1:20,C2:30,C3:40,C4:0,C5:0>"]
        return ["YGAS,090,2,900.0,0.0"]

    def drain_input_nonblock(self, drain_s=0.35, read_timeout_s=0.05):
        self.commands.append(("drain", drain_s, read_timeout_s))
        return ["YGAS,090,2,900.0,0.0"]


class _RuntimeRebindGasAnalyzer(_FakeGasAnalyzer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ser = _RuntimeRebindSerial()


def test_component_getco_snapshot_captures_old_groups_without_runtime_writes(monkeypatch, tmp_path):
    _FakeGasAnalyzer.instances = []
    monkeypatch.setattr(probe, "GasAnalyzer", _FakeGasAnalyzer)
    cfg_path = tmp_path / "cfg.json"
    out_dir = tmp_path / "out"
    _write_json(cfg_path, _config(tmp_path))

    rc = probe.main(
        [
            "--config",
            str(cfg_path),
            "--output-dir",
            str(out_dir),
            "--device-id",
            "023",
            "--groups",
            "1,3",
            "--command-gap-s",
            "0",
            "--pre-drain-s",
            "0",
        ]
    )

    assert rc == 0
    rows = _read_csv(out_dir / "getco_component_snapshot_rows.csv")
    assert {row["getco_group"] for row in rows if row["coefficient_found"] == "True"} == {"1", "3"}
    conclusion = _read_csv(out_dir / "getco_component_snapshot_conclusion.csv")[0]
    assert conclusion["writes_senco"] == "False"
    assert conclusion["writes_device_id"] == "False"
    assert conclusion["controls_water_or_gas_routes"] == "False"
    snapshot = json.loads((out_dir / "old_component_coefficients_snapshot.json").read_text(encoding="utf-8"))
    assert snapshot["023"]["GETCO1_before"] == [1.0, 2.0, 3.0, 4.0, 0.0, 0.0]
    assert snapshot["023"]["GETCO3_before"] == [5.0, 6.0, 7.0, 8.0, 9.0, 0.0]
    ga = _FakeGasAnalyzer.instances[0]
    commands = [item[1] for item in ga.ser.commands if item[0] == "exchange"]
    assert "SETCOMWAY,YGAS,FFF,0" not in commands
    assert "SETCOMWAY,YGAS,FFF,1" not in commands


def test_component_getco_snapshot_accepts_senco5_and_senco6_two_value_groups(monkeypatch, tmp_path):
    _FakeGasAnalyzer.instances = []
    monkeypatch.setattr(probe, "GasAnalyzer", _FakeGasAnalyzer)
    cfg_path = tmp_path / "cfg.json"
    out_dir = tmp_path / "out"
    _write_json(cfg_path, _config(tmp_path))

    rc = probe.main(
        [
            "--config",
            str(cfg_path),
            "--output-dir",
            str(out_dir),
            "--device-id",
            "023",
            "--groups",
            "5,6",
            "--command-gap-s",
            "0",
            "--pre-drain-s",
            "0",
        ]
    )

    assert rc == 0
    rows = _read_csv(out_dir / "getco_component_snapshot_rows.csv")
    valid = {row["getco_group"]: row for row in rows if row["coefficient_valid"] == "True"}
    assert valid["5"]["min_coefficients_per_group"] == "2"
    assert valid["6"]["min_coefficients_per_group"] == "2"
    snapshot = json.loads((out_dir / "old_component_coefficients_snapshot.json").read_text(encoding="utf-8"))
    assert snapshot["023"]["GETCO5_before"] == [-20.1, 1.003]
    assert snapshot["023"]["GETCO6_before"] == [0.2, 0.998]


def test_component_getco_snapshot_can_optionally_quiet_active_upload(monkeypatch, tmp_path):
    _FakeGasAnalyzer.instances = []
    monkeypatch.setattr(probe, "GasAnalyzer", _FakeGasAnalyzer)
    cfg_path = tmp_path / "cfg.json"
    out_dir = tmp_path / "out"
    _write_json(cfg_path, _config(tmp_path))

    rc = probe.main(
        [
            "--config",
            str(cfg_path),
            "--output-dir",
            str(out_dir),
            "--device-id",
            "023",
            "--groups",
            "1",
            "--allow-quiet-setcomway",
            "--command-gap-s",
            "0",
            "--pre-drain-s",
            "0",
        ]
    )

    assert rc == 0
    ga = _FakeGasAnalyzer.instances[0]
    commands = [item[1] for item in ga.ser.commands if item[0] == "exchange"]
    assert "SETCOMWAY,YGAS,FFF,0" in commands
    assert "SETCOMWAY,YGAS,FFF,1" in commands
    conclusion = _read_csv(out_dir / "getco_component_snapshot_conclusion.csv")[0]
    assert conclusion["allow_quiet_setcomway"] == "True"


def test_component_getco_snapshot_blocks_identity_mismatch_by_default(monkeypatch, tmp_path):
    _RuntimeRebindGasAnalyzer.instances = []
    monkeypatch.setattr(probe, "GasAnalyzer", _RuntimeRebindGasAnalyzer)
    cfg_path = tmp_path / "cfg.json"
    out_dir = tmp_path / "out"
    _write_json(cfg_path, _config(tmp_path))

    rc = probe.main(
        [
            "--config",
            str(cfg_path),
            "--output-dir",
            str(out_dir),
            "--groups",
            "1",
            "--command-gap-s",
            "0",
            "--pre-drain-s",
            "0",
        ]
    )

    assert rc == 1
    identity = _read_csv(out_dir / "getco_component_snapshot_identity.csv")[0]
    assert identity["configured_device_id"] == "023"
    assert identity["analyzer_device_id"] == "023"
    assert identity["identity_before"] == "090"
    assert "identity_mismatch" in identity["error"]


def test_component_getco_snapshot_can_rebind_runtime_identity_without_writing(monkeypatch, tmp_path):
    _RuntimeRebindGasAnalyzer.instances = []
    monkeypatch.setattr(probe, "GasAnalyzer", _RuntimeRebindGasAnalyzer)
    cfg_path = tmp_path / "cfg.json"
    out_dir = tmp_path / "out"
    _write_json(cfg_path, _config(tmp_path))

    rc = probe.main(
        [
            "--config",
            str(cfg_path),
            "--output-dir",
            str(out_dir),
            "--groups",
            "1",
            "--command-gap-s",
            "0",
            "--pre-drain-s",
            "0",
            "--allow-runtime-identity-rebind",
        ]
    )

    assert rc == 0
    identity = _read_csv(out_dir / "getco_component_snapshot_identity.csv")[0]
    assert identity["configured_device_id"] == "023"
    assert identity["analyzer_device_id"] == "090"
    assert identity["runtime_device_id"] == "090"
    assert identity["runtime_identity_rebound"] == "True"
    conclusion = _read_csv(out_dir / "getco_component_snapshot_conclusion.csv")[0]
    assert conclusion["allow_runtime_identity_rebind"] == "True"
    assert conclusion["runtime_identity_rebound_count"] == "1"
    snapshot = json.loads((out_dir / "old_component_coefficients_snapshot.json").read_text(encoding="utf-8"))
    assert "090" in snapshot
    assert snapshot["090"]["configured_device_id"] == "023"
    assert snapshot["090"]["GETCO1_before"] == [10.0, 20.0, 30.0, 40.0, 0.0, 0.0]
    bound_config = json.loads((out_dir / "runtime_identity_bound_config.json").read_text(encoding="utf-8"))
    bound_analyzer = bound_config["devices"]["gas_analyzers"][0]
    assert bound_analyzer["configured_device_id"] == "023"
    assert bound_analyzer["device_id"] == "090"
    assert bound_analyzer["runtime_identity_bound"] is True
    assert bound_config["v1_5_identity_binding"]["frozen_for_run"] is True
    assert bound_config["v1_5_identity_binding"]["writes_device_id"] is False
