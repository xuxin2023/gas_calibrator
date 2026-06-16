import csv
import json

from gas_calibrator.tools import probe_v1_5_getco9_protocol as probe


DIAGNOSTIC_GUARD_ARGS = [
    "--engineering-diagnostic",
    "--not-real-acceptance",
    "--operator-confirmation",
    "DIAGNOSTIC_ONLY",
]


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
        self.commands.append(("exchange", command.strip(), response_timeout_s, clear_input))
        if command.strip() == "GETCO9,YGAS,023":
            return ["<C0:0.704736,C1:1,C2:0,C3:0>"]
        return ["YGAS,023,2,900.0,0.0,0,0,1,1,1,1,100,100,100,25,25,101.3,0"]

    def flush_input(self):
        self.commands.append(("flush",))


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

    def read_current_mode_snapshot(self, *args, **kwargs):
        self.calls.append(("snapshot", args, kwargs))
        return {"ok": True, "id": self.device_id, "mode": 2, "raw": f"YGAS,{self.device_id},..."}

    def set_mode_with_ack(self, mode, require_ack=True):
        self.calls.append(("mode", mode, require_ack))
        return True

    def set_active_freq_with_ack(self, hz, require_ack=True):
        self.calls.append(("ftd", hz, require_ack))
        return True

    def set_average_filter_with_ack(self, window_n, require_ack=True):
        self.calls.append(("avg", window_n, require_ack))
        return True

    def set_comm_way_with_ack(self, active, require_ack=True):
        self.calls.append(("comm", active, require_ack))
        return True

    def set_senco(self, *_args, **_kwargs):
        raise AssertionError("GETCO9 probe must not write SENCO")


def test_getco9_probe_is_read_only_and_restores_runtime(monkeypatch, tmp_path):
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
            "--command-gap-s",
            "1",
            "--quiet-settle-s",
            "0",
            "--restore-command-gap-s",
            "1",
            *DIAGNOSTIC_GUARD_ARGS,
        ]
    )

    assert rc == 0
    rows = _read_csv(out_dir / "getco9_protocol_probe_rows.csv")
    assert any(row["coefficient_found"] == "True" for row in rows)
    assert {row["probe_mode"] for row in rows} == {"gentle"}
    assert len(rows) <= 4
    conclusion = _read_csv(out_dir / "getco9_protocol_probe_conclusion.csv")[0]
    assert conclusion["writes_senco"] == "False"
    assert conclusion["controls_water_or_gas_routes"] == "False"
    ga = _FakeGasAnalyzer.instances[0]
    assert ("mode", 2, False) in ga.calls
    assert ("ftd", 1, False) in ga.calls
    assert ("avg", 49, False) in ga.calls
    assert ("comm", True, False) in ga.calls
    assert not any(call and call[0] == "senco" for call in ga.calls)


def test_getco9_probe_blocks_when_no_coefficients(monkeypatch, tmp_path):
    class NoCoefficientGasAnalyzer(_FakeGasAnalyzer):
        pass

    class NoCoefficientSerial(_FakeSerial):
        def exchange_readlines(self, command, *, response_timeout_s, read_timeout_s=0.1, clear_input=False):
            self.commands.append(("exchange", command.strip(), response_timeout_s, clear_input))
            return ["YGAS,023,2,900.0,0.0,0,0,1,1,1,1,100,100,100,25,25,101.3,0"]

    def factory(*args, **kwargs):
        ga = NoCoefficientGasAnalyzer(*args, **kwargs)
        ga.ser = NoCoefficientSerial()
        return ga

    monkeypatch.setattr(probe, "GasAnalyzer", factory)
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
            "--command-gap-s",
            "1",
            "--quiet-settle-s",
            "0",
            "--restore-command-gap-s",
            "1",
            *DIAGNOSTIC_GUARD_ARGS,
        ]
    )

    assert rc == 1
    conclusion = _read_csv(out_dir / "getco9_protocol_probe_conclusion.csv")[0]
    assert conclusion["status"] == "blocked"
    assert conclusion["writes_device_id"] == "False"
