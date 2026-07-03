import json

from gas_calibrator.tools import run_v1_5_temperature_senco78_neutral_controlled_write as writer


def _write_json(path, payload):
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _config(tmp_path):
    return {
        "devices": {
            "gas_analyzers": [
                {
                    "name": "ga02",
                    "enabled": True,
                    "port": "COM36",
                    "baud": 115200,
                    "device_id": "002",
                    "mode": 2,
                    "active_send": True,
                    "ftd_hz": 1,
                    "average_filter": 49,
                },
                {
                    "name": "ga11",
                    "enabled": True,
                    "port": "COM41",
                    "baud": 115200,
                    "device_id": "011",
                    "mode": 2,
                    "active_send": True,
                    "ftd_hz": 1,
                    "average_filter": 49,
                },
            ],
            "relay": {"enabled": False},
            "relay_8": {"enabled": False},
            "humidity_generator": {"enabled": False},
            "dewpoint_meter": {"enabled": False},
        },
        "paths": {"output_dir": str(tmp_path / "logs")},
    }


class _FakeGasAnalyzer:
    instances = {}

    def __init__(self, port, baudrate=115200, timeout=1.0, device_id="000", **_kwargs):
        self.port = port
        self.device_id = device_id
        self.mode = 2
        self.active = True
        self.ftd = 1
        self.average = 49
        self.coeffs = {
            7: [8.0, 0.75, 0.0, 0.0] if device_id == "002" else [0.0, 1.0, 0.0, 0.0],
            8: [-3.0, 1.25, 0.0, 0.0] if device_id == "002" else [0.0, 1.0, 0.0, 0.0],
        }
        self.calls = []
        _FakeGasAnalyzer.instances[device_id] = self

    def open(self):
        self.calls.append(("open",))

    def close(self):
        self.calls.append(("close",))

    def read_current_mode_snapshot(self, *args, **kwargs):
        self.calls.append(("snapshot", args, kwargs))
        return {"id": self.device_id, "mode": self.mode, "raw": f"YGAS,{self.device_id},..."}

    def set_mode_with_ack(self, mode, require_ack=True):
        self.calls.append(("mode", mode, require_ack))
        self.mode = int(mode)
        return True

    def read_coefficient_group(self, group, **_kwargs):
        self.calls.append(("getco", int(group)))
        return {f"C{idx}": value for idx, value in enumerate(self.coeffs[int(group)])}

    def _send_config_with_retries(self, payload, **kwargs):
        self.calls.append(("send", payload, kwargs))
        if payload.startswith("SENCO7,YGAS,FFF,"):
            self.coeffs[7] = [0.0, 1.0, 0.0, 0.0]
        elif payload.startswith("SENCO8,YGAS,FFF,"):
            self.coeffs[8] = [0.0, 1.0, 0.0, 0.0]
        elif payload == "CLEARSENCO7,YGAS,FFF":
            self.coeffs[7] = [0.0, 1.0, 0.0, 0.0]
        elif payload == "CLEARSENCO8,YGAS,FFF":
            self.coeffs[8] = [0.0, 1.0, 0.0, 0.0]
        else:
            raise AssertionError(payload)
        return True

    def set_comm_way_with_ack(self, active, require_ack=True):
        self.calls.append(("comm", active, require_ack))
        self.active = bool(active)
        return True

    def set_active_freq_with_ack(self, hz, require_ack=True):
        self.calls.append(("ftd", hz, require_ack))
        self.ftd = int(hz)
        return True

    def set_average_filter_with_ack(self, window_n, require_ack=True):
        self.calls.append(("avg", window_n, require_ack))
        self.average = int(window_n)
        return True


def test_senco78_neutral_writer_requires_explicit_unlock(tmp_path):
    cfg_path = tmp_path / "cfg.json"
    _write_json(cfg_path, _config(tmp_path))

    rc = writer.main(
        [
            "--config",
            str(cfg_path),
            "--output-dir",
            str(tmp_path / "out"),
            "--device-id",
            "002",
        ]
    )

    assert rc == 2


def test_senco78_neutral_writer_writes_only_nonneutral_temperature_groups(monkeypatch, tmp_path):
    monkeypatch.setattr(writer, "GasAnalyzer", _FakeGasAnalyzer)
    _FakeGasAnalyzer.instances.clear()
    cfg_path = tmp_path / "cfg.json"
    out_dir = tmp_path / "out"
    _write_json(cfg_path, _config(tmp_path))

    rc = writer.main(
        [
            "--config",
            str(cfg_path),
            "--output-dir",
            str(out_dir),
            "--device-id",
            "002",
            "--device-id",
            "011",
            "--write-all-nonneutral",
            "--enable-senco78-write",
            "--operator-confirmation",
            writer.CONFIRMATION_TEXT,
            "--reviewer",
            "test-reviewer",
            "--approver",
            "test-approver",
            "--pre-device-cooldown-s",
            "0",
            "--inter-device-delay-s",
            "0",
            "--post-write-settle-s",
            "1",
            "--readback-retry-delay-s",
            "1",
            "--restore-command-gap-s",
            "1",
        ]
    )

    assert rc == 0
    events = (out_dir / "senco78_neutral_write_events.csv").read_text(encoding="utf-8-sig")
    assert "002" in events
    assert "SENCO7" in events
    assert "SENCO8" in events
    assert "written_readback_verified" in events
    assert "011" in events
    assert "already_neutral" in events
    assert "SENCO7,YGAS,FFF,0.00000e00,1.00000e00,0.00000e00,0.00000e00" in events
    assert "SENCO8,YGAS,FFF,0.00000e00,1.00000e00,0.00000e00,0.00000e00" in events
    assert "CLEARSENCO" not in events
    assert "SENCO1" not in events
    assert "SENCO9" not in events

    calls_002 = _FakeGasAnalyzer.instances["002"].calls
    calls_011 = _FakeGasAnalyzer.instances["011"].calls
    sent_002 = [call[1] for call in calls_002 if call[0] == "send"]
    assert sent_002 == [
        "SENCO7,YGAS,FFF,0.00000e00,1.00000e00,0.00000e00,0.00000e00",
        "SENCO8,YGAS,FFF,0.00000e00,1.00000e00,0.00000e00,0.00000e00",
    ]
    assert not any(call[0] == "send" for call in calls_011)


def test_senco78_neutral_writer_can_use_clear_method(monkeypatch, tmp_path):
    monkeypatch.setattr(writer, "GasAnalyzer", _FakeGasAnalyzer)
    _FakeGasAnalyzer.instances.clear()
    cfg_path = tmp_path / "cfg.json"
    out_dir = tmp_path / "out"
    _write_json(cfg_path, _config(tmp_path))

    rc = writer.main(
        [
            "--config",
            str(cfg_path),
            "--output-dir",
            str(out_dir),
            "--device-id",
            "002",
            "--channel",
            "7",
            "--write-all-nonneutral",
            "--enable-senco78-write",
            "--operator-confirmation",
            writer.CONFIRMATION_TEXT,
            "--reviewer",
            "test-reviewer",
            "--approver",
            "test-approver",
            "--method",
            "clear",
            "--pre-device-cooldown-s",
            "0",
            "--inter-device-delay-s",
            "0",
            "--post-write-settle-s",
            "1",
            "--readback-retry-delay-s",
            "1",
            "--restore-command-gap-s",
            "1",
        ]
    )

    assert rc == 0
    events = (out_dir / "senco78_neutral_write_events.csv").read_text(encoding="utf-8-sig")
    assert "CLEARSENCO7,YGAS,FFF" in events
    assert "CLEARSENCO8" not in events
