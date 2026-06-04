import json

from gas_calibrator.tools import run_v1_5_co2_senco5_neutral_controlled_write as writer


def _write_json(path, payload):
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _config(tmp_path):
    return {
        "devices": {
            "gas_analyzers": [
                {
                    "name": "ga03",
                    "enabled": True,
                    "port": "COM37",
                    "baud": 115200,
                    "device_id": "022",
                    "mode": 2,
                    "active_send": True,
                    "ftd_hz": 1,
                    "average_filter": 49,
                },
                {
                    "name": "ga02",
                    "enabled": True,
                    "port": "COM36",
                    "baud": 115200,
                    "device_id": "030",
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
        self.coeff5 = [11.0, 0.66] if device_id == "022" else [0.0, 1.0]
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
        assert int(group) == 5
        return {f"C{idx}": value for idx, value in enumerate(self.coeff5)}

    def _send_config_with_retries(self, payload, **kwargs):
        self.calls.append(("send", payload, kwargs))
        assert payload == "CLEARSENCO5,YGAS,FFF"
        self.coeff5 = [0.0, 1.0]
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


def test_senco5_neutral_writer_requires_explicit_unlock(tmp_path):
    cfg_path = tmp_path / "cfg.json"
    _write_json(cfg_path, _config(tmp_path))

    rc = writer.main(
        [
            "--config",
            str(cfg_path),
            "--output-dir",
            str(tmp_path / "out"),
            "--device-id",
            "022",
        ]
    )

    assert rc == 2


def test_senco5_neutral_writer_uses_clear_command_and_skips_neutral(monkeypatch, tmp_path):
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
            "022",
            "--device-id",
            "030",
            "--write-all-nonneutral",
            "--enable-senco5-write",
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
            "0",
            "--readback-retry-delay-s",
            "0",
            "--restore-command-gap-s",
            "0",
        ]
    )

    assert rc == 0
    events = (out_dir / "senco5_neutral_write_events.csv").read_text(encoding="utf-8-sig")
    assert "022" in events
    assert "written_readback_verified" in events
    assert "030" in events
    assert "already_neutral" in events
    assert "CLEARSENCO5,YGAS,FFF" in events
    assert "e00" not in events

    calls_022 = _FakeGasAnalyzer.instances["022"].calls
    calls_030 = _FakeGasAnalyzer.instances["030"].calls
    assert any(call[0] == "send" for call in calls_022)
    assert not any(call[0] == "send" for call in calls_030)
