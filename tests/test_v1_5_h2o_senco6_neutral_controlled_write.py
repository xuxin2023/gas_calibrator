import json

from gas_calibrator.tools import run_v1_5_h2o_senco6_neutral_controlled_write as writer


def _write_json(path, payload):
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _config(tmp_path):
    return {
        "devices": {
            "gas_analyzers": [
                {
                    "name": "ga033",
                    "enabled": True,
                    "port": "COM33",
                    "baud": 115200,
                    "device_id": "033",
                    "mode": 2,
                    "active_send": True,
                    "ftd_hz": 1,
                    "average_filter": 49,
                },
                {
                    "name": "ga051",
                    "enabled": True,
                    "port": "COM51",
                    "baud": 115200,
                    "device_id": "051",
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
        self.coeff6 = [24.4, 1.0] if device_id == "033" else [0.0, 1.0]
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
        assert int(group) == 6
        return {f"C{idx}": value for idx, value in enumerate(self.coeff6)}

    def _send_config_with_retries(self, payload, **kwargs):
        self.calls.append(("send", payload, kwargs))
        assert payload == "CLEARSENCO6,YGAS,FFF"
        self.coeff6 = [0.0, 1.0]
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


def test_senco6_neutral_writer_requires_explicit_unlock(tmp_path):
    cfg_path = tmp_path / "cfg.json"
    _write_json(cfg_path, _config(tmp_path))

    rc = writer.main(
        [
            "--config",
            str(cfg_path),
            "--output-dir",
            str(tmp_path / "out"),
            "--device-id",
            "033",
        ]
    )

    assert rc == 2


def test_senco6_neutral_writer_uses_clear_command_and_skips_neutral(monkeypatch, tmp_path):
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
            "033",
            "--device-id",
            "051",
            "--write-all-nonneutral",
            "--enable-senco6-write",
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
    events = (out_dir / "senco6_neutral_write_events.csv").read_text(encoding="utf-8-sig")
    assert "033" in events
    assert "written_readback_verified" in events
    assert "051" in events
    assert "already_neutral" in events
    assert "CLEARSENCO6,YGAS,FFF" in events
    assert "CLEARSENCO5" not in events

    calls_033 = _FakeGasAnalyzer.instances["033"].calls
    calls_051 = _FakeGasAnalyzer.instances["051"].calls
    assert any(call[0] == "send" for call in calls_033)
    assert not any(call[0] == "send" for call in calls_051)
