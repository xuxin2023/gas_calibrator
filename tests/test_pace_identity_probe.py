from pathlib import Path

from gas_calibrator.tools import pace_identity_probe


class _FakePace:
    PROFILE_OLD_PACE5000 = "OLD_PACE5000"
    PROFILE_PACE5000E = "PACE5000E"
    PROFILE_UNKNOWN = "UNKNOWN"

    def __init__(self, *args, **kwargs):
        self.opened = False
        self.closed = False

    def open(self):
        self.opened = True

    def close(self):
        self.closed = True

    def probe_identity(self, commands):
        assert list(commands) == list(pace_identity_probe.DEFAULT_COMMANDS)
        return [
            {"command": "*CLS", "response": "", "duration_ms": 1.0, "error": ""},
            {
                "command": "*IDN?",
                "response": "GE Druck,Pace5000 User Interface,3213201,02.00.07",
                "duration_ms": 1.2,
                "error": "",
            },
            {
                "command": ":INST:MOD?",
                "response": '-113,"Undefined header"',
                "duration_ms": 1.3,
                "error": "",
            },
            {
                "command": ":SYST:ECHO?",
                "response": '-113,"Undefined header"',
                "duration_ms": 1.1,
                "error": "",
            },
            {
                "command": ":SYST:ERR?",
                "response": ':SYST:ERR 0,"No error"',
                "duration_ms": 1.0,
                "error": "",
            },
        ]


def test_run_probe_writes_artifacts_and_detects_old_profile(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(
        pace_identity_probe,
        "load_config",
        lambda _path: {
            "devices": {
                "pressure_controller": {
                    "port": "COM31",
                    "baud": 9600,
                    "timeout": 1.0,
                    "line_ending": "LF",
                    "query_line_endings": ["LF"],
                    "pressure_queries": [":SENS:PRES:CONT?"],
                }
            }
        },
    )
    monkeypatch.setattr(pace_identity_probe, "Pace5000", _FakePace)

    result = pace_identity_probe.run_probe(
        config_path="configs/default_config.json",
        output_dir=str(tmp_path),
    )

    assert result["profile"] == "OLD_PACE5000"
    assert Path(result["csv_path"]).exists()
    assert Path(result["json_path"]).exists()
    assert Path(result["io_path"]).exists()
    assert any(row["command"] == "*IDN?" for row in result["rows"])
