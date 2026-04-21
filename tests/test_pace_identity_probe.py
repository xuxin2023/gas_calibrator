from pathlib import Path

from gas_calibrator.tools import pace_identity_probe


class _FakePace:
    PROFILE_OLD_PACE5000 = "OLD_PACE5000"
    PROFILE_PACE5000E = "PACE5000E"
    PROFILE_UNKNOWN = "UNKNOWN"
    last_commands = None

    def __init__(self, *args, **kwargs):
        self.opened = False
        self.closed = False

    def open(self):
        self.opened = True

    def close(self):
        self.closed = True

    def probe_identity(self, commands):
        _FakePace.last_commands = list(commands)
        rows = []
        for command in commands:
            if command == "*CLS":
                rows.append({"command": "*CLS", "response": "", "duration_ms": 1.0, "error": ""})
            elif command == "*IDN?":
                rows.append(
                    {
                        "command": "*IDN?",
                        "response": "GE Druck,Pace5000 User Interface,3213201,02.00.07",
                        "duration_ms": 1.2,
                        "error": "",
                    }
                )
            elif command == ":INST:MOD?":
                rows.append(
                    {
                        "command": ":INST:MOD?",
                        "response": '-113,"Undefined header"',
                        "duration_ms": 1.3,
                        "error": "",
                    }
                )
            elif command == ":SYST:ECHO?":
                rows.append(
                    {
                        "command": ":SYST:ECHO?",
                        "response": '-113,"Undefined header"',
                        "duration_ms": 1.1,
                        "error": "",
                    }
                )
            elif command == ":SYST:ERR?":
                rows.append(
                    {
                        "command": ":SYST:ERR?",
                        "response": ':SYST:ERR 0,"No error"',
                        "duration_ms": 1.0,
                        "error": "",
                    }
                )
            else:
                rows.append({"command": command, "response": "", "duration_ms": 1.0, "error": ""})
        return rows


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
    assert "*CLS" not in result["commands"]
    assert result["state_changing_clear_executed"] is False
    assert Path(result["csv_path"]).exists()
    assert Path(result["json_path"]).exists()
    assert Path(result["io_path"]).exists()
    assert any(row["command"] == "*IDN?" for row in result["rows"])


def test_pace_identity_probe_default_does_not_include_cls() -> None:
    assert "*CLS" not in pace_identity_probe.DEFAULT_COMMANDS


def test_pace_identity_probe_clear_before_probe_opt_in_sends_cls(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(
        pace_identity_probe,
        "load_config",
        lambda _path: {
            "devices": {
                "pressure_controller": {
                    "port": "COM31",
                    "baud": 9600,
                    "timeout": 1.0,
                }
            }
        },
    )
    monkeypatch.setattr(pace_identity_probe, "Pace5000", _FakePace)

    result = pace_identity_probe.run_probe(
        config_path="configs/default_config.json",
        output_dir=str(tmp_path),
        clear_before_probe=True,
    )

    assert result["commands"][0] == "*CLS"
    assert _FakePace.last_commands[0] == "*CLS"


def test_pace_identity_probe_reports_state_changing_clear_executed(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(
        pace_identity_probe,
        "load_config",
        lambda _path: {
            "devices": {
                "pressure_controller": {
                    "port": "COM31",
                    "baud": 9600,
                    "timeout": 1.0,
                }
            }
        },
    )
    monkeypatch.setattr(pace_identity_probe, "Pace5000", _FakePace)

    default_result = pace_identity_probe.run_probe(
        config_path="configs/default_config.json",
        output_dir=str(tmp_path / "default"),
    )
    clear_result = pace_identity_probe.run_probe(
        config_path="configs/default_config.json",
        output_dir=str(tmp_path / "clear"),
        clear_before_probe=True,
    )

    assert default_result["state_changing_clear_executed"] is False
    assert clear_result["state_changing_clear_executed"] is True
