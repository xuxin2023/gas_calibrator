from pathlib import Path

from gas_calibrator.v2.config import AppConfig
from gas_calibrator.v2.core.models import CalibrationPhase, CalibrationPoint
from gas_calibrator.v2.core.session import RunSession


def _config() -> AppConfig:
    return AppConfig.from_dict(
        {
            "devices": {
                "pressure_controller": {"port": "COM1", "enabled": True},
                "relay_a": {"port": "COM2", "enabled": False},
                "gas_analyzers": [{"port": "COM3", "enabled": True}],
            },
            "paths": {"output_dir": "logs"},
        }
    )


def test_session_initializes_run_context() -> None:
    session = RunSession(_config())

    assert session.run_id.startswith("run_")
    assert session.phase is CalibrationPhase.IDLE
    assert session.output_dir == Path("logs") / session.run_id
    assert session.enabled_devices == {"pressure_controller", "gas_analyzer_0"}


def test_session_start_end_and_to_dict() -> None:
    session = RunSession(_config())
    session.start()
    session.phase = CalibrationPhase.SAMPLING
    session.current_point = CalibrationPoint(index=1, temperature_c=25.0, route="co2")
    session.add_warning("warn")
    session.add_error("err")
    session.end("manual stop")

    payload = session.to_dict()

    assert payload["phase"] == "sampling"
    assert payload["stop_reason"] == "manual stop"
    assert payload["warnings"] == ["warn"]
    assert payload["errors"] == ["err"]
    assert payload["current_point"]["index"] == 1
