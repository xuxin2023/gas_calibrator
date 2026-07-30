from pathlib import Path

import gas_calibrator.v2.adapters.legacy_runner as legacy_runner
from gas_calibrator.v2.adapters.legacy_runner import create_runner
from gas_calibrator.v2.core.calibration_service import CalibrationService


def test_create_runner_returns_calibration_service_when_use_v2_enabled(tmp_path: Path) -> None:
    cfg = {
        "devices": {
            "temperature_chamber": {"port": "COM1", "enabled": True},
            "gas_analyzers": [{"port": "COM2", "enabled": True}],
        },
        "paths": {
            "output_dir": str(tmp_path / "out"),
            "points_excel": str(tmp_path / "points.json"),
        },
        "features": {
            "use_v2": True,
            "simulation_mode": True,
        },
    }

    runner = create_runner(cfg, devices={}, logger=None, use_v2=True)

    assert isinstance(runner, CalibrationService)
    assert runner.config.features.simulation_mode is True
    assert not hasattr(runner.config.features, "use_v2")
    assert runner._raw_cfg == cfg


def test_create_runner_keeps_v1_fallback_when_use_v2_is_false(monkeypatch) -> None:
    calls = []

    class LegacyRunner:
        def __init__(self, cfg, devices, logger) -> None:
            calls.append((cfg, devices, logger))

    monkeypatch.setattr(legacy_runner, "_get_legacy_runner", lambda: LegacyRunner)
    cfg = {"features": {"use_v2": True, "simulation_mode": True}}
    devices = {"legacy": object()}
    logger = object()

    runner = create_runner(cfg, devices=devices, logger=logger, use_v2=False)

    assert isinstance(runner, LegacyRunner)
    assert calls == [(cfg, devices, logger)]
