import json
from pathlib import Path

from gas_calibrator.v2 import create_calibration_service, run_calibration
from gas_calibrator.v2.core.calibration_service import CalibrationPhase
from gas_calibrator.v2.core.point_parser import PointFilter


def _write_points(tmp_path: Path) -> Path:
    points_path = tmp_path / "points.json"
    points_path.write_text(
        json.dumps(
            {
                "points": [
                    {
                        "index": 1,
                        "temperature": 25.0,
                        "humidity": 35.0,
                        "pressure": 1000.0,
                        "route": "h2o",
                    },
                    {
                        "index": 2,
                        "temperature": 25.0,
                        "co2": 400.0,
                        "pressure": 1005.0,
                        "route": "co2",
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    return points_path


def _write_config(tmp_path: Path, points_path: Path) -> Path:
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "devices": {
                    "pressure_controller": {"port": "SIM-PC", "enabled": True},
                    "pressure_meter": {"port": "SIM-PM", "enabled": True},
                    "dewpoint_meter": {"port": "SIM-DP", "enabled": True},
                    "humidity_generator": {"port": "SIM-HG", "enabled": True},
                    "temperature_chamber": {"port": "SIM-TC", "enabled": True},
                    "relay_a": {"port": "SIM-R1", "enabled": True},
                    "gas_analyzers": [{"port": "SIM-GA1", "enabled": True}],
                },
                "workflow": {
                    "sampling": {
                        "count": 2,
                        "interval_s": 0.0,
                        "discard_first_n": 0,
                    },
                    "precheck": {
                        "enabled": True,
                        "device_connection": True,
                        "pressure_leak_test": True,
                        "sensor_check": True,
                    },
                    "stability": {
                        "temperature": {"tol": 0.5, "window_s": 0.2, "timeout_s": 2.0},
                        "humidity": {"tol_dp": 0.5, "window_s": 0.2, "timeout_s": 2.0},
                        "pressure": {"tol_hpa": 1.0, "window_s": 0.2, "timeout_s": 2.0},
                        "signal": {"tol_pct": 1.0, "window_s": 0.2, "timeout_s": 2.0},
                    },
                },
                "paths": {
                    "points_excel": str(points_path),
                },
                "features": {
                    "simulation_mode": True,
                },
            }
        ),
        encoding="utf-8",
    )
    return config_path


def test_create_calibration_service_loads_points(tmp_path: Path) -> None:
    points_path = _write_points(tmp_path)
    config_path = _write_config(tmp_path, points_path)

    service = create_calibration_service(str(config_path), simulation_mode=True)

    status = service.get_status()
    assert status.total_points == 2
    assert service.point_parser is not None
    assert service.device_factory.simulation_mode is True


def test_create_calibration_service_applies_point_filter(tmp_path: Path) -> None:
    points_path = _write_points(tmp_path)
    config_path = _write_config(tmp_path, points_path)

    service = create_calibration_service(
        str(config_path),
        simulation_mode=True,
        point_filter=PointFilter(routes=["h2o"], max_points=1),
    )

    status = service.get_status()
    assert status.total_points == 1


def test_create_calibration_service_resolves_paths_relative_to_config(tmp_path: Path, monkeypatch) -> None:
    config_dir = tmp_path / "config_dir"
    config_dir.mkdir()
    points_path = _write_points(config_dir)
    config_path = config_dir / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "devices": {
                    "gas_analyzers": [{"port": "SIM-GA1", "enabled": True}],
                },
                "paths": {
                    "points_excel": "points.json",
                    "output_dir": "output",
                    "logs_dir": "logs",
                },
                "features": {
                    "simulation_mode": True,
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    service = create_calibration_service(str(Path("config_dir") / "config.json"), simulation_mode=True)

    assert Path(service.config.paths.points_excel) == points_path.resolve()
    assert Path(service.config.paths.output_dir) == (config_dir / "output").resolve()
    assert Path(service.config.paths.logs_dir) == (config_dir / "logs").resolve()
    assert service.get_status().total_points == 2


def test_create_calibration_service_attaches_raw_cfg_and_preserves_alias_fields(tmp_path: Path) -> None:
    points_path = _write_points(tmp_path)
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "devices": {
                    "pressure_gauge": {"port": "SIM-PG", "enabled": True, "dest_id": "01"},
                    "relay": {"port": "SIM-R1", "enabled": True},
                    "relay_8": {"port": "SIM-R2", "enabled": True},
                    "gas_analyzer": {"port": "SIM-GA1", "enabled": True, "device_id": "001"},
                },
                "workflow": {
                    "missing_pressure_policy": "carry_forward",
                    "collect_only": True,
                    "pressure": {"pressurize_timeout_s": 12.5},
                },
                "paths": {
                    "points_excel": str(points_path),
                },
                "features": {
                    "simulation_mode": True,
                },
            }
        ),
        encoding="utf-8",
    )

    service = create_calibration_service(str(config_path), simulation_mode=True)

    assert service._raw_cfg is not None
    assert service._raw_cfg["workflow"]["pressure"]["pressurize_timeout_s"] == 12.5
    assert service._raw_cfg["workflow"]["collect_only"] is True
    assert service.config.devices.pressure_meter is not None
    assert service.config.devices.pressure_meter.port == "SIM-PG"
    assert service.config.devices.relay_a is not None
    assert service.config.devices.relay_b is not None
    assert len(service.config.devices.gas_analyzers) == 1
    assert service.config.workflow.missing_pressure_policy == "carry_forward"


def test_run_calibration_in_simulation_mode(tmp_path: Path) -> None:
    points_path = _write_points(tmp_path)
    config_path = _write_config(tmp_path, points_path)
    phases: list[CalibrationPhase] = []
    logs: list[str] = []

    results = run_calibration(
        str(config_path),
        simulation_mode=True,
        on_progress=lambda status: phases.append(status.phase),
        on_log=logs.append,
    )

    assert len(results) == 4
    assert CalibrationPhase.COMPLETED in phases
    assert results[0].analyzer_id == "gas_analyzer_0"
    assert results[0].temperature_c is not None
