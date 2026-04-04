import json
from pathlib import Path
import time

from gas_calibrator.v2.config import AppConfig
from gas_calibrator.v2.core.calibration_service import (
    CalibrationPhase,
    CalibrationService,
)
from gas_calibrator.v2.core.device_manager import DeviceManager
from gas_calibrator.v2.core.runners.route_run_result import RouteRunResult
from gas_calibrator.v2.core.stability_checker import StabilityResult, StabilityType


class FakeTemperatureChamber:
    def __init__(self) -> None:
        self.target = 20.0
        self.opened = False
        self.closed = False

    def open(self) -> None:
        self.opened = True

    def close(self) -> None:
        self.closed = True

    def selftest(self):
        return {"ok": True}

    def set_temp_c(self, value: float) -> None:
        self.target = value

    def start(self) -> None:
        return None

    def read_temp_c(self) -> float:
        return self.target


class FakeGasAnalyzer:
    def __init__(self) -> None:
        self.opened = False
        self.closed = False
        self.calls = 0

    def open(self) -> None:
        self.opened = True

    def close(self) -> None:
        self.closed = True

    def selftest(self):
        return {"ok": True}

    def fetch_all(self):
        self.calls += 1
        return {
            "data": {
                "co2_signal": 400.0 + self.calls,
                "h2o_signal": 10.0 + self.calls,
                "co2_ratio_f": 1.0 + self.calls * 0.01,
                "h2o_ratio_f": 0.2 + self.calls * 0.01,
                "pressure_kpa": 100.0,
                "chamber_temp_c": 25.0,
                "case_temp_c": 26.0,
                "ref_signal": 2000.0,
                "co2_ppm": 400.0 + self.calls,
                "h2o_mmol": 10.0 + self.calls,
                "temperature_c": 25.0,
                "pressure_hpa": 1000.0,
                "dewpoint_c": 5.0,
            }
        }


class FitReadyGasAnalyzer:
    def __init__(self) -> None:
        self.opened = False
        self.closed = False
        self.point_provider = None

    def open(self) -> None:
        self.opened = True

    def close(self) -> None:
        self.closed = True

    def selftest(self):
        return {"ok": True}

    def fetch_all(self):
        point = None if self.point_provider is None else self.point_provider()
        temp = 25.0 if point is None else float(point.temperature_c)
        pressure_hpa = 1000.0 if point is None or point.pressure_hpa is None else float(point.pressure_hpa)
        co2_target = 0.0 if point is None or point.co2_ppm is None else float(point.co2_ppm)
        humidity = 30.0 if point is None or point.humidity_pct is None else float(point.humidity_pct)
        route = "co2" if point is None else str(point.route)
        if route == "h2o":
            dewpoint = -8.0 + 0.35 * float(point.index)
            h2o_ratio = 0.25 + 0.008 * float(point.index) + 0.002 * temp + 0.0002 * pressure_hpa
            co2_ratio = 1.0 + 0.0001 * float(point.index)
        else:
            dewpoint = 1.5 + 0.05 * float(point.index)
            h2o_ratio = 0.15 + 0.0001 * float(point.index)
            co2_ratio = 1.0 + 0.0006 * co2_target + 0.002 * temp + 0.0003 * pressure_hpa
        return {
            "data": {
                "co2_signal": 1000.0 + float(point.index if point is not None else 0),
                "h2o_signal": 500.0 + float(point.index if point is not None else 0),
                "co2_ratio_f": co2_ratio,
                "h2o_ratio_f": h2o_ratio,
                "pressure_kpa": pressure_hpa / 10.0,
                "chamber_temp_c": temp + 0.2,
                "case_temp_c": temp + 0.8,
                "ref_signal": 2000.0 + float(point.index if point is not None else 0),
                "co2_ppm": co2_target,
                "h2o_mmol": humidity,
                "temperature_c": temp,
                "pressure_hpa": pressure_hpa,
                "dewpoint_c": dewpoint,
            }
        }


class ImmediateStabilityChecker:
    def __init__(self) -> None:
        self.calls: list[StabilityType] = []

    def wait_for_stability(self, stability_type, read_func, stop_event):
        self.calls.append(stability_type)
        value = read_func() if read_func is not None else None
        return StabilityResult(
            stability_type=stability_type,
            stable=True,
            readings=[] if value is None else [float(value)],
            range_value=0.0,
            tolerance=1.0,
            elapsed_s=0.0,
            window_s=0.0,
            timeout_s=1.0,
            sample_count=1 if value is not None else 0,
            last_value=None if value is None else float(value),
        )


class BlockingStabilityChecker:
    def wait_for_stability(self, stability_type, read_func, stop_event):
        while not stop_event.is_set():
            time.sleep(0.01)
        return StabilityResult(
            stability_type=stability_type,
            stable=False,
            readings=[],
            range_value=None,
            tolerance=1.0,
            elapsed_s=0.0,
            window_s=0.0,
            timeout_s=1.0,
            sample_count=0,
            last_value=None,
            stopped=True,
        )


def _write_points_file(tmp_path: Path, points: list[dict]) -> Path:
    path = tmp_path / "points.json"
    path.write_text(json.dumps({"points": points}), encoding="utf-8")
    return path


def _stub_route_services(service: CalibrationService, *, suppress_qc_export: bool = True) -> None:
    service._wait_co2_route_soak_before_seal = lambda point: True
    service.pressure_control_service.pressurize_and_hold = (
        lambda point, route="co2": type("PressureResult", (), {"ok": True})()
    )
    service.pressure_control_service.set_pressure_to_target = (
        lambda point, recovery_attempted=False: type("PressureResult", (), {"ok": True})()
    )
    service.pressure_control_service.wait_after_pressure_stable_before_sampling = (
        lambda point: type("PressureResult", (), {"ok": True})()
    )
    service.dewpoint_alignment_service.open_h2o_route_and_wait_ready = lambda point: True
    service.dewpoint_alignment_service.wait_dewpoint_alignment_stable = lambda point: True
    service.valve_routing_service.set_co2_route_baseline = lambda reason="": None
    service.valve_routing_service.set_valves_for_co2 = lambda point: None
    service.valve_routing_service.cleanup_co2_route = lambda reason="": None
    if suppress_qc_export:
        service.qc_service.export_qc_report = lambda: None


def _workflow_config(*, sampling_count: int = 2) -> dict:
    return {
        "sampling": {
            "count": sampling_count,
            "interval_s": 0.0,
            "discard_first_n": 0,
        },
        "precheck": {
            "enabled": True,
            "device_connection": True,
            "sensor_check": False,
            "pressure_leak_test": False,
        },
        "stability": {
            "temperature": {
                "analyzer_chamber_temp_enabled": False,
            }
        },
    }


def _make_service(points_path: Path, stability_checker) -> CalibrationService:
    config = AppConfig.from_dict(
        {
            "devices": {
                "temperature_chamber": {"port": "COM1", "enabled": True},
                "gas_analyzers": [{"port": "COM2", "enabled": True}],
            },
            "workflow": _workflow_config(sampling_count=2),
            "paths": {"points_excel": str(points_path)},
        }
    )
    device_manager = DeviceManager(config.devices)
    device_manager.register_device("temperature_chamber", FakeTemperatureChamber())
    device_manager.register_device("gas_analyzer_0", FakeGasAnalyzer())
    service = CalibrationService(
        config=config,
        device_manager=device_manager,
        stability_checker=stability_checker,
    )
    _stub_route_services(service)
    return service


def test_initial_status_is_idle(tmp_path: Path) -> None:
    points_path = _write_points_file(tmp_path, [])
    service = _make_service(points_path, ImmediateStabilityChecker())

    status = service.get_status()

    assert status.phase is CalibrationPhase.IDLE
    assert status.total_points == 0
    assert status.completed_points == 0
    assert service.is_running is False
    assert service.get_results() == []


def test_load_points_updates_status(tmp_path: Path) -> None:
    points_path = _write_points_file(
        tmp_path,
        [
            {"index": 1, "temperature_c": 25.0, "co2_ppm": 400.0, "pressure_hpa": 1000.0, "route": "co2"},
            {"index": 2, "temperature_c": 25.0, "humidity_pct": 35.0, "route": "h2o"},
        ],
    )
    service = _make_service(points_path, ImmediateStabilityChecker())

    count = service.load_points(str(points_path))
    status = service.get_status()

    assert count == 2
    assert status.total_points == 2
    assert "Loaded 2 calibration points" in status.message


def test_start_and_stop_service(tmp_path: Path) -> None:
    points_path = _write_points_file(
        tmp_path,
        [{"index": 1, "temperature_c": 25.0, "co2_ppm": 400.0, "route": "co2"}],
    )
    service = _make_service(points_path, BlockingStabilityChecker())

    service.start(str(points_path))
    time.sleep(0.05)
    assert service.is_running is True

    service.stop(wait=True, timeout=2.0)

    assert service.wait(timeout=0.5) is True
    assert service.get_status().phase is CalibrationPhase.STOPPED


def test_progress_callback_receives_state_updates(tmp_path: Path) -> None:
    points_path = _write_points_file(
        tmp_path,
        [
            {"index": 1, "temperature_c": 25.0, "humidity_pct": 30.0, "route": "h2o"},
            {"index": 2, "temperature_c": 25.0, "co2_ppm": 400.0, "route": "co2"},
        ],
    )
    checker = ImmediateStabilityChecker()
    service = _make_service(points_path, checker)
    phases: list[CalibrationPhase] = []

    service.set_progress_callback(lambda status: phases.append(status.phase))
    service.start(str(points_path))

    assert service.wait(timeout=2.0) is True

    final_status = service.get_status()
    assert final_status.phase is CalibrationPhase.COMPLETED
    assert final_status.progress == 1.0
    assert CalibrationPhase.INITIALIZING in phases
    assert CalibrationPhase.PRECHECK in phases
    assert CalibrationPhase.SAMPLING in phases
    assert CalibrationPhase.COMPLETED in phases
    assert StabilityType.TEMPERATURE in checker.calls


def test_sampling_results_are_recorded(tmp_path: Path) -> None:
    points_path = _write_points_file(
        tmp_path,
        [{"index": 1, "temperature_c": 25.0, "co2_ppm": 500.0, "pressure_hpa": 1010.0, "route": "co2"}],
    )
    service = _make_service(points_path, ImmediateStabilityChecker())

    service.start(str(points_path))
    assert service.wait(timeout=2.0) is True

    results = service.get_results()
    assert len(results) == 2
    assert results[0].analyzer_id == "gas_analyzer_0"
    assert results[0].point.index == 1
    assert results[0].co2_signal is not None
    assert results[0].h2o_signal is not None
    assert results[0].temperature_c == 25.0
    assert results[0].pressure_hpa == 1000.0
    assert results[0].dew_point_c == 5.0


def test_full_run_exports_ratio_poly_coefficient_report(tmp_path: Path) -> None:
    points: list[dict[str, object]] = []
    for index, co2_target in enumerate([0.0, 200.0, 400.0, 600.0, 800.0, 1000.0, 1200.0, 1400.0, 1600.0], start=1):
        points.append(
            {
                "index": index,
                "temperature_c": 20.0 + float(index % 3),
                "co2_ppm": co2_target,
                "pressure_hpa": 900.0 + float((index % 4) * 50.0),
                "route": "co2",
            }
        )
    for index in range(10, 19):
        points.append(
            {
                "index": index,
                "temperature_c": 10.0 + float(index % 4),
                "humidity_pct": 25.0 + float(index),
                "pressure_hpa": 850.0 + float((index % 3) * 40.0),
                "route": "h2o",
            }
        )

    points_path = _write_points_file(tmp_path, points)
    config = AppConfig.from_dict(
        {
            "devices": {
                "temperature_chamber": {"port": "COM1", "enabled": True},
                "gas_analyzers": [{"port": "COM2", "enabled": True}],
            },
            "workflow": {
                **_workflow_config(sampling_count=1),
            },
            "paths": {"points_excel": str(points_path), "output_dir": str(tmp_path / "out")},
            "coefficients": {
                "enabled": True,
                "auto_fit": True,
                "model": "ratio_poly_rt_p",
                "summary_columns": {
                    "co2": {"target": "ppm_CO2_Tank", "ratio": "R_CO2", "temperature": "Temp", "pressure": "BAR"},
                    "h2o": {"target": "ppm_H2O_Dew", "ratio": "R_H2O", "temperature": "Temp", "pressure": "BAR"},
                },
            },
        }
    )
    device_manager = DeviceManager(config.devices)
    analyzer = FitReadyGasAnalyzer()
    device_manager.register_device("temperature_chamber", FakeTemperatureChamber())
    device_manager.register_device("gas_analyzer_0", analyzer)
    service = CalibrationService(
        config=config,
        device_manager=device_manager,
        stability_checker=ImmediateStabilityChecker(),
    )
    _stub_route_services(service)
    analyzer.point_provider = lambda: service.session.current_point

    service.start(str(points_path))
    assert service.wait(timeout=5.0) is True

    status = service.get_status()
    output_files = service.get_output_files()
    assert status.completed_points <= status.total_points
    assert status.progress == 1.0
    assert any(path.endswith("calibration_coefficients.xlsx") for path in output_files)


def test_v2_replacement_contract_minimal_flow_persists_results_and_artifacts(tmp_path: Path) -> None:
    points: list[dict[str, object]] = []
    for index, co2_target in enumerate([0.0, 200.0, 400.0, 600.0, 800.0, 1000.0, 1200.0, 1400.0, 1600.0], start=1):
        points.append(
            {
                "index": index,
                "temperature_c": 20.0 + float(index % 3),
                "co2_ppm": co2_target,
                "pressure_hpa": 900.0 + float((index % 4) * 50.0),
                "route": "co2",
            }
        )
    for index in range(10, 19):
        points.append(
            {
                "index": index,
                "temperature_c": 10.0 + float(index % 4),
                "humidity_pct": 25.0 + float(index),
                "pressure_hpa": 850.0 + float((index % 3) * 40.0),
                "route": "h2o",
            }
        )

    points_path = _write_points_file(tmp_path, points)
    output_dir = tmp_path / "out"
    config = AppConfig.from_dict(
        {
            "devices": {
                "temperature_chamber": {"port": "COM1", "enabled": True},
                "gas_analyzers": [{"port": "COM2", "enabled": True}],
            },
            "workflow": {
                **_workflow_config(sampling_count=1),
            },
            "paths": {"points_excel": str(points_path), "output_dir": str(output_dir)},
            "coefficients": {
                "enabled": True,
                "auto_fit": True,
                "model": "ratio_poly_rt_p",
                "summary_columns": {
                    "co2": {"target": "ppm_CO2_Tank", "ratio": "R_CO2", "temperature": "Temp", "pressure": "BAR"},
                    "h2o": {"target": "ppm_H2O_Dew", "ratio": "R_H2O", "temperature": "Temp", "pressure": "BAR"},
                },
            },
        }
    )
    device_manager = DeviceManager(config.devices)
    analyzer = FitReadyGasAnalyzer()
    device_manager.register_device("temperature_chamber", FakeTemperatureChamber())
    device_manager.register_device("gas_analyzer_0", analyzer)
    service = CalibrationService(
        config=config,
        device_manager=device_manager,
        stability_checker=ImmediateStabilityChecker(),
    )
    _stub_route_services(service, suppress_qc_export=False)
    analyzer.point_provider = lambda: service.session.current_point

    service.start(str(points_path))
    assert service.wait(timeout=5.0) is True

    status = service.get_status()
    output_files = service.get_output_files()
    output_names = {Path(path).name for path in output_files}

    assert status.phase is CalibrationPhase.COMPLETED
    assert service.get_results()
    assert "summary.json" in output_names
    assert "manifest.json" in output_names
    assert "results.json" in output_names
    assert "qc_report.json" in output_names
    assert "qc_report.csv" in output_names
    assert "calibration_coefficients.xlsx" in output_names


def test_route_failure_does_not_produce_fake_completed_summary(tmp_path: Path, monkeypatch) -> None:
    points_path = _write_points_file(
        tmp_path,
        [{"index": 1, "temperature_c": 25.0, "co2_ppm": 400.0, "pressure_hpa": 1000.0, "route": "co2"}],
    )
    config = AppConfig.from_dict(
        {
            "devices": {
                "temperature_chamber": {"port": "COM1", "enabled": True},
                "gas_analyzers": [{"port": "COM2", "enabled": True}],
            },
            "workflow": {
                **_workflow_config(sampling_count=1),
            },
            "paths": {"points_excel": str(points_path), "output_dir": str(tmp_path / "out")},
        }
    )
    device_manager = DeviceManager(config.devices)
    device_manager.register_device("temperature_chamber", FakeTemperatureChamber())
    device_manager.register_device("gas_analyzer_0", FakeGasAnalyzer())
    service = CalibrationService(
        config=config,
        device_manager=device_manager,
        stability_checker=ImmediateStabilityChecker(),
    )

    monkeypatch.setattr(
        "gas_calibrator.v2.core.runners.temperature_group_runner.Co2RouteRunner.execute",
        lambda self: RouteRunResult(
            success=False,
            skipped_point_indices=[self.point.index],
            error="route sealing failed",
        ),
    )

    service.start(str(points_path))
    assert service.wait(timeout=2.0) is True

    status = service.get_status()
    summary_path = service.result_store.run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))

    assert status.phase is CalibrationPhase.ERROR
    assert status.completed_points == 0
    assert service.get_results() == []
    assert summary["status"]["phase"] == "error"
    assert summary["status"]["completed_points"] == 0
