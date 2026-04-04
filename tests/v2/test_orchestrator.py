import json
from pathlib import Path

from gas_calibrator.v2.config import AppConfig
from gas_calibrator.v2.core.calibration_service import CalibrationService
from gas_calibrator.v2.core import orchestrator as orchestrator_mod
from gas_calibrator.v2.core.device_manager import DeviceManager, DeviceStatus
from gas_calibrator.v2.core.models import CalibrationPoint
from gas_calibrator.v2.core.stability_checker import StabilityResult, StabilityType


class FakeTemperatureChamber:
    def open(self) -> None:
        return None

    def close(self) -> None:
        return None

    def selftest(self):
        return {"ok": True}

    def set_temp_c(self, value: float) -> None:
        self.value = value

    def start(self) -> None:
        return None

    def read_temp_c(self) -> float:
        return 25.0


class FakeGasAnalyzer:
    def open(self) -> None:
        return None

    def close(self) -> None:
        return None

    def selftest(self):
        return {"ok": True}

    def fetch_all(self):
        return {
            "data": {
                "co2_signal": 400.0,
                "h2o_signal": 10.0,
                "temperature_c": 25.0,
                "pressure_hpa": 1000.0,
                "dewpoint_c": 5.0,
            }
        }


class ImmediateStabilityChecker:
    def wait_for_stability(self, stability_type, read_func, stop_event):
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


def _write_points_file(tmp_path: Path) -> Path:
    path = tmp_path / "points.json"
    path.write_text(
        json.dumps({"points": [{"index": 1, "temperature_c": 25.0, "co2_ppm": 400.0, "route": "co2"}]}),
        encoding="utf-8",
    )
    return path


def _make_service(points_path: Path) -> CalibrationService:
    config = AppConfig.from_dict(
        {
            "devices": {
                "temperature_chamber": {"port": "COM1", "enabled": True},
                "gas_analyzers": [{"port": "COM2", "enabled": True}],
            },
            "workflow": {
                "sampling": {"count": 1, "interval_s": 0.0, "discard_first_n": 0},
                "precheck": {"enabled": True, "device_connection": True, "sensor_check": False, "pressure_leak_test": False},
                "stability": {"temperature": {"analyzer_chamber_temp_enabled": False}},
            },
            "paths": {"points_excel": str(points_path)},
        }
    )
    device_manager = DeviceManager(config.devices)
    device_manager.register_device("temperature_chamber", FakeTemperatureChamber())
    device_manager.register_device("gas_analyzer_0", FakeGasAnalyzer())
    return CalibrationService(
        config=config,
        device_manager=device_manager,
        stability_checker=ImmediateStabilityChecker(),
    )


def _disable_long_waits(service: CalibrationService) -> None:
    service.orchestrator._wait_co2_route_soak_before_seal = lambda point: True
    service.orchestrator.pressure_control_service.pressurize_and_hold = (
        lambda point, route="co2": type("R", (), {"ok": True})()
    )
    service.orchestrator.pressure_control_service.set_pressure_to_target = (
        lambda point: type("R", (), {"ok": True})()
    )
    service.orchestrator.pressure_control_service.wait_after_pressure_stable_before_sampling = (
        lambda point: type("R", (), {"ok": True})()
    )
    service.orchestrator.valve_routing_service.set_co2_route_baseline = lambda reason="": None
    service.orchestrator.valve_routing_service.set_valves_for_co2 = lambda point: None
    service.orchestrator.valve_routing_service.cleanup_co2_route = lambda reason="": None
    service.orchestrator._export_qc_report = lambda: None


def test_orchestrator_runs_points_and_records_results(tmp_path: Path) -> None:
    points_path = _write_points_file(tmp_path)
    service = _make_service(points_path)
    _disable_long_waits(service)
    service.load_points(str(points_path))
    service.session.start()
    service.orchestrator.reset_run_state()
    service.state_manager.prepare_run(len(service._points))
    service.state_manager.start()

    service.orchestrator.run(service._points, service._temperature_groups)
    service._run_finalization()

    assert len(service.get_results()) == 1
    assert service.get_output_files()


def test_orchestrator_initialization_runs_sensor_precheck_when_enabled(tmp_path: Path) -> None:
    points_path = _write_points_file(tmp_path)
    service = _make_service(points_path)
    service.config.workflow.sensor_precheck = {"enabled": True}
    calls: list[str] = []
    service.orchestrator.analyzer_fleet_service.apply_analyzer_setup = lambda: calls.append("analyzer_setup")
    service.orchestrator.analyzer_fleet_service.run_sensor_precheck = lambda: calls.append("sensor_precheck")

    service.orchestrator._run_initialization_impl()

    assert calls == ["analyzer_setup", "sensor_precheck"]


def test_orchestrator_runs_startup_pressure_precheck_before_temperature_groups(tmp_path: Path) -> None:
    points_path = _write_points_file(tmp_path)
    service = _make_service(points_path)
    _disable_long_waits(service)
    service.load_points(str(points_path))
    calls: list[str] = []
    service.orchestrator.pressure_control_service.run_startup_pressure_precheck = (
        lambda points: calls.append(f"startup:{len(points)}")
    )
    service._run_temperature_group = lambda points, next_group=None: calls.append(f"group:{len(points)}")

    service.orchestrator.run(service._points, service._temperature_groups)

    assert calls == ["startup:1", "group:1"]


def test_orchestrator_consumes_first_point_preseal_soak_once(tmp_path: Path, monkeypatch) -> None:
    points_path = _write_points_file(tmp_path)
    service = _make_service(points_path)
    service.config.workflow.stability.co2_route = {
        "first_point_preseal_soak_s": 0.1,
        "preseal_soak_s": 0.0,
    }
    service.orchestrator.reset_run_state()
    point = service._points[0] if service._points else CalibrationPoint(index=1, temperature_c=25.0, co2_ppm=400.0, route="co2")
    logs: list[str] = []
    now = {"value": 100.0}

    monkeypatch.setattr(orchestrator_mod.time, "time", lambda: now["value"])
    monkeypatch.setattr(
        orchestrator_mod.time,
        "sleep",
        lambda seconds: now.__setitem__("value", now["value"] + float(seconds)),
    )
    service.orchestrator._log = logs.append

    assert service.orchestrator.run_state.humidity.first_co2_route_soak_pending is True
    assert service.orchestrator._wait_co2_route_soak_before_seal(point) is True
    assert service.orchestrator.run_state.humidity.first_co2_route_soak_pending is False
    assert any("first gas-point flush" in message for message in logs)

    logs.clear()
    assert service.orchestrator._wait_co2_route_soak_before_seal(point) is True
    assert not any("first gas-point flush" in message for message in logs)


def test_orchestrator_initialization_logs_profile_disabled_devices_as_info(tmp_path: Path) -> None:
    points_path = _write_points_file(tmp_path)
    service = _make_service(points_path)
    logs: list[str] = []

    service.orchestrator._create_devices = lambda: None
    service.orchestrator.device_manager.open_all = lambda: {
        "humidity_generator": False,
        "gas_analyzer_0": True,
    }
    service.orchestrator.device_manager.get_status = (
        lambda name: DeviceStatus.DISABLED if name == "humidity_generator" else DeviceStatus.ONLINE
    )
    service.orchestrator.analyzer_fleet_service.apply_analyzer_setup = lambda: None
    service.orchestrator.analyzer_fleet_service.run_sensor_precheck = lambda: None
    service.orchestrator._configure_pressure_controller_in_limits = lambda: None
    service.orchestrator._log = logs.append

    service.orchestrator._run_initialization_impl()

    assert any("Devices skipped by profile: humidity_generator" in message for message in logs)
    assert not any("Device open warnings" in message for message in logs)


def test_orchestrator_precheck_logs_profile_disabled_devices_as_info(tmp_path: Path) -> None:
    points_path = _write_points_file(tmp_path)
    service = _make_service(points_path)
    logs: list[str] = []

    service.orchestrator.device_manager.health_check = lambda: {
        "humidity_generator": False,
        "gas_analyzer_0": True,
    }
    service.orchestrator.device_manager.get_status = (
        lambda name: DeviceStatus.DISABLED if name == "humidity_generator" else DeviceStatus.ONLINE
    )
    service.orchestrator._log = logs.append

    service.orchestrator._run_precheck_impl()

    assert any("Devices skipped by profile: humidity_generator" in message for message in logs)
    assert not any("Device precheck warnings" in message for message in logs)
