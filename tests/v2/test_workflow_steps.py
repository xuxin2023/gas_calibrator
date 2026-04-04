import json
from pathlib import Path

from gas_calibrator.v2.config import AppConfig
from gas_calibrator.v2.core.calibration_service import CalibrationPhase, CalibrationService
from gas_calibrator.v2.core.device_manager import DeviceManager
from gas_calibrator.v2.core.event_bus import EventType
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


def test_workflow_steps_publish_events(tmp_path: Path) -> None:
    points_path = _write_points_file(tmp_path)
    config = AppConfig.from_dict(
        {
            "devices": {
                "temperature_chamber": {"port": "COM1", "enabled": True},
                "gas_analyzers": [{"port": "COM2", "enabled": True}],
            },
            "workflow": {
                "sampling": {"count": 1, "interval_s": 0.0, "discard_first_n": 0},
                "precheck": {"enabled": True, "device_connection": True, "sensor_check": False, "pressure_leak_test": False},
            },
            "paths": {"points_excel": str(points_path)},
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
    events: list[EventType] = []
    for event_type in EventType:
        service.event_bus.subscribe(event_type, lambda event, bucket=events: bucket.append(event.type))

    service.start(str(points_path))

    assert service.wait(timeout=2.0) is True
    assert service.get_status().phase is CalibrationPhase.COMPLETED
    assert EventType.WORKFLOW_STARTED in events
    assert EventType.POINT_STARTED in events
    assert EventType.SAMPLE_COLLECTED in events
    assert EventType.WORKFLOW_COMPLETED in events
