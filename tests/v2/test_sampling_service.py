from __future__ import annotations

import csv
from pathlib import Path
from types import SimpleNamespace
import threading
from statistics import mean

from gas_calibrator.v2.config import AppConfig
from gas_calibrator.v2.core.device_manager import DeviceManager
from gas_calibrator.v2.core.event_bus import EventBus
from gas_calibrator.v2.core.models import CalibrationPoint
from gas_calibrator.v2.core.orchestration_context import OrchestrationContext
from gas_calibrator.v2.core.result_store import ResultStore
from gas_calibrator.v2.core.run_logger import RunLogger
from gas_calibrator.v2.core.run_state import RunState
from gas_calibrator.v2.core.services import SamplingService
from gas_calibrator.v2.core.session import RunSession
from gas_calibrator.v2.core.stability_checker import StabilityChecker
from gas_calibrator.v2.core.state_manager import StateManager


class FakeGasAnalyzer:
    def __init__(self) -> None:
        self.snapshot = {
            "data": {
                "co2_ppm": 401.2,
                "h2o_mmol": 9.8,
                "co2_signal": 123.4,
                "h2o_signal": 56.7,
                "co2_ratio_f": 0.111,
                "h2o_ratio_f": 0.222,
                "ref_signal": 88.8,
                "pressure_kpa": 100.0,
                "chamber_temp_c": 24.8,
                "case_temp_c": 26.1,
            },
            "mode": 2,
        }
        self.snapshots: list[object] = []

    def fetch_all(self):
        if self.snapshots:
            next_item = self.snapshots.pop(0)
            if isinstance(next_item, Exception):
                raise next_item
            self.snapshot = dict(next_item)
        return dict(self.snapshot)


class FakePressureDevice:
    def read_pressure_hpa(self) -> float:
        return 998.5

    def status(self) -> dict[str, object]:
        return {
            "pressure_gauge_hpa": 998.5,
            "pressure_reference_status": "healthy",
        }


class FakeThermometer:
    def read_temp_c(self) -> float:
        return 24.4

    def read_current(self) -> dict[str, object]:
        return {
            "temp_c": 24.4,
            "thermometer_reference_status": "healthy",
        }


class FakeDewpointMeter:
    def fetch_all(self):
        return {"dewpoint_c": 5.1, "temp_c": 24.9, "rh_pct": 52.0}


class FakeTemperatureChamber:
    def read_temp_c(self) -> float:
        return 25.0

    def read_rh_pct(self) -> float:
        return 50.5


class FakeHumidityGenerator:
    def fetch_all(self):
        return {"data": {"temp_c": 25.0, "rh_pct": 50.0, "status": "stable"}}


def _config(tmp_path: Path) -> AppConfig:
    return AppConfig.from_dict(
        {
            "paths": {"output_dir": str(tmp_path)},
            "devices": {
                "pressure_controller": {"port": "COM1", "enabled": True},
                "dewpoint_meter": {"port": "COM2", "enabled": True},
                "humidity_generator": {"port": "COM3", "enabled": True},
                "temperature_chamber": {"port": "COM4", "enabled": True},
                "gas_analyzers": [{"port": "COM5", "enabled": True, "name": "ga01"}],
            },
            "workflow": {
                "sampling": {
                    "count": 1,
                    "stable_count": 1,
                    "interval_s": 0.0,
                    "quality": {"enabled": True, "max_span_co2_ppm": 10.0},
                }
            },
        }
    )


def _build_service(tmp_path: Path) -> tuple[SamplingService, OrchestrationContext, RunState, SimpleNamespace]:
    config = _config(tmp_path)
    session = RunSession(config)
    event_bus = EventBus()
    state_manager = StateManager(event_bus)
    result_store = ResultStore(tmp_path, session.run_id)
    run_logger = RunLogger(str(tmp_path), session.run_id)
    device_manager = DeviceManager(config.devices)
    device_manager.register_device("gas_analyzer_0", FakeGasAnalyzer())
    device_manager.register_device("pressure_controller", FakePressureDevice())
    device_manager.register_device("pressure_meter", FakePressureDevice())
    device_manager.register_device("thermometer", FakeThermometer())
    device_manager.register_device("dewpoint_meter", FakeDewpointMeter())
    device_manager.register_device("temperature_chamber", FakeTemperatureChamber())
    device_manager.register_device("humidity_generator", FakeHumidityGenerator())
    stability_checker = StabilityChecker(config.workflow.stability)
    stop_event = threading.Event()
    pause_event = threading.Event()
    pause_event.set()
    context = OrchestrationContext(
        config=config,
        session=session,
        state_manager=state_manager,
        event_bus=event_bus,
        result_store=result_store,
        run_logger=run_logger,
        device_manager=device_manager,
        stability_checker=stability_checker,
        stop_event=stop_event,
        pause_event=pause_event,
    )
    run_state = RunState()
    logs: list[str] = []
    disabled: list[tuple[list[str], str]] = []
    point = CalibrationPoint(index=1, temperature_c=25.0, co2_ppm=400.0, pressure_hpa=1000.0, route="co2")

    class Host(SimpleNamespace):
        def _cfg_get(self, path: str, default=None):
            node = {
                "workflow.sampling.stable_count": 1,
                "workflow.sampling.count": 1,
                "workflow.sampling.interval_s": 0.0,
                "workflow.sampling.quality": {"enabled": True, "max_span_co2_ppm": 10.0},
                "workflow.sensor_read_retry.retries": 1,
                "workflow.sensor_read_retry.delay_s": 0.0,
            }
            return node.get(path, default)

        def _collect_only_fast_path_enabled(self):
            return False

        def _first_method(self, device, method_names):
            for name in method_names:
                method = getattr(device, name, None)
                if callable(method):
                    return method
            return None

        def _point_timing(self, current_point, *, phase="", point_tag=""):
            return {"stability_time_s": 1.25, "total_time_s": 2.5}

        def _finish_point_timing(self, current_point, *, phase="", point_tag=""):
            return {"stability_time_s": 1.25, "total_time_s": 2.5}

        def _active_gas_analyzers(self):
            return [("ga01", device_manager.get_device("gas_analyzer_0"), config.devices.gas_analyzers[0])]

        def _all_gas_analyzers(self):
            return self._active_gas_analyzers()

        def _check_stop(self):
            if stop_event.is_set():
                raise RuntimeError("stop requested")

        def _disable_analyzers(self, labels, reason):
            disabled.append((list(labels), reason))

        def _device(self, *names):
            for name in names:
                device = device_manager.get_device(name)
                if device is not None:
                    return device
            return None

        def _append_result(self, result):
            result_store.save_sample(result)

        def _log(self, message):
            logs.append(message)

        def _as_float(self, value):
            if value is None:
                return None
            return float(value)

    host = Host(logs=logs, disabled=disabled, point=point)
    service = SamplingService(context, run_state, host=host)
    return service, context, run_state, host


def test_sampling_service_collects_and_persists_point_samples(tmp_path: Path) -> None:
    service, context, _, host = _build_service(tmp_path)
    point = host.point

    results = service.sample_point(point, phase="co2", point_tag="co2_tag")
    filtered = service.samples_for_point(point, phase="co2", point_tag="co2_tag")

    assert len(results) == 1
    assert len(filtered) == 1
    assert context.result_store.get_samples() == results
    assert results[0].point_phase == "co2"
    assert results[0].point_tag == "co2_tag"
    assert results[0].stability_time_s == 1.25
    assert results[0].total_time_s == 2.5
    assert results[0].pressure_gauge_hpa == 998.5
    assert results[0].pressure_reference_status == "healthy"
    assert results[0].thermometer_temp_c == 24.4
    assert results[0].thermometer_reference_status == "healthy"
    sample_row = context.run_logger.samples_path.read_text(encoding="utf-8")
    assert "ga01_frame_has_data" in sample_row
    assert "pressure_gauge_hpa" in sample_row
    assert "thermometer_temp_c" in sample_row
    assert "co2_tag" in sample_row
    assert any("Point 1 sampled:" in message for message in host.logs)

    context.run_logger.finalize()


def test_sampling_service_retries_analyzer_read_then_succeeds(tmp_path: Path) -> None:
    service, context, _, host = _build_service(tmp_path)
    point = host.point
    analyzer = context.device_manager.get_device("gas_analyzer_0")
    analyzer.snapshots = [
        RuntimeError("transient read failure"),
        {
            "data": {
                "co2_ppm": 402.5,
                "h2o_mmol": 9.9,
                "co2_signal": 120.0,
                "h2o_signal": 57.0,
                "co2_ratio_f": 0.113,
                "h2o_ratio_f": 0.223,
                "ref_signal": 89.1,
            }
        },
    ]

    results = service.sample_point(point, phase="co2", point_tag="retry_ok")

    assert len(results) == 1
    assert results[0].co2_ppm == 402.5
    assert any("Sensor read retry (analyzer ga01 batch read) 1/1: error=transient read failure" in message for message in host.logs)

    context.run_logger.finalize()


def test_sampling_service_logs_final_failure_when_retry_exhausted(tmp_path: Path) -> None:
    service, context, _, host = _build_service(tmp_path)
    point = host.point
    analyzer = context.device_manager.get_device("gas_analyzer_0")
    analyzer.snapshots = [
        RuntimeError("read failure one"),
        RuntimeError("read failure two"),
    ]

    results = service.sample_point(point, phase="co2", point_tag="retry_fail")
    sample_rows = context.run_logger.samples_path.read_text(encoding="utf-8")

    assert results == []
    assert "ga01_error" in sample_rows
    assert any("Sensor read failed (analyzer ga01 batch read) after 2 attempts: error=read failure two" in message for message in host.logs)

    context.run_logger.finalize()


def test_sampling_service_falls_back_to_first_usable_analyzer_when_primary_frame_is_bad(tmp_path: Path) -> None:
    service, context, _, host = _build_service(tmp_path)
    point = host.point
    primary = context.device_manager.get_device("gas_analyzer_0")
    primary.snapshot = {"data": {"status": "bad frame"}}

    secondary = FakeGasAnalyzer()
    secondary.snapshots = [
        {
            "data": {
                "co2_ppm": 401.0,
                "h2o_mmol": 9.7,
                "co2_signal": 121.0,
                "h2o_signal": 56.5,
                "co2_ratio_f": 0.112,
                "h2o_ratio_f": 0.221,
                "ref_signal": 88.5,
                "chamber_temp_c": 24.9,
                "case_temp_c": 26.0,
            }
        },
        {
            "data": {
                "co2_ppm": 402.0,
                "h2o_mmol": 10.1,
                "co2_signal": 122.0,
                "h2o_signal": 57.5,
                "co2_ratio_f": 0.114,
                "h2o_ratio_f": 0.223,
                "ref_signal": 89.0,
                "chamber_temp_c": 25.0,
                "case_temp_c": 26.2,
            }
        },
    ]
    context.device_manager.register_device("gas_analyzer_1", secondary)
    host._active_gas_analyzers = lambda: [
        ("ga01", context.device_manager.get_device("gas_analyzer_0"), None),
        ("ga02", context.device_manager.get_device("gas_analyzer_1"), None),
    ]
    host._all_gas_analyzers = host._active_gas_analyzers

    original_cfg_get = host._cfg_get
    host._cfg_get = lambda path, default=None: {
        "workflow.sampling.stable_count": 2,
        "workflow.sampling.count": 2,
        "workflow.sampling.quality": {
            "enabled": True,
            "max_span_co2_ppm": 5.0,
            "max_span_h2o_mmol": 1.0,
        },
    }.get(path, original_cfg_get(path, default))

    rows, batch_results = service.collect_sample_batch(
        point,
        count=2,
        interval_s=0.0,
        phase="co2",
        point_tag="fallback_batch",
    )
    quality_ok, spans = service.evaluate_sample_quality(rows)

    assert len(batch_results) == 2
    assert all(row.get("co2_ppm") is not None for row in rows)
    assert all(row.get("h2o_mmol") is not None for row in rows)
    assert all(row.get("co2_ppm") == row.get("ga02_co2_ppm") for row in rows)
    assert all(row.get("h2o_mmol") == row.get("ga02_h2o_mmol") for row in rows)
    assert all(row.get("pressure_gauge_hpa") == 998.5 for row in rows)
    assert all(row.get("pressure_reference_status") == "healthy" for row in rows)
    assert all(row.get("thermometer_temp_c") == 24.4 for row in rows)
    assert all(row.get("thermometer_reference_status") == "healthy" for row in rows)
    assert quality_ok is True
    assert spans["co2_ppm"] == 1.0
    assert round(float(spans["h2o_mmol"]), 6) == 0.4
    assert round(mean(float(row["co2_ppm"]) for row in rows), 6) == 401.5
    assert round(mean(float(row["h2o_mmol"]) for row in rows), 6) == 9.9

    secondary.snapshots = [
        {
            "data": {
                "co2_ppm": 401.0,
                "h2o_mmol": 9.7,
                "co2_signal": 121.0,
                "h2o_signal": 56.5,
                "co2_ratio_f": 0.112,
                "h2o_ratio_f": 0.221,
                "ref_signal": 88.5,
            }
        },
        {
            "data": {
                "co2_ppm": 402.0,
                "h2o_mmol": 10.1,
                "co2_signal": 122.0,
                "h2o_signal": 57.5,
                "co2_ratio_f": 0.114,
                "h2o_ratio_f": 0.223,
                "ref_signal": 89.0,
            }
        },
    ]
    host.logs.clear()
    results = service.sample_point(point, phase="co2", point_tag="fallback_logged")
    sample_rows = list(csv.DictReader(context.run_logger.samples_path.open("r", encoding="utf-8")))
    point_log = next(message for message in reversed(host.logs) if "Point 1 sampled:" in message)

    assert len(results) == 2
    assert "co2_mean=None" not in point_log
    assert "h2o_mean=None" not in point_log
    assert any(row["point_tag"] == "fallback_logged" and row["co2_ppm"] and row["h2o_mmol"] for row in sample_rows)

    context.run_logger.finalize()
