from __future__ import annotations

import json
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from gas_calibrator.v2.exceptions import WorkflowValidationError
from gas_calibrator.v2.core import orchestrator as orchestrator_module
from gas_calibrator.v2.core.no_write_guard import build_no_write_guard_from_raw_config
from gas_calibrator.v2.core.models import CalibrationPoint
from gas_calibrator.v2.core.orchestrator import WorkflowOrchestrator
from gas_calibrator.v2.core.run_state import RunState
from gas_calibrator.v2.core.run001_a2_no_write import (
    A2_AUTHORIZED_PRESSURE_POINTS_HPA,
    RUN001_FAIL,
    RUN001_NOT_EXECUTED,
    RUN001_PASS,
    _build_co2_route_conditioning_evidence,
    _final_safe_stop_evidence,
    _finalize_artifact_decision,
    _build_pressure_read_latency_diagnostics,
    build_run001_a2_evidence_payload,
    evaluate_run001_a2_readiness,
    write_run001_a2_artifacts,
)
from gas_calibrator.v2.core.runners.co2_route_runner import Co2RouteRunner
from gas_calibrator.v2.core.services.pressure_control_service import PressureControlService
from gas_calibrator.v2.core.services.temperature_control_service import TemperatureControlService
from gas_calibrator.v2.core.services.valve_routing_service import ValveRoutingService


def _truth_row(port: str, device_id: str) -> dict:
    return {
        "port": port,
        "configured_port": port,
        "read_only": True,
        "commands_sent": [],
        "port_open": True,
        "bytes_received": 100,
        "raw_frame_count": 4,
        "mode1_frame_count": 0,
        "mode2_frame_count": 4,
        "active_send_detected": True,
        "stable_device_id": device_id,
    }


def _a2_points() -> list[dict]:
    return [
        {
            "index": index + 1,
            "temperature_c": 20.0,
            "pressure_hpa": pressure,
            "route": "co2",
            "co2_ppm": 100.0,
            "co2_group": "A",
            "cylinder_nominal_ppm": 100.0,
        }
        for index, pressure in enumerate(A2_AUTHORIZED_PRESSURE_POINTS_HPA)
    ]


def _a2_raw_config(truth_path: str) -> dict:
    return {
        "run001_a2": {
            "mode": "real_machine_dry_run",
            "scope": "run001_a2_co2_no_write_pressure_sweep",
            "guard_scope": "run001_a2",
            "no_write": True,
            "co2_only": True,
            "single_route": True,
            "single_temperature_group": True,
            "allow_real_route": True,
            "allow_real_pressure": True,
            "allow_real_wait": True,
            "allow_real_sample": True,
            "allow_artifact": True,
            "allow_write_coefficients": False,
            "allow_write_zero": False,
            "allow_write_span": False,
            "allow_write_calibration_parameters": False,
            "default_cutover_to_v2": False,
            "disable_v1": False,
            "full_h2o_co2_group": False,
            "authorized_pressure_points_hpa": A2_AUTHORIZED_PRESSURE_POINTS_HPA,
            "mode2_truth_audit_path": truth_path,
        },
        "devices": {
            "dewpoint_meter": {"enabled": False},
            "humidity_generator": {"enabled": False},
            "gas_analyzers": [
                {"name": "ga01", "enabled": True, "port": "COM35", "device_id": "001"},
                {"name": "ga02", "enabled": True, "port": "COM37", "device_id": "029"},
                {"name": "ga03", "enabled": True, "port": "COM41", "device_id": "003"},
                {"name": "ga04", "enabled": True, "port": "COM42", "device_id": "004"},
            ],
        },
        "workflow": {
            "route_mode": "co2_only",
            "skip_co2_ppm": [0],
            "selected_temps_c": [20.0],
        },
    }


class _FakePressureController:
    def __init__(self) -> None:
        self.vent_on = True
        self.vent_status = 1
        self.output_state = 0
        self.isolation_state = 1
        self.pressure_hpa = 1009.5
        self.port = "COM31"
        self.setpoints: list[float] = []

    def set_output(self, enabled: bool) -> bool:
        self.output_state = 1 if enabled else 0
        return True

    def enable_control_output(self) -> bool:
        self.output_state = 1
        return True

    def disable_control_output(self) -> bool:
        self.output_state = 0
        return True

    def set_isolation_open(self, opened: bool) -> bool:
        self.isolation_state = 1 if opened else 0
        return True

    def vent(self, enabled: bool) -> bool:
        self.vent_on = bool(enabled)
        self.vent_status = 1 if enabled else 0
        return True

    def get_vent_status(self) -> int:
        return self.vent_status

    def get_output_state(self) -> int:
        return self.output_state

    def get_isolation_state(self) -> int:
        return self.isolation_state

    def get_atmosphere_mode(self) -> bool:
        return self.vent_on

    def set_setpoint(self, pressure_hpa: float) -> bool:
        self.setpoints.append(float(pressure_hpa))
        return True

    def set_pressure_hpa(self, pressure_hpa: float) -> bool:
        return self.set_setpoint(pressure_hpa)

    def read_pressure_hpa(self) -> float:
        return float(self.pressure_hpa)


class _QueuedPressureGauge:
    def __init__(self, values: list[float]) -> None:
        self.values = list(values)
        self.last = self.values[-1] if self.values else 1010.0
        self.port = "COM30"
        self.continuous_active = False
        self.continuous_mode = ""
        self.continuous_sequence_id = 0
        self.blocking_read_count = 0
        self.fast_read_count = 0
        self.normal_read_count = 0
        self.continuous_read_count = 0
        self.continuous_return_none = False
        self.fast_read_error: Exception | None = None
        self.normal_read_error: Exception | None = None

    def read_pressure_hpa(self) -> float:
        self.blocking_read_count += 1
        self.normal_read_count += 1
        if self.values:
            self.last = float(self.values.pop(0))
        return float(self.last)

    def read_pressure(self, **_kwargs) -> float:
        self.normal_read_count += 1
        if self.normal_read_error is not None:
            raise self.normal_read_error
        self.blocking_read_count += 1
        if self.values:
            self.last = float(self.values.pop(0))
        return float(self.last)

    def read_pressure_fast(self, **_kwargs) -> float:
        self.fast_read_count += 1
        if self.fast_read_error is not None:
            raise self.fast_read_error
        if self.values:
            self.last = float(self.values.pop(0))
        return float(self.last)

    def pressure_continuous_active(self) -> bool:
        return bool(self.continuous_active)

    def start_pressure_continuous(self, mode: str = "P4", clear_buffer: bool = True) -> bool:
        self.continuous_active = True
        self.continuous_mode = str(mode or "P4").upper()
        if clear_buffer:
            self.continuous_sequence_id = 0
        return True

    def stop_pressure_continuous(self) -> bool:
        self.continuous_active = False
        return True

    def read_pressure_continuous_latest(self, drain_s: float = 0.0, read_timeout_s: float = 0.0) -> dict:
        self.continuous_read_count += 1
        if self.continuous_return_none:
            return None
        if self.values:
            self.last = float(self.values.pop(0))
        self.continuous_sequence_id += 1
        return {
            "pressure_hpa": float(self.last),
            "source": "digital_pressure_gauge_continuous",
            "raw_line": f"{self.continuous_mode or 'P4'} {float(self.last):.3f}",
            "monotonic_timestamp": time.monotonic(),
            "sequence_id": self.continuous_sequence_id,
        }


class _TraceStatus:
    def __init__(self) -> None:
        self.rows: list[dict] = []

    def record_route_trace(self, **kwargs) -> None:
        self.rows.append(kwargs)


def _high_pressure_sample(pressure: float, *, stale: bool = False, sequence: int = 1) -> dict:
    return {
        "stage": "high_pressure_first_point_prearm",
        "point_index": 1,
        "pressure_hpa": float(pressure),
        "pressure_sample_source": "digital_pressure_gauge_continuous",
        "source": "digital_pressure_gauge_continuous",
        "request_sent_at": "2026-04-26T10:00:00+00:00",
        "response_received_at": "2026-04-26T10:00:00.010000+00:00",
        "request_sent_monotonic_s": 100.0,
        "response_received_monotonic_s": 100.01,
        "read_latency_s": 0.01,
        "sample_recorded_at": "2026-04-26T10:00:00.010000+00:00",
        "sample_recorded_monotonic_s": 100.01,
        "sample_age_s": 0.0 if not stale else 3.0,
        "pressure_sample_age_s": 0.0 if not stale else 3.0,
        "is_stale": stale,
        "pressure_sample_is_stale": stale,
        "sequence_id": sequence,
        "pressure_sample_sequence_id": sequence,
        "parse_ok": True,
        "usable_for_abort": not stale,
        "usable_for_ready": not stale,
        "usable_for_seal": not stale,
        "primary_pressure_source": "digital_pressure_gauge_continuous",
        "pressure_source_used_for_decision": "digital_pressure_gauge_continuous" if not stale else "",
        "pressure_source_used_for_abort": "digital_pressure_gauge_continuous" if not stale else "",
        "pressure_source_used_for_ready": "digital_pressure_gauge_continuous" if not stale else "",
        "pressure_source_used_for_seal": "digital_pressure_gauge_continuous" if not stale else "",
        "source_selection_reason": "digital_gauge_continuous_latest_fresh",
        "digital_gauge_mode": "continuous",
        "digital_gauge_continuous_active": True,
        "digital_gauge_continuous_enabled": True,
        "digital_gauge_continuous_mode": "P4",
        "latest_frame_age_s": 0.0 if not stale else 3.0,
        "latest_frame_sequence_id": sequence,
        "critical_window_uses_latest_frame": True,
        "critical_window_uses_query": False,
        "critical_window_blocking_query_count": 0,
        "critical_window_blocking_query_total_s": 0.0,
        "digital_gauge_pressure_sample": {
            "pressure_hpa": float(pressure),
            "pressure_sample_source": "digital_pressure_gauge_continuous",
            "source": "digital_pressure_gauge_continuous",
            "request_sent_at": "2026-04-26T10:00:00+00:00",
            "response_received_at": "2026-04-26T10:00:00.010000+00:00",
            "request_sent_monotonic_s": 100.0,
            "response_received_monotonic_s": 100.01,
            "read_latency_s": 0.01,
            "sample_age_s": 0.0 if not stale else 3.0,
            "is_stale": stale,
            "pressure_sample_is_stale": stale,
            "parse_ok": True,
            "digital_gauge_mode": "continuous",
            "latest_frame_age_s": 0.0 if not stale else 3.0,
            "latest_frame_sequence_id": sequence,
        },
        "pace_pressure_sample": {
            "pressure_hpa": None,
            "pressure_sample_source": "pace_controller",
            "source": "pace_controller",
            "parse_ok": False,
            "error": "pace_cross_check_deferred_for_high_pressure_first_point",
        },
        "high_pressure_first_point_mode": True,
    }


def _high_pressure_orchestrator(sample: dict, *, cfg_overrides: dict | None = None):
    orchestrator = WorkflowOrchestrator.__new__(WorkflowOrchestrator)
    values = {
        "workflow.pressure.high_pressure_first_point_mode_enabled": True,
        "workflow.pressure.high_pressure_first_point_margin_hpa": 0.0,
        "workflow.pressure.primary_pressure_source": "digital_pressure_gauge",
        "workflow.pressure.pressure_source_cross_check_enabled": True,
        "workflow.pressure.pressure_read_latency_warn_s": 0.5,
        "workflow.pressure.preseal_ready_pressure_hpa": 1110.0,
        "workflow.pressure.preseal_abort_pressure_hpa": 1150.0,
    }
    values.update(cfg_overrides or {})
    timing_events: list[dict] = []
    route_traces: list[dict] = []
    remembered: list[dict] = []
    orchestrator._cfg_get = lambda path, default=None: values.get(path, default)
    orchestrator._as_float = lambda value: None if value in (None, "") else float(value)
    orchestrator._workflow_timing_enabled = lambda: True
    orchestrator._workflow_no_write_guard_active = lambda: True
    orchestrator._record_workflow_timing = lambda event_name, event_type="info", **kwargs: timing_events.append(
        {"event_name": event_name, "event_type": event_type, **kwargs}
    )
    orchestrator.status_service = SimpleNamespace(
        record_route_trace=lambda **kwargs: route_traces.append(kwargs),
        log=lambda message: None,
    )
    orchestrator.pressure_control_service = SimpleNamespace(
        _current_high_pressure_first_point_sample=lambda **kwargs: {**sample, **kwargs},
        _remember_ambient_reference_pressure=lambda *args, **kwargs: remembered.append(
            {"args": args, "kwargs": kwargs}
        ),
    )
    return orchestrator, timing_events, route_traces, remembered


def _positive_preseal_service(
    pressures: list[float],
    *,
    cfg_overrides: dict | None = None,
) -> tuple[PressureControlService, SimpleNamespace, _FakePressureController, _TraceStatus]:
    controller = _FakePressureController()
    gauge = _QueuedPressureGauge(pressures)
    status = _TraceStatus()
    values = {
        "workflow.pressure.positive_preseal_pressurization_enabled": True,
        "workflow.pressure.preseal_ready_pressure_hpa": 1110.0,
        "workflow.pressure.preseal_abort_pressure_hpa": 1150.0,
        "workflow.pressure.preseal_ready_timeout_s": 0.3,
        "workflow.pressure.preseal_pressure_poll_interval_s": 0.05,
        "workflow.pressure.fail_if_sealed_pressure_below_target": True,
        "workflow.pressure.sealed_pressure_min_margin_hpa": 0.0,
        "workflow.pressure.vent_transition_timeout_s": 1.0,
        "workflow.pressure.continuous_atmosphere_hold": False,
        "workflow.pressure.vent_hold_interval_s": 2.0,
        "workflow.pressure.stabilize_timeout_s": 0.2,
        "workflow.pressure.restabilize_retries": 0,
        "workflow.pressure.digital_gauge_continuous_enabled": True,
        "workflow.pressure.digital_gauge_continuous_mode": "P4",
        "workflow.pressure.digital_gauge_stream_first_frame_timeout_s": 0.2,
        "workflow.pressure.digital_gauge_stream_poll_interval_s": 0.01,
        "workflow.pressure.digital_gauge_latest_frame_stale_max_s": 0.5,
        "workflow.pressure.critical_pressure_latest_frame_stale_max_s": 0.5,
        "workflow.pressure.pace_aux_enabled": True,
        "workflow.pressure.pace_aux_read_when_digital_fresh": False,
        "workflow.pressure.pace_aux_disagreement_warn_hpa": 10.0,
        "workflow.pressure.pace_aux_main_line_topology_connected": True,
    }
    values.update(cfg_overrides or {})
    host = SimpleNamespace()
    host.run_state = RunState()
    host.status_service = status
    host._cfg_get = lambda path, default=None: values.get(path, default)
    host._as_float = lambda value: None if value is None else float(value)
    host._log = lambda message: None
    host._check_stop = lambda: None
    host._capture_preseal_dewpoint_snapshot = lambda: None
    host._apply_valve_states = lambda open_valves: {}
    host._make_pressure_reader = lambda: gauge.read_pressure_hpa
    host._recorded_timing = []
    host._record_workflow_timing = lambda event_name, event_type="info", **kwargs: host._recorded_timing.append(
        {"event_name": event_name, "event_type": event_type, **kwargs}
    )

    def device(*names):
        if "pressure_controller" in names or "pace" in names:
            return controller
        if "pressure_meter" in names or "pressure_gauge" in names:
            return gauge
        return None

    def call_first(device_obj, method_names, *args, **kwargs):
        for method_name in method_names:
            method = getattr(device_obj, method_name, None)
            if callable(method):
                return method(*args, **kwargs)
        return None

    service = PressureControlService(SimpleNamespace(), host.run_state, host=host)
    host._device = device
    host._call_first = call_first
    host._set_pressure_controller_vent = lambda vent_on, reason="", wait_after_command=True: service.set_pressure_controller_vent(
        vent_on,
        reason=reason,
        wait_after_command=wait_after_command,
    )
    host._enable_pressure_controller_output = lambda reason="": service.enable_pressure_controller_output(reason=reason)
    return service, host, controller, status


def _seed_digital_stream_latest(
    service: PressureControlService,
    pressure_hpa: float,
    *,
    age_s: float = 0.0,
    sequence: int = 1,
) -> dict:
    recorded_mono = time.monotonic() - float(age_s)
    frame = service._pressure_sample_payload(
        {
            "pressure_hpa": float(pressure_hpa),
            "source": "digital_pressure_gauge_continuous",
            "pressure_sample_source": "digital_pressure_gauge_continuous",
            "sample_recorded_monotonic_s": recorded_mono,
            "pressure_sample_monotonic_s": recorded_mono,
            "monotonic_timestamp": recorded_mono,
            "sample_age_s": float(age_s),
            "pressure_sample_age_s": float(age_s),
            "raw_line": f"P4 {float(pressure_hpa):.3f}",
            "sequence_id": sequence,
            "pressure_sample_sequence_id": sequence,
            "digital_gauge_mode": "continuous",
            "digital_gauge_continuous_enabled": True,
            "digital_gauge_continuous_active": True,
            "digital_gauge_continuous_mode": "P4",
        },
        source="digital_pressure_gauge_continuous",
        is_cached=False,
        stale_threshold_s=0.5,
    )
    state = service._digital_gauge_stream_state()
    state.update(
        {
            "digital_gauge_continuous_enabled": True,
            "digital_gauge_continuous_active": True,
            "digital_gauge_continuous_mode": "P4",
            "stream_started_at": "2026-04-26T10:00:00+00:00",
            "stream_first_frame_at": "2026-04-26T10:00:00+00:00",
            "stream_frame_count": 1,
            "latest_frame": dict(frame),
            "continuous_unavailable_reason": "",
        }
    )
    return frame


def test_a2_continuous_latest_fast_snapshot_is_metadata_only() -> None:
    service, host, _controller, _status = _positive_preseal_service(
        [1010.0],
        cfg_overrides={"workflow.pressure.continuous_latest_fresh_budget_ms": 10.0},
    )
    gauge = host._device("pressure_meter")
    _seed_digital_stream_latest(service, 1009.5, age_s=0.01, sequence=3)

    snapshot = service.digital_gauge_continuous_latest_fast_snapshot(
        stage="co2_route_conditioning_at_atmosphere",
        point_index=1,
        budget_ms=10.0,
    )

    assert snapshot["pressure_source_selection_reason"] == "digital_gauge_continuous_latest_fresh"
    assert snapshot["continuous_latest_fresh_fast_path_used"] is True
    assert snapshot["continuous_latest_fresh_duration_ms"] <= 10.0
    assert snapshot["continuous_latest_fresh_lock_timeout"] is False
    assert snapshot["continuous_latest_fresh_waited_for_frame"] is False
    assert snapshot["continuous_latest_fresh_performed_io"] is False
    assert snapshot["continuous_latest_fresh_triggered_stream_restart"] is False
    assert snapshot["continuous_latest_fresh_triggered_drain"] is False
    assert snapshot["continuous_latest_fresh_triggered_p3_fallback"] is False
    assert snapshot["continuous_latest_fresh_budget_exceeded"] is False
    assert gauge.continuous_read_count == 0
    assert gauge.fast_read_count == 0
    assert gauge.normal_read_count == 0
    assert host._recorded_timing == []


def test_a2_continuous_latest_fast_snapshot_lock_timeout_defers_without_io() -> None:
    service, host, _controller, _status = _positive_preseal_service(
        [1010.0],
        cfg_overrides={"workflow.pressure.continuous_latest_fresh_budget_ms": 1.0},
    )
    gauge = host._device("pressure_meter")
    _seed_digital_stream_latest(service, 1009.5, age_s=0.01, sequence=3)

    class TimeoutLock:
        def acquire(self, *args, **kwargs):
            return False

        def release(self):
            raise AssertionError("release must not be called when acquire returns false")

    service._digital_gauge_stream_lock_obj = TimeoutLock()

    snapshot = service.digital_gauge_continuous_latest_fast_snapshot(
        stage="co2_route_conditioning_at_atmosphere",
        point_index=1,
        budget_ms=1.0,
    )

    assert snapshot["pressure_source_selected"] == ""
    assert snapshot["pressure_source_selection_reason"] == "continuous_latest_fresh_lock_timeout"
    assert snapshot["continuous_latest_fresh_lock_timeout"] is True
    assert snapshot["continuous_latest_fresh_waited_for_frame"] is False
    assert snapshot["continuous_latest_fresh_performed_io"] is False
    assert snapshot["continuous_latest_fresh_triggered_stream_restart"] is False
    assert snapshot["continuous_latest_fresh_triggered_drain"] is False
    assert snapshot["continuous_latest_fresh_triggered_p3_fallback"] is False
    assert gauge.continuous_read_count == 0
    assert gauge.fast_read_count == 0
    assert gauge.normal_read_count == 0
    assert host._recorded_timing == []


def test_a2_continuous_latest_fast_snapshot_stale_does_not_restart_or_fallback() -> None:
    service, host, _controller, _status = _positive_preseal_service([1010.0])
    gauge = host._device("pressure_meter")
    _seed_digital_stream_latest(service, 1009.5, age_s=5.0, sequence=3)

    snapshot = service.digital_gauge_continuous_latest_fast_snapshot(
        stage="co2_route_conditioning_at_atmosphere",
        point_index=1,
        budget_ms=10.0,
    )

    assert snapshot["pressure_source_selected"] == ""
    assert snapshot["pressure_source_selection_reason"] == "digital_gauge_continuous_latest_stale"
    assert snapshot["latest_frame_stale"] is True
    assert snapshot["continuous_latest_fresh_waited_for_frame"] is False
    assert snapshot["continuous_latest_fresh_performed_io"] is False
    assert snapshot["continuous_latest_fresh_triggered_stream_restart"] is False
    assert snapshot["continuous_latest_fresh_triggered_p3_fallback"] is False
    assert gauge.continuous_read_count == 0
    assert gauge.fast_read_count == 0
    assert gauge.normal_read_count == 0


def test_dual_pressure_sample_records_source_latency_and_age() -> None:
    service, _host, controller, _status = _positive_preseal_service(
        [1010.0],
        cfg_overrides={
            "workflow.pressure.primary_pressure_source": "digital_pressure_gauge",
            "workflow.pressure.pressure_source_cross_check_enabled": True,
            "workflow.pressure.pressure_source_disagreement_warn_hpa": 1.0,
            "workflow.pressure.pressure_sample_stale_threshold_s": 2.0,
        },
    )
    controller.pressure_hpa = 1005.0

    sample = service._current_dual_pressure_sample(stage="preseal_atmosphere_flush_hold", point_index=1)

    assert sample["pressure_sample_source"] == "digital_pressure_gauge"
    assert sample["pressure_source_used_for_abort"] == "digital_pressure_gauge"
    assert sample["digital_gauge_pressure_hpa"] == 1010.0
    assert sample["pace_pressure_hpa"] == 1005.0
    assert sample["digital_gauge_latency_s"] is not None
    assert sample["digital_gauge_age_s"] is not None
    assert sample["pressure_source_disagreement_warning"] is True
    assert sample["usable_for_abort"] is True
    assert sample["usable_for_ready"] is True
    assert sample["usable_for_seal"] is True


def test_stale_pressure_sample_is_not_usable_for_abort_ready_or_seal() -> None:
    service, _host, _controller, _status = _positive_preseal_service([1010.0])

    sample = service._pressure_sample_payload(
        {
            "pressure_hpa": 1200.0,
            "pressure_sample_source": "digital_pressure_gauge",
            "pressure_sample_age_s": 5.0,
        },
        source="digital_pressure_gauge",
    )

    assert sample["pressure_sample_is_stale"] is True
    assert sample["usable_for_abort"] is False
    assert sample["usable_for_ready"] is False
    assert sample["usable_for_seal"] is False


def test_a2_high_pressure_first_point_starts_continuous_stream_and_uses_latest_frame() -> None:
    service, host, _controller, _status = _positive_preseal_service(
        [1009.0],
        cfg_overrides={"workflow.pressure.pace_aux_enabled": False},
    )
    host._a2_high_pressure_first_point_mode_enabled = True

    sample = service._current_high_pressure_first_point_sample(
        stage="high_pressure_first_point_prearm",
        point_index=1,
    )
    gauge = host._device("pressure_gauge")
    stop_event = getattr(service, "_digital_gauge_continuous_stop_event", None)
    if stop_event is not None:
        stop_event.set()

    assert gauge.continuous_active is True
    assert gauge.continuous_mode == "P4"
    assert gauge.continuous_read_count >= 1
    assert gauge.blocking_read_count == 0
    assert sample["pressure_sample_source"] == "digital_pressure_gauge_continuous"
    assert sample["critical_window_uses_latest_frame"] is True
    assert sample["critical_window_uses_query"] is False
    assert sample["pressure_source_used_for_ready"] == "digital_pressure_gauge_continuous"
    assert sample["latest_frame_age_s"] <= 0.5


def test_a2_high_pressure_first_point_stale_latest_frame_not_usable_for_decisions() -> None:
    service, host, _controller, _status = _positive_preseal_service(
        [],
        cfg_overrides={"workflow.pressure.pace_aux_enabled": False},
    )
    host._a2_high_pressure_first_point_mode_enabled = True
    _seed_digital_stream_latest(service, 1111.0, age_s=1.0)

    sample = service._current_high_pressure_first_point_sample(
        stage="high_pressure_first_point",
        point_index=1,
    )

    assert sample["pressure_hpa"] == 1111.0
    assert sample["pressure_sample_is_stale"] is True
    assert sample["usable_for_ready"] is False
    assert sample["usable_for_seal"] is False
    assert sample["usable_for_abort"] is False
    assert sample["pressure_source_used_for_ready"] == ""
    assert sample["pressure_source_used_for_abort"] == ""
    assert sample["source_selection_reason"] == "digital_latest_unusable_fail_closed"


def test_a2_high_pressure_first_point_continuous_unavailable_fails_closed() -> None:
    service, host, controller, status = _positive_preseal_service(
        [],
        cfg_overrides={
            "workflow.pressure.digital_gauge_continuous_enabled": False,
            "workflow.pressure.preseal_ready_timeout_s": 0.05,
        },
    )
    controller.vent_on = False
    controller.vent_status = 0
    host._a2_high_pressure_first_point_mode_enabled = True
    host._a2_high_pressure_first_point_vent_preclosed = True
    point = CalibrationPoint(index=1, temperature_c=20.0, co2_ppm=100.0, pressure_hpa=1100.0, route="co2")

    result = service.pressurize_and_hold(point, route="co2")

    assert result.ok is False
    assert result.diagnostics["seal_command_blocked_reason"] == "critical_pressure_sample_unavailable"
    assert not any(row["action"] == "seal_route" for row in status.rows)
    assert any(event["event_name"] == "digital_gauge_latest_frame_stale" for event in host._recorded_timing)


def test_a2_high_pressure_pace_auxiliary_can_trigger_when_digital_stale_and_consistent() -> None:
    service, host, controller, _status = _positive_preseal_service(
        [],
        cfg_overrides={
            "workflow.pressure.pace_aux_enabled": True,
            "workflow.pressure.pace_aux_disagreement_warn_hpa": 5.0,
        },
    )
    host._a2_high_pressure_first_point_mode_enabled = True
    controller.pressure_hpa = 1111.0
    _seed_digital_stream_latest(service, 1110.5, age_s=1.0)

    sample = service._current_high_pressure_first_point_sample(
        stage="high_pressure_first_point",
        point_index=1,
    )

    assert sample["source"] == "pace_controller_auxiliary"
    assert sample["pressure_source_used_for_ready"] == "pace_controller_auxiliary"
    assert sample["pace_aux_trigger_candidate"] is True
    assert sample["source_selection_reason"] == "digital_latest_stale_pace_aux_consistent"


def test_a2_high_pressure_pace_auxiliary_disagreement_does_not_replace_digital() -> None:
    service, host, controller, _status = _positive_preseal_service(
        [],
        cfg_overrides={
            "workflow.pressure.pace_aux_enabled": True,
            "workflow.pressure.pace_aux_disagreement_warn_hpa": 5.0,
        },
    )
    host._a2_high_pressure_first_point_mode_enabled = True
    controller.pressure_hpa = 1200.0
    _seed_digital_stream_latest(service, 1110.0, age_s=1.0)

    sample = service._current_high_pressure_first_point_sample(
        stage="high_pressure_first_point",
        point_index=1,
    )

    assert sample["source"] != "pace_controller_auxiliary"
    assert sample["pace_aux_trigger_candidate"] is False
    assert sample["pressure_source_used_for_ready"] == ""
    assert sample["pressure_source_disagreement_warning"] is True
    assert sample["source_selection_reason"] == "digital_latest_stale_pace_aux_disagreement"


def test_a2_critical_window_blocking_digital_query_is_counted() -> None:
    service, host, _controller, _status = _positive_preseal_service([1010.0])
    host._a2_high_pressure_first_point_mode_enabled = True

    sample = service._pressure_sample_from_device("digital_pressure_gauge")
    snapshot = service.digital_gauge_continuous_stream_snapshot()

    assert sample["pressure_hpa"] == 1010.0
    assert snapshot["blocking_query_count_in_critical_window"] == 1
    assert snapshot["critical_window_blocking_query_total_s"] >= 0.0
    assert any(event["event_name"] == "critical_window_blocking_query" for event in host._recorded_timing)


def test_a2_conditioning_continuous_stream_stale_restart_recovers_fresh_frame() -> None:
    service, host, _controller, _status = _positive_preseal_service(
        [1019.5],
        cfg_overrides={
            "workflow.pressure.a2_conditioning_restart_continuous_on_stale": True,
            "workflow.pressure.a2_conditioning_continuous_restart_fresh_timeout_s": 0.2,
            "workflow.pressure.pace_aux_enabled": False,
        },
    )
    _seed_digital_stream_latest(service, 1018.0, age_s=1.0, sequence=3)

    try:
        sample = service._current_high_pressure_first_point_sample(
            stage="co2_route_conditioning_at_atmosphere",
            point_index=1,
        )
    finally:
        stop_event = getattr(service, "_digital_gauge_continuous_stop_event", None)
        if stop_event is not None:
            stop_event.set()

    gauge = host._device("pressure_gauge")
    assert sample["pressure_sample_is_stale"] is False
    assert sample["continuous_restart_attempted"] is True
    assert sample["continuous_restart_result"] == "recovered"
    assert sample["pressure_source_used_for_abort"] == "digital_pressure_gauge_continuous"
    assert gauge.continuous_read_count >= 1
    assert any(event["event_name"] == "digital_gauge_stream_restart_result" for event in host._recorded_timing)


def test_a2_conditioning_continuous_stream_stale_restart_failure_stays_fail_closed() -> None:
    service, host, _controller, _status = _positive_preseal_service(
        [],
        cfg_overrides={
            "workflow.pressure.a2_conditioning_restart_continuous_on_stale": True,
            "workflow.pressure.a2_conditioning_continuous_restart_fresh_timeout_s": 0.05,
            "workflow.pressure.digital_gauge_stream_first_frame_timeout_s": 0.05,
            "workflow.pressure.pace_aux_enabled": False,
        },
    )
    gauge = host._device("pressure_gauge")
    gauge.continuous_return_none = True
    _seed_digital_stream_latest(service, 1018.0, age_s=1.0, sequence=3)

    try:
        sample = service._current_high_pressure_first_point_sample(
            stage="co2_route_conditioning_at_atmosphere",
            point_index=1,
        )
    finally:
        stop_event = getattr(service, "_digital_gauge_continuous_stop_event", None)
        if stop_event is not None:
            stop_event.set()

    assert sample["pressure_sample_is_stale"] is True
    assert sample["continuous_restart_attempted"] is True
    assert sample["continuous_restart_result"] == "failed"
    assert sample["pressure_source_used_for_abort"] == ""
    assert any(
        event["event_name"] == "digital_gauge_stream_restart_result" and event["event_type"] == "fail"
        for event in host._recorded_timing
    )


def test_a2_conditioning_p3_query_while_continuous_active_is_marked_as_may_cancel() -> None:
    service, host, _controller, _status = _positive_preseal_service([1010.2])
    _seed_digital_stream_latest(service, 1010.0, age_s=0.1, sequence=2)

    sample = service._pressure_sample_from_device("digital_pressure_gauge")
    snapshot = service.digital_gauge_continuous_stream_snapshot()

    assert sample["pressure_hpa"] == 1010.2
    assert sample["last_pressure_command"] == "read_pressure"
    assert sample["last_pressure_command_may_cancel_continuous"] is True
    assert sample["continuous_interrupted_by_command"] is True
    assert snapshot["continuous_interrupted_by_command"] is True
    assert any(event["event_name"] == "digital_gauge_continuous_command_may_cancel" for event in host._recorded_timing)


def test_a2_high_pressure_first_point_mode_enables_when_1100_exceeds_ambient() -> None:
    orchestrator, timing_events, route_traces, remembered = _high_pressure_orchestrator(
        _high_pressure_sample(1009.0)
    )
    point = CalibrationPoint(index=1, temperature_c=20.0, co2_ppm=100.0, pressure_hpa=1100.0, route="co2")
    pressure_points = [
        CalibrationPoint(index=index + 1, temperature_c=20.0, co2_ppm=100.0, pressure_hpa=pressure, route="co2")
        for index, pressure in enumerate(A2_AUTHORIZED_PRESSURE_POINTS_HPA)
    ]

    context = orchestrator._prearm_a2_high_pressure_first_point_mode(point, pressure_points)

    assert context["enabled"] is True
    assert context["first_target_pressure_hpa"] == 1100.0
    assert context["ambient_reference_pressure_hpa"] == 1009.0
    assert context["trigger_reason"] == "first_target_above_ambient_reference"
    assert getattr(orchestrator, "_a2_high_pressure_first_point_mode_enabled") is True
    assert remembered
    assert any(event["event_name"] == "pressure_polling_prearmed" for event in timing_events)
    assert route_traces[-1]["action"] == "high_pressure_first_point_mode_enabled"


def test_a2_high_pressure_first_point_rejects_stale_baseline_sample() -> None:
    orchestrator, timing_events, _route_traces, _remembered = _high_pressure_orchestrator(
        _high_pressure_sample(1009.0, stale=True)
    )
    point = CalibrationPoint(index=1, temperature_c=20.0, co2_ppm=100.0, pressure_hpa=1100.0, route="co2")
    pressure_points = [
        CalibrationPoint(index=index + 1, temperature_c=20.0, co2_ppm=100.0, pressure_hpa=pressure, route="co2")
        for index, pressure in enumerate(A2_AUTHORIZED_PRESSURE_POINTS_HPA)
    ]

    with pytest.raises(WorkflowValidationError) as exc_info:
        orchestrator._prearm_a2_high_pressure_first_point_mode(point, pressure_points)

    details = exc_info.value.context
    assert details["high_pressure_first_point_prearm_started"] is True
    assert details["high_pressure_first_point_prearm_blocked"] is True
    assert details["high_pressure_first_point_prearm_block_reason"] == "baseline_pressure_sample_stale"
    assert details["baseline_pressure_sample_age_s"] == 3.0
    assert details["baseline_pressure_freshness_ok"] is False
    assert details["baseline_pressure_stale_reason"]
    assert any(
        event["event_name"] == "pressure_polling_prearmed" and event["event_type"] == "fail"
        for event in timing_events
    )


def test_a2_high_pressure_first_point_uses_latest_route_conditioning_baseline_when_close_to_atmosphere() -> None:
    stale = _high_pressure_sample(1014.508, stale=True)
    stale.update(
        {
            "pressure_source_selected": "",
            "pressure_source_selection_reason": "digital_latest_stale_pace_aux_disagreement",
            "source_selection_reason": "digital_latest_stale_pace_aux_disagreement",
            "pace_pressure_sample": {
                "pressure_hpa": 3.336,
                "pressure_sample_source": "pace_controller",
                "source": "pace_controller",
                "parse_ok": True,
            },
        }
    )
    orchestrator, _timing_events, _route_traces, remembered = _high_pressure_orchestrator(
        stale,
        cfg_overrides={"workflow.pressure.a2_conditioning_pressure_source": "v1_aligned"},
    )
    now = time.monotonic()
    orchestrator._a2_co2_route_conditioning_completed = True
    orchestrator._a2_co2_route_conditioning_completed_at = "2026-04-30T14:00:00+00:00"
    orchestrator._a2_co2_route_conditioning_at_atmosphere_context = {
        "route_conditioning_phase": "ready_to_seal_phase",
        "a2_conditioning_pressure_source_strategy": "v1_aligned",
        "selected_pressure_source_for_conditioning_monitor": "digital_pressure_gauge_p3",
        "latest_route_conditioning_pressure_hpa": 1007.819,
        "latest_route_conditioning_pressure_source": "digital_pressure_gauge_p3",
        "latest_route_conditioning_pressure_recorded_monotonic_s": now - 0.2,
        "measured_atmospheric_pressure_hpa": 1007.817,
        "route_conditioning_pressure_before_route_open_hpa": 1007.817,
        "route_conditioning_pressure_overlimit": False,
        "route_conditioning_hard_abort_exceeded": False,
        "seal_command_sent": False,
        "pressure_setpoint_command_sent": False,
        "sampling_started": False,
        "sample_count": 0,
        "points_completed": 0,
    }
    point = CalibrationPoint(index=1, temperature_c=20.0, co2_ppm=100.0, pressure_hpa=1100.0, route="co2")
    pressure_points = [
        CalibrationPoint(index=index + 1, temperature_c=20.0, co2_ppm=100.0, pressure_hpa=pressure, route="co2")
        for index, pressure in enumerate(A2_AUTHORIZED_PRESSURE_POINTS_HPA)
    ]

    context = orchestrator._prearm_a2_high_pressure_first_point_mode(point, pressure_points)

    assert context["enabled"] is True
    assert context["baseline_pressure_hpa"] == 1007.819
    assert context["baseline_pressure_source"] == "digital_pressure_gauge_p3"
    assert context["baseline_pressure_freshness_ok"] is True
    assert context["baseline_pressure_stale_reason"] == ""
    assert context["prearm_pressure_source_expected"] == "v1_aligned"
    assert context["prearm_pressure_source_disagreement"] is True
    assert context["prearm_pressure_source_alignment_ok"] is True
    assert context["prearm_primary_source_disagreement"] is False
    assert context["prearm_aux_source_disagreement"] is True
    assert context["prearm_aux_source_disagreement_nonblocking"] is True
    assert context["prearm_aux_source_disagreement_reason"] == "digital_latest_stale_pace_aux_disagreement"
    assert context["v1_aligned_pressure_source_decision"] == (
        "latest_route_conditioning_pressure_selected_for_prearm_baseline"
    )
    assert context["latest_route_conditioning_pressure_eligible_for_prearm_baseline"] is True
    assert context["latest_route_conditioning_pressure_atmosphere_delta_hpa"] == 0.002
    assert remembered[0]["args"][0] == 1007.819


def test_a2_high_pressure_first_point_accepts_fresh_digital_baseline_with_nonblocking_pace_aux_disagreement() -> None:
    stale = _high_pressure_sample(1014.508, stale=True)
    stale.update(
        {
            "sample_age_s": 0.788,
            "pressure_sample_age_s": 0.788,
            "latest_frame_age_s": 0.788,
            "digital_gauge_pressure_sample": {
                **stale["digital_gauge_pressure_sample"],
                "sample_age_s": 0.788,
                "pressure_sample_age_s": 0.788,
                "latest_frame_age_s": 0.788,
            },
            "pressure_source_selected": "",
            "pressure_source_selection_reason": "digital_latest_stale_pace_aux_disagreement",
            "source_selection_reason": "digital_latest_stale_pace_aux_disagreement",
            "pace_pressure_sample": {
                "pressure_hpa": 3.4828198,
                "pressure_sample_source": "pace_controller",
                "source": "pace_controller",
                "parse_ok": True,
            },
        }
    )
    orchestrator, _timing_events, _route_traces, _remembered = _high_pressure_orchestrator(
        stale,
        cfg_overrides={"workflow.pressure.a2_conditioning_pressure_source": "v1_aligned"},
    )
    point = CalibrationPoint(index=1, temperature_c=20.0, co2_ppm=100.0, pressure_hpa=1100.0, route="co2")
    pressure_points = [
        CalibrationPoint(index=index + 1, temperature_c=20.0, co2_ppm=100.0, pressure_hpa=pressure, route="co2")
        for index, pressure in enumerate(A2_AUTHORIZED_PRESSURE_POINTS_HPA)
    ]

    details = orchestrator._prearm_a2_high_pressure_first_point_mode(point, pressure_points)

    assert details["enabled"] is True
    assert details["baseline_pressure_hpa"] == 1014.508
    assert details["baseline_pressure_sample_age_s"] == 0.788
    assert details["a2_prearm_baseline_freshness_max_s"] == 2.0
    assert details["baseline_pressure_freshness_ok"] is True
    assert details["baseline_primary_freshness_ok"] is True
    assert details["baseline_pressure_stale_reason"] == ""
    assert details["prearm_pressure_source_expected"] == "v1_aligned"
    assert details["prearm_pressure_source_observed"] == (
        "digital_pressure_gauge_continuous vs pace_controller"
    )
    assert details["prearm_pressure_source_disagreement"] is True
    assert details["prearm_pressure_source_disagreement_reason"] == (
        "digital_latest_stale_pace_aux_disagreement"
    )
    assert details["prearm_primary_source_disagreement"] is False
    assert details["prearm_aux_source_disagreement"] is True
    assert details["prearm_aux_source_disagreement_nonblocking"] is True
    assert details["baseline_aux_disagreement_nonblocking"] is True
    assert details["baseline_aux_disagreement_reason"] == "digital_latest_stale_pace_aux_disagreement"
    assert details["baseline_pressure_primary_source"] == "digital_pressure_gauge_continuous"
    assert details["baseline_pressure_aux_source"] == "pace_controller"
    assert details["pace_aux_absolute_pressure_comparable"] is False
    assert details["latest_route_conditioning_pressure_eligible_for_prearm_baseline"] is False
    assert details["latest_route_conditioning_pressure_ineligible_reason"] == (
        "route_conditioning_context_unavailable"
    )


def test_co2_conditioning_pressure_ready_does_not_trigger_handoff(monkeypatch) -> None:
    orchestrator, point, route_traces, timing_events, _vent_calls = _preseal_arm_orchestrator(
        monkeypatch,
        [1112.0],
    )
    orchestrator._a2_co2_route_conditioning_at_atmosphere_active = True

    decision = orchestrator._verify_co2_preseal_atmosphere_hold_pressure(point)

    assert decision == "conditioning_pressure_monitor_ok"
    assert not any(row["action"] == "preseal_atmosphere_flush_ready_handoff" for row in route_traces)
    assert not any(row["action"] == "preseal_vent_close_arm_triggered" for row in route_traces)
    assert orchestrator._a2_co2_route_conditioning_at_atmosphere_context["seal_command_sent"] is False
    assert orchestrator._a2_co2_route_conditioning_at_atmosphere_context["route_conditioning_phase"] == (
        "route_conditioning_flush_phase"
    )
    assert any(event["event_name"] == "co2_route_conditioning_pressure_sample" for event in timing_events)
    assert not any(event["event_name"] == "high_pressure_seal_command_sent" for event in timing_events)


def test_co2_conditioning_pressure_above_abort_remains_hard_fail(monkeypatch) -> None:
    orchestrator, point, route_traces, timing_events, _vent_calls = _preseal_arm_orchestrator(
        monkeypatch,
        [1155.0],
    )
    orchestrator._a2_co2_route_conditioning_at_atmosphere_active = True

    with pytest.raises(WorkflowValidationError):
        orchestrator._verify_co2_preseal_atmosphere_hold_pressure(point)

    assert route_traces[-1]["action"] == "co2_preseal_atmosphere_hold_pressure_guard"
    assert route_traces[-1]["actual"]["pressure_hpa"] == 1155.0
    assert route_traces[-1]["actual"]["fail_closed_reason"] == "route_conditioning_pressure_overlimit"
    assert route_traces[-1]["actual"]["vent_off_sent_at"] == ""
    assert route_traces[-1]["actual"]["seal_command_sent"] is False
    assert not any(row["action"] == "preseal_atmosphere_flush_ready_handoff" for row in route_traces)
    assert any(event["event_name"] == "co2_route_conditioning_pressure_warning" for event in timing_events)


def test_route_conditioning_high_pressure_seen_before_preseal_blocks_completion(monkeypatch) -> None:
    orchestrator, point, _clock, _timing_events, route_traces, _vent_calls = _conditioning_guard_orchestrator(
        monkeypatch,
        [{"pressure_hpa": 1009.0, "age_s": 0.1, "sequence_id": 1}],
    )
    orchestrator._a2_co2_route_conditioning_at_atmosphere_context.update(
        {
            "measured_atmospheric_pressure_hpa": 1009.0,
            "latest_route_conditioning_pressure_hpa": 1009.1,
            "latest_route_conditioning_pressure_source": "digital_pressure_gauge_continuous",
            "pressure_max_during_conditioning_hpa": 1154.0,
            "route_conditioning_peak_pressure_hpa": 1154.0,
            "route_conditioning_hard_abort_pressure_hpa": 1150.0,
        }
    )

    with pytest.raises(WorkflowValidationError) as exc_info:
        orchestrator._end_a2_co2_route_conditioning_at_atmosphere(point, route_soak_ok=True)

    details = exc_info.value.context
    assert details["route_conditioning_high_pressure_seen_before_preseal"] is True
    assert details["route_conditioning_high_pressure_seen_before_preseal_hpa"] == 1154.0
    assert details["route_conditioning_high_pressure_seen_decision"] == "fail_closed"
    assert details["fail_closed_reason"] == "route_conditioning_high_pressure_seen_before_preseal"
    assert details["vent_off_sent_at"] == ""
    assert details["seal_command_sent"] is False
    assert route_traces[-1]["action"] == "co2_route_conditioning_high_pressure_before_preseal"


def test_route_conditioning_not_returned_to_atmosphere_blocks_preseal(monkeypatch) -> None:
    orchestrator, point, _clock, _timing_events, route_traces, _vent_calls = _conditioning_guard_orchestrator(
        monkeypatch,
        [{"pressure_hpa": 1009.0, "age_s": 0.1, "sequence_id": 1}],
    )
    orchestrator._a2_co2_route_conditioning_at_atmosphere_context.update(
        {
            "measured_atmospheric_pressure_hpa": 1009.0,
            "latest_route_conditioning_pressure_hpa": 1120.0,
            "latest_route_conditioning_pressure_source": "digital_pressure_gauge_continuous",
            "pressure_max_during_conditioning_hpa": 1120.0,
            "route_conditioning_hard_abort_pressure_hpa": 1150.0,
        }
    )

    with pytest.raises(WorkflowValidationError) as exc_info:
        orchestrator._end_a2_co2_route_conditioning_at_atmosphere(point, route_soak_ok=True)

    details = exc_info.value.context
    assert details["route_conditioning_pressure_returned_to_atmosphere"] is False
    assert details["route_conditioning_atmosphere_stable_before_flush"] is False
    assert details["fail_closed_reason"] == "route_conditioning_not_atmosphere_stable_before_preseal"
    assert details["vent_off_sent_at"] == ""
    assert details["seal_command_sent"] is False
    assert route_traces[-1]["action"] == "co2_route_conditioning_not_atmosphere_stable"


def test_co2_conditioning_vent_tick_records_atmosphere_reassert(monkeypatch) -> None:
    orchestrator = WorkflowOrchestrator.__new__(WorkflowOrchestrator)
    timing_events: list[dict] = []
    monkeypatch.setattr(orchestrator_module.time, "sleep", lambda seconds: (_ for _ in ()).throw(AssertionError("vent tick must not sleep")))
    orchestrator._cfg_get = lambda path, default=None: {
        "workflow.pressure.vent_hold_interval_s": 2.0,
        "workflow.pressure.preseal_abort_pressure_hpa": 1150.0,
    }.get(path, default)
    orchestrator._as_float = lambda value: None if value in (None, "") else float(value)
    orchestrator._record_workflow_timing = lambda event_name, event_type="info", **kwargs: timing_events.append(
        {"event_name": event_name, "event_type": event_type, **kwargs}
    )
    orchestrator.pressure_control_service = SimpleNamespace(
        set_pressure_controller_vent=lambda *args, **kwargs: {
            "output_state": 0,
            "isolation_state": 1,
            "vent_status_raw": 1,
        },
        set_pressure_controller_vent_fast_reassert=lambda *args, **kwargs: {
            "fast_vent_reassert_supported": True,
            "fast_vent_reassert_used": True,
            "vent_command_write_started_at": "2026-04-28T00:00:00+00:00",
            "vent_command_write_sent_at": "2026-04-28T00:00:00+00:00",
            "vent_command_write_completed_at": "2026-04-28T00:00:00+00:00",
            "vent_command_write_started_monotonic_s": orchestrator_module.time.monotonic(),
            "vent_command_write_sent_monotonic_s": orchestrator_module.time.monotonic(),
            "vent_command_write_completed_monotonic_s": orchestrator_module.time.monotonic(),
            "vent_command_write_duration_ms": 0.0,
            "vent_command_total_duration_ms": 0.0,
            "vent_command_wait_after_command_s": 0.0,
            "vent_command_capture_pressure_enabled": False,
            "vent_command_query_state_enabled": False,
            "vent_command_confirm_transition_enabled": False,
            "vent_command_blocking_phase": "fast_vent_write",
            "route_conditioning_fast_vent_command_timeout": False,
            "route_conditioning_fast_vent_not_supported": False,
            "command_result": "ok",
            "command_error": "",
            "command_method": "test_fast_vent_reassert",
        },
        digital_gauge_continuous_stream_snapshot=lambda: {
            "stream_frame_count": 4,
            "latest_frame_age_s": 0.1,
            "latest_frame_sequence_id": 4,
            "latest_frame_stale": False,
            "latest_frame": {
                "pressure_hpa": 1009.5,
                "sample_age_s": 0.1,
                "sequence_id": 4,
                "latest_frame_interval_s": 0.05,
            },
        },
    )
    orchestrator._a2_co2_route_conditioning_at_atmosphere_context = {
        "conditioning_started_monotonic_s": orchestrator_module.time.monotonic(),
        "vent_ticks": [],
        "pressure_samples": [],
    }
    point = CalibrationPoint(index=1, temperature_c=20.0, co2_ppm=100.0, pressure_hpa=1100.0, route="co2")

    tick = orchestrator._record_a2_co2_conditioning_vent_tick(point, phase="conditioning_hold")

    assert tick["command_result"] == "ok"
    assert tick["fast_vent_reassert_used"] is True
    assert tick["vent_command_capture_pressure_enabled"] is False
    assert tick["vent_command_query_state_enabled"] is False
    assert tick["vent_command_wait_after_command_s"] == 0.0
    assert tick["output_state"] is None
    assert tick["isolation_state"] is None
    assert tick["vent_status"] is None
    assert tick["digital_gauge_pressure_hpa"] is None
    assert tick["pressure_sample_age_s"] is None
    assert tick["pressure_abnormal"] is False
    assert any(event["event_name"] == "co2_route_conditioning_vent_tick" for event in timing_events)


def test_co2_conditioning_fast_tick_does_not_call_legacy_slow_vent(monkeypatch) -> None:
    orchestrator, point, _clock, _timing_events, route_traces, vent_calls = _conditioning_guard_orchestrator(
        monkeypatch,
        [{"pressure_hpa": 1009.0, "age_s": 0.1, "sequence_id": 2}],
    )

    def legacy_slow_vent(*_args, **_kwargs):
        raise AssertionError("A2.5 fast vent tick must not call legacy blocking vent path")

    orchestrator.pressure_control_service.set_pressure_controller_vent = legacy_slow_vent

    tick = orchestrator._record_a2_co2_conditioning_vent_tick(point, phase="before_route_open")

    assert tick["fast_vent_reassert_used"] is True
    assert tick["vent_command_capture_pressure_enabled"] is False
    assert tick["vent_command_query_state_enabled"] is False
    assert tick["vent_command_wait_after_command_s"] == 0.0
    assert len(vent_calls) == 1
    assert route_traces == []


def test_co2_conditioning_flush_records_vent_pulse_drop_and_blocks_seal(monkeypatch) -> None:
    orchestrator, point, clock, _timing_events, route_traces, _vent_calls = _conditioning_guard_orchestrator(
        monkeypatch,
        [
            {"pressure_hpa": 1010.0, "age_s": 0.1, "sequence_id": 2},
            {"pressure_hpa": 1008.0, "age_s": 0.1, "sequence_id": 3},
        ],
    )

    first = orchestrator._record_a2_co2_conditioning_vent_tick(point, phase="before_route_open")
    first_monitor = orchestrator._record_a2_co2_conditioning_pressure_monitor(point, phase="conditioning_pressure_monitor")
    clock["now"] += 0.2
    orchestrator._a2_co2_route_conditioning_at_atmosphere_context["route_open_completed_monotonic_s"] = clock["now"]
    second = orchestrator._record_a2_co2_conditioning_vent_tick(point, phase="after_route_open")
    second_monitor = orchestrator._record_a2_co2_conditioning_pressure_monitor(point, phase="conditioning_pressure_monitor")

    assert first["vent_pulse_count"] == 1
    assert second["vent_pulse_count"] == 2
    assert first["pressure_drop_after_vent_hpa"] is None
    assert second["pressure_drop_after_vent_hpa"] is None
    assert second["route_open_to_first_vent_write_ms"] == 0.0
    assert second_monitor["route_open_to_first_pressure_read_ms"] == 0.0
    assert first_monitor["route_conditioning_pressure_before_route_open_hpa"] == 1010.0
    assert second_monitor["route_conditioning_pressure_after_route_open_hpa"] == 1008.0
    assert second["vent_off_blocked_during_flush"] is True
    assert second["seal_blocked_during_flush"] is True
    assert second["pressure_setpoint_blocked_during_flush"] is True
    assert second["sample_blocked_during_flush"] is True
    assert second["vent_off_command_sent"] is False
    assert second["seal_command_sent"] is False
    assert second["pressure_setpoint_command_sent"] is False
    assert route_traces == []


def test_co2_conditioning_begin_initializes_flush_phase_boundaries(monkeypatch) -> None:
    orchestrator, point, _clock, _timing_events, route_traces, _vent_calls = _conditioning_guard_orchestrator(
        monkeypatch,
        [{"pressure_hpa": 1009.0, "age_s": 0.1, "sequence_id": 2}],
    )
    orchestrator._a2_co2_route_conditioning_required = lambda point, pressure_points=None: True
    orchestrator._co2_conditioning_soak_s = lambda point: 180.0

    context = orchestrator._begin_a2_co2_route_conditioning_at_atmosphere(point, [point])

    assert context["route_conditioning_phase"] == "route_conditioning_flush_phase"
    assert context["ready_to_seal_phase_started"] is False
    assert context["route_conditioning_flush_min_time_completed"] is False
    assert context["vent_off_command_sent"] is False
    assert context["seal_command_sent"] is False
    assert context["pressure_setpoint_command_sent"] is False
    assert context["vent_pulse_count"] == 1
    assert route_traces == []


def test_co2_conditioning_flush_pressure_rise_triggers_repeat_vent_pulse(monkeypatch) -> None:
    orchestrator, point, clock, _timing_events, route_traces, vent_calls = _conditioning_guard_orchestrator(
        monkeypatch,
        [
            {"pressure_hpa": 1000.0, "age_s": 0.1, "sequence_id": 2},
            {"pressure_hpa": 1003.0, "age_s": 0.1, "sequence_id": 3},
            {"pressure_hpa": 1002.0, "age_s": 0.1, "sequence_id": 4},
        ],
        cfg_overrides={
            "workflow.pressure.atmosphere_vent_heartbeat_interval_s": 5.0,
            "workflow.pressure.atmosphere_vent_max_gap_s": 10.0,
            "workflow.pressure.route_conditioning_high_frequency_vent_interval_s": 5.0,
            "workflow.pressure.pressure_monitor_interval_s": 0.5,
            "workflow.pressure.route_conditioning_pressure_rise_vent_trigger_hpa": 2.0,
            "workflow.pressure.route_conditioning_pressure_rise_vent_min_interval_s": 0.1,
        },
    )
    orchestrator._a2_co2_route_conditioning_at_atmosphere_context["route_open_completed_monotonic_s"] = clock["now"]
    orchestrator._record_a2_co2_conditioning_vent_tick(point, phase="after_route_open")
    orchestrator._record_a2_co2_conditioning_pressure_monitor(point, phase="conditioning_pressure_monitor")
    clock["now"] += 0.6

    orchestrator._maybe_reassert_a2_conditioning_vent(point)

    context = orchestrator._a2_co2_route_conditioning_at_atmosphere_context
    assert len(vent_calls) == 2
    assert context["vent_ticks"][-1]["phase"] == "pressure_rise_vent_pulse"
    assert context["pressure_rise_since_last_vent_hpa"] == 3.0
    assert context["vent_pulse_count"] == 2
    assert route_traces == []


def test_co2_conditioning_high_frequency_window_keeps_vent_gap_under_one_second(monkeypatch) -> None:
    orchestrator, point, clock, _timing_events, route_traces, _vent_calls = _conditioning_guard_orchestrator(
        monkeypatch,
        [{"pressure_hpa": 1009.0, "age_s": 0.1, "sequence_id": 2}],
        cfg_overrides={
            "workflow.pressure.route_conditioning_high_frequency_vent_interval_s": 0.5,
            "workflow.pressure.route_conditioning_high_frequency_max_gap_s": 1.0,
            "workflow.pressure.route_conditioning_high_frequency_vent_window_s": 20.0,
            "workflow.pressure.route_conditioning_scheduler_sleep_step_s": 0.1,
        },
    )
    orchestrator._a2_co2_route_conditioning_at_atmosphere_context["route_open_completed_monotonic_s"] = clock["now"]
    orchestrator._record_a2_co2_conditioning_vent_tick(point, phase="after_route_open")

    deadline = clock["now"] + 20.0
    while clock["now"] < deadline:
        clock["now"] = round(clock["now"] + 0.1, 6)
        orchestrator._maybe_reassert_a2_conditioning_vent(point)

    context = orchestrator._a2_co2_route_conditioning_at_atmosphere_context
    assert context["vent_pulse_count"] >= 39
    assert context["max_vent_pulse_gap_ms"] <= 1000.0
    assert context["max_vent_pulse_write_gap_ms"] <= 1000.0
    assert context["route_conditioning_vent_gap_exceeded"] is False
    assert route_traces == []


def test_co2_conditioning_long_outer_wait_never_produces_nine_second_vent_gap(monkeypatch) -> None:
    orchestrator, point, _clock, _timing_events, route_traces, _vent_calls = _conditioning_guard_orchestrator(
        monkeypatch,
        [{"pressure_hpa": 1009.0, "age_s": 0.1, "sequence_id": 2}],
        cfg_overrides={
            "workflow.stability.co2_route.preseal_soak_s": 26.0,
            "workflow.pressure.route_conditioning_high_frequency_vent_interval_s": 0.5,
            "workflow.pressure.route_conditioning_high_frequency_max_gap_s": 1.0,
            "workflow.pressure.route_conditioning_high_frequency_vent_window_s": 20.0,
            "workflow.pressure.route_conditioning_vent_maintenance_interval_s": 1.0,
            "workflow.pressure.route_conditioning_vent_maintenance_max_gap_s": 2.0,
            "workflow.pressure.route_conditioning_scheduler_sleep_step_s": 0.1,
        },
    )
    orchestrator.run_state = RunState()
    orchestrator._collect_only_fast_path_enabled = lambda: False
    orchestrator._has_special_co2_zero_flush_pending = lambda: False
    orchestrator._is_zero_co2_point = lambda point: False
    orchestrator._first_co2_route_soak_pending = False
    orchestrator._post_h2o_co2_zero_flush_pending = False
    orchestrator._initial_co2_zero_flush_pending = False
    orchestrator._active_post_h2o_co2_zero_flush = False
    orchestrator._check_stop = lambda: None
    orchestrator._log = lambda message: None
    orchestrator._refresh_live_analyzer_snapshots = lambda **kwargs: False
    orchestrator._wait_co2_route_dewpoint_gate_before_seal = lambda *args, **kwargs: True
    orchestrator._a2_co2_route_conditioning_at_atmosphere_context["route_open_completed_monotonic_s"] = 100.0
    orchestrator._record_a2_co2_conditioning_vent_tick(point, phase="after_route_open")

    assert orchestrator._wait_co2_route_soak_before_seal(point) is True

    context = orchestrator._a2_co2_route_conditioning_at_atmosphere_context
    assert context["max_vent_pulse_gap_ms"] <= 2000.0
    assert context["max_vent_scheduler_loop_gap_ms"] <= 200.0
    assert context["route_conditioning_vent_gap_exceeded"] is False
    assert route_traces == []


def test_co2_conditioning_blocking_vent_tick_completion_gap_fails_closed(monkeypatch) -> None:
    orchestrator, point, clock, timing_events, route_traces, vent_calls = _conditioning_guard_orchestrator(
        monkeypatch,
        [
            {"pressure_hpa": 1009.0, "age_s": 0.1, "sequence_id": 2},
            {"pressure_hpa": 1009.1, "age_s": 0.1, "sequence_id": 3},
        ],
    )

    def blocking_fast_vent(vent_on, **kwargs):
        started = clock["now"]
        vent_calls.append({"vent_on": vent_on, **kwargs, "fast_reassert": True})
        clock["now"] += 0.8
        return {
            "fast_vent_reassert_supported": True,
            "fast_vent_reassert_used": True,
            "vent_command_write_started_at": "2026-04-28T00:00:00+00:00",
            "vent_command_write_sent_at": "2026-04-28T00:00:00+00:00",
            "vent_command_write_completed_at": "2026-04-28T00:00:00+00:00",
            "vent_command_write_started_monotonic_s": started,
            "vent_command_write_sent_monotonic_s": started,
            "vent_command_write_completed_monotonic_s": clock["now"],
            "vent_command_write_duration_ms": 800.0,
            "vent_command_total_duration_ms": 800.0,
            "vent_command_wait_after_command_s": 0.0,
            "vent_command_capture_pressure_enabled": False,
            "vent_command_query_state_enabled": False,
            "vent_command_confirm_transition_enabled": False,
            "vent_command_blocking_phase": "fast_vent_write",
            "route_conditioning_fast_vent_command_timeout": True,
            "route_conditioning_fast_vent_not_supported": False,
            "command_result": "timeout",
            "command_error": "route_conditioning_fast_vent_command_timeout",
        }

    orchestrator.pressure_control_service.set_pressure_controller_vent_fast_reassert = blocking_fast_vent

    with pytest.raises(WorkflowValidationError):
        orchestrator._record_a2_co2_conditioning_vent_tick(point, phase="before_route_open")

    context = orchestrator._a2_co2_route_conditioning_at_atmosphere_context
    assert context["pre_route_fast_vent_timeout"] is True
    assert context["route_conditioning_fast_vent_command_timeout"] is True
    assert context["route_conditioning_vent_gap_exceeded"] is False
    assert context["fail_closed_reason"] == "route_conditioning_fast_vent_command_timeout"
    assert route_traces[-1]["action"] == "co2_route_conditioning_fast_vent_command_timeout"
    assert any(event["event_name"] == "co2_route_conditioning_fast_vent_command_timeout" for event in timing_events)


def test_co2_conditioning_pressure_read_blocking_span_is_deferred_before_silent_vent_starvation(monkeypatch) -> None:
    orchestrator, point, clock, timing_events, route_traces, _vent_calls = _conditioning_guard_orchestrator(
        monkeypatch,
        [
            {"pressure_hpa": 1009.0, "age_s": 0.1, "sequence_id": 2},
            {"pressure_hpa": 1009.1, "age_s": 0.1, "sequence_id": 3},
            {"pressure_hpa": 1009.2, "age_s": 0.1, "sequence_id": 4},
        ],
    )
    blocking_reader_called = {"count": 0}

    def blocking_sample(**kwargs):
        blocking_reader_called["count"] += 1
        clock["now"] += 3.8
        raise AssertionError("high-frequency pressure monitor must not run blocking diagnostics")

    orchestrator._record_a2_co2_conditioning_vent_tick(point, phase="conditioning_hold")
    orchestrator._a2_co2_route_conditioning_at_atmosphere_context["route_open_completed_monotonic_s"] = clock["now"]
    clock["now"] += 0.4
    orchestrator.pressure_control_service._current_high_pressure_first_point_sample = blocking_sample
    monitor = orchestrator._record_a2_co2_conditioning_pressure_monitor(
        point,
        phase="conditioning_pressure_monitor",
    )

    context = orchestrator._a2_co2_route_conditioning_at_atmosphere_context
    assert blocking_reader_called["count"] == 0
    assert monitor["pressure_monitor_nonblocking"] is True
    assert monitor["critical_window_uses_latest_frame"] is True
    assert monitor["critical_window_uses_query"] is False
    assert monitor["continuous_latest_fresh_fast_path_used"] is True
    assert monitor["continuous_latest_fresh_duration_ms"] <= monitor["continuous_latest_fresh_budget_ms"]
    assert monitor["continuous_latest_fresh_waited_for_frame"] is False
    assert monitor["continuous_latest_fresh_performed_io"] is False
    assert monitor["continuous_latest_fresh_triggered_stream_restart"] is False
    assert monitor["continuous_latest_fresh_triggered_drain"] is False
    assert monitor["continuous_latest_fresh_triggered_p3_fallback"] is False
    assert monitor["selected_pressure_sample_stale_performed_io"] is False
    assert monitor["selected_pressure_sample_stale_triggered_source_selection"] is False
    assert monitor["selected_pressure_sample_stale_triggered_p3_fallback"] is False
    assert monitor["pressure_monitor_duration_ms"] <= monitor["pressure_monitor_budget_ms"]
    assert context["vent_scheduler_checked_before_diagnostic"] is True
    assert context["pressure_monitor_blocked_vent_scheduler"] is False
    assert context["route_conditioning_diagnostic_blocked_vent_scheduler"] is False
    assert context["route_conditioning_vent_gap_exceeded"] is False
    assert route_traces == []
    assert not any(event["event_name"] == "co2_route_conditioning_diagnostic_blocked_vent_scheduler" for event in timing_events)


def test_co2_conditioning_stale_monitor_defers_p3_fallback_in_high_frequency_window(monkeypatch) -> None:
    orchestrator, point, clock, _timing_events, route_traces, _vent_calls = _conditioning_guard_orchestrator(
        monkeypatch,
        [{"pressure_hpa": 1009.0, "age_s": 4.0, "sequence_id": 2, "is_stale": True}],
        cfg_overrides={"workflow.pressure.a2_conditioning_pressure_source": "v1_aligned"},
    )

    def forbidden_p3_fallback(**_kwargs):
        raise AssertionError("P3 fallback must be deferred while high-frequency vent has priority")

    orchestrator.pressure_control_service._a2_v1_aligned_pressure_gauge_sample = forbidden_p3_fallback
    orchestrator._record_a2_co2_conditioning_vent_tick(point, phase="after_route_open")
    orchestrator._a2_co2_route_conditioning_at_atmosphere_context["route_open_completed_monotonic_s"] = clock["now"]
    clock["now"] += 0.2

    monitor = orchestrator._record_a2_co2_conditioning_pressure_monitor(
        point,
        phase="conditioning_pressure_monitor",
    )

    context = orchestrator._a2_co2_route_conditioning_at_atmosphere_context
    assert monitor["pressure_monitor_deferred_for_vent_priority"] is True
    assert context["conditioning_monitor_pressure_deferred"] is True
    assert context["pressure_monitor_blocked_vent_scheduler"] is False
    assert context["route_conditioning_diagnostic_blocked_vent_scheduler"] is False
    assert context["diagnostic_blocking_component"] == "pressure_monitor"
    assert context["diagnostic_blocking_operation"] == "selected_pressure_sample_stale"
    assert context["diagnostic_deferred_count"] == 1
    assert route_traces == []


def test_co2_conditioning_near_gap_budget_vents_before_diagnostic(monkeypatch) -> None:
    orchestrator, point, clock, _timing_events, route_traces, vent_calls = _conditioning_guard_orchestrator(
        monkeypatch,
        [{"pressure_hpa": 1009.0, "age_s": 0.1, "sequence_id": 2}],
        cfg_overrides={
            "workflow.pressure.route_conditioning_high_frequency_vent_interval_s": 0.95,
            "workflow.pressure.route_conditioning_high_frequency_max_gap_s": 1.0,
            "workflow.pressure.pressure_monitor_interval_s": 0.5,
            "workflow.pressure.route_conditioning_pressure_monitor_budget_ms": 100.0,
        },
    )
    orchestrator._record_a2_co2_conditioning_vent_tick(point, phase="after_route_open")
    orchestrator._a2_co2_route_conditioning_at_atmosphere_context["route_open_completed_monotonic_s"] = clock["now"]
    clock["now"] += 0.86

    orchestrator._maybe_reassert_a2_conditioning_vent(point)

    context = orchestrator._a2_co2_route_conditioning_at_atmosphere_context
    assert len(vent_calls) == 2
    assert context["diagnostic_deferred_for_vent_priority"] is True
    assert context["diagnostic_deferred_count"] == 1
    assert context["vent_pulse_count"] == 2
    assert context["max_vent_pulse_write_gap_ms"] <= 1000.0
    assert context["route_conditioning_vent_gap_exceeded"] is False
    assert route_traces == []


def test_co2_conditioning_trace_write_is_deferred_in_high_frequency_window(monkeypatch) -> None:
    orchestrator, point, clock, _timing_events, route_traces, _vent_calls = _conditioning_guard_orchestrator(
        monkeypatch,
        [{"pressure_hpa": 1009.0, "age_s": 0.1, "sequence_id": 2}],
    )

    def blocking_trace_write(*_args, **_kwargs):
        clock["now"] += 3.8
        raise AssertionError("high-frequency trace write must be deferred")

    orchestrator._record_a2_co2_conditioning_vent_tick(point, phase="after_route_open")
    orchestrator._a2_co2_route_conditioning_at_atmosphere_context["route_open_completed_monotonic_s"] = clock["now"]
    orchestrator._record_workflow_timing = blocking_trace_write
    clock["now"] += 0.2

    monitor = orchestrator._record_a2_co2_conditioning_pressure_monitor(
        point,
        phase="conditioning_pressure_monitor",
    )

    context = orchestrator._a2_co2_route_conditioning_at_atmosphere_context
    assert monitor["trace_write_deferred_for_vent_priority"] is True
    assert context["trace_write_deferred_for_vent_priority"] is True
    assert context["trace_write_blocked_vent_scheduler"] is False
    assert context["route_conditioning_vent_gap_exceeded"] is False
    assert route_traces == []


def test_co2_conditioning_vent_tick_defers_latency_timing_in_high_frequency_window(monkeypatch) -> None:
    orchestrator, point, clock, _timing_events, route_traces, vent_calls = _conditioning_guard_orchestrator(
        monkeypatch,
        [{"pressure_hpa": 1009.0, "age_s": 0.1, "sequence_id": 2}],
    )
    orchestrator._a2_co2_route_open_monotonic_s = clock["now"]
    orchestrator._a2_co2_route_conditioning_at_atmosphere_context["route_open_completed_monotonic_s"] = clock["now"]

    def blocking_trace_write(*_args, **_kwargs):
        clock["now"] += 3.8
        raise AssertionError("high-frequency vent tick timing writes must be deferred")

    orchestrator._record_workflow_timing = blocking_trace_write
    tick_context = orchestrator._record_a2_co2_conditioning_vent_tick(point, phase="after_route_open")

    context = orchestrator._a2_co2_route_conditioning_at_atmosphere_context
    assert tick_context["trace_write_deferred_for_vent_priority"] is True
    assert context["trace_write_deferred_for_vent_priority"] is True
    assert context["trace_write_blocked_vent_scheduler"] is False
    assert context["route_conditioning_vent_gap_exceeded"] is False
    assert len(vent_calls) == 1
    assert route_traces == []


def _start_a2_route_open_transition(orchestrator, point, clock, *, command_duration_s: float = 0.01) -> None:
    orchestrator._record_a2_co2_conditioning_vent_tick(point, phase="before_route_open")
    orchestrator._begin_a2_co2_route_open_transition(point)
    orchestrator._mark_a2_co2_route_open_command_write_started(point)
    clock["now"] += command_duration_s
    orchestrator._mark_a2_co2_route_open_command_write_completed(point)
    orchestrator._refresh_a2_co2_conditioning_after_route_open(point)


def test_co2_conditioning_route_open_settle_wait_is_sliced_and_keeps_terminal_gap_under_one_second(
    monkeypatch,
) -> None:
    orchestrator, point, clock, _timing_events, route_traces, _vent_calls = _conditioning_guard_orchestrator(
        monkeypatch,
        [{"pressure_hpa": 1009.0, "age_s": 0.1, "sequence_id": 2}],
        cfg_overrides={
            "workflow.pressure.route_open_settle_wait_s": 2.2,
            "workflow.pressure.route_open_settle_wait_slice_s": 0.1,
            "workflow.pressure.route_conditioning_high_frequency_vent_interval_s": 0.5,
            "workflow.pressure.route_conditioning_high_frequency_max_gap_s": 1.0,
            "workflow.pressure.route_conditioning_scheduler_sleep_step_s": 0.1,
        },
    )

    _start_a2_route_open_transition(orchestrator, point, clock)
    orchestrator._wait_a2_co2_route_open_settle_before_conditioning(point)
    orchestrator._complete_a2_co2_route_open_transition(point)

    context = orchestrator._a2_co2_route_conditioning_at_atmosphere_context
    assert context["route_open_settle_wait_sliced"] is True
    assert context["route_open_settle_wait_slice_count"] >= 20
    assert context["vent_ticks_during_route_open_transition"] >= 2
    assert context["route_open_to_first_vent_write_ms"] == 0.0
    assert context["route_open_transition_max_vent_write_gap_ms"] <= 1000.0
    assert context["max_vent_pulse_write_gap_ms_including_terminal_gap"] <= 1000.0
    assert context["terminal_vent_write_age_ms_at_gap_gate"] <= 1000.0
    assert context["route_conditioning_vent_gap_exceeded"] is False
    assert route_traces == []


def test_co2_route_open_transition_timing_write_is_deferred_in_high_frequency_window(monkeypatch) -> None:
    orchestrator, point, clock, _timing_events, route_traces, _vent_calls = _conditioning_guard_orchestrator(
        monkeypatch,
        [{"pressure_hpa": 1009.0, "age_s": 0.1, "sequence_id": 2}],
        cfg_overrides={
            "workflow.pressure.route_open_settle_wait_s": 0.2,
            "workflow.pressure.route_open_settle_wait_slice_s": 0.1,
            "workflow.pressure.route_conditioning_high_frequency_vent_interval_s": 0.5,
            "workflow.pressure.route_conditioning_high_frequency_max_gap_s": 1.0,
            "workflow.pressure.route_conditioning_trace_write_budget_ms": 50.0,
        },
    )

    orchestrator._record_a2_co2_conditioning_vent_tick(point, phase="before_route_open")
    orchestrator._begin_a2_co2_route_open_transition(point)
    orchestrator._mark_a2_co2_route_open_command_write_started(point)

    def blocking_timing_write(*args, **kwargs):
        clock["now"] += 3.8
        raise AssertionError("high-frequency workflow timing write must be deferred")

    orchestrator._record_workflow_timing = blocking_timing_write
    clock["now"] += 0.01
    orchestrator._mark_a2_co2_route_open_command_write_completed(point)
    orchestrator._wait_a2_co2_route_open_settle_before_conditioning(point)
    orchestrator._complete_a2_co2_route_open_transition(point)

    context = orchestrator._a2_co2_route_conditioning_at_atmosphere_context
    assert context["trace_write_deferred_for_vent_priority"] is True
    assert context["trace_write_blocked_vent_scheduler"] is False
    assert context["route_conditioning_vent_gap_exceeded"] is False
    assert context["max_vent_pulse_write_gap_ms_including_terminal_gap"] <= 1000.0
    assert route_traces == []


def test_co2_conditioning_terminal_gap_enters_including_terminal_gate(monkeypatch) -> None:
    orchestrator, point, clock, _timing_events, route_traces, _vent_calls = _conditioning_guard_orchestrator(
        monkeypatch,
        [{"pressure_hpa": 1009.0, "age_s": 0.1, "sequence_id": 2}],
    )
    orchestrator._record_a2_co2_conditioning_vent_tick(point, phase="before_route_open")
    clock["now"] += 0.01
    orchestrator._a2_co2_route_open_monotonic_s = clock["now"]
    orchestrator._a2_co2_route_conditioning_at_atmosphere_context["route_open_completed_monotonic_s"] = clock["now"]
    orchestrator._refresh_a2_co2_conditioning_after_route_open(point)
    clock["now"] += 2.2

    with pytest.raises(WorkflowValidationError):
        orchestrator._maybe_reassert_a2_conditioning_vent(point)

    context = orchestrator._a2_co2_route_conditioning_at_atmosphere_context
    assert context["route_conditioning_vent_gap_exceeded"] is True
    assert context["route_conditioning_vent_gap_exceeded_source"] == "unknown"
    assert context["terminal_gap_source"] == "unknown"
    assert context["terminal_gap_operation"] != ""
    assert context["terminal_vent_write_age_ms_at_gap_gate"] == 2200.0
    assert context["max_vent_pulse_write_gap_ms_including_terminal_gap"] == 2200.0
    assert context["max_vent_scheduler_loop_gap_ms"] is not None


def test_co2_conditioning_defer_returns_to_vent_loop_before_preseal_work(monkeypatch) -> None:
    orchestrator, point, clock, _timing_events, route_traces, _vent_calls = _conditioning_guard_orchestrator(
        monkeypatch,
        [{"pressure_hpa": 1009.0, "age_s": 4.0, "sequence_id": 2, "is_stale": True, "hold_sequence": True}],
        cfg_overrides={
            "workflow.stability.co2_route.preseal_soak_s": 0.8,
            "workflow.pressure.continuous_atmosphere_hold": True,
            "workflow.pressure.positive_preseal_pressurization_enabled": True,
            "workflow.pressure.preseal_pressure_poll_interval_s": 0.1,
            "workflow.pressure.route_conditioning_scheduler_sleep_step_s": 0.1,
            "workflow.pressure.a2_conditioning_pressure_source": "v1_aligned",
        },
    )
    orchestrator.run_state = RunState()
    orchestrator._collect_only_fast_path_enabled = lambda: False
    orchestrator._has_special_co2_zero_flush_pending = lambda: False
    orchestrator._is_zero_co2_point = lambda point: False
    orchestrator._first_co2_route_soak_pending = False
    orchestrator._post_h2o_co2_zero_flush_pending = False
    orchestrator._initial_co2_zero_flush_pending = False
    orchestrator._active_post_h2o_co2_zero_flush = False
    orchestrator._check_stop = lambda: None
    orchestrator._log = lambda message: None
    orchestrator._wait_co2_route_dewpoint_gate_before_seal = lambda *args, **kwargs: True
    orchestrator._verify_co2_preseal_atmosphere_hold_pressure = lambda point: (_ for _ in ()).throw(
        AssertionError("deferred A2 conditioning must return to vent loop before preseal pressure reads")
    )
    orchestrator._refresh_live_analyzer_snapshots = lambda **kwargs: (_ for _ in ()).throw(
        AssertionError("deferred A2 conditioning must return to vent loop before stream snapshots")
    )
    orchestrator._a2_co2_route_conditioning_at_atmosphere_context["route_open_completed_monotonic_s"] = clock["now"]
    orchestrator._record_a2_co2_conditioning_vent_tick(point, phase="after_route_open")

    assert orchestrator._wait_co2_route_soak_before_seal(point) is True

    context = orchestrator._a2_co2_route_conditioning_at_atmosphere_context
    assert context["conditioning_monitor_pressure_deferred"] is True
    assert context["defer_source"] == "pressure_monitor"
    assert context["defer_operation"] in {
        "selected_pressure_sample_stale",
        "continuous_snapshot_not_fresh",
        "conditioning_pressure_monitor_budget_check",
        "conditioning_pressure_monitor_pre_loop_budget_check",
    }
    assert context["defer_reschedule_requested"] is True
    assert context["defer_reschedule_completed"] is True
    assert context["defer_returned_to_vent_loop"] is True
    assert context["defer_to_next_vent_loop_ms"] <= 200.0
    assert context["vent_tick_after_defer_ms"] <= 200.0
    assert context["terminal_gap_after_defer"] is False
    assert context["route_conditioning_vent_gap_exceeded"] is False
    assert context["max_vent_pulse_write_gap_ms_including_terminal_gap"] <= 1000.0
    assert route_traces == []


def test_co2_conditioning_defer_without_reschedule_records_terminal_gap_source(monkeypatch) -> None:
    orchestrator, point, clock, _timing_events, route_traces, _vent_calls = _conditioning_guard_orchestrator(
        monkeypatch,
        [{"pressure_hpa": 1009.0, "age_s": 4.0, "sequence_id": 2, "is_stale": True}],
    )
    orchestrator._a2_co2_route_conditioning_at_atmosphere_context["route_open_completed_monotonic_s"] = clock["now"]
    orchestrator._record_a2_co2_conditioning_vent_tick(point, phase="after_route_open")
    context = orchestrator._a2_co2_route_conditioning_at_atmosphere_context
    context.update(
        {
            "last_diagnostic_defer_monotonic_s": clock["now"],
            "last_diagnostic_defer_operation": "selected_pressure_sample_stale",
            "_last_diagnostic_defer_reschedule_recorded": False,
            "diagnostic_deferred_for_vent_priority": True,
            "conditioning_monitor_pressure_deferred": True,
        }
    )
    clock["now"] += 1.25

    with pytest.raises(WorkflowValidationError):
        orchestrator._maybe_reassert_a2_conditioning_vent(point)

    context = orchestrator._a2_co2_route_conditioning_at_atmosphere_context
    assert context["defer_path_no_reschedule"] is True
    assert context["defer_reschedule_requested"] is True
    assert context["defer_reschedule_completed"] is False
    assert context["defer_returned_to_vent_loop"] is True
    assert context["defer_reschedule_latency_exceeded"] is True
    assert context["defer_reschedule_latency_warning"] is True
    assert context["defer_reschedule_caused_vent_gap_exceeded"] is True
    assert context["defer_path_no_reschedule_reason"] == "actual_vent_gap_exceeded_after_defer"
    assert context["terminal_gap_source"] == "defer_path_no_reschedule"
    assert context["route_conditioning_vent_gap_exceeded_source"] == "defer_path_no_reschedule"
    assert context["terminal_gap_after_defer"] is True
    assert context["terminal_gap_after_defer_ms"] == 1250.0
    assert context["vent_gap_after_defer_ms"] == 1250.0
    assert context["vent_gap_after_defer_threshold_ms"] == 1000.0
    assert context["vent_gap_exceeded_after_defer"] is True
    assert context["max_vent_pulse_write_gap_ms_including_terminal_gap"] == 1250.0
    assert route_traces[-1]["actual"]["terminal_gap_source"] == "defer_path_no_reschedule"


def test_co2_conditioning_defer_over_200ms_warns_when_actual_vent_gap_ok(monkeypatch) -> None:
    orchestrator, point, clock, _timing_events, route_traces, _vent_calls = _conditioning_guard_orchestrator(
        monkeypatch,
        [{"pressure_hpa": 1009.0, "age_s": 0.1, "sequence_id": 2}],
        cfg_overrides={
            "workflow.pressure.route_conditioning_high_frequency_max_gap_s": 2.0,
            "workflow.pressure.route_conditioning_high_frequency_vent_interval_s": 1.0,
        },
    )
    orchestrator._a2_co2_route_conditioning_at_atmosphere_context["route_open_completed_monotonic_s"] = clock["now"]
    orchestrator._record_a2_co2_conditioning_vent_tick(point, phase="after_route_open")
    context = orchestrator._a2_co2_route_conditioning_at_atmosphere_context
    context = orchestrator._a2_conditioning_defer_diagnostic_for_vent_priority(
        context,
        point=point,
        component="transient_evaluation",
        operation="route_open_transient_recovery_evaluation",
        now_mono=clock["now"],
    )
    clock["now"] += 0.25

    orchestrator._maybe_reassert_a2_conditioning_vent(point)

    context = orchestrator._a2_co2_route_conditioning_at_atmosphere_context
    assert context["defer_to_next_vent_loop_ms"] == 250.0
    assert context["defer_reschedule_latency_ms"] == 250.0
    assert context["defer_reschedule_latency_budget_ms"] == 200.0
    assert context["defer_reschedule_latency_exceeded"] is True
    assert context["defer_reschedule_latency_warning"] is True
    assert context["defer_returned_to_vent_loop"] is True
    assert context["defer_reschedule_completed"] is True
    assert context["defer_path_no_reschedule"] is False
    assert context["defer_path_no_reschedule_reason"] == ""
    assert context["terminal_gap_after_defer"] is False
    assert context["terminal_gap_after_defer_ms"] is None
    assert context["vent_gap_after_defer_ms"] == 250.0
    assert context["vent_gap_after_defer_threshold_ms"] == 2000.0
    assert context["vent_gap_exceeded_after_defer"] is False
    assert context["route_conditioning_vent_gap_exceeded"] is False
    assert context["route_open_transient_evaluation_state"] == "continuing_after_defer_warning"
    assert context["route_open_transient_rejection_reason"] == ""
    assert route_traces == []


def test_co2_conditioning_defer_505ms_fast_vent_keeps_actual_gap_ok(monkeypatch) -> None:
    orchestrator, point, clock, _timing_events, route_traces, vent_calls = _conditioning_guard_orchestrator(
        monkeypatch,
        [{"pressure_hpa": 1009.0, "age_s": 0.1, "sequence_id": 2}],
        cfg_overrides={
            "workflow.pressure.route_conditioning_high_frequency_vent_interval_s": 0.5,
            "workflow.pressure.route_conditioning_high_frequency_max_gap_s": 1.0,
        },
    )
    orchestrator._a2_co2_route_conditioning_at_atmosphere_context["route_open_completed_monotonic_s"] = clock["now"]
    orchestrator._record_a2_co2_conditioning_vent_tick(point, phase="after_route_open")
    context = orchestrator._a2_co2_route_conditioning_at_atmosphere_context
    context = orchestrator._a2_conditioning_defer_diagnostic_for_vent_priority(
        context,
        point=point,
        component="pressure_monitor",
        operation="selected_pressure_sample_stale",
        now_mono=clock["now"],
        pressure_monitor=True,
    )
    clock["now"] += 0.505

    orchestrator._maybe_reassert_a2_conditioning_vent(point)

    context = orchestrator._a2_co2_route_conditioning_at_atmosphere_context
    assert context["defer_to_next_vent_loop_ms"] == 505.0
    assert context["defer_reschedule_latency_ms"] == 505.0
    assert context["defer_reschedule_latency_warning"] is True
    assert context["defer_returned_to_vent_loop"] is True
    assert context["defer_reschedule_completed"] is True
    assert context["fast_vent_after_defer_sent"] is True
    assert context["terminal_gap_after_defer"] is False
    assert context["defer_path_no_reschedule"] is False
    assert context["vent_gap_after_defer_ms"] == 505.0
    assert context["vent_gap_after_defer_threshold_ms"] == 1000.0
    assert context["vent_gap_exceeded_after_defer"] is False
    assert context["max_vent_pulse_write_gap_ms_including_terminal_gap"] <= 1000.0
    assert context["route_conditioning_vent_gap_exceeded"] is False
    assert context["route_open_transient_evaluation_state"] == "continuing_after_defer_warning"
    assert len(vent_calls) >= 2
    assert route_traces == []


@pytest.mark.parametrize(
    ("component", "operation"),
    [
        ("transient_evaluation", "route_open_transient_recovery_evaluation"),
        ("trace_write", "co2_route_conditioning_timing_trace"),
    ],
)
def test_co2_conditioning_deferred_work_reschedules_to_vent_loop_within_200ms(
    monkeypatch,
    component,
    operation,
) -> None:
    orchestrator, point, clock, _timing_events, route_traces, vent_calls = _conditioning_guard_orchestrator(
        monkeypatch,
        [{"pressure_hpa": 1009.0, "age_s": 0.1, "sequence_id": 2}],
        cfg_overrides={
            "workflow.pressure.route_conditioning_high_frequency_vent_interval_s": 0.05,
            "workflow.pressure.route_conditioning_high_frequency_max_gap_s": 1.0,
        },
    )
    orchestrator._a2_co2_route_conditioning_at_atmosphere_context["route_open_completed_monotonic_s"] = clock["now"]
    orchestrator._record_a2_co2_conditioning_vent_tick(point, phase="after_route_open")
    context = orchestrator._a2_co2_route_conditioning_at_atmosphere_context
    context = orchestrator._a2_conditioning_defer_diagnostic_for_vent_priority(
        context,
        point=point,
        component=component,
        operation=operation,
        now_mono=clock["now"],
    )
    clock["now"] += 0.12

    orchestrator._maybe_reassert_a2_conditioning_vent(point)

    context = orchestrator._a2_co2_route_conditioning_at_atmosphere_context
    assert context["defer_source"] == component
    assert context["defer_operation"] == operation
    assert context["defer_reschedule_requested"] is True
    assert context["defer_reschedule_completed"] is True
    assert context["defer_returned_to_vent_loop"] is True
    assert context["defer_to_next_vent_loop_ms"] <= 200.0
    assert context["vent_tick_after_defer_ms"] <= 200.0
    assert context["fast_vent_after_defer_sent"] is True
    assert context["fast_vent_after_defer_write_ms"] is not None
    assert context["defer_path_no_reschedule"] is False
    assert context["route_conditioning_vent_gap_exceeded"] is False
    assert len(vent_calls) >= 2
    assert route_traces == []


def test_co2_conditioning_fail_closed_while_route_open_records_vent_maintenance(monkeypatch) -> None:
    orchestrator, point, clock, _timing_events, route_traces, _vent_calls = _conditioning_guard_orchestrator(
        monkeypatch,
        [{"pressure_hpa": 1009.0, "age_s": 0.1, "sequence_id": 2}],
    )
    orchestrator._a2_co2_route_conditioning_at_atmosphere_context["route_open_completed_monotonic_s"] = clock["now"]
    orchestrator._record_a2_co2_conditioning_vent_tick(point, phase="after_route_open")
    clock["now"] += 0.1
    orchestrator._record_workflow_timing = lambda *args, **kwargs: (_ for _ in ()).throw(
        AssertionError("fail-closed timing aggregation must be deferred while route remains open")
    )

    with pytest.raises(WorkflowValidationError):
        orchestrator._fail_a2_co2_route_conditioning_closed(
            point,
            reason="route_conditioning_vent_gap_exceeded",
            details={"route_conditioning_vent_gap_exceeded": True},
            event_name="co2_route_conditioning_vent_heartbeat_gap",
            route_trace_action="co2_route_conditioning_vent_heartbeat_gap",
        )

    context = orchestrator._a2_co2_route_conditioning_at_atmosphere_context
    assert context["fail_closed_path_started"] is True
    assert context["fail_closed_path_started_while_route_open"] is True
    assert context["fail_closed_path_vent_maintenance_required"] is True
    assert context["fail_closed_path_vent_maintenance_active"] is True
    assert context["fail_closed_path_blocked_vent_scheduler"] is False
    assert context["trace_write_deferred_for_vent_priority"] is True
    assert context["trace_write_blocked_vent_scheduler"] is False
    assert route_traces[-1]["actual"]["fail_closed_path_vent_maintenance_required"] is True
    assert route_traces[-1]["action"] == "co2_route_conditioning_vent_heartbeat_gap"


def test_co2_conditioning_route_open_command_block_reports_transition_block_not_pulse_gap(
    monkeypatch,
) -> None:
    orchestrator, point, clock, _timing_events, route_traces, _vent_calls = _conditioning_guard_orchestrator(
        monkeypatch,
        [{"pressure_hpa": 1009.0, "age_s": 0.1, "sequence_id": 2}],
        cfg_overrides={
            "workflow.pressure.route_open_transition_blocked_vent_scheduler_threshold_s": 1.0,
        },
    )

    _start_a2_route_open_transition(orchestrator, point, clock, command_duration_s=1.2)
    with pytest.raises(WorkflowValidationError):
        orchestrator._fail_a2_route_open_transition_if_blocked(point)

    context = orchestrator._a2_co2_route_conditioning_at_atmosphere_context
    assert context["fail_closed_reason"] == "route_open_transition_blocked_vent_scheduler"
    assert context["route_open_transition_blocked_vent_scheduler"] is True
    assert context["route_conditioning_vent_gap_exceeded"] is True
    assert context["route_conditioning_vent_gap_exceeded_source"] == "route_open_transition"
    assert context["route_open_command_write_duration_ms"] == 1200.0
    assert context["route_open_to_first_vent_write_ms"] == 0.0
    assert route_traces[-1]["action"] == "co2_route_open_transition_blocked_vent_scheduler"


def _conditioning_guard_orchestrator(monkeypatch, frames: list[dict], *, cfg_overrides: dict | None = None):
    orchestrator = WorkflowOrchestrator.__new__(WorkflowOrchestrator)
    clock = {"now": 100.0}
    monkeypatch.setattr(orchestrator_module.time, "time", lambda: clock["now"])
    monkeypatch.setattr(orchestrator_module.time, "monotonic", lambda: clock["now"])
    monkeypatch.setattr(orchestrator_module.time, "sleep", lambda seconds: clock.__setitem__("now", clock["now"] + float(seconds)))
    timing_events: list[dict] = []
    route_traces: list[dict] = []
    vent_calls: list[dict] = []
    values = {
        "workflow.pressure.atmosphere_vent_heartbeat_interval_s": 1.0,
        "workflow.pressure.atmosphere_vent_max_gap_s": 3.0,
        "workflow.pressure.route_conditioning_high_frequency_vent_interval_s": 0.5,
        "workflow.pressure.route_conditioning_high_frequency_max_gap_s": 1.0,
        "workflow.pressure.route_conditioning_high_frequency_vent_window_s": 20.0,
        "workflow.pressure.route_conditioning_vent_maintenance_interval_s": 1.0,
        "workflow.pressure.route_conditioning_vent_maintenance_max_gap_s": 2.0,
        "workflow.pressure.route_conditioning_scheduler_sleep_step_s": 0.1,
        "workflow.pressure.pressure_monitor_interval_s": 0.5,
        "workflow.pressure.conditioning_digital_gauge_max_age_s": 3.0,
        "workflow.pressure.conditioning_pressure_abort_hpa": 1150.0,
        "workflow.pressure.route_conditioning_hard_abort_pressure_hpa": 1150.0,
        "workflow.pressure.route_open_transient_window_enabled": True,
        "workflow.pressure.route_open_transient_recovery_timeout_s": 3.0,
        "workflow.pressure.route_open_transient_recovery_band_hpa": 10.0,
        "workflow.pressure.route_open_transient_stable_hold_s": 0.2,
        "workflow.pressure.route_open_transient_stable_pressure_span_hpa": 10.0,
        "workflow.pressure.route_open_transient_stable_span_hpa": 10.0,
        "workflow.pressure.route_open_transient_stable_slope_hpa_per_s": 10.0,
        "workflow.pressure.preseal_atmosphere_hold_reassert_timeout_s": 0.1,
        "workflow.pressure.pressure_read_latency_warn_s": 0.5,
    }
    values.update(cfg_overrides or {})
    frame_queue = [dict(item) for item in frames]
    last_frame = dict(frame_queue[-1]) if frame_queue else {"pressure_hpa": 1009.0, "age_s": 0.1, "sequence_id": 1}

    def next_frame() -> dict:
        nonlocal last_frame
        if frame_queue:
            last_frame = dict(frame_queue.pop(0))
        elif (
            not bool(last_frame.get("hold_sequence", False))
            and not bool(last_frame.get("is_stale", last_frame.get("pressure_sample_is_stale", False)))
        ):
            last_frame = {
                **last_frame,
                "sequence_id": int(last_frame.get("sequence_id", 0)) + 1,
            }
        return dict(last_frame)

    def sample(**kwargs):
        frame = next_frame()
        pressure_raw = frame.get("pressure_hpa")
        pressure = None if pressure_raw is None else float(pressure_raw)
        age = float(frame.get("age_s", 0.1))
        sequence = int(frame.get("sequence_id", 1))
        extra = {
            key: value
            for key, value in frame.items()
            if key not in {"pressure_hpa", "age_s", "sequence_id"}
        }
        parse_ok = bool(extra.get("parse_ok", pressure is not None))
        digital = {
            "pressure_hpa": pressure,
            "source": "digital_pressure_gauge_continuous",
            "pressure_sample_source": "digital_pressure_gauge_continuous",
            "sample_age_s": age,
            "pressure_sample_age_s": age,
            "is_stale": bool(extra.get("is_stale", False)),
            "pressure_sample_is_stale": bool(extra.get("pressure_sample_is_stale", extra.get("is_stale", False))),
            "sequence_id": sequence,
            "pressure_sample_sequence_id": sequence,
            "latest_frame_age_s": age,
            "latest_frame_sequence_id": sequence,
            "parse_ok": parse_ok,
            **extra,
        }
        return {
            **digital,
            **kwargs,
            "pressure_source_used_for_decision": "digital_pressure_gauge_continuous",
            "pressure_source_used_for_abort": "digital_pressure_gauge_continuous",
            "digital_gauge_pressure_sample": dict(digital),
            "digital_gauge_pressure_hpa": pressure,
            "digital_gauge_age_s": age,
            "digital_gauge_stale": bool(digital.get("is_stale")),
            "critical_window_uses_latest_frame": True,
            "critical_window_uses_query": False,
        }

    def direct_sample(source: str) -> dict:
        frame = next_frame()
        pressure_raw = frame.get("pressure_hpa")
        pressure = None if pressure_raw is None else float(pressure_raw)
        age = float(frame.get("age_s", 0.1))
        sequence = int(frame.get("sequence_id", 1))
        extra = {
            key: value
            for key, value in frame.items()
            if key not in {"pressure_hpa", "age_s", "sequence_id"}
        }
        stale = bool(extra.get("is_stale", extra.get("pressure_sample_is_stale", False)))
        parse_ok = bool(extra.get("parse_ok", pressure is not None))
        return {
            "pressure_hpa": pressure,
            "source": source,
            "pressure_sample_source": source,
            "sample_age_s": age,
            "pressure_sample_age_s": age,
            "is_stale": stale,
            "pressure_sample_is_stale": stale,
            "sequence_id": sequence,
            "pressure_sample_sequence_id": sequence,
            "read_latency_s": extra.get("read_latency_s", 0.05),
            "raw_response": extra.get("raw_response", "" if pressure is None else f"{pressure:.3f}"),
            "parse_ok": parse_ok,
            "error": extra.get("error", "" if parse_ok else "pressure_read_failed"),
            **extra,
        }

    def v1_aligned_sample(**kwargs) -> dict:
        sample = direct_sample("digital_pressure_gauge")
        ok = bool(sample.get("pressure_hpa") is not None and not sample.get("is_stale") and sample.get("parse_ok"))
        sample.update(
            {
                **kwargs,
                "source": "digital_pressure_gauge_p3",
                "pressure_sample_source": "digital_pressure_gauge_p3",
                "digital_gauge_mode": "v1_aligned_p3_fast",
                "a2_3_pressure_source_strategy": "v1_aligned",
                "pressure_source_selected": "digital_pressure_gauge_p3" if ok else "",
                "pressure_source_selection_reason": (
                    "continuous_stale_fallback_to_p3_fast"
                    if ok
                    else "digital_gauge_v1_aligned_read_unavailable"
                ),
                "source_selection_reason": (
                    "continuous_stale_fallback_to_p3_fast"
                    if ok
                    else "digital_gauge_v1_aligned_read_unavailable"
                ),
                "critical_window_uses_latest_frame": False,
                "critical_window_uses_query": True,
                "p3_fast_fallback_attempted": True,
                "p3_fast_fallback_result": "success" if ok else "failed",
                "normal_p3_fallback_attempted": False,
                "normal_p3_fallback_result": "",
                "fail_closed_reason": "" if ok else "digital_gauge_v1_aligned_read_unavailable",
            }
        )
        return sample

    def snapshot() -> dict:
        frame = dict(next_frame())
        pressure_raw = frame.get("pressure_hpa")
        pressure = None if pressure_raw is None else float(pressure_raw)
        age = float(frame.get("age_s", 0.1))
        sequence = int(frame.get("sequence_id", 1))
        extra = {
            key: value
            for key, value in frame.items()
            if key not in {"pressure_hpa", "age_s", "sequence_id"}
        }
        return {
            "stream_frame_count": sequence,
            "latest_frame_age_s": age,
            "latest_frame_sequence_id": sequence,
            "latest_frame_stale": bool(extra.get("is_stale", False)),
            **extra,
            "latest_frame": {
                "pressure_hpa": pressure,
                "sample_age_s": age,
                "sequence_id": sequence,
                "latest_frame_interval_s": 0.1,
                **extra,
            },
        }

    def fast_snapshot(**kwargs) -> dict:
        payload = snapshot()
        latest = dict(payload.get("latest_frame") or {})
        pressure = latest.get("pressure_hpa")
        age = float(payload.get("latest_frame_age_s", 0.0) or 0.0)
        stale = bool(payload.get("latest_frame_stale") or age > float(values["workflow.pressure.conditioning_digital_gauge_max_age_s"]))
        selection_reason = (
            "digital_gauge_continuous_latest_fresh"
            if pressure is not None and not stale
            else ("digital_gauge_continuous_latest_unavailable" if pressure is None else "digital_gauge_continuous_latest_stale")
        )
        payload.update(
            {
                **kwargs,
                "pressure_hpa": pressure,
                "source": "digital_pressure_gauge_continuous",
                "pressure_sample_source": "digital_pressure_gauge_continuous",
                "pressure_sample_age_s": age,
                "sample_age_s": age,
                "is_stale": stale,
                "pressure_sample_is_stale": stale,
                "parse_ok": pressure is not None,
                "pressure_source_selected": "digital_pressure_gauge_continuous" if pressure is not None and not stale else "",
                "pressure_source_selection_reason": selection_reason,
                "source_selection_reason": selection_reason,
                "continuous_latest_fresh_fast_path_used": True,
                "continuous_latest_fresh_duration_ms": 0.0,
                "continuous_latest_fresh_lock_acquire_ms": 0.0,
                "continuous_latest_fresh_lock_timeout": False,
                "continuous_latest_fresh_waited_for_frame": False,
                "continuous_latest_fresh_performed_io": False,
                "continuous_latest_fresh_triggered_stream_restart": False,
                "continuous_latest_fresh_triggered_drain": False,
                "continuous_latest_fresh_triggered_p3_fallback": False,
                "continuous_latest_fresh_budget_ms": kwargs.get("budget_ms", 5.0),
                "continuous_latest_fresh_budget_exceeded": False,
            }
        )
        return payload

    def fast_vent_reassert(vent_on, **kwargs):
        started = clock["now"]
        duration_s = float(kwargs.pop("duration_s", 0.0) or 0.0)
        if duration_s:
            clock["now"] += duration_s
        completed = clock["now"]
        vent_calls.append({"vent_on": vent_on, **kwargs, "fast_reassert": True})
        return {
            "fast_vent_reassert_supported": True,
            "fast_vent_reassert_used": True,
            "vent_command_write_started_at": "2026-04-28T00:00:00+00:00",
            "vent_command_write_sent_at": "2026-04-28T00:00:00+00:00",
            "vent_command_write_completed_at": "2026-04-28T00:00:00+00:00",
            "vent_command_write_started_monotonic_s": started,
            "vent_command_write_sent_monotonic_s": started,
            "vent_command_write_completed_monotonic_s": completed,
            "vent_command_write_duration_ms": round(max(0.0, completed - started) * 1000.0, 3),
            "vent_command_total_duration_ms": round(max(0.0, completed - started) * 1000.0, 3),
            "vent_command_wait_after_command_s": 0.0,
            "vent_command_capture_pressure_enabled": False,
            "vent_command_query_state_enabled": False,
            "vent_command_confirm_transition_enabled": False,
            "vent_command_blocking_phase": "fast_vent_write",
            "route_conditioning_fast_vent_command_timeout": False,
            "route_conditioning_fast_vent_not_supported": False,
            "command_result": "ok",
            "command_error": "",
            "command_method": "test_fast_vent_reassert",
        }

    orchestrator._cfg_get = lambda path, default=None: values.get(path, default)
    orchestrator._as_float = lambda value: None if value in (None, "") else float(value)
    orchestrator._record_workflow_timing = lambda event_name, event_type="info", **kwargs: timing_events.append(
        {"event_name": event_name, "event_type": event_type, **kwargs}
    )
    orchestrator.status_service = SimpleNamespace(record_route_trace=lambda **kwargs: route_traces.append(kwargs))
    orchestrator.pressure_control_service = SimpleNamespace(
        set_pressure_controller_vent=lambda vent_on, **kwargs: vent_calls.append({"vent_on": vent_on, **kwargs})
        or {"output_state": 0, "isolation_state": 1, "vent_status_raw": 1},
        set_pressure_controller_vent_fast_reassert=fast_vent_reassert,
        digital_gauge_continuous_stream_snapshot=snapshot,
        digital_gauge_continuous_latest_fast_snapshot=fast_snapshot,
        _current_high_pressure_first_point_sample=sample,
        _pressure_sample_from_device=direct_sample,
        _a2_v1_aligned_pressure_gauge_sample=v1_aligned_sample,
    )
    point = CalibrationPoint(index=1, temperature_c=20.0, co2_ppm=100.0, pressure_hpa=1100.0, route="co2")
    orchestrator._a2_co2_route_conditioning_at_atmosphere_active = True
    orchestrator._a2_co2_route_conditioning_at_atmosphere_context = {
        "conditioning_started_monotonic_s": clock["now"],
        "vent_ticks": [],
        "pressure_samples": [],
        "digital_gauge_monitoring_required": True,
        "conditioning_pressure_abort_hpa": 1150.0,
        "vent_heartbeat_interval_s": 1.0,
        "atmosphere_vent_max_gap_s": 3.0,
        "pressure_monitor_interval_s": 0.5,
        "fail_closed_before_vent_off": False,
        "vent_off_sent_at": "",
        "seal_command_sent": False,
        "sample_count": 0,
        "points_completed": 0,
    }
    return orchestrator, point, clock, timing_events, route_traces, vent_calls


def _install_v1_aligned_selected_p3_reader(orchestrator, **overrides) -> None:
    def selected_p3_reader(**kwargs) -> dict:
        payload = {
            **kwargs,
            "pressure_hpa": 1009.4,
            "source": "digital_pressure_gauge_p3",
            "pressure_sample_source": "digital_pressure_gauge_p3",
            "pressure_sample_age_s": 0.02,
            "sample_age_s": 0.02,
            "pressure_sample_is_stale": False,
            "is_stale": False,
            "parse_ok": True,
            "error": "",
            "read_latency_s": 0.05,
            "raw_response": "1009.400",
            "digital_gauge_mode": "v1_aligned_p3_normal",
            "a2_3_pressure_source_strategy": "v1_aligned",
            "pressure_source_selected": "digital_pressure_gauge_p3",
            "pressure_source_selection_reason": "p3_fast_failed_fallback_normal_p3",
            "source_selection_reason": "p3_fast_failed_fallback_normal_p3",
            "critical_window_uses_latest_frame": False,
            "critical_window_uses_query": True,
            "p3_fast_fallback_attempted": True,
            "p3_fast_fallback_result": "failed",
            "normal_p3_fallback_attempted": True,
            "normal_p3_fallback_result": "success",
            "last_pressure_command": "read_pressure",
            "last_pressure_command_may_cancel_continuous": True,
            "continuous_interrupted_by_command": True,
            "continuous_restart_required_before_return_to_continuous": True,
            "fail_closed_reason": "",
        }
        payload.update(overrides)
        return payload

    orchestrator.pressure_control_service._a2_v1_aligned_pressure_gauge_sample = selected_p3_reader


def test_co2_conditioning_vent_gap_exceeding_max_fails_closed(monkeypatch) -> None:
    orchestrator, point, clock, timing_events, route_traces, vent_calls = _conditioning_guard_orchestrator(
        monkeypatch,
        [{"pressure_hpa": 1009.0, "age_s": 0.1, "sequence_id": 2}],
    )
    orchestrator._a2_co2_route_conditioning_at_atmosphere_context["last_vent_tick_monotonic_s"] = clock["now"] - 3.5
    orchestrator._a2_co2_route_conditioning_at_atmosphere_context["route_open_completed_monotonic_s"] = clock["now"] - 3.0

    with pytest.raises(WorkflowValidationError):
        orchestrator._maybe_reassert_a2_conditioning_vent(point)

    context = orchestrator._a2_co2_route_conditioning_at_atmosphere_context
    assert context["fail_closed_before_vent_off"] is True
    assert context["vent_heartbeat_gap_exceeded"] is True
    assert context["route_conditioning_vent_gap_exceeded"] is True
    assert context["heartbeat_gap_threshold_ms"] == 1000.0
    assert context["heartbeat_gap_observed_ms"] == 3500.0
    assert context["fail_closed_reason"] == "route_conditioning_vent_gap_exceeded"
    assert context["whether_safe_to_continue"] is False
    assert context["vent_off_sent_at"] == ""
    assert context["seal_command_sent"] is False
    assert vent_calls == []
    assert route_traces[-1]["actual"]["fail_closed_before_vent_off"] is True
    assert context["trace_write_deferred_for_vent_priority"] is True
    assert not any(event["event_name"] == "co2_route_conditioning_vent_heartbeat_gap" for event in timing_events)


def test_co2_route_open_first_vent_delay_fails_closed(monkeypatch) -> None:
    orchestrator, point, clock, timing_events, route_traces, vent_calls = _conditioning_guard_orchestrator(
        monkeypatch,
        [{"pressure_hpa": 1009.0, "age_s": 0.1, "sequence_id": 2}],
    )
    orchestrator._a2_co2_route_conditioning_at_atmosphere_context["route_open_completed_monotonic_s"] = clock["now"] - 3.5

    with pytest.raises(WorkflowValidationError):
        orchestrator._record_a2_co2_conditioning_vent_tick(point, phase="after_route_open")

    context = orchestrator._a2_co2_route_conditioning_at_atmosphere_context
    assert context["route_open_to_first_vent_s"] == 3.5
    assert context["route_open_to_first_vent_ms"] == 3500.0
    assert context["vent_heartbeat_gap_exceeded"] is True
    assert context["route_conditioning_vent_gap_exceeded"] is True
    assert vent_calls == []
    assert route_traces[-1]["action"] == "co2_route_conditioning_route_open_first_vent_gap"
    assert context["trace_write_deferred_for_vent_priority"] is True
    assert not any(event["event_name"] == "co2_route_conditioning_route_open_first_vent_gap" for event in timing_events)


def test_co2_conditioning_blocks_vent_after_ready_to_seal_started(monkeypatch) -> None:
    orchestrator, point, _clock, timing_events, route_traces, vent_calls = _conditioning_guard_orchestrator(
        monkeypatch,
        [{"pressure_hpa": 1009.0, "age_s": 0.1, "sequence_id": 2}],
    )
    orchestrator._a2_co2_route_conditioning_at_atmosphere_context.update(
        {
            "route_conditioning_phase": "ready_to_seal_phase",
            "ready_to_seal_phase_started": True,
            "route_conditioning_flush_min_time_completed": True,
        }
    )

    with pytest.raises(WorkflowValidationError):
        orchestrator._record_a2_co2_conditioning_vent_tick(point, phase="after_flush_attempt")

    context = orchestrator._a2_co2_route_conditioning_at_atmosphere_context
    assert vent_calls == []
    assert context["vent_pulse_blocked_after_flush_phase"] is True
    assert context["vent_pulse_blocked_reason"] == "route_conditioning_phase_not_flush"
    assert context["unsafe_vent_after_seal_or_pressure_control_command_sent"] is False
    assert route_traces[-1]["action"] == "co2_route_conditioning_vent_blocked_after_flush_phase"
    assert any(
        event["event_name"] == "co2_route_conditioning_vent_blocked_after_flush_phase"
        for event in timing_events
    )


@pytest.mark.parametrize(
    ("context_update", "blocked_reason"),
    [
        ({"seal_command_sent": True}, "seal_command_sent"),
        ({"pressure_setpoint_command_sent": True}, "pressure_setpoint_command_sent"),
    ],
)
def test_co2_conditioning_blocks_vent_after_seal_or_pressure_control_command(
    monkeypatch,
    context_update,
    blocked_reason,
) -> None:
    orchestrator, point, _clock, _timing_events, route_traces, vent_calls = _conditioning_guard_orchestrator(
        monkeypatch,
        [{"pressure_hpa": 1009.0, "age_s": 0.1, "sequence_id": 2}],
    )
    orchestrator._a2_co2_route_conditioning_at_atmosphere_context.update(context_update)

    with pytest.raises(WorkflowValidationError):
        orchestrator._record_a2_co2_conditioning_vent_tick(point, phase="unsafe_attempt")

    context = orchestrator._a2_co2_route_conditioning_at_atmosphere_context
    assert vent_calls == []
    assert context["vent_pulse_blocked_after_flush_phase"] is True
    assert context["vent_pulse_blocked_reason"] == blocked_reason
    assert context["attempted_unsafe_vent_after_seal_or_pressure_control"] is True
    assert context["unsafe_vent_after_seal_or_pressure_control_command_sent"] is False
    assert route_traces[-1]["action"] == "co2_route_conditioning_vent_blocked_after_flush_phase"


def test_co2_conditioning_fresh_gauge_over_abort_fails_closed(monkeypatch) -> None:
    orchestrator, point, clock, timing_events, route_traces, vent_calls = _conditioning_guard_orchestrator(
        monkeypatch,
        [{"pressure_hpa": 1155.0, "age_s": 0.1, "sequence_id": 2}],
    )
    orchestrator._a2_co2_route_conditioning_at_atmosphere_context["route_open_completed_monotonic_s"] = (
        clock["now"] - 1.0
    )
    orchestrator._record_a2_co2_conditioning_vent_tick(point, phase="after_route_open")

    with pytest.raises(WorkflowValidationError):
        orchestrator._record_a2_co2_conditioning_pressure_monitor(point, phase="conditioning_pressure_monitor")

    context = orchestrator._a2_co2_route_conditioning_at_atmosphere_context
    assert context["pressure_overlimit_seen"] is True
    assert context["pressure_overlimit_hpa"] == 1155.0
    assert context["conditioning_pressure_abort_hpa"] == 1150.0
    assert context["route_conditioning_hard_abort_pressure_hpa"] == 1150.0
    assert context["route_conditioning_hard_abort_exceeded"] is True
    assert context["fail_closed_before_vent_off"] is True
    assert context["fail_closed_reason"] == "route_conditioning_pressure_overlimit"
    assert context["route_conditioning_pressure_overlimit"] is True
    assert context["route_open_to_first_pressure_read_ms"] == 1000.0
    assert context["route_open_to_overlimit_ms"] == 1000.0
    assert context["fail_closed_vent_pulse_sent"] is True
    assert vent_calls[-1]["vent_on"] is True
    assert route_traces[-1]["action"] == "co2_preseal_atmosphere_hold_pressure_guard"
    assert context["trace_write_deferred_for_vent_priority"] is True
    assert not any(event["event_name"] == "co2_route_conditioning_pressure_overlimit" for event in timing_events)


def test_co2_conditioning_accepts_route_open_transient_after_recovery(monkeypatch) -> None:
    orchestrator, point, clock, _timing_events, route_traces, _vent_calls = _conditioning_guard_orchestrator(
        monkeypatch,
        [
            {"pressure_hpa": 1009.0, "age_s": 0.1, "sequence_id": 2},
            {"pressure_hpa": 1160.0, "age_s": 0.1, "sequence_id": 3},
            {"pressure_hpa": 1011.0, "age_s": 0.1, "sequence_id": 4},
            {"pressure_hpa": 1009.5, "age_s": 0.1, "sequence_id": 5},
        ],
        cfg_overrides={
            "workflow.pressure.route_conditioning_hard_abort_pressure_hpa": 1250.0,
            "workflow.pressure.route_open_transient_recovery_band_hpa": 5.0,
            "workflow.pressure.route_open_transient_recovery_timeout_s": 2.0,
            "workflow.pressure.route_open_transient_stable_hold_s": 0.2,
            "workflow.pressure.route_open_transient_stable_pressure_span_hpa": 5.0,
            "workflow.pressure.route_open_transient_stable_slope_hpa_per_s": 10.0,
        },
    )
    baseline = orchestrator._record_a2_co2_conditioning_pressure_monitor(
        point,
        phase="before_route_open_atmospheric_baseline",
    )
    clock["now"] += 0.1
    orchestrator._a2_co2_route_conditioning_at_atmosphere_context["route_open_completed_monotonic_s"] = clock["now"]
    orchestrator._record_a2_co2_conditioning_vent_tick(point, phase="after_route_open")
    spike = orchestrator._record_a2_co2_conditioning_pressure_monitor(point, phase="conditioning_pressure_monitor")
    clock["now"] += 0.1
    recovered = orchestrator._record_a2_co2_conditioning_pressure_monitor(point, phase="conditioning_pressure_monitor")
    clock["now"] += 0.3
    stable = orchestrator._record_a2_co2_conditioning_pressure_monitor(point, phase="conditioning_pressure_monitor")

    assert baseline["measured_atmospheric_pressure_hpa"] == 1009.0
    assert spike["route_open_transient_peak_pressure_hpa"] == 1160.0
    assert spike["route_open_transient_recovery_required"] is True
    assert spike["route_conditioning_hard_abort_exceeded"] is False
    assert spike["route_conditioning_hard_abort_pressure_hpa"] == 1250.0
    assert recovered["route_open_transient_recovered_to_atmosphere"] is True
    assert stable["route_open_transient_accepted"] is True
    assert stable["route_open_transient_recovery_target_hpa"] == 1009.0
    assert stable["route_open_transient_rejection_reason"] == ""
    assert stable["vent_off_blocked_during_flush"] is True
    assert stable["seal_blocked_during_flush"] is True
    assert stable["pressure_setpoint_blocked_during_flush"] is True
    assert stable["sample_blocked_during_flush"] is True
    assert route_traces == []


def test_co2_conditioning_route_open_transient_rise_after_valid_vent_fails_closed(monkeypatch) -> None:
    orchestrator, point, clock, _timing_events, route_traces, _vent_calls = _conditioning_guard_orchestrator(
        monkeypatch,
        [
            {"pressure_hpa": 1009.0, "age_s": 0.1, "sequence_id": 2},
            {"pressure_hpa": 1120.0, "age_s": 0.1, "sequence_id": 3},
            {"pressure_hpa": 1130.0, "age_s": 0.1, "sequence_id": 4},
            {"pressure_hpa": 1140.0, "age_s": 0.1, "sequence_id": 5},
        ],
        cfg_overrides={
            "workflow.pressure.route_conditioning_hard_abort_pressure_hpa": 1250.0,
            "workflow.pressure.route_open_transient_recovery_band_hpa": 5.0,
            "workflow.pressure.route_open_transient_sustained_rise_min_samples": 3,
        },
    )
    orchestrator._record_a2_co2_conditioning_pressure_monitor(
        point,
        phase="before_route_open_atmospheric_baseline",
    )
    clock["now"] += 0.1
    orchestrator._a2_co2_route_conditioning_at_atmosphere_context["route_open_completed_monotonic_s"] = clock["now"]
    orchestrator._record_a2_co2_conditioning_vent_tick(point, phase="after_route_open")
    orchestrator._record_a2_co2_conditioning_pressure_monitor(point, phase="conditioning_pressure_monitor")
    clock["now"] += 0.1

    with pytest.raises(WorkflowValidationError):
        orchestrator._record_a2_co2_conditioning_pressure_monitor(point, phase="conditioning_pressure_monitor")

    context = orchestrator._a2_co2_route_conditioning_at_atmosphere_context
    assert context["route_conditioning_hard_abort_exceeded"] is False
    assert context["pressure_rise_despite_valid_vent_scheduler"] is True
    assert context["route_open_transient_rejection_reason"] == "pressure_rise_despite_valid_vent_scheduler"
    assert route_traces[-1]["action"] == "co2_route_conditioning_transient_recovery_failed"


def test_co2_conditioning_route_open_transient_sustained_rise_fails_closed(monkeypatch) -> None:
    orchestrator, point, clock, _timing_events, route_traces, _vent_calls = _conditioning_guard_orchestrator(
        monkeypatch,
        [
            {"pressure_hpa": 1009.0, "age_s": 0.1, "sequence_id": 2},
            {"pressure_hpa": 1120.0, "age_s": 0.1, "sequence_id": 3},
            {"pressure_hpa": 1130.0, "age_s": 0.1, "sequence_id": 4},
            {"pressure_hpa": 1140.0, "age_s": 0.1, "sequence_id": 5},
        ],
        cfg_overrides={
            "workflow.pressure.route_conditioning_hard_abort_pressure_hpa": 1250.0,
            "workflow.pressure.route_open_transient_recovery_band_hpa": 5.0,
            "workflow.pressure.route_open_transient_sustained_rise_min_samples": 3,
            "workflow.pressure.route_conditioning_pressure_rise_vent_trigger_hpa": 999.0,
        },
    )
    orchestrator._record_a2_co2_conditioning_pressure_monitor(
        point,
        phase="before_route_open_atmospheric_baseline",
    )
    clock["now"] += 0.1
    orchestrator._a2_co2_route_conditioning_at_atmosphere_context["route_open_completed_monotonic_s"] = clock["now"]
    orchestrator._record_a2_co2_conditioning_vent_tick(point, phase="after_route_open")
    orchestrator._record_a2_co2_conditioning_pressure_monitor(point, phase="conditioning_pressure_monitor")
    clock["now"] += 0.1
    orchestrator._record_a2_co2_conditioning_pressure_monitor(point, phase="conditioning_pressure_monitor")
    clock["now"] += 0.1

    with pytest.raises(WorkflowValidationError):
        orchestrator._record_a2_co2_conditioning_pressure_monitor(point, phase="conditioning_pressure_monitor")

    context = orchestrator._a2_co2_route_conditioning_at_atmosphere_context
    assert context["route_conditioning_hard_abort_exceeded"] is False
    assert context["sustained_pressure_rise_after_route_open"] is True
    assert context.get("pressure_rise_despite_valid_vent_scheduler") is not True
    assert context["route_open_transient_rejection_reason"] == "sustained_pressure_rise_after_route_open"
    assert route_traces[-1]["action"] == "co2_route_conditioning_transient_recovery_failed"


def test_co2_conditioning_overlimit_safety_vent_is_blocked_after_seal(monkeypatch) -> None:
    orchestrator, point, _clock, _timing_events, route_traces, vent_calls = _conditioning_guard_orchestrator(
        monkeypatch,
        [{"pressure_hpa": 1155.0, "age_s": 0.1, "sequence_id": 2}],
    )
    orchestrator._a2_co2_route_conditioning_at_atmosphere_context["seal_command_sent"] = True

    with pytest.raises(WorkflowValidationError):
        orchestrator._record_a2_co2_conditioning_vent_tick(point, phase="conditioning_hold")

    context = orchestrator._a2_co2_route_conditioning_at_atmosphere_context
    assert vent_calls == []
    assert context["vent_pulse_blocked_after_flush_phase"] is True
    assert context["vent_pulse_blocked_reason"] == "seal_command_sent"
    assert context["unsafe_vent_after_seal_or_pressure_control_command_sent"] is False
    assert route_traces[-1]["action"] == "co2_route_conditioning_vent_blocked_after_flush_phase"


def test_positive_preseal_abort_emergency_relief_allowed_when_unsealed() -> None:
    orchestrator = WorkflowOrchestrator.__new__(WorkflowOrchestrator)
    orchestrator._a2_co2_route_conditioning_at_atmosphere_context = {
        "route_conditioning_phase": "positive_preseal_pressurization",
        "positive_preseal_pressure_overlimit": True,
        "positive_preseal_abort_reason": "preseal_abort_pressure_exceeded",
        "positive_preseal_pressure_hpa": 1280.989,
        "positive_preseal_route_open": True,
        "seal_command_sent": False,
        "pressure_setpoint_command_sent": False,
        "sampling_started": False,
        "sample_count": 0,
        "points_completed": 0,
        "any_write_command_sent": False,
    }

    decision = orchestrator._guard_a2_conditioning_vent_command(
        reason="final safe stop after positive preseal abort",
        vent_classification="emergency_abort_relief",
        emergency_abort_relief=True,
    )

    assert decision.get("vent_command_blocked") is not True
    assert decision["emergency_abort_relief_vent_required"] is True
    assert decision["emergency_abort_relief_vent_allowed"] is True
    assert decision["emergency_abort_relief_vent_command_sent"] is True
    assert decision["emergency_abort_relief_pressure_hpa"] == 1280.989
    assert decision["emergency_abort_relief_may_mix_air"] is False
    assert decision["normal_maintenance_vent_blocked_after_flush_phase"] is False
    assert decision["cleanup_vent_classification"] == "emergency_abort_relief"
    assert decision["safe_stop_pressure_relief_result"] == "command_sent"


@pytest.mark.parametrize(
    ("context_update", "blocked_reason"),
    [
        ({"seal_command_sent": True}, "seal_command_sent"),
        ({"sampling_started": True}, "sample_started"),
        ({"pressure_setpoint_command_sent": True}, "pressure_setpoint_command_sent"),
    ],
)
def test_positive_preseal_abort_emergency_relief_blocks_mix_risk(
    context_update: dict,
    blocked_reason: str,
) -> None:
    orchestrator = WorkflowOrchestrator.__new__(WorkflowOrchestrator)
    context = {
        "route_conditioning_phase": "positive_preseal_pressurization",
        "positive_preseal_pressure_overlimit": True,
        "positive_preseal_abort_reason": "preseal_abort_pressure_exceeded",
        "positive_preseal_pressure_hpa": 1280.989,
        "positive_preseal_route_open": True,
        "seal_command_sent": False,
        "pressure_setpoint_command_sent": False,
        "sampling_started": False,
        "sample_count": 0,
        "points_completed": 0,
        "any_write_command_sent": False,
    }
    context.update(context_update)
    orchestrator._a2_co2_route_conditioning_at_atmosphere_context = context

    decision = orchestrator._guard_a2_conditioning_vent_command(
        reason="final safe stop after positive preseal abort",
        vent_classification="emergency_abort_relief",
        emergency_abort_relief=True,
    )

    assert decision["vent_command_blocked"] is True
    assert decision["emergency_abort_relief_vent_allowed"] is False
    assert decision["emergency_abort_relief_vent_command_sent"] is False
    assert blocked_reason in decision["emergency_abort_relief_vent_blocked_reason"]
    assert decision["cleanup_vent_classification"] == "emergency_abort_relief"
    assert decision["normal_maintenance_vent_blocked_after_flush_phase"] is False


def test_cleanup_safe_stop_relief_allowed_after_flush_when_unsealed() -> None:
    orchestrator = WorkflowOrchestrator.__new__(WorkflowOrchestrator)
    orchestrator._a2_co2_route_conditioning_at_atmosphere_context = {
        "route_conditioning_phase": "ready_to_seal_phase",
        "seal_command_sent": False,
        "pressure_setpoint_command_sent": False,
        "pressure_ready_started": False,
        "sampling_started": False,
        "sample_count": 0,
        "points_completed": 0,
        "any_write_command_sent": False,
    }

    decision = orchestrator._guard_a2_conditioning_vent_command(
        reason="after CO2 route fail-closed",
        vent_classification="cleanup_relief",
    )

    assert decision.get("vent_command_blocked") is not True
    assert decision["cleanup_vent_requested"] is True
    assert decision["cleanup_vent_classification"] == "cleanup_relief"
    assert decision["cleanup_vent_allowed"] is True
    assert decision["cleanup_vent_is_normal_maintenance"] is False
    assert decision["cleanup_vent_is_safe_stop_relief"] is True
    assert decision["safe_stop_relief_required"] is True
    assert decision["safe_stop_relief_allowed"] is True
    assert decision["safe_stop_relief_command_sent"] is True
    assert decision["normal_maintenance_vent_blocked_after_flush_phase"] is False
    assert decision["vent_blocked_after_flush_phase_is_failure"] is False


def test_normal_maintenance_vent_after_flush_remains_blocked() -> None:
    orchestrator = WorkflowOrchestrator.__new__(WorkflowOrchestrator)
    orchestrator._cfg_get = lambda _path, default=None: default
    orchestrator._as_float = lambda value: None if value in (None, "") else float(value)
    orchestrator._a2_co2_route_conditioning_at_atmosphere_context = {
        "route_conditioning_phase": "ready_to_seal_phase",
        "seal_command_sent": False,
        "pressure_setpoint_command_sent": False,
        "sampling_started": False,
        "sample_count": 0,
        "points_completed": 0,
    }

    decision = orchestrator._guard_a2_conditioning_vent_command(
        reason="periodic maintenance",
        vent_classification="normal_maintenance_vent",
    )

    assert decision["vent_command_blocked"] is True
    assert decision["vent_pulse_blocked_reason"] == "route_conditioning_phase_not_flush"
    assert decision["normal_maintenance_vent_blocked_after_flush_phase"] is True
    assert decision["cleanup_vent_is_normal_maintenance"] is True
    assert decision["cleanup_vent_is_safe_stop_relief"] is False
    assert decision["vent_blocked_after_flush_phase_is_failure"] is True


@pytest.mark.parametrize(
    ("context_update", "blocked_reason"),
    [
        ({"seal_command_sent": True}, "seal_command_sent"),
        ({"sampling_started": True}, "sample_started"),
        ({"pressure_setpoint_command_sent": True}, "pressure_setpoint_command_sent"),
    ],
)
def test_cleanup_safe_stop_relief_blocks_mix_risk(
    context_update: dict,
    blocked_reason: str,
) -> None:
    orchestrator = WorkflowOrchestrator.__new__(WorkflowOrchestrator)
    context = {
        "route_conditioning_phase": "ready_to_seal_phase",
        "seal_command_sent": False,
        "pressure_setpoint_command_sent": False,
        "sampling_started": False,
        "sample_count": 0,
        "points_completed": 0,
        "any_write_command_sent": False,
    }
    context.update(context_update)
    orchestrator._a2_co2_route_conditioning_at_atmosphere_context = context

    decision = orchestrator._guard_a2_conditioning_vent_command(
        reason="final safe stop",
        vent_classification="safe_stop_relief",
    )

    assert decision["vent_command_blocked"] is True
    assert decision["cleanup_vent_allowed"] is False
    assert decision["cleanup_vent_is_normal_maintenance"] is False
    assert decision["cleanup_vent_is_safe_stop_relief"] is True
    assert decision["safe_stop_relief_allowed"] is False
    assert decision["safe_stop_relief_command_sent"] is False
    assert blocked_reason in decision["safe_stop_relief_blocked_reason"]
    assert decision["normal_maintenance_vent_blocked_after_flush_phase"] is False
    assert decision["vent_blocked_after_flush_phase_is_failure"] is False


def test_max_vent_gap_phase_and_threshold_explain_non_exceeded(monkeypatch) -> None:
    orchestrator, _point, _clock, _timing_events, _route_traces, _vent_calls = _conditioning_guard_orchestrator(
        monkeypatch,
        [{"pressure_hpa": 1009.0, "age_s": 0.1, "sequence_id": 2}],
    )
    context = {
        "route_conditioning_phase": "ready_to_seal_phase",
        "max_vent_pulse_write_gap_ms_including_terminal_gap": 1107.851,
        "max_vent_pulse_gap_limit_ms": 2000.0,
        "vent_ticks": [],
        "vent_pulse_interval_ms": [],
    }

    updated = orchestrator._a2_conditioning_context_with_counts(context)

    assert updated["max_vent_pulse_write_gap_phase"] == "ready_to_seal_phase"
    assert updated["max_vent_pulse_write_gap_threshold_ms"] == 2000.0
    assert updated["max_vent_pulse_write_gap_threshold_source"] == (
        "route_conditioning_vent_maintenance_max_gap_s"
    )
    assert updated["max_vent_pulse_write_gap_exceeded"] is False
    assert updated["max_vent_pulse_write_gap_not_exceeded_reason"] == (
        "1107.851ms <= 2000.0ms in ready_to_seal_phase"
    )


def test_co2_conditioning_gauge_sequence_stop_defers_pressure_monitor(monkeypatch) -> None:
    orchestrator, point, clock, timing_events, route_traces, _vent_calls = _conditioning_guard_orchestrator(
        monkeypatch,
        [
            {"pressure_hpa": 1009.0, "age_s": 0.1, "sequence_id": 4},
            {"pressure_hpa": 1009.2, "age_s": 0.1, "sequence_id": 4, "hold_sequence": True},
        ],
    )
    first = orchestrator._record_a2_co2_conditioning_pressure_monitor(point, phase="conditioning_pressure_monitor")
    clock["now"] += 0.6

    second = orchestrator._record_a2_co2_conditioning_pressure_monitor(point, phase="conditioning_pressure_monitor")

    context = orchestrator._a2_co2_route_conditioning_at_atmosphere_context
    assert first["digital_gauge_sequence_progress"] is True
    assert context["digital_gauge_sequence_progress"] is False
    assert second["pressure_monitor_nonblocking"] is True
    assert second["conditioning_monitor_pressure_deferred"] is True
    assert second["selected_pressure_sample_stale_deferred_for_vent_priority"] is True
    assert second["selected_pressure_sample_stale_triggered_p3_fallback"] is False
    assert context["fail_closed_before_vent_off"] is False
    assert route_traces == []
    assert not any(event["event_name"] == "co2_route_conditioning_stream_stale" for event in timing_events)


def test_co2_conditioning_p3_interruption_without_restart_defers_pressure_monitor(monkeypatch) -> None:
    orchestrator, point, _clock, _timing_events, route_traces, _vent_calls = _conditioning_guard_orchestrator(
        monkeypatch,
        [
            {
                "pressure_hpa": 1009.0,
                "age_s": 0.1,
                "sequence_id": 4,
                "continuous_interrupted_by_command": True,
                "continuous_restart_attempted": False,
                "continuous_restart_result": "",
            }
        ],
    )

    tick = orchestrator._record_a2_co2_conditioning_pressure_monitor(point, phase="conditioning_pressure_monitor")

    context = orchestrator._a2_co2_route_conditioning_at_atmosphere_context
    assert context["continuous_interrupted_by_command"] is True
    assert context["continuous_restart_result"] == ""
    assert tick["pressure_monitor_nonblocking"] is True
    assert tick["conditioning_monitor_pressure_deferred"] is True
    assert tick["continuous_latest_fresh_triggered_p3_fallback"] is False
    assert context["fail_closed_before_vent_off"] is False
    assert context["vent_off_sent_at"] == ""
    assert context["seal_command_sent"] is False
    assert route_traces == []


def test_co2_conditioning_p3_fast_poll_fresh_read_passes_freshness_gate(monkeypatch) -> None:
    orchestrator, point, _clock, timing_events, route_traces, _vent_calls = _conditioning_guard_orchestrator(
        monkeypatch,
        [{"pressure_hpa": 1009.0, "age_s": 0.0, "sequence_id": 8, "raw_response": "1009.000"}],
        cfg_overrides={"workflow.pressure.a2_conditioning_pressure_source": "p3_fast_poll"},
    )

    tick = orchestrator._record_a2_co2_conditioning_pressure_monitor(point, phase="conditioning_pressure_monitor")

    assert tick["pressure_source_selected"] == "digital_pressure_gauge_continuous"
    assert tick["pressure_source_selection_reason"] == "digital_gauge_continuous_latest_fresh"
    assert tick["digital_gauge_stream_stale"] is False
    assert tick["selected_pressure_freshness_ok"] is True
    assert tick["selected_pressure_sample_is_stale"] is False
    assert tick["pressure_monitor_nonblocking"] is True
    assert tick["critical_window_uses_query"] is False
    assert tick["whether_safe_to_continue"] is True
    assert route_traces == []
    assert any(event["event_name"] == "co2_route_conditioning_pressure_sample" for event in timing_events)


def test_co2_conditioning_p3_fast_poll_unfresh_read_defers_pressure_monitor(monkeypatch) -> None:
    orchestrator, point, _clock, _timing_events, route_traces, _vent_calls = _conditioning_guard_orchestrator(
        monkeypatch,
        [
            {
                "pressure_hpa": None,
                "age_s": 0.0,
                "sequence_id": 1,
                "parse_ok": False,
                "error": "p3_fast_poll_no_pressure_frame",
            }
        ],
        cfg_overrides={"workflow.pressure.a2_conditioning_pressure_source": "p3_fast_poll"},
    )

    tick = orchestrator._record_a2_co2_conditioning_pressure_monitor(point, phase="conditioning_pressure_monitor")

    context = orchestrator._a2_co2_route_conditioning_at_atmosphere_context
    assert tick["pressure_source_selected"] == "digital_pressure_gauge_continuous"
    assert context["selected_pressure_fail_closed_reason"] == "selected_pressure_unavailable"
    assert context["conditioning_monitor_pressure_deferred"] is True
    assert context["fail_closed_before_vent_off"] is False
    assert context["vent_off_sent_at"] == ""
    assert context["seal_command_sent"] is False
    assert route_traces == []


def test_co2_conditioning_v1_aligned_continuous_fresh_does_not_trigger_p3(monkeypatch) -> None:
    orchestrator, point, _clock, _timing_events, route_traces, _vent_calls = _conditioning_guard_orchestrator(
        monkeypatch,
        [{"pressure_hpa": 1009.0, "age_s": 0.1, "sequence_id": 8}],
        cfg_overrides={"workflow.pressure.a2_conditioning_pressure_source": "v1_aligned"},
    )

    def fail_if_called(**_kwargs):
        raise AssertionError("P3 fallback should not run while continuous frame is fresh")

    orchestrator.pressure_control_service._a2_v1_aligned_pressure_gauge_sample = fail_if_called

    tick = orchestrator._record_a2_co2_conditioning_pressure_monitor(point, phase="conditioning_pressure_monitor")

    assert tick["pressure_source_selected"] == "digital_pressure_gauge_continuous"
    assert tick["pressure_source_selection_reason"] == "digital_gauge_continuous_latest_fresh"
    assert tick["selected_pressure_source"] == "digital_pressure_gauge_continuous"
    assert tick["selected_pressure_freshness_ok"] is True
    assert tick["continuous_stream_stale"] is False
    assert tick["critical_window_uses_latest_frame"] is True
    assert tick["critical_window_uses_query"] is False
    assert tick["p3_fast_fallback_attempted"] is False
    assert tick["whether_safe_to_continue"] is True
    assert route_traces == []


def test_co2_conditioning_v1_aligned_continuous_stale_defers_without_p3(monkeypatch) -> None:
    orchestrator, point, _clock, _timing_events, route_traces, _vent_calls = _conditioning_guard_orchestrator(
        monkeypatch,
        [
            {"pressure_hpa": 1009.0, "age_s": 10.0, "sequence_id": 8, "is_stale": True},
            {"pressure_hpa": 1009.2, "age_s": 0.0, "sequence_id": 9, "raw_response": "1009.200"},
        ],
        cfg_overrides={"workflow.pressure.a2_conditioning_pressure_source": "v1_aligned"},
    )

    tick = orchestrator._record_a2_co2_conditioning_pressure_monitor(point, phase="conditioning_pressure_monitor")

    assert tick["pressure_source_selected"] == "digital_pressure_gauge_continuous"
    assert tick["pressure_source_selection_reason"] == "digital_gauge_continuous_latest_stale"
    assert tick["critical_window_uses_latest_frame"] is True
    assert tick["critical_window_uses_query"] is False
    assert tick["p3_fast_fallback_attempted"] is False
    assert tick["continuous_latest_fresh_triggered_p3_fallback"] is False
    assert tick["conditioning_monitor_pressure_deferred"] is True
    assert tick["selected_pressure_sample_stale_deferred_for_vent_priority"] is True
    assert route_traces == []


def test_co2_conditioning_v1_aligned_continuous_stale_skips_normal_p3_in_flush(monkeypatch) -> None:
    orchestrator, point, _clock, _timing_events, route_traces, _vent_calls = _conditioning_guard_orchestrator(
        monkeypatch,
        [{"pressure_hpa": 1009.0, "age_s": 10.0, "sequence_id": 8, "is_stale": True}],
        cfg_overrides={"workflow.pressure.a2_conditioning_pressure_source": "v1_aligned"},
    )
    _install_v1_aligned_selected_p3_reader(orchestrator)

    tick = orchestrator._record_a2_co2_conditioning_pressure_monitor(point, phase="conditioning_pressure_monitor")

    assert tick["pressure_source_selected"] == "digital_pressure_gauge_continuous"
    assert tick["pressure_source_selection_reason"] == "digital_gauge_continuous_latest_stale"
    assert tick["continuous_stream_stale"] is True
    assert tick["digital_gauge_stream_stale"] is True
    assert tick["selected_pressure_sample_is_stale"] is True
    assert tick["selected_pressure_freshness_ok"] is False
    assert tick["conditioning_monitor_pressure_deferred"] is True
    assert tick["normal_p3_fallback_attempted"] is False
    assert route_traces == []


def test_co2_conditioning_v1_aligned_continuous_stale_skips_fast_p3_in_flush(monkeypatch) -> None:
    orchestrator, point, _clock, _timing_events, route_traces, _vent_calls = _conditioning_guard_orchestrator(
        monkeypatch,
        [{"pressure_hpa": 1009.0, "age_s": 10.0, "sequence_id": 8, "is_stale": True}],
        cfg_overrides={"workflow.pressure.a2_conditioning_pressure_source": "v1_aligned"},
    )
    _install_v1_aligned_selected_p3_reader(
        orchestrator,
        digital_gauge_mode="v1_aligned_p3_fast",
        pressure_source_selection_reason="continuous_stale_fallback_to_p3_fast",
        source_selection_reason="continuous_stale_fallback_to_p3_fast",
        p3_fast_fallback_result="success",
        normal_p3_fallback_attempted=False,
        normal_p3_fallback_result="",
        last_pressure_command="read_pressure_fast",
    )

    tick = orchestrator._record_a2_co2_conditioning_pressure_monitor(point, phase="conditioning_pressure_monitor")

    assert tick["pressure_source_selection_reason"] == "digital_gauge_continuous_latest_stale"
    assert tick["continuous_stream_stale"] is True
    assert tick["selected_pressure_freshness_ok"] is False
    assert tick["conditioning_monitor_pressure_deferred"] is True
    assert tick["p3_fast_fallback_attempted"] is False
    assert route_traces == []


def test_co2_conditioning_v1_aligned_stale_defers_before_p3_sample_stale(monkeypatch) -> None:
    orchestrator, point, _clock, _timing_events, route_traces, _vent_calls = _conditioning_guard_orchestrator(
        monkeypatch,
        [{"pressure_hpa": 1009.0, "age_s": 10.0, "sequence_id": 8, "is_stale": True}],
        cfg_overrides={"workflow.pressure.a2_conditioning_pressure_source": "v1_aligned"},
    )
    _install_v1_aligned_selected_p3_reader(
        orchestrator,
        pressure_sample_age_s=3.5,
        sample_age_s=3.5,
        pressure_sample_is_stale=True,
        is_stale=True,
    )

    tick = orchestrator._record_a2_co2_conditioning_pressure_monitor(point, phase="conditioning_pressure_monitor")

    context = orchestrator._a2_co2_route_conditioning_at_atmosphere_context
    assert context["selected_pressure_freshness_ok"] is False
    assert context["selected_pressure_fail_closed_reason"] == "selected_pressure_sample_stale"
    assert tick["conditioning_monitor_pressure_deferred"] is True
    assert tick["p3_fast_fallback_attempted"] is False
    assert context["fail_closed_before_vent_off"] is False
    assert route_traces == []


def test_co2_conditioning_v1_aligned_stale_defers_before_p3_parse(monkeypatch) -> None:
    orchestrator, point, _clock, _timing_events, route_traces, _vent_calls = _conditioning_guard_orchestrator(
        monkeypatch,
        [{"pressure_hpa": 1009.0, "age_s": 10.0, "sequence_id": 8, "is_stale": True}],
        cfg_overrides={"workflow.pressure.a2_conditioning_pressure_source": "v1_aligned"},
    )
    _install_v1_aligned_selected_p3_reader(
        orchestrator,
        pressure_hpa=None,
        parse_ok=False,
        error="bad p3 response",
        pressure_sample_age_s=0.02,
        sample_age_s=0.02,
    )

    tick = orchestrator._record_a2_co2_conditioning_pressure_monitor(point, phase="conditioning_pressure_monitor")

    context = orchestrator._a2_co2_route_conditioning_at_atmosphere_context
    assert context["selected_pressure_freshness_ok"] is False
    assert context["selected_pressure_fail_closed_reason"] == "selected_pressure_sample_stale"
    assert tick["conditioning_monitor_pressure_deferred"] is True
    assert tick["continuous_latest_fresh_triggered_p3_fallback"] is False
    assert context["fail_closed_before_vent_off"] is False
    assert route_traces == []


def test_co2_conditioning_stale_defers_before_pressure_controller_substitution(monkeypatch) -> None:
    orchestrator, point, _clock, _timing_events, route_traces, _vent_calls = _conditioning_guard_orchestrator(
        monkeypatch,
        [{"pressure_hpa": 1009.0, "age_s": 10.0, "sequence_id": 8, "is_stale": True}],
        cfg_overrides={"workflow.pressure.a2_conditioning_pressure_source": "v1_aligned"},
    )
    _install_v1_aligned_selected_p3_reader(
        orchestrator,
        source="pressure_controller",
        pressure_sample_source="pressure_controller",
        pressure_source_selected="pressure_controller",
        pressure_source_selection_reason="controller_substitution_attempt",
        source_selection_reason="controller_substitution_attempt",
    )

    tick = orchestrator._record_a2_co2_conditioning_pressure_monitor(point, phase="conditioning_pressure_monitor")

    context = orchestrator._a2_co2_route_conditioning_at_atmosphere_context
    assert context["selected_pressure_source"] == "digital_pressure_gauge_continuous"
    assert context["selected_pressure_fail_closed_reason"] == "selected_pressure_sample_stale"
    assert tick["conditioning_monitor_pressure_deferred"] is True
    assert tick["selected_pressure_sample_stale_triggered_source_selection"] is False
    assert context["fail_closed_before_vent_off"] is False
    assert route_traces == []


def test_co2_conditioning_v1_aligned_blocks_fallback_after_seal(monkeypatch) -> None:
    orchestrator, point, _clock, _timing_events, route_traces, _vent_calls = _conditioning_guard_orchestrator(
        monkeypatch,
        [{"pressure_hpa": 1009.0, "age_s": 10.0, "sequence_id": 8, "is_stale": True}],
        cfg_overrides={"workflow.pressure.a2_conditioning_pressure_source": "v1_aligned"},
    )
    orchestrator._a2_co2_route_conditioning_at_atmosphere_context["seal_command_sent"] = True

    def fail_if_called(**_kwargs):
        raise AssertionError("P3 fallback must not run after seal")

    orchestrator.pressure_control_service._a2_v1_aligned_pressure_gauge_sample = fail_if_called

    with pytest.raises(WorkflowValidationError):
        orchestrator._record_a2_co2_conditioning_vent_tick(point, phase="conditioning_hold")

    context = orchestrator._a2_co2_route_conditioning_at_atmosphere_context
    assert context["pressure_source_selection_reason"] == "v1_aligned_fallback_not_allowed_outside_atmosphere_conditioning"
    assert context["fail_closed_before_vent_off"] is True
    assert context["vent_off_sent_at"] == ""
    assert context["seal_command_sent"] is True
    assert route_traces[-1]["action"] == "co2_route_conditioning_vent_blocked_after_flush_phase"
    assert context["vent_pulse_blocked_reason"] == "seal_command_sent"


@pytest.mark.parametrize(
    "context_update",
    [
        {"vent_off_sent_at": "2026-04-28T00:00:00+00:00"},
        {"pressure_setpoint_command_sent": True},
    ],
)
def test_co2_conditioning_v1_aligned_blocks_fallback_after_safe_window(monkeypatch, context_update) -> None:
    orchestrator, point, _clock, _timing_events, route_traces, _vent_calls = _conditioning_guard_orchestrator(
        monkeypatch,
        [{"pressure_hpa": 1009.0, "age_s": 10.0, "sequence_id": 8, "is_stale": True}],
        cfg_overrides={"workflow.pressure.a2_conditioning_pressure_source": "v1_aligned"},
    )
    orchestrator._a2_co2_route_conditioning_at_atmosphere_context.update(context_update)

    def fail_if_called(**_kwargs):
        raise AssertionError("P3 fallback must stay inside A2 atmosphere conditioning safe window")

    orchestrator.pressure_control_service._a2_v1_aligned_pressure_gauge_sample = fail_if_called

    if context_update.get("pressure_setpoint_command_sent"):
        with pytest.raises(WorkflowValidationError):
            orchestrator._record_a2_co2_conditioning_pressure_monitor(point, phase="conditioning_pressure_monitor")
        context = orchestrator._a2_co2_route_conditioning_at_atmosphere_context
        assert context["pressure_source_selection_reason"] == (
            "v1_aligned_fallback_not_allowed_outside_atmosphere_conditioning"
        )
        assert context["fail_closed_before_vent_off"] is True
        assert route_traces[-1]["action"] == "co2_route_conditioning_stream_stale"
    else:
        tick = orchestrator._record_a2_co2_conditioning_pressure_monitor(point, phase="conditioning_pressure_monitor")
        context = orchestrator._a2_co2_route_conditioning_at_atmosphere_context
        assert context["pressure_source_selection_reason"] == "digital_gauge_continuous_latest_stale"
        assert tick["conditioning_monitor_pressure_deferred"] is True
        assert tick["continuous_latest_fresh_triggered_p3_fallback"] is False
        assert context["fail_closed_before_vent_off"] is False
        assert route_traces == []


def test_a2_v1_aligned_service_fast_p3_success_marks_continuous_interruption() -> None:
    service, host, _controller, _status = _positive_preseal_service([1010.2])
    _seed_digital_stream_latest(service, 1009.0, age_s=2.0, sequence=1)
    gauge = host._device("pressure_meter")

    sample = service._a2_v1_aligned_pressure_gauge_sample(
        stage="co2_route_conditioning_at_atmosphere",
        point_index=1,
        continuous_sample={"is_stale": True},
    )

    assert sample["pressure_source_selected"] == "digital_pressure_gauge_p3"
    assert sample["pressure_source_selection_reason"] == "continuous_stale_fallback_to_p3_fast"
    assert sample["p3_fast_fallback_attempted"] is True
    assert sample["p3_fast_fallback_result"] == "success"
    assert sample["normal_p3_fallback_attempted"] is False
    assert sample["critical_window_uses_query"] is True
    assert sample["last_pressure_command"] == "read_pressure_fast"
    assert sample["last_pressure_command_may_cancel_continuous"] is True
    assert sample["continuous_interrupted_by_command"] is True
    assert sample["continuous_restart_required_before_return_to_continuous"] is True
    assert gauge.fast_read_count == 1
    assert gauge.normal_read_count == 0
    assert any(event["event_name"] == "digital_gauge_continuous_command_may_cancel" for event in host._recorded_timing)


def test_a2_v1_aligned_service_fast_p3_failure_falls_back_to_normal_p3() -> None:
    service, host, _controller, _status = _positive_preseal_service([1010.4])
    _seed_digital_stream_latest(service, 1009.0, age_s=2.0, sequence=1)
    gauge = host._device("pressure_meter")
    gauge.fast_read_error = RuntimeError("fast timeout")

    sample = service._a2_v1_aligned_pressure_gauge_sample(
        stage="co2_route_conditioning_at_atmosphere",
        point_index=1,
        continuous_sample={"is_stale": True},
    )

    assert sample["pressure_source_selected"] == "digital_pressure_gauge_p3"
    assert sample["pressure_source_selection_reason"] == "p3_fast_failed_fallback_normal_p3"
    assert sample["p3_fast_fallback_attempted"] is True
    assert sample["p3_fast_fallback_result"] == "failed"
    assert sample["normal_p3_fallback_attempted"] is True
    assert sample["normal_p3_fallback_result"] == "success"
    assert sample["pressure_hpa"] == 1010.4
    assert gauge.fast_read_count == 1
    assert gauge.normal_read_count == 1


def test_a2_v1_aligned_service_all_p3_reads_fail_closed_payload() -> None:
    service, host, _controller, _status = _positive_preseal_service([1010.4])
    _seed_digital_stream_latest(service, 1009.0, age_s=2.0, sequence=1)
    gauge = host._device("pressure_meter")
    gauge.fast_read_error = RuntimeError("fast timeout")
    gauge.normal_read_error = RuntimeError("normal timeout")

    sample = service._a2_v1_aligned_pressure_gauge_sample(
        stage="co2_route_conditioning_at_atmosphere",
        point_index=1,
        continuous_sample={"is_stale": True},
    )

    assert sample["pressure_source_selected"] == ""
    assert sample["pressure_source_selection_reason"] == "digital_gauge_v1_aligned_read_unavailable"
    assert sample["p3_fast_fallback_result"] == "failed"
    assert sample["normal_p3_fallback_attempted"] is True
    assert sample["normal_p3_fallback_result"] == "failed"
    assert sample["fail_closed_reason"] == "digital_gauge_v1_aligned_read_unavailable"
    assert sample["usable_for_abort"] is False
    assert gauge.fast_read_count == 1
    assert gauge.normal_read_count == 1


def test_final_safe_stop_warning_records_stop_state_mismatch(tmp_path) -> None:
    route_traces: list[dict] = []
    logs: list[str] = []

    class Chamber:
        def read_run_state(self) -> str:
            return "RUN"

    chamber = Chamber()

    def device(name: str):
        return chamber if name == "temperature_chamber" else None

    def call_first(_device, _methods, *args):
        raise RuntimeError("STOP_STATE_MISMATCH")

    host = SimpleNamespace(
        _device=device,
        _call_first=call_first,
        _log=logs.append,
        _cfg_get=lambda _path, default=None: default,
        _as_int=lambda value: None if value in (None, "") else int(value),
        status_service=SimpleNamespace(record_route_trace=lambda **kwargs: route_traces.append(kwargs)),
    )
    service = ValveRoutingService(SimpleNamespace(), RunState(), host=host)

    summary = service.safe_stop_after_run(baseline_already_restored=True, reason="test")

    assert summary["final_safe_stop_warning_count"] == 1
    assert summary["final_safe_stop_chamber_stop_warning"] == "chamber stop failed: STOP_STATE_MISMATCH"
    assert summary["final_safe_stop_chamber_stop_attempted"] is True
    assert summary["final_safe_stop_chamber_stop_command_sent"] is True
    assert summary["final_safe_stop_chamber_stop_result"] == "failed"
    assert "Final safe stop warning: chamber stop failed: STOP_STATE_MISMATCH" in logs
    assert route_traces[-1]["actual"]["final_safe_stop_chamber_stop_result"] == "failed"

    trace_path = tmp_path / "route_trace.jsonl"
    trace_path.write_text(json.dumps(route_traces[-1]) + "\n", encoding="utf-8")
    evidence = _final_safe_stop_evidence(tmp_path)
    assert evidence["final_safe_stop_warning_count"] == 1
    assert evidence["final_safe_stop_chamber_stop_warning"] == "chamber stop failed: STOP_STATE_MISMATCH"
    assert evidence["final_safe_stop_chamber_stop_command_sent"] is True


def test_a2_no_write_final_safe_stop_blocks_chamber_stop_command(tmp_path) -> None:
    route_traces: list[dict] = []
    logs: list[str] = []
    stop_calls: list[str] = []

    class Chamber:
        def read_run_state(self) -> str:
            return "RUN"

    chamber = Chamber()

    def device(name: str):
        return chamber if name == "temperature_chamber" else None

    def call_first(_device, _methods, *args):
        stop_calls.append("stop")

    host = SimpleNamespace(
        service=SimpleNamespace(
            no_write_guard=SimpleNamespace(enabled=True),
            _raw_cfg={
                "run001_a2": {
                    "scope": "run001_a2_co2_no_write_pressure_sweep",
                    "no_write": True,
                    "chamber_stop_enabled": False,
                }
            },
        ),
        _device=device,
        _call_first=call_first,
        _log=logs.append,
        _cfg_get=lambda path, default=None: {
            "run001_a2.chamber_stop_enabled": False,
            "a2_co2_7_pressure_no_write_probe.chamber_stop_enabled": False,
            "chamber_stop_enabled": False,
        }.get(path, default),
        _as_int=lambda value: None if value in (None, "") else int(value),
        status_service=SimpleNamespace(record_route_trace=lambda **kwargs: route_traces.append(kwargs)),
    )
    service = ValveRoutingService(SimpleNamespace(), RunState(), host=host)

    summary = service.safe_stop_after_run(baseline_already_restored=True, reason="test")

    assert stop_calls == []
    assert summary["final_safe_stop_chamber_stop_attempted"] is True
    assert summary["final_safe_stop_chamber_stop_command_sent"] is False
    assert summary["final_safe_stop_chamber_stop_blocked_by_no_write"] is True
    assert summary["final_safe_stop_chamber_stop_result"] == "blocked_by_no_write"
    assert route_traces[-1]["actual"]["final_safe_stop_chamber_stop_command_sent"] is False
    assert route_traces[-1]["actual"]["final_safe_stop_chamber_stop_blocked_by_no_write"] is True

    trace_path = tmp_path / "route_trace.jsonl"
    trace_path.write_text(json.dumps(route_traces[-1]) + "\n", encoding="utf-8")
    evidence = _final_safe_stop_evidence(tmp_path)
    assert evidence["final_safe_stop_chamber_stop_command_sent"] is False
    assert evidence["final_safe_stop_chamber_stop_blocked_by_no_write"] is True


def test_co2_runner_conditioning_fail_closed_does_not_vent_off_seal_or_sample() -> None:
    point = CalibrationPoint(index=1, temperature_c=20.0, co2_ppm=100.0, pressure_hpa=1100.0, route="co2")
    order: list[str] = []
    service = SimpleNamespace()
    service._as_float = lambda value: None if value in (None, "") else float(value)
    service.route_context = SimpleNamespace(enter=lambda **kwargs: None, update=lambda **kwargs: None, clear=lambda: None)
    service.route_planner = SimpleNamespace(build_co2_pressure_point=lambda source, item: item, co2_point_tag=lambda item: "co2")
    service.event_bus = SimpleNamespace(publish=lambda *args, **kwargs: None)
    service.status_service = SimpleNamespace(
        check_stop=lambda: None,
        update_status=lambda **kwargs: None,
        record_route_trace=lambda **kwargs: None,
        log=lambda message: None,
    )
    service.temperature_control_service = SimpleNamespace(
        set_temperature_for_point=lambda point, phase: SimpleNamespace(ok=True, timed_out=False, error=""),
        capture_temperature_calibration_snapshot=lambda *args, **kwargs: None,
    )
    service.valve_routing_service = SimpleNamespace(
        set_co2_route_baseline=lambda **kwargs: order.append("baseline"),
        set_valves_for_co2=lambda point: order.append("route_open"),
        cleanup_co2_route=lambda **kwargs: order.append("cleanup"),
    )
    service.pressure_control_service = SimpleNamespace(
        _current_pressure=lambda: 1009.0,
        pressurize_and_hold=lambda point, route: order.append("pressurize") or SimpleNamespace(ok=True),
        set_pressure_to_target=lambda point: order.append("high_pressure") or SimpleNamespace(ok=True),
    )
    service.sampling_service = SimpleNamespace(collect_samples=lambda *args, **kwargs: order.append("sample") or [])
    service.qc_service = SimpleNamespace(run_point_qc=lambda *args, **kwargs: None)
    service._record_workflow_timing = lambda event_name, event_type="info", **kwargs: order.append(event_name)
    service._record_a2_conditioning_workflow_timing = (
        lambda context, event_name, event_type="info", **kwargs: order.append(f"a2_timing:{event_name}") or {}
    )
    service._begin_a2_co2_route_conditioning_at_atmosphere = lambda point, pressure_refs: order.append("conditioning_start")
    service._wait_co2_route_soak_before_seal = lambda point: (_ for _ in ()).throw(
        WorkflowValidationError("conditioning fail closed", details={"fail_closed_before_vent_off": True})
    )

    result = Co2RouteRunner(service, point, [point]).execute()

    assert result.success is False
    assert "cleanup" in order
    assert "pressurize" not in order
    assert "high_pressure" not in order
    assert "sample" not in order


def test_co2_runner_defers_high_pressure_until_after_conditioning() -> None:
    point = CalibrationPoint(index=1, temperature_c=20.0, co2_ppm=100.0, pressure_hpa=1100.0, route="co2")
    pressure_points = [
        CalibrationPoint(index=index + 1, temperature_c=20.0, co2_ppm=100.0, pressure_hpa=pressure, route="co2")
        for index, pressure in enumerate(A2_AUTHORIZED_PRESSURE_POINTS_HPA)
    ]
    order: list[str] = []
    route_traces: list[dict] = []

    class SlowPressure:
        def _current_pressure(self):
            order.append("route_open_pressure_snapshot")
            return 1009.0

        def pressurize_and_hold(self, point, route):
            order.append("pressurize")
            return SimpleNamespace(ok=False)

    service = SimpleNamespace()
    service._a2_high_pressure_first_point_mode_enabled = False
    service._as_float = lambda value: None if value in (None, "") else float(value)
    service.route_context = SimpleNamespace(enter=lambda **kwargs: None, update=lambda **kwargs: None, clear=lambda: None)
    service.route_planner = SimpleNamespace(
        build_co2_pressure_point=lambda source, item: item,
        co2_point_tag=lambda item: "co2_groupa_100ppm_1100hpa",
    )
    service.event_bus = SimpleNamespace(publish=lambda *args, **kwargs: None)
    service.status_service = SimpleNamespace(
        check_stop=lambda: None,
        update_status=lambda **kwargs: None,
        record_route_trace=lambda **kwargs: route_traces.append(kwargs),
        log=lambda message: None,
        begin_point_timing=lambda *args, **kwargs: None,
        clear_point_timing=lambda *args, **kwargs: None,
    )
    service.temperature_control_service = SimpleNamespace(
        set_temperature_for_point=lambda point, phase: SimpleNamespace(ok=True, timed_out=False, error=""),
        capture_temperature_calibration_snapshot=lambda *args, **kwargs: None,
    )
    service.valve_routing_service = SimpleNamespace(
        set_co2_route_baseline=lambda **kwargs: order.append("baseline"),
        set_valves_for_co2=lambda point: order.append("route_open"),
        cleanup_co2_route=lambda **kwargs: order.append("cleanup"),
    )
    service.pressure_control_service = SlowPressure()
    service._record_workflow_timing = lambda event_name, event_type="info", **kwargs: order.append(event_name)
    service._record_a2_conditioning_workflow_timing = (
        lambda context, event_name, event_type="info", **kwargs: order.append(f"a2_timing:{event_name}") or {}
    )

    def begin_conditioning(point, pressure_refs):
        order.append("conditioning_start")
        service._a2_co2_route_conditioning_at_atmosphere_active = True
        service._a2_high_pressure_first_point_mode_enabled = False

    def end_conditioning(point, **kwargs):
        order.append("conditioning_end")
        service._a2_co2_route_conditioning_completed = True
        service._a2_co2_route_conditioning_at_atmosphere_active = False

    def preseal_gate(point, **kwargs):
        order.append("preseal_gate")
        service._a2_preseal_analyzer_gate_passed = True
        return True

    def prearm(point, pressure_refs):
        order.append("prearm")
        service._a2_high_pressure_first_point_mode_enabled = True
        return {"enabled": True, "baseline_pressure_hpa": 1009.0, "first_target_pressure_hpa": 1100.0}

    def preclose(point):
        order.append("preclose_vent")
        return {"ok": True}

    def request(point):
        order.append("first_pressure_request")
        service._record_workflow_timing(
            "route_open_pressure_poll_request",
            "info",
            stage="high_pressure_first_point",
            point=point,
        )
        return "positive_preseal_ready_handoff"

    def prepare_after_conditioning(point, pressure_refs):
        order.append("seal_preparation")
        prearm(point, pressure_refs)
        preclose(point)
        request(point)

    service._begin_a2_co2_route_conditioning_at_atmosphere = begin_conditioning
    service._end_a2_co2_route_conditioning_at_atmosphere = end_conditioning
    service._preseal_analyzer_gate_after_conditioning = preseal_gate
    service._prepare_a2_high_pressure_first_point_after_conditioning = prepare_after_conditioning
    service._wait_co2_route_soak_before_seal = lambda point: order.append("wait_route_soak") or True

    result = Co2RouteRunner(service, point, pressure_points).execute()

    assert result.success is False
    assert order.index("conditioning_start") < order.index("co2_route_open_start") < order.index("route_open")
    assert order.index("route_open") < order.index("wait_route_soak") < order.index("conditioning_end")
    assert order.index("conditioning_end") < order.index("preseal_gate") < order.index("seal_preparation")
    assert order.index("seal_preparation") < order.index("prearm") < order.index("preclose_vent")
    assert order.index("preclose_vent") < order.index("first_pressure_request") < order.index("pressurize")
    assert "a2_timing:co2_route_open_end" in order
    assert "route_open_pressure_snapshot" not in order


def test_a2_readiness_locks_no_write_fleet_and_pressure_points(tmp_path) -> None:
    truth = {
        "read_only": True,
        "passive_listen_only": True,
        "commands_sent": [],
        "analyzers": [
            _truth_row("COM35", "001"),
            _truth_row("COM37", "029"),
            _truth_row("COM41", "003"),
            _truth_row("COM42", "004"),
        ],
    }
    (tmp_path / "truth.json").write_text(json.dumps(truth), encoding="utf-8")
    raw_cfg = _a2_raw_config("truth.json")

    readiness = evaluate_run001_a2_readiness(
        raw_cfg,
        config_path=tmp_path / "config.json",
        point_rows=_a2_points(),
    )
    guard = build_no_write_guard_from_raw_config(raw_cfg)

    assert readiness["readiness_result"] == RUN001_PASS
    assert readiness["a2_point_pressure_points_hpa"] == A2_AUTHORIZED_PRESSURE_POINTS_HPA
    assert readiness["effective_analyzer_fleet_summary"]["configured_ports"] == [
        "COM35",
        "COM37",
        "COM41",
        "COM42",
    ]
    assert guard is not None
    assert guard.scope == "run001_a2"


def test_a2_no_write_guard_remains_zero_for_a2_payload(tmp_path) -> None:
    truth = {
        "read_only": True,
        "passive_listen_only": True,
        "commands_sent": [],
        "analyzers": [
            _truth_row("COM35", "001"),
            _truth_row("COM37", "029"),
            _truth_row("COM41", "003"),
            _truth_row("COM42", "004"),
        ],
    }
    (tmp_path / "truth.json").write_text(json.dumps(truth), encoding="utf-8")
    raw_cfg = _a2_raw_config("truth.json")

    payload = build_run001_a2_evidence_payload(
        raw_cfg,
        config_path=tmp_path / "config.json",
        run_dir=tmp_path / "artifacts",
        point_rows=_a2_points(),
        guard=build_no_write_guard_from_raw_config(raw_cfg),
        require_runtime_artifacts=False,
    )

    assert payload["no_write"] is True
    assert payload["attempted_write_count"] == 0
    assert payload["identity_write_command_sent"] is False
    assert payload["persistent_write_command_sent"] is False


def test_conditioning_overlimit_evidence_matches_latency_samples_and_route_trace(tmp_path) -> None:
    run_id = "run-conditioning-overlimit"
    pressure_state = {
        "pressure_hpa": 1155.0,
        "pressure_sample_source": "digital_pressure_gauge_continuous",
        "source": "digital_pressure_gauge_continuous",
        "latest_frame_age_s": 0.1,
        "latest_frame_sequence_id": 8,
        "digital_gauge_sequence_progress": True,
        "conditioning_pressure_abort_hpa": 1150.0,
        "pressure_overlimit_seen": True,
        "pressure_overlimit_source": "digital_pressure_gauge_continuous",
        "pressure_overlimit_hpa": 1155.0,
        "route_conditioning_phase": "route_conditioning_flush_phase",
        "ready_to_seal_phase_started": False,
        "route_conditioning_flush_min_time_completed": False,
        "vent_pulse_count": 2,
        "vent_pulse_interval_ms": [500.0],
        "pressure_drop_after_vent_hpa": [None, -145.0],
        "route_open_to_first_pressure_read_ms": 500.0,
        "route_open_to_overlimit_ms": 500.0,
        "route_conditioning_pressure_before_route_open_hpa": 1010.0,
        "route_conditioning_pressure_after_route_open_hpa": 1155.0,
        "route_conditioning_pressure_rise_rate_hpa_per_s": 290.0,
        "route_conditioning_peak_pressure_hpa": 1155.0,
        "route_conditioning_pressure_overlimit": True,
        "fail_closed_reason": "route_conditioning_pressure_overlimit",
        "fail_closed_before_vent_off": True,
        "vent_off_sent_at": "",
        "seal_command_sent": False,
        "sample_count": 0,
        "points_completed": 0,
    }
    events = [
        {
            "event_name": "co2_route_conditioning_start",
            "event_type": "start",
            "stage": "co2_route_conditioning_at_atmosphere",
            "point_index": 1,
            "timestamp_monotonic_s": 10.0,
            "timestamp_local": "2026-04-27T10:00:00+00:00",
            "route_state": {
                "route_state": {
                    "vent_heartbeat_interval_s": 1.0,
                    "pressure_monitor_interval_s": 0.5,
                    "conditioning_pressure_abort_hpa": 1150.0,
                }
            },
        },
        {
            "event_name": "co2_route_open_end",
            "event_type": "end",
            "stage": "co2_route_open",
            "point_index": 1,
            "timestamp_monotonic_s": 12.0,
            "timestamp_local": "2026-04-27T10:00:02+00:00",
            "route_state": {"route_state": {}},
        },
        {
            "event_name": "co2_route_conditioning_vent_tick",
            "event_type": "tick",
            "stage": "co2_route_conditioning_at_atmosphere",
            "point_index": 1,
            "timestamp_monotonic_s": 12.5,
            "timestamp_local": "2026-04-27T10:00:02.500000+00:00",
            "pressure_hpa": 1155.0,
            "route_state": {
                "route_state": {
                    **pressure_state,
                    "phase": "after_route_open",
                    "route_open_to_first_vent_s": 0.5,
                    "vent_heartbeat_gap_s": 1.0,
                }
            },
        },
        {
            "event_name": "pressure_source_selected",
            "event_type": "info",
            "stage": "co2_route_conditioning_at_atmosphere",
            "point_index": 1,
            "timestamp_monotonic_s": 12.5,
            "timestamp_local": "2026-04-27T10:00:02.500000+00:00",
            "pressure_hpa": 1155.0,
            "route_state": {
                "route_state": {
                    **pressure_state,
                    "digital_gauge_pressure_sample": {
                        **pressure_state,
                        "request_sent_monotonic_s": 12.49,
                        "response_received_monotonic_s": 12.5,
                        "read_latency_s": 0.01,
                        "sample_recorded_monotonic_s": 12.5,
                        "sample_age_s": 0.1,
                        "sequence_id": 8,
                        "parse_ok": True,
                    },
                }
            },
        },
        {
            "event_name": "co2_route_conditioning_pressure_overlimit",
            "event_type": "fail",
            "stage": "co2_route_conditioning_at_atmosphere",
            "point_index": 1,
            "timestamp_monotonic_s": 12.5,
            "timestamp_local": "2026-04-27T10:00:02.500000+00:00",
            "pressure_hpa": 1155.0,
            "route_state": {"route_state": dict(pressure_state)},
        },
    ]
    (tmp_path / "workflow_timing_trace.jsonl").write_text(
        "".join(json.dumps(event) + "\n" for event in events),
        encoding="utf-8",
    )
    route_row = {
        "action": "co2_preseal_atmosphere_hold_pressure_guard",
        "result": "fail",
        "actual": dict(pressure_state),
    }
    (tmp_path / "route_trace.jsonl").write_text(json.dumps(route_row) + "\n", encoding="utf-8")
    payload = {"run_id": run_id, "sample_count": 0, "points_completed": 0, "a2_final_decision": "FAIL"}

    conditioning = _build_co2_route_conditioning_evidence(tmp_path, payload, timing_summary={})
    _latency_payload, latency_samples = _build_pressure_read_latency_diagnostics(tmp_path, payload)
    route_trace = json.loads((tmp_path / "route_trace.jsonl").read_text(encoding="utf-8").splitlines()[0])

    assert conditioning["pressure_overlimit_seen"] is True
    assert conditioning["pressure_overlimit_source"] == "digital_pressure_gauge_continuous"
    assert conditioning["pressure_overlimit_hpa"] == 1155.0
    assert conditioning["fail_closed_before_vent_off"] is True
    assert conditioning["route_open_to_first_vent_s"] == 0.5
    assert conditioning["route_open_to_first_vent_ms"] == 500.0
    assert conditioning["vent_pulse_count"] == 2
    assert conditioning["max_vent_pulse_gap_ms"] == 500.0
    assert conditioning["route_open_to_first_pressure_read_ms"] == 500.0
    assert conditioning["route_open_to_overlimit_ms"] == 500.0
    assert conditioning["route_conditioning_pressure_rise_rate_hpa_per_s"] == 290.0
    assert conditioning["route_conditioning_pressure_overlimit"] is True
    assert conditioning["fail_closed_reason"] == "route_conditioning_pressure_overlimit"
    assert any(sample["pressure_overlimit_seen"] is True for sample in latency_samples)
    assert route_trace["actual"]["pressure_overlimit_seen"] is True
    assert route_trace["actual"]["fail_closed_before_vent_off"] is True


def test_conditioning_evidence_fails_when_flush_actions_escape_before_completion(tmp_path) -> None:
    def event(name: str, mono: float, *, route_state: dict | None = None) -> dict:
        return {
            "event_name": name,
            "event_type": "info",
            "stage": "co2_route_conditioning_at_atmosphere",
            "point_index": 1,
            "timestamp_monotonic_s": mono,
            "timestamp_local": f"2026-04-27T10:00:{int(mono):02d}+00:00",
            "route_state": {"route_state": route_state or {}},
        }

    events = [
        event(
            "co2_route_conditioning_start",
            10.0,
            route_state={
                "route_conditioning_phase": "route_conditioning_flush_phase",
                "route_conditioning_vent_maintenance_active": True,
                "vent_maintenance_started_monotonic_s": 10.0,
            },
        ),
        event(
            "co2_route_conditioning_vent_tick",
            10.5,
            route_state={
                "phase": "before_route_open",
                "vent_pulse_count": 1,
                "vent_pulse_interval_ms": [],
                "vent_scheduler_tick_count": 1,
                "vent_off_blocked_during_flush": True,
                "seal_blocked_during_flush": True,
                "pressure_setpoint_blocked_during_flush": True,
                "sample_blocked_during_flush": True,
            },
        ),
        event("seal_preparation_vent_off", 11.0),
        event("positive_preseal_seal_start", 11.1),
        event("pressure_setpoint_start", 11.2),
        event("sample_start", 11.3),
        event("co2_route_conditioning_end", 12.0, route_state={"conditioning_decision": "PASS"}),
    ]
    (tmp_path / "workflow_timing_trace.jsonl").write_text(
        "".join(json.dumps(item) + "\n" for item in events),
        encoding="utf-8",
    )
    payload = {"run_id": "unsafe-flush-actions", "a2_final_decision": "PASS"}

    conditioning = _build_co2_route_conditioning_evidence(tmp_path, payload, timing_summary={})
    finalized = _finalize_artifact_decision(
        {
            "a2_final_decision": RUN001_PASS,
            "co2_route_conditioning_evidence": conditioning,
            "a2_decision_reasons": [],
        },
        tmp_path,
    )

    assert conditioning["vent_off_command_during_flush"] is True
    assert conditioning["seal_command_during_flush"] is True
    assert conditioning["pressure_setpoint_command_during_flush"] is True
    assert conditioning["sample_command_during_flush"] is True
    assert finalized["final_decision"] == RUN001_FAIL
    assert "a2_co2_route_conditioning_vent_off_before_flush_completed" in finalized["a2_decision_reasons"]
    assert "a2_co2_route_conditioning_seal_before_flush_completed" in finalized["a2_decision_reasons"]
    assert "a2_co2_route_conditioning_pressure_setpoint_before_flush_completed" in finalized["a2_decision_reasons"]
    assert "a2_co2_route_conditioning_sample_before_flush_completed" in finalized["a2_decision_reasons"]


def test_a2_temperature_skip_probe_reads_current_pv_without_chamber_commands() -> None:
    calls: list[tuple] = []
    timing_events: list[dict] = []

    class Chamber:
        def read_temp_c(self) -> float:
            calls.append(("read_temp_c",))
            return 23.4

        def set_temp_c(self, value: float) -> None:
            calls.append(("set_temp_c", value))

        def start(self) -> None:
            calls.append(("start",))

        def stop(self) -> None:
            calls.append(("stop",))

    chamber = Chamber()
    cfg = {
        "workflow.stability.temperature.skip_temperature_stabilization_wait": True,
        "workflow.stability.temperature.temperature_gate_mode": "current_pv_engineering_probe",
        "workflow.stability.temperature.temperature_not_part_of_acceptance": True,
    }
    host = SimpleNamespace()
    host.service = SimpleNamespace(no_write_guard=SimpleNamespace(enabled=True))
    host._cfg_get = lambda path, default=None: cfg.get(path, default)
    host._device = lambda name: chamber if name == "temperature_chamber" else None
    host._first_method = lambda device, names: next(
        (getattr(device, name) for name in names if hasattr(device, name)),
        None,
    )
    host._log = lambda message: None
    host._record_workflow_timing = lambda event_name, event_type="info", **kwargs: timing_events.append(
        {"event_name": event_name, "event_type": event_type, **kwargs}
    )
    host._as_int = lambda value: int(float(value))
    point = CalibrationPoint(index=1, temperature_c=20.0, co2_ppm=100.0, pressure_hpa=1100.0, route="co2")

    service = TemperatureControlService(SimpleNamespace(), RunState(), host=host)
    result = service.set_temperature_for_point(point, phase="co2")

    assert result.ok is True
    assert result.final_temp_c == 23.4
    assert result.diagnostics["temperature_stabilization_wait_skipped"] is True
    assert result.diagnostics["temperature_gate_mode"] == "current_pv_engineering_probe"
    assert result.diagnostics["temperature_not_part_of_acceptance"] is True
    assert calls == [("read_temp_c",)]
    assert not any(event["event_name"] == "temperature_chamber_settle_timeout" for event in timing_events)


def _a2_device_policy_raw_config(
    *,
    skip_temp_wait: bool = True,
    explicit_temperature_control: bool = False,
    multi_temperature: bool = False,
) -> dict:
    gate_mode = "current_pv_engineering_probe" if skip_temp_wait else "temperature_acceptance"
    return {
        "run001_a2": {
            "scope": "run001_a2_co2_no_write_pressure_sweep",
            "no_write": True,
            "co2_only": True,
            "single_temperature_group": not multi_temperature,
            "multi_temperature_enabled": multi_temperature,
            "chamber_set_temperature_enabled": explicit_temperature_control,
            "chamber_start_enabled": explicit_temperature_control,
            "chamber_stop_enabled": explicit_temperature_control,
        },
        "a2_co2_7_pressure_no_write_probe": {
            "scope": "a2_co2_7_pressure_no_write",
            "co2_only": True,
            "single_temperature": not multi_temperature,
            "temperature_stabilization_wait_skipped": skip_temp_wait,
            "temperature_gate_mode": gate_mode,
            "temperature_not_part_of_acceptance": skip_temp_wait,
            "multi_temperature_enabled": multi_temperature,
            "chamber_set_temperature_enabled": explicit_temperature_control,
            "chamber_start_enabled": explicit_temperature_control,
            "chamber_stop_enabled": explicit_temperature_control,
        },
        "workflow": {
            "route_mode": "co2_only",
            "selected_temps_c": [20.0] if not multi_temperature else [20.0, 25.0],
            "stability": {
                "temperature": {
                    "skip_temperature_stabilization_wait": skip_temp_wait,
                    "temperature_stabilization_wait_skipped": skip_temp_wait,
                    "temperature_gate_mode": gate_mode,
                    "temperature_not_part_of_acceptance": skip_temp_wait,
                }
            },
        },
    }


def _device_policy_orchestrator(raw_cfg: dict) -> WorkflowOrchestrator:
    orchestrator = WorkflowOrchestrator.__new__(WorkflowOrchestrator)
    orchestrator.service = SimpleNamespace(_raw_cfg=raw_cfg)
    orchestrator.config = SimpleNamespace()
    orchestrator._device_init_policy_evidence = {}
    orchestrator._record_workflow_timing = lambda *_args, **_kwargs: {}
    orchestrator._log_messages = []
    orchestrator._log = orchestrator._log_messages.append
    orchestrator._event_rows = []
    orchestrator.event_bus = SimpleNamespace(
        publish=lambda event_type, payload: orchestrator._event_rows.append((event_type, payload))
    )
    orchestrator._warnings = []
    orchestrator.session = SimpleNamespace(add_warning=orchestrator._warnings.append)
    return orchestrator


def test_a2_skip_temp_chamber_init_failed_is_optional_context_device() -> None:
    raw_cfg = _a2_device_policy_raw_config(skip_temp_wait=True)
    orchestrator = _device_policy_orchestrator(raw_cfg)
    all_devices = [
        "pressure_controller",
        "pressure_meter",
        "relay_a",
        "relay_b",
        "temperature_chamber",
        "gas_analyzer_0",
    ]

    policy = orchestrator._handle_device_failures(
        ["temperature_chamber"],
        all_devices=all_devices,
        error_message="Critical device initialization failed",
        warning_prefix="Device open warnings",
        stage="initialization",
    )

    assert policy["critical_devices_failed"] == []
    assert policy["optional_context_devices_failed"] == ["temperature_chamber"]
    assert policy["temperature_chamber_required_for_a2"] is False
    assert policy["temperature_chamber_init_attempted"] is True
    assert policy["temperature_chamber_init_ok"] is False
    assert policy["temperature_chamber_init_failed"] is True
    assert policy["temperature_chamber_init_failure_blocks_a2"] is False
    assert policy["temperature_chamber_optional_in_skip_temp_wait"] is True
    assert policy["temperature_context_available"] is False
    assert policy["temperature_context_unavailable_reason"] == "temperature_chamber_init_failed"
    assert policy["temperature_chamber_readonly_probe_attempted"] is True
    assert policy["temperature_chamber_readonly_probe_result"] == "unavailable"
    assert policy["optional_context_failure_blocks_probe"] is False
    assert orchestrator._warnings


def test_a2_explicit_temperature_control_keeps_chamber_critical() -> None:
    raw_cfg = _a2_device_policy_raw_config(
        skip_temp_wait=False,
        explicit_temperature_control=True,
    )
    orchestrator = _device_policy_orchestrator(raw_cfg)
    all_devices = ["pressure_controller", "pressure_meter", "relay_a", "relay_b", "temperature_chamber"]

    policy = orchestrator._classify_device_failures(
        ["temperature_chamber"],
        all_devices=all_devices,
        stage="initialization",
    )

    assert policy["critical_devices_failed"] == ["temperature_chamber"]
    assert policy["optional_context_devices_failed"] == []
    assert policy["temperature_chamber_required_for_a2"] is True
    assert policy["temperature_chamber_init_failure_blocks_a2"] is True
    assert policy["temperature_chamber_optional_in_skip_temp_wait"] is False


def test_a2_multi_temperature_keeps_chamber_critical() -> None:
    raw_cfg = _a2_device_policy_raw_config(
        skip_temp_wait=False,
        explicit_temperature_control=True,
        multi_temperature=True,
    )
    orchestrator = _device_policy_orchestrator(raw_cfg)

    policy = orchestrator._classify_device_failures(
        ["temperature_chamber"],
        all_devices=["temperature_chamber", "gas_analyzer_0"],
        stage="initialization",
    )

    assert policy["critical_devices_failed"] == ["temperature_chamber"]
    assert policy["temperature_chamber_required_for_a2"] is True
    assert policy["temperature_chamber_init_failure_blocks_a2"] is True


def test_a2_route_pressure_device_failure_still_blocks_probe() -> None:
    raw_cfg = _a2_device_policy_raw_config(skip_temp_wait=True)
    orchestrator = _device_policy_orchestrator(raw_cfg)
    all_devices = [
        "pressure_controller",
        "pressure_meter",
        "relay_a",
        "relay_b",
        "temperature_chamber",
        "gas_analyzer_0",
    ]

    with pytest.raises(WorkflowValidationError) as excinfo:
        orchestrator._handle_device_failures(
            ["pressure_controller", "relay_a", "gas_analyzer_0"],
            all_devices=all_devices,
            error_message="Critical device initialization failed",
            warning_prefix="Device open warnings",
            stage="initialization",
        )

    assert excinfo.value.context["failed_devices"] == [
        "gas_analyzer_0",
        "pressure_controller",
        "relay_a",
    ]
    assert excinfo.value.context["critical_device_init_failure_blocks_probe"] is True


def test_a2_artifacts_keep_preflight_distinct_from_execute_pass(tmp_path) -> None:
    truth = {
        "read_only": True,
        "passive_listen_only": True,
        "commands_sent": [],
        "analyzers": [
            _truth_row("COM35", "001"),
            _truth_row("COM37", "029"),
            _truth_row("COM41", "003"),
            _truth_row("COM42", "004"),
        ],
    }
    (tmp_path / "truth.json").write_text(json.dumps(truth), encoding="utf-8")
    raw_cfg = _a2_raw_config("truth.json")
    payload = build_run001_a2_evidence_payload(
        raw_cfg,
        config_path=tmp_path / "config.json",
        run_dir=tmp_path / "artifacts",
        point_rows=_a2_points(),
        guard=build_no_write_guard_from_raw_config(raw_cfg),
        require_runtime_artifacts=False,
    )

    written = write_run001_a2_artifacts(tmp_path / "artifacts", payload)
    summary = json.loads((tmp_path / "artifacts" / "summary.json").read_text(encoding="utf-8"))
    guard = json.loads((tmp_path / "artifacts" / "no_write_guard.json").read_text(encoding="utf-8"))
    manifest = json.loads((tmp_path / "artifacts" / "run_manifest.json").read_text(encoding="utf-8"))
    report = (tmp_path / "artifacts" / "human_readable_report.md").read_text(encoding="utf-8")

    assert written["pressure_gate_evidence"].endswith("pressure_gate_evidence.json")
    assert written["positive_preseal_pressurization_evidence"].endswith(
        "positive_preseal_pressurization_evidence.json"
    )
    assert written["positive_preseal_pressurization_samples"].endswith(
        "positive_preseal_pressurization_samples.csv"
    )
    assert written["positive_preseal_timing_diagnostics"].endswith(
        "positive_preseal_timing_diagnostics.json"
    )
    assert written["co2_route_conditioning_evidence"].endswith("co2_route_conditioning_evidence.json")
    assert written["route_open_pressure_surge_evidence"].endswith("route_open_pressure_surge_evidence.json")
    assert written["pressure_read_latency_diagnostics"].endswith("pressure_read_latency_diagnostics.json")
    assert written["pressure_read_latency_samples"].endswith("pressure_read_latency_samples.csv")
    assert written["high_pressure_first_point_evidence"].endswith("high_pressure_first_point_evidence.json")
    assert written["critical_pressure_freshness_evidence"].endswith("critical_pressure_freshness_evidence.json")
    assert written["workflow_timing_trace"].endswith("workflow_timing_trace.jsonl")
    assert written["workflow_timing_summary"].endswith("workflow_timing_summary.json")
    assert summary["a2_final_decision"] == RUN001_NOT_EXECUTED
    assert summary["final_decision"] == RUN001_PASS
    assert guard["a2_final_decision"] == RUN001_NOT_EXECUTED
    assert summary["not_real_acceptance_evidence"] is True
    assert summary["v2_replaces_v1_claim"] is False
    assert summary["workflow_timing_trace_artifact"].endswith("workflow_timing_trace.jsonl")
    assert summary["workflow_timing_summary_artifact"].endswith("workflow_timing_summary.json")
    assert summary["positive_preseal_pressurization_evidence_artifact"].endswith(
        "positive_preseal_pressurization_evidence.json"
    )
    assert summary["co2_route_conditioning_evidence_artifact"].endswith(
        "co2_route_conditioning_evidence.json"
    )
    assert summary["high_pressure_first_point_evidence_artifact"].endswith(
        "high_pressure_first_point_evidence.json"
    )
    assert manifest["high_pressure_first_point_evidence_artifact"].endswith(
        "high_pressure_first_point_evidence.json"
    )
    assert summary["critical_pressure_freshness_evidence_artifact"].endswith(
        "critical_pressure_freshness_evidence.json"
    )
    assert manifest["critical_pressure_freshness_evidence_artifact"].endswith(
        "critical_pressure_freshness_evidence.json"
    )
    assert "workflow_timing_artifacts" in manifest
    assert "Positive preseal pressurization summary" in report
    assert "CO2 通大气洗刷与封路时序审计" in report
    assert "流程时序摘要" in report or "workflow_timing_trace" in report
    assert "route_open_pressure_surge_evidence" in report
    assert "high_pressure_first_point_evidence" in report
    assert "关键压力取数新鲜度诊断" in report
    assert "stale pressure samples are not usable" in report


def test_a2_artifacts_include_co2_conditioning_evidence_and_vent_ticks(tmp_path) -> None:
    truth = {
        "read_only": True,
        "passive_listen_only": True,
        "commands_sent": [],
        "analyzers": [
            _truth_row("COM35", "001"),
            _truth_row("COM37", "029"),
            _truth_row("COM41", "003"),
            _truth_row("COM42", "004"),
        ],
    }
    (tmp_path / "truth.json").write_text(json.dumps(truth), encoding="utf-8")
    raw_cfg = _a2_raw_config("truth.json")
    raw_cfg["workflow"]["pressure"] = {
        "co2_route_conditioning_atmosphere_required": True,
        "vent_hold_interval_s": 2.0,
        "positive_preseal_pressurization_enabled": True,
        "preseal_ready_pressure_hpa": 1110.0,
        "preseal_abort_pressure_hpa": 1150.0,
    }
    artifact_dir = tmp_path / "artifacts"
    artifact_dir.mkdir()
    for filename, content in {
        "summary.json": "{}",
        "run_manifest.json": "{}",
        "points.csv": "timestamp,point_index,status\n",
        "io_log.csv": "timestamp,device,direction,data\n",
        "run.log": "conditioning evidence\n",
        "samples.csv": "timestamp,point_index\n",
        "route_trace.jsonl": "",
    }.items():
        (artifact_dir / filename).write_text(content, encoding="utf-8")

    def timing_event(name, event_type, mono, *, stage, pressure=None, route_state=None, decision=None):
        return {
            "event_name": name,
            "event_type": event_type,
            "timestamp_local": "2026-04-26T10:00:00+00:00",
            "timestamp_monotonic_s": mono,
            "elapsed_from_run_start_s": mono - 100.0,
            "stage": stage,
            "point_index": 1,
            "target_pressure_hpa": 1100.0,
            "duration_s": None,
            "expected_max_s": None,
            "wait_reason": None,
            "blocking_condition": None,
            "decision": decision,
            "route_state": route_state,
            "pressure_hpa": pressure,
            "chamber_temperature_c": None,
            "dewpoint_c": None,
            "pace_output_state": None,
            "pace_isolation_state": None,
            "pace_vent_status": None,
            "sample_count": None,
            "warning_code": None,
            "error_code": None,
            "no_write_guard_active": True,
        }

    timing_rows = [
        timing_event(
            "co2_route_conditioning_start",
            "start",
            100.0,
            stage="co2_route_conditioning_at_atmosphere",
            route_state={"conditioning_soak_s": 10.0, "atmosphere_vent_enabled": True},
        ),
        timing_event("co2_route_open_start", "start", 100.1, stage="co2_route_open"),
        timing_event("co2_route_open_end", "end", 100.2, stage="co2_route_open", pressure=1009.0),
        timing_event(
            "co2_route_conditioning_vent_tick",
            "tick",
            100.3,
            stage="co2_route_conditioning_at_atmosphere",
            pressure=1009.0,
            route_state={
                "phase": "before_route_open",
                "command_result": "ok",
                "output_state": 0,
                "isolation_state": 1,
                "vent_status": 1,
                "digital_gauge_pressure_hpa": 1009.0,
                "pressure_sample_age_s": 0.05,
            },
        ),
        timing_event(
            "co2_route_conditioning_vent_tick",
            "tick",
            102.3,
            stage="co2_route_conditioning_at_atmosphere",
            pressure=1112.0,
            route_state={
                "phase": "conditioning_hold",
                "command_result": "ok",
                "output_state": 0,
                "isolation_state": 1,
                "vent_status": 1,
                "digital_gauge_pressure_hpa": 1112.0,
                "pressure_sample_age_s": 0.06,
            },
        ),
        timing_event(
            "co2_route_conditioning_pressure_sample",
            "tick",
            102.3,
            stage="co2_route_conditioning_at_atmosphere",
            pressure=1112.0,
            decision="monitor_only_no_seal",
        ),
        timing_event(
            "co2_route_conditioning_end",
            "end",
            110.0,
            stage="co2_route_conditioning_at_atmosphere",
            decision="PASS",
        ),
        timing_event("preseal_analyzer_gate_start", "start", 110.1, stage="preseal_analyzer_gate"),
        timing_event("preseal_analyzer_gate_end", "end", 110.2, stage="preseal_analyzer_gate", decision="PASS"),
        timing_event("seal_preparation_after_conditioning_start", "start", 110.3, stage="seal_preparation_after_conditioning"),
        timing_event("high_pressure_first_point_mode_start", "start", 110.4, stage="high_pressure_first_point"),
        timing_event(
            "high_pressure_ready_detected_after_conditioning",
            "info",
            111.0,
            stage="high_pressure_first_point",
            pressure=1110.5,
        ),
        timing_event(
            "high_pressure_seal_command_sent",
            "info",
            111.1,
            stage="high_pressure_first_point",
            pressure=1110.5,
            route_state={"conditioning_completed_before_high_pressure_mode": True},
        ),
    ]
    (artifact_dir / "workflow_timing_trace.jsonl").write_text(
        "\n".join(json.dumps(row) for row in timing_rows) + "\n",
        encoding="utf-8",
    )
    payload = build_run001_a2_evidence_payload(
        raw_cfg,
        config_path=tmp_path / "config.json",
        run_dir=artifact_dir,
        point_rows=_a2_points(),
        guard=build_no_write_guard_from_raw_config(raw_cfg),
        artifact_paths={
            "summary": str(artifact_dir / "summary.json"),
            "manifest": str(artifact_dir / "run_manifest.json"),
            "trace": str(artifact_dir / "route_trace.jsonl"),
        },
        require_runtime_artifacts=True,
        service_status={"phase": "failed", "completed_points": 0},
    )

    written = write_run001_a2_artifacts(artifact_dir, payload)
    evidence = json.loads((artifact_dir / "co2_route_conditioning_evidence.json").read_text(encoding="utf-8"))
    high_pressure = json.loads((artifact_dir / "high_pressure_first_point_evidence.json").read_text(encoding="utf-8"))
    summary = json.loads((artifact_dir / "summary.json").read_text(encoding="utf-8"))
    guard = json.loads((artifact_dir / "no_write_guard.json").read_text(encoding="utf-8"))
    report = (artifact_dir / "human_readable_report.md").read_text(encoding="utf-8")

    assert written["co2_route_conditioning_evidence"].endswith("co2_route_conditioning_evidence.json")
    assert evidence["vent_command_before_route_open"] is True
    assert evidence["vent_tick_count"] == 2
    assert evidence["pressure_max_during_conditioning_hpa"] == 1112.0
    assert evidence["did_not_seal_during_conditioning"] is True
    assert high_pressure["conditioning_completed_before_high_pressure_mode"] is True
    assert summary["co2_route_conditioning_vent_tick_count"] == 2
    assert guard["attempted_write_count"] == 0
    assert guard["identity_write_command_sent"] is False
    assert guard["persistent_write_command_sent"] is False
    assert "co2_route_conditioning_evidence" in report


def test_a2_config_splits_temperature_chamber_and_analyzer_timeouts() -> None:
    config_path = (
        Path(__file__).resolve().parents[1]
        / "configs"
        / "validation"
        / "run001_a2_co2_only_7_pressure_no_write_real_machine.json"
    )
    raw = json.loads(config_path.read_text(encoding="utf-8"))
    temperature = raw["workflow"]["stability"]["temperature"]
    pressure = raw["workflow"]["pressure"]

    assert temperature["timeout_s"] == 3600
    assert temperature["require_chamber_settle_before_analyzer"] is True
    assert temperature["analyzer_chamber_temp_timeout_s"] == 1800
    assert temperature["analyzer_chamber_temp_span_c"] == 0.08
    assert temperature["analyzer_chamber_temp_window_s"] == 60
    assert pressure["pressure_rise_detection_threshold_hpa"] == 2.0
    assert pressure["preseal_vent_close_arm_pressure_hpa"] == 1080.0
    assert pressure["preseal_vent_close_arm_margin_hpa"] == 30.0
    assert pressure["preseal_vent_close_arm_time_to_ready_s"] == 3.0
    assert pressure["preseal_vent_close_command_timeout_s"] == 1.0
    assert pressure["preseal_vent_close_verify_timeout_s"] == 1.0
    assert pressure["preseal_vent_close_verify_capture_pressure"] is False
    assert pressure["co2_route_conditioning_atmosphere_required"] is True
    assert pressure["seal_preparation_vent_off_settle_s"] == 0.1
    assert pressure["primary_pressure_source"] == "digital_pressure_gauge"
    assert pressure["pressure_source_cross_check_enabled"] is True
    assert pressure["pressure_source_disagreement_warn_hpa"] == 10.0
    assert pressure["pressure_sample_stale_threshold_s"] == 2.0
    assert pressure["pressure_read_latency_warn_s"] == 0.5
    assert pressure["digital_gauge_continuous_enabled"] is True
    assert pressure["digital_gauge_continuous_mode"] == "P4"
    assert pressure["digital_gauge_latest_frame_stale_max_s"] == 0.5
    assert pressure["critical_pressure_latest_frame_stale_max_s"] == 0.5
    assert pressure["pace_aux_enabled"] is True
    assert pressure["pace_aux_disagreement_warn_hpa"] == 10.0
    assert pressure["route_open_first_pressure_request_expected_max_s"] == 0.5
    assert pressure["route_open_first_pressure_response_expected_max_s"] == 1.0
    assert pressure["pressure_latency_warning_only"] is True
    assert pressure["high_pressure_first_point_mode_enabled"] is True
    assert pressure["high_pressure_first_point_margin_hpa"] == 0.0
    assert pressure["high_pressure_first_point_route_open_request_expected_max_s"] == 0.05
    assert pressure["high_pressure_first_point_route_open_response_expected_max_s"] == 1.0
    assert pressure["expected_route_open_to_ready_max_s"] == 40.0
    assert pressure["expected_positive_preseal_to_ready_max_s"] == 30.0
    assert pressure["expected_ready_to_seal_command_max_s"] == 0.5
    assert pressure["expected_ready_to_seal_confirm_max_s"] == 2.0
    assert pressure["expected_max_pressure_increase_after_ready_hpa"] == 10.0
    assert pressure["expected_vent_hold_tick_interval_s"] == 2.0
    assert pressure["preseal_abort_pressure_hpa"] == 1150.0
    assert pressure["timing_warning_only"] is True


def test_a2_fail_artifacts_include_preseal_atmosphere_hold_evidence(tmp_path) -> None:
    truth = {
        "read_only": True,
        "passive_listen_only": True,
        "commands_sent": [],
        "analyzers": [
            _truth_row("COM35", "001"),
            _truth_row("COM37", "029"),
            _truth_row("COM41", "003"),
            _truth_row("COM42", "004"),
        ],
    }
    (tmp_path / "truth.json").write_text(json.dumps(truth), encoding="utf-8")
    raw_cfg = _a2_raw_config("truth.json")
    raw_cfg["workflow"]["pressure"] = {
        "continuous_atmosphere_hold": True,
        "vent_hold_interval_s": 2.0,
        "preseal_atmosphere_hold_max_hpa": 1110.0,
    }
    artifact_dir = tmp_path / "artifacts"
    artifact_dir.mkdir()
    (artifact_dir / "summary.json").write_text("{}", encoding="utf-8")
    (artifact_dir / "run_manifest.json").write_text("{}", encoding="utf-8")
    (artifact_dir / "points.csv").write_text("timestamp,point_index,status\n", encoding="utf-8")
    (artifact_dir / "io_log.csv").write_text("timestamp,device,direction,data\n", encoding="utf-8")
    (artifact_dir / "run.log").write_text("aborted\n", encoding="utf-8")
    (artifact_dir / "samples.csv").write_text("timestamp,point_index\n", encoding="utf-8")
    trace_rows = [
        {
            "ts": "2026-04-26T04:11:52+00:00",
            "action": "set_co2_valves",
            "route": "co2",
            "point_index": 1,
            "actual": {},
            "target": {},
            "result": "ok",
        },
        {
            "ts": "2026-04-26T04:11:54+00:00",
            "action": "set_vent",
            "route": "co2",
            "point_index": 1,
            "target": {"vent_on": True},
            "actual": {
                "pressure_hpa": 1008.0,
                "vent_status_raw": 1,
                "output_state": 0,
                "isolation_state": 1,
            },
            "result": "ok",
        },
        {
            "ts": "2026-04-26T04:12:00+00:00",
            "action": "co2_preseal_atmosphere_hold_pressure_guard",
            "route": "co2",
            "point_index": 1,
            "actual": {
                "pressure_hpa": 1985.0,
                "limit_hpa": 1110.0,
                "reason": "co2_preseal_atmosphere_hold_pressure_exceeded",
                "pressure_sample_source": "digital_pressure_gauge",
                "request_sent_at": "2026-04-26T04:11:59.800000+00:00",
                "response_received_at": "2026-04-26T04:12:00+00:00",
                "read_latency_s": 0.2,
                "pressure_sample_age_s": 0.0,
                "pressure_sample_is_stale": False,
                "is_cached": False,
                "usable_for_abort": True,
                "usable_for_ready": True,
                "usable_for_seal": True,
                "primary_pressure_source": "digital_pressure_gauge",
                "pressure_source_used_for_decision": "digital_pressure_gauge",
                "pressure_source_used_for_abort": "digital_pressure_gauge",
                "pace_pressure_hpa": 1970.0,
                "pace_pressure_latency_s": 0.05,
                "pace_pressure_age_s": 0.0,
                "digital_gauge_pressure_hpa": 1985.0,
                "digital_gauge_latency_s": 0.2,
                "digital_gauge_age_s": 0.0,
                "pressure_source_disagreement_hpa": 15.0,
                "pressure_source_disagreement_warning": True,
            },
            "result": "fail",
            "message": "CO2 pre-seal atmosphere hold pressure exceeded limit",
        },
    ]
    (artifact_dir / "route_trace.jsonl").write_text(
        "\n".join(json.dumps(row) for row in trace_rows) + "\n",
        encoding="utf-8",
    )
    payload = build_run001_a2_evidence_payload(
        raw_cfg,
        config_path=tmp_path / "config.json",
        run_dir=artifact_dir,
        point_rows=_a2_points(),
        guard=build_no_write_guard_from_raw_config(raw_cfg),
        artifact_paths={
            "summary": str(artifact_dir / "summary.json"),
            "manifest": str(artifact_dir / "run_manifest.json"),
            "trace": str(artifact_dir / "route_trace.jsonl"),
        },
        require_runtime_artifacts=True,
        service_status={
            "phase": "failed",
            "completed_points": 0,
            "message": "CO2 pre-seal atmosphere hold pressure exceeded limit",
        },
    )

    write_run001_a2_artifacts(artifact_dir, payload)
    summary = json.loads((artifact_dir / "summary.json").read_text(encoding="utf-8"))
    guard = json.loads((artifact_dir / "no_write_guard.json").read_text(encoding="utf-8"))
    evidence = json.loads((artifact_dir / "preseal_atmosphere_hold_evidence.json").read_text(encoding="utf-8"))
    route_surge = json.loads((artifact_dir / "route_open_pressure_surge_evidence.json").read_text(encoding="utf-8"))
    latency = json.loads((artifact_dir / "pressure_read_latency_diagnostics.json").read_text(encoding="utf-8"))
    timing_summary = json.loads((artifact_dir / "workflow_timing_summary.json").read_text(encoding="utf-8"))
    timing_events = [
        json.loads(line)
        for line in (artifact_dir / "workflow_timing_trace.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    manifest = json.loads((artifact_dir / "run_manifest.json").read_text(encoding="utf-8"))
    report = (artifact_dir / "human_readable_report.md").read_text(encoding="utf-8")
    samples = (artifact_dir / "preseal_atmosphere_hold_samples.csv").read_text(encoding="utf-8")
    latency_samples = (artifact_dir / "pressure_read_latency_samples.csv").read_text(encoding="utf-8")

    assert summary["a2_final_decision"] == "FAIL"
    assert summary["preseal_atmosphere_hold_decision"] == "FAIL"
    assert summary["preseal_atmosphere_hold_pressure_limit_hpa"] == 1110.0
    assert summary["preseal_atmosphere_hold_max_measured_pressure_hpa"] == 1985.0
    assert summary["preseal_atmosphere_hold_pressure_limit_exceeded"] is True
    assert evidence["vent_status_2_is_not_continuous_atmosphere_evidence"] is True
    assert evidence["pressure_at_abort_hpa"] == 1985.0
    assert evidence["pressure_max_before_abort_hpa"] == 1985.0
    assert evidence["pressure_max_before_seal_hpa"] == 1985.0
    assert evidence["pressure_control_started"] is False
    assert evidence["sample_started"] is False
    assert guard["attempted_write_count"] == 0
    assert guard["identity_write_command_sent"] is False
    assert "pressure_limit_exceeded" in samples
    assert "digital_pressure_gauge" in latency_samples
    assert route_surge["pressure_first_sample_after_route_open_hpa"] == 1985.0
    assert route_surge["abort_decision_pressure_source"] == "digital_pressure_gauge"
    assert route_surge["gauge_read_latency_s"] == 0.2
    assert latency["first_pressure_source"] == "digital_pressure_gauge"
    assert latency["pressure_source_used_for_abort"] == "digital_pressure_gauge"
    assert timing_summary["a2_final_decision"] == "FAIL"
    assert timing_summary["final_decision"] == "FAIL"
    assert timing_summary["preseal_pressure_max_hpa"] == 1985.0
    assert timing_summary["route_open_first_sample_exceeded_abort"] is True
    assert timing_summary["abort_decision_pressure_source"] == "digital_pressure_gauge"
    assert any(event["event_name"] == "preseal_pressure_check" for event in timing_events)
    assert any(event["event_name"] == "run_fail" for event in timing_events)
    assert all(event["no_write_guard_active"] is True for event in timing_events)
    assert manifest["workflow_timing_artifacts"]["trace"].endswith("workflow_timing_trace.jsonl")
    assert manifest["pressure_read_latency_diagnostics_artifact"].endswith("pressure_read_latency_diagnostics.json")
    assert manifest["route_open_pressure_surge_evidence_artifact"].endswith("route_open_pressure_surge_evidence.json")
    assert "流程时序摘要" in report
    assert "开阀瞬间升压诊断" in report
    assert "压力读取延迟与双压力源诊断" in report


def test_a2_artifacts_include_positive_preseal_pressurization_evidence(tmp_path) -> None:
    truth = {
        "read_only": True,
        "passive_listen_only": True,
        "commands_sent": [],
        "analyzers": [
            _truth_row("COM35", "001"),
            _truth_row("COM37", "029"),
            _truth_row("COM41", "003"),
            _truth_row("COM42", "004"),
        ],
    }
    (tmp_path / "truth.json").write_text(json.dumps(truth), encoding="utf-8")
    raw_cfg = _a2_raw_config("truth.json")
    raw_cfg["workflow"]["pressure"] = {
        "positive_preseal_pressurization_enabled": True,
        "preseal_ready_pressure_hpa": 1110.0,
        "preseal_abort_pressure_hpa": 1150.0,
        "preseal_ready_timeout_s": 30.0,
        "preseal_pressure_poll_interval_s": 0.2,
        "pressure_rise_detection_threshold_hpa": 2.0,
        "expected_route_open_to_first_pressure_rise_max_s": 10.0,
        "expected_route_open_to_ready_max_s": 40.0,
        "expected_positive_preseal_to_ready_max_s": 30.0,
        "expected_ready_to_seal_command_max_s": 0.5,
        "expected_ready_to_seal_confirm_max_s": 2.0,
        "expected_max_pressure_increase_after_ready_hpa": 10.0,
        "expected_vent_hold_tick_interval_s": 4.0,
        "expected_vent_hold_pressure_rise_rate_max_hpa_per_s": 100.0,
        "timing_warning_only": True,
        "fail_if_sealed_pressure_below_target": True,
    }
    artifact_dir = tmp_path / "artifacts"
    artifact_dir.mkdir()
    for filename, content in {
        "summary.json": "{}",
        "run_manifest.json": "{}",
        "points.csv": "timestamp,point_index,status\n",
        "io_log.csv": "timestamp,device,direction,data\n",
        "run.log": "ok\n",
        "samples.csv": "timestamp,point_index\n",
    }.items():
        (artifact_dir / filename).write_text(content, encoding="utf-8")
    trace_rows = [
        {
            "ts": "2026-04-26T04:10:50+00:00",
            "action": "set_co2_valves",
            "route": "co2",
            "point_index": 1,
            "target": {"pressure_hpa": 1100.0},
            "actual": {"pressure_hpa": 1009.0},
            "result": "ok",
        },
        {
            "ts": "2026-04-26T04:10:55+00:00",
            "action": "set_vent",
            "route": "co2",
            "point_index": 1,
            "target": {"vent_on": True},
            "actual": {
                "pressure_hpa": 1009.0,
                "atmosphere_ready": True,
                "vent_status_raw": 1,
                "output_state": 0,
                "isolation_state": 1,
            },
            "result": "ok",
        },
        {
            "ts": "2026-04-26T04:10:58+00:00",
            "action": "co2_preseal_atmosphere_hold_pressure_guard",
            "route": "co2",
            "point_index": 1,
            "actual": {
                "pressure_hpa": 1009.5,
                "ready_pressure_hpa": 1110.0,
                "abort_pressure_hpa": 1150.0,
                "reason": "within_limit",
            },
            "result": "ok",
        },
        {
            "ts": "2026-04-26T04:10:59+00:00",
            "action": "preseal_atmosphere_flush_ready_handoff",
            "route": "co2",
            "point_index": 1,
            "actual": {
                "pressure_hpa": 1111.0,
                "ready_pressure_hpa": 1110.0,
                "abort_pressure_hpa": 1150.0,
                "reason": "positive_preseal_ready_handoff",
            },
            "result": "ok",
        },
        {
            "ts": "2026-04-26T04:11:00+00:00",
            "action": "positive_preseal_pressurization_start",
            "route": "co2",
            "point_index": 1,
            "actual": {
                "stage": "positive_preseal_pressurization",
                "target_pressure_hpa": 1100.0,
                "measured_atmospheric_pressure_hpa": 1246.758,
                "current_line_pressure_hpa": 1105.0,
                "preseal_ready_pressure_hpa": 1110.0,
                "preseal_abort_pressure_hpa": 1150.0,
                "preseal_ready_timeout_s": 30.0,
                "preseal_pressure_poll_interval_s": 0.2,
            },
            "result": "ok",
        },
        {
            "ts": "2026-04-26T04:11:00.200000+00:00",
            "action": "set_vent",
            "route": "co2",
            "point_index": 1,
            "target": {"vent_on": False},
            "actual": {
                "pressure_hpa": 1105.0,
                "vent_status_raw": 0,
                "output_state": 0,
                "isolation_state": 1,
            },
            "result": "ok",
            "message": "positive CO2 preseal pressurization before route seal",
        },
        {
            "ts": "2026-04-26T04:11:01+00:00",
            "action": "positive_preseal_pressure_check",
            "route": "co2",
            "point_index": 1,
            "actual": {
                "target_pressure_hpa": 1100.0,
                "pressure_hpa": 1105.0,
                "elapsed_s": 1.0,
                "preseal_ready_pressure_hpa": 1110.0,
                "preseal_abort_pressure_hpa": 1150.0,
            },
            "result": "ok",
        },
        {
            "ts": "2026-04-26T04:11:02+00:00",
            "action": "positive_preseal_ready",
            "route": "co2",
            "point_index": 1,
            "actual": {
                "target_pressure_hpa": 1100.0,
                "pressure_hpa": 1110.5,
                "elapsed_s": 2.0,
                "ready_reached": True,
                "ready_reached_at_pressure_hpa": 1110.5,
                "seal_trigger_pressure_hpa": 1110.5,
                "seal_trigger_elapsed_s": 2.0,
            },
            "result": "ok",
        },
        {
            "ts": "2026-04-26T04:11:02.200000+00:00",
            "action": "seal_route",
            "route": "co2",
            "point_index": 1,
            "actual": {
                "pressure_hpa": 1109.8,
                "positive_preseal_pressurization_enabled": True,
                "preseal_trigger": "positive_preseal_ready",
                "preseal_trigger_pressure_hpa": 1110.5,
                "preseal_trigger_threshold_hpa": 1110.0,
            },
            "result": "ok",
        },
        {
            "ts": "2026-04-26T04:11:04+00:00",
            "action": "set_pressure",
            "route": "co2",
            "point_index": 1,
            "target": {"pressure_hpa": 1100.0},
            "actual": {"pressure_hpa": 1100.0, "attempt_count": 1},
            "result": "ok",
        },
    ]
    (artifact_dir / "route_trace.jsonl").write_text(
        "\n".join(json.dumps(row) for row in trace_rows) + "\n",
        encoding="utf-8",
    )
    payload = build_run001_a2_evidence_payload(
        raw_cfg,
        config_path=tmp_path / "config.json",
        run_dir=artifact_dir,
        point_rows=_a2_points(),
        guard=build_no_write_guard_from_raw_config(raw_cfg),
        artifact_paths={
            "summary": str(artifact_dir / "summary.json"),
            "manifest": str(artifact_dir / "run_manifest.json"),
            "trace": str(artifact_dir / "route_trace.jsonl"),
        },
        require_runtime_artifacts=True,
        service_status={"phase": "failed", "completed_points": 1},
    )

    write_run001_a2_artifacts(artifact_dir, payload)
    summary = json.loads((artifact_dir / "summary.json").read_text(encoding="utf-8"))
    evidence = json.loads(
        (artifact_dir / "positive_preseal_pressurization_evidence.json").read_text(encoding="utf-8")
    )
    timing_diagnostics = json.loads(
        (artifact_dir / "positive_preseal_timing_diagnostics.json").read_text(encoding="utf-8")
    )
    timing_summary = json.loads((artifact_dir / "workflow_timing_summary.json").read_text(encoding="utf-8"))
    manifest = json.loads((artifact_dir / "run_manifest.json").read_text(encoding="utf-8"))
    report = (artifact_dir / "human_readable_report.md").read_text(encoding="utf-8")

    assert evidence["ready_reached"] is True
    assert evidence["seal_command_sent"] is True
    assert evidence["seal_trigger_pressure_hpa"] == 1110.5
    assert evidence["preseal_abort_pressure_hpa"] == 1150.0
    assert evidence["ambient_reference_pressure_hpa"] == 1009.0
    assert evidence["measured_atmospheric_pressure_hpa"] == 1009.0
    assert evidence["measured_atmospheric_pressure_source"] == "deprecated_alias_of_ambient_reference_pressure_hpa"
    assert evidence["current_line_pressure_hpa"] == 1109.8
    assert evidence["pressure_samples_count"] == 2
    assert summary["positive_preseal_ready_reached"] is True
    assert summary["positive_preseal_seal_trigger_pressure_hpa"] == 1110.5
    assert summary["ambient_reference_pressure_hpa"] == 1009.0
    assert timing_summary["positive_preseal_ready_pressure_hpa"] == 1110.5
    assert timing_summary["positive_preseal_abort_pressure_hpa"] == 1150.0
    assert timing_summary["ambient_reference_pressure_hpa"] == 1009.0
    assert timing_diagnostics["first_pressure_rise_detected_elapsed_s"] == 9.0
    assert timing_diagnostics["time_from_route_open_to_ready_s"] == 9.0
    assert timing_diagnostics["time_from_positive_preseal_start_to_ready_s"] == 2.0
    assert timing_diagnostics["vent_hold_pressure_rise_rate_hpa_per_s"] == 25.5
    assert timing_diagnostics["positive_preseal_pressure_rise_rate_hpa_per_s"] == 2.75
    assert timing_diagnostics["warning_codes"] == []
    assert timing_summary["preseal_timing_warning_count_total"] == timing_diagnostics["warning_count"]
    assert timing_summary["preseal_timing_warnings_all"] == []
    assert timing_diagnostics["no_write_guard_active"] is True
    assert timing_summary["route_open_to_first_pressure_rise_s"] == 9.0
    assert timing_summary["route_open_to_ready_s"] == 9.0
    assert timing_summary["positive_preseal_start_to_ready_s"] == 2.0
    assert timing_summary["vent_hold_pressure_rise_rate_hpa_per_s"] == 25.5
    assert timing_summary["positive_preseal_pressure_rise_rate_hpa_per_s"] == 2.75
    assert manifest["positive_preseal_pressurization_evidence_artifact"].endswith(
        "positive_preseal_pressurization_evidence.json"
    )
    assert manifest["positive_preseal_timing_diagnostics_artifact"].endswith(
        "positive_preseal_timing_diagnostics.json"
    )
    assert str(artifact_dir / "positive_preseal_timing_diagnostics.json") in manifest["artifacts"]["output_files"]
    assert "Positive preseal pressurization summary" in report
    assert "正压封路升压时序诊断" in report
    assert "preseal_timing_warning_count_total: 0" in report

def test_a2_artifacts_audit_positive_preseal_overlimit_timing_without_threshold_raise(tmp_path) -> None:
    truth = {
        "read_only": True,
        "passive_listen_only": True,
        "commands_sent": [],
        "analyzers": [
            _truth_row("COM35", "001"),
            _truth_row("COM37", "029"),
            _truth_row("COM41", "003"),
            _truth_row("COM42", "004"),
        ],
    }
    (tmp_path / "truth.json").write_text(json.dumps(truth), encoding="utf-8")
    raw_cfg = _a2_raw_config("truth.json")
    raw_cfg["workflow"]["pressure"] = {
        "positive_preseal_pressurization_enabled": True,
        "preseal_ready_pressure_hpa": 1110.0,
        "preseal_abort_pressure_hpa": 1150.0,
        "preseal_ready_timeout_s": 30.0,
        "preseal_pressure_poll_interval_s": 0.2,
        "pressure_rise_detection_threshold_hpa": 2.0,
        "expected_route_open_to_first_pressure_rise_max_s": 10.0,
        "expected_route_open_to_ready_max_s": 40.0,
        "expected_positive_preseal_to_ready_max_s": 30.0,
        "expected_ready_to_seal_command_max_s": 0.5,
        "expected_ready_to_seal_confirm_max_s": 2.0,
        "expected_max_pressure_increase_after_ready_hpa": 10.0,
        "expected_vent_hold_tick_interval_s": 4.0,
        "expected_vent_hold_pressure_rise_rate_max_hpa_per_s": 100.0,
        "timing_warning_only": True,
    }
    artifact_dir = tmp_path / "artifacts"
    artifact_dir.mkdir()
    for filename, content in {
        "summary.json": "{}",
        "run_manifest.json": "{}",
        "points.csv": "timestamp,point_index,status\n",
        "run.log": "ok\n",
        "samples.csv": "timestamp,point_index\n",
    }.items():
        (artifact_dir / filename).write_text(content, encoding="utf-8")
    (artifact_dir / "io_log.csv").write_text(
        "\n".join(
            [
                "timestamp,device,direction,data",
                "2026-04-26T12:10:58.700,pressure_controller,TX,vent(False)",
                "2026-04-26T12:11:02.730,pressure_controller,TX,set_output(False)",
                "2026-04-26T12:11:04.860,pressure_controller,TX,vent(True)",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    trace_rows = [
        {
            "ts": "2026-04-26T04:10:50+00:00",
            "action": "set_co2_valves",
            "route": "co2",
            "point_index": 1,
            "target": {"pressure_hpa": 1100.0},
            "actual": {"pressure_hpa": 1014.967},
            "result": "ok",
        },
        {
            "ts": "2026-04-26T04:10:58.700000+00:00",
            "action": "set_vent",
            "route": "co2",
            "point_index": 1,
            "target": {"vent_on": False},
            "actual": {"pressure_hpa": 1105.0, "output_state": 0, "isolation_state": 1},
            "result": "ok",
            "message": "positive CO2 preseal pressurization before route seal",
        },
        {
            "ts": "2026-04-26T04:11:00+00:00",
            "action": "positive_preseal_pressurization_start",
            "route": "co2",
            "point_index": 1,
            "actual": {
                "target_pressure_hpa": 1100.0,
                "current_line_pressure_hpa": 1105.0,
                "preseal_ready_pressure_hpa": 1110.0,
                "preseal_abort_pressure_hpa": 1150.0,
                "preseal_ready_timeout_s": 30.0,
                "preseal_pressure_poll_interval_s": 0.2,
            },
            "result": "ok",
        },
        {
            "ts": "2026-04-26T04:11:02.200000+00:00",
            "action": "positive_preseal_abort",
            "route": "co2",
            "point_index": 1,
            "actual": {
                "target_pressure_hpa": 1100.0,
                "pressure_hpa": 1305.784,
                "elapsed_s": 2.2,
                "preseal_ready_pressure_hpa": 1110.0,
                "preseal_abort_pressure_hpa": 1150.0,
                "abort_reason": "preseal_abort_pressure_exceeded",
                "pressure_sample_source": "digital_pressure_gauge",
                "pressure_sample_age_s": 0.0,
                "pressure_sample_sequence_id": 230,
                "decision": "FAIL",
            },
            "result": "fail",
        },
    ]
    (artifact_dir / "route_trace.jsonl").write_text(
        "\n".join(json.dumps(row) for row in trace_rows) + "\n",
        encoding="utf-8",
    )
    timing_events = [
        {
            "timestamp": "2026-04-26T04:10:58.700000+00:00",
            "timestamp_local": "2026-04-26T04:10:58.700000+00:00",
            "event_name": "seal_preparation_vent_off_settle_start",
            "stage": "high_pressure_first_point",
        },
        {
            "timestamp": "2026-04-26T04:10:59.200000+00:00",
            "timestamp_local": "2026-04-26T04:10:59.200000+00:00",
            "event_name": "seal_preparation_vent_off_settle_end",
            "stage": "high_pressure_first_point",
        },
        {
            "timestamp": "2026-04-26T04:10:59.900000+00:00",
            "timestamp_local": "2026-04-26T04:10:59.900000+00:00",
            "event_name": "gauge_pressure_read_end",
            "stage": "high_pressure_first_point",
            "route_state": {
                "source": "digital_pressure_gauge_continuous",
                "pressure_hpa": 1153.465,
                "sample_recorded_at": "2026-04-26T04:10:59.900000+00:00",
                "sample_age_s": 0.0,
                "sequence_id": 226,
                "parse_ok": True,
                "usable_for_abort": True,
                "usable_for_ready": True,
            },
        },
        {
            "timestamp": "2026-04-26T04:11:00+00:00",
            "timestamp_local": "2026-04-26T04:11:00+00:00",
            "event_name": "positive_preseal_pressurization_start",
            "stage": "positive_preseal_pressurization",
        },
        {
            "timestamp": "2026-04-26T04:11:01.400000+00:00",
            "timestamp_local": "2026-04-26T04:11:01.400000+00:00",
            "event_name": "pace_pressure_read_end",
            "stage": "positive_preseal_pressurization",
            "route_state": {
                "source": "pace_controller",
                "pressure_hpa": 171.897,
                "sample_recorded_at": "2026-04-26T04:11:01.400000+00:00",
                "sample_age_s": 0.0,
                "parse_ok": True,
            },
        },
        {
            "timestamp": "2026-04-26T04:11:02.200000+00:00",
            "timestamp_local": "2026-04-26T04:11:02.200000+00:00",
            "event_name": "gauge_pressure_read_end",
            "stage": "positive_preseal_pressurization",
            "route_state": {
                "source": "digital_pressure_gauge",
                "pressure_hpa": 1305.784,
                "sample_recorded_at": "2026-04-26T04:11:02.200000+00:00",
                "sample_age_s": 0.0,
                "sequence_id": 230,
                "parse_ok": True,
                "usable_for_abort": True,
            },
        },
        {
            "timestamp": "2026-04-26T04:11:02.200000+00:00",
            "timestamp_local": "2026-04-26T04:11:02.200000+00:00",
            "event_name": "positive_preseal_abort",
            "stage": "positive_preseal_pressurization",
            "pressure_hpa": 1305.784,
        },
    ]
    (artifact_dir / "workflow_timing_trace.jsonl").write_text(
        "\n".join(json.dumps(row) for row in timing_events) + "\n",
        encoding="utf-8",
    )
    payload = build_run001_a2_evidence_payload(
        raw_cfg,
        config_path=tmp_path / "config.json",
        run_dir=artifact_dir,
        point_rows=_a2_points(),
        guard=build_no_write_guard_from_raw_config(raw_cfg),
        artifact_paths={
            "summary": str(artifact_dir / "summary.json"),
            "manifest": str(artifact_dir / "run_manifest.json"),
            "trace": str(artifact_dir / "route_trace.jsonl"),
        },
        require_runtime_artifacts=True,
        service_status={"phase": "failed", "completed_points": 0},
    )

    write_run001_a2_artifacts(artifact_dir, payload)
    summary = json.loads((artifact_dir / "summary.json").read_text(encoding="utf-8"))
    evidence = json.loads(
        (artifact_dir / "positive_preseal_pressurization_evidence.json").read_text(encoding="utf-8")
    )
    timing_diagnostics = json.loads(
        (artifact_dir / "positive_preseal_timing_diagnostics.json").read_text(encoding="utf-8")
    )
    timing_summary = json.loads((artifact_dir / "workflow_timing_summary.json").read_text(encoding="utf-8"))
    manifest = json.loads((artifact_dir / "run_manifest.json").read_text(encoding="utf-8"))

    for artifact in (summary, evidence, timing_diagnostics, timing_summary, manifest):
        assert artifact["positive_preseal_overlimit_root_cause_candidate"] == (
            "vent_close_timing_positive_preseal_ramp_exceeded_abort_cutoff_before_setpoint_or_output_enable"
        )
        assert artifact["positive_preseal_overlimit_first_seen_pressure_hpa"] == 1153.465
        assert artifact["positive_preseal_pressure_peak_hpa"] == 1305.784
        assert artifact["positive_preseal_setpoint_command_sent"] is False
        assert artifact["positive_preseal_output_enable_sent"] is False
        assert artifact["positive_preseal_vent_close_command_sent"] is True
        assert artifact["positive_preseal_pressure_source_used_for_abort"] == "digital_pressure_gauge_continuous"

    assert evidence["preseal_abort_pressure_hpa"] == 1150.0
    assert evidence["positive_preseal_overlimit_first_seen_elapsed_s"] == 0.0
    assert evidence["positive_preseal_overlimit_elapsed_s_nonnegative"] is True
    assert evidence["positive_preseal_overlimit_elapsed_source"] == (
        "pre_positive_preseal_start_clamped_to_zero"
    )
    assert evidence["overlimit_elapsed_s_nonnegative"] is True
    assert evidence["overlimit_elapsed_source"] == "pre_positive_preseal_start_clamped_to_zero"
    assert evidence["positive_preseal_overlimit_first_seen_source"] == "digital_pressure_gauge_continuous"
    assert evidence["positive_preseal_overlimit_first_seen_sample_age_s"] == 0.0
    assert evidence["positive_preseal_overlimit_first_seen_sequence_id"] == 226
    assert evidence["positive_preseal_pressure_peak_elapsed_s"] == 2.2
    assert evidence["positive_preseal_pressure_peak_source"] == "digital_pressure_gauge"
    assert evidence["positive_preseal_pressure_rise_rate_peak_hpa_per_s"] == 66.226
    assert evidence["positive_preseal_output_disable_sent"] is True
    assert evidence["positive_preseal_output_disable_latency_s"] == 0.53
    assert evidence["positive_preseal_ready_reached_before_vent_close_completed"] is False
    assert evidence["positive_preseal_ready_reached_during_vent_close"] is False
    assert evidence["positive_preseal_ready_to_abort_latency_s"] == 2.3
    assert evidence["positive_preseal_abort_to_relief_latency_s"] == 2.66
    assert evidence["positive_preseal_digital_gauge_pressure_hpa"] == 1305.784
    assert evidence["positive_preseal_pace_pressure_hpa"] == 171.897
    assert evidence["positive_preseal_source_disagreement_hpa"] == 1133.887
    assert summary["positive_preseal_abort_reason"] == "preseal_abort_pressure_exceeded"
    assert summary["positive_preseal_pressure_max_hpa"] == 1305.784


def test_a2_high_pressure_first_point_artifact_uses_first_pressure_after_route_not_baseline(tmp_path) -> None:
    truth = {
        "read_only": True,
        "passive_listen_only": True,
        "commands_sent": [],
        "analyzers": [
            _truth_row("COM35", "001"),
            _truth_row("COM37", "029"),
            _truth_row("COM41", "003"),
            _truth_row("COM42", "004"),
        ],
    }
    (tmp_path / "truth.json").write_text(json.dumps(truth), encoding="utf-8")
    raw_cfg = _a2_raw_config("truth.json")
    raw_cfg["workflow"]["pressure"] = {
        "positive_preseal_pressurization_enabled": True,
        "preseal_ready_pressure_hpa": 1110.0,
        "preseal_abort_pressure_hpa": 1150.0,
        "high_pressure_first_point_mode_enabled": True,
        "high_pressure_first_point_margin_hpa": 0.0,
        "high_pressure_first_point_route_open_request_expected_max_s": 0.05,
        "high_pressure_first_point_route_open_response_expected_max_s": 1.0,
        "route_open_first_pressure_request_expected_max_s": 0.5,
        "route_open_first_pressure_response_expected_max_s": 1.0,
        "pressure_read_latency_warn_s": 0.5,
        "pressure_latency_warning_only": True,
    }
    artifact_dir = tmp_path / "artifacts"
    artifact_dir.mkdir()
    for filename, content in {
        "summary.json": "{}",
        "run_manifest.json": "{}",
        "points.csv": "timestamp,point_index,status\n",
        "io_log.csv": "timestamp,device,direction,data\n",
        "run.log": "ok\n",
        "samples.csv": "timestamp,point_index\n",
    }.items():
        (artifact_dir / filename).write_text(content, encoding="utf-8")

    def event(name, event_type, mono, *, stage="", pressure=None, route_state=None, decision=None, duration=None):
        return {
            "event_name": name,
            "event_type": event_type,
            "timestamp_local": "2026-04-26T10:00:00+00:00",
            "timestamp_monotonic_s": mono,
            "elapsed_from_run_start_s": mono - 99.0,
            "stage": stage,
            "point_index": 1,
            "target_pressure_hpa": 1100.0,
            "duration_s": duration,
            "expected_max_s": None,
            "wait_reason": None,
            "blocking_condition": None,
            "decision": decision,
            "route_state": route_state,
            "pressure_hpa": pressure,
            "chamber_temperature_c": None,
            "dewpoint_c": None,
            "pace_output_state": None,
            "pace_isolation_state": None,
            "pace_vent_status": None,
            "sample_count": None,
            "warning_code": None,
            "error_code": None,
            "no_write_guard_active": True,
        }

    baseline = _high_pressure_sample(1009.0, sequence=1)
    first = _high_pressure_sample(1111.0, sequence=2)
    first.update(
        {
            "stage": "high_pressure_first_point",
            "request_sent_at": "2026-04-26T10:00:00.101000+00:00",
            "response_received_at": "2026-04-26T10:00:00.121000+00:00",
            "request_sent_monotonic_s": 100.101,
            "response_received_monotonic_s": 100.121,
            "read_latency_s": 0.02,
        }
    )
    first["digital_gauge_pressure_sample"].update(
        {
            "pressure_hpa": 1111.0,
            "request_sent_at": "2026-04-26T10:00:00.101000+00:00",
            "response_received_at": "2026-04-26T10:00:00.121000+00:00",
            "request_sent_monotonic_s": 100.101,
            "response_received_monotonic_s": 100.121,
            "read_latency_s": 0.02,
        }
    )
    mode_context = {
        "enabled": True,
        "first_target_pressure_hpa": 1100.0,
        "ambient_reference_pressure_hpa": 1009.0,
        "baseline_pressure_hpa": 1009.0,
        "baseline_pressure_source": "digital_pressure_gauge_continuous",
        "baseline_pressure_age_s": 0.0,
        "trigger_reason": "first_target_above_ambient_reference",
    }
    events = [
        event("high_pressure_first_point_mode_enabled", "info", 99.9, stage="high_pressure_first_point", pressure=1009.0, route_state=mode_context, decision="enabled"),
        event("gauge_pressure_read_end", "end", 99.91, stage="high_pressure_first_point_prearm", pressure=1009.0, route_state=baseline, duration=0.01),
        event("pressure_polling_prearmed", "info", 99.92, stage="high_pressure_first_point", pressure=1009.0, route_state=mode_context),
        event("co2_route_open_start", "start", 100.0, stage="co2_route_open"),
        event("co2_route_open_end", "end", 100.1, stage="co2_route_open", pressure=1009.0, route_state={"high_pressure_first_point_mode": True}),
        event("route_open_pressure_poll_request", "info", 100.101, stage="high_pressure_first_point", route_state=mode_context, decision="request_sent"),
        event("gauge_pressure_read_end", "end", 100.121, stage="high_pressure_first_point", pressure=1111.0, route_state=first, duration=0.02),
        event("route_open_pressure_poll_response", "info", 100.121, stage="high_pressure_first_point", pressure=1111.0, route_state=first, duration=0.02),
        event("route_open_pressure_first_sample", "info", 100.121, stage="high_pressure_first_point", pressure=1111.0, route_state=first),
        event("high_pressure_ready_detected", "info", 100.122, stage="high_pressure_first_point", pressure=1111.0, route_state=first, decision="ready"),
        event("high_pressure_seal_command_sent", "info", 100.123, stage="high_pressure_first_point", pressure=1111.0, route_state=first, decision="seal_command_sent", duration=0.001),
        event("high_pressure_seal_confirmed", "end", 100.2, stage="high_pressure_first_point", pressure=1109.8, route_state={"high_pressure_first_point_mode": True}, decision="sealed"),
    ]
    (artifact_dir / "workflow_timing_trace.jsonl").write_text(
        "\n".join(json.dumps(item) for item in events) + "\n",
        encoding="utf-8",
    )
    route_rows = [
        {
            "ts": "2026-04-26T10:00:00.100000+00:00",
            "action": "set_co2_valves",
            "route": "co2",
            "point_index": 1,
            "target": {"pressure_hpa": 1100.0},
            "actual": {"pressure_hpa": 1009.0},
            "result": "ok",
        }
    ]
    (artifact_dir / "route_trace.jsonl").write_text(
        "\n".join(json.dumps(row) for row in route_rows) + "\n",
        encoding="utf-8",
    )
    payload = build_run001_a2_evidence_payload(
        raw_cfg,
        config_path=tmp_path / "config.json",
        run_dir=artifact_dir,
        point_rows=_a2_points(),
        guard=build_no_write_guard_from_raw_config(raw_cfg),
        artifact_paths={
            "summary": str(artifact_dir / "summary.json"),
            "manifest": str(artifact_dir / "run_manifest.json"),
            "trace": str(artifact_dir / "route_trace.jsonl"),
        },
        require_runtime_artifacts=True,
        service_status={"phase": "failed", "completed_points": 0},
    )

    write_run001_a2_artifacts(artifact_dir, payload)
    latency = json.loads((artifact_dir / "pressure_read_latency_diagnostics.json").read_text(encoding="utf-8"))
    high = json.loads((artifact_dir / "high_pressure_first_point_evidence.json").read_text(encoding="utf-8"))
    critical = json.loads((artifact_dir / "critical_pressure_freshness_evidence.json").read_text(encoding="utf-8"))
    manifest = json.loads((artifact_dir / "run_manifest.json").read_text(encoding="utf-8"))
    report = (artifact_dir / "human_readable_report.md").read_text(encoding="utf-8")

    assert latency["first_pressure_hpa"] == 1111.0
    assert latency["first_pressure_hpa"] != 1009.0
    assert latency["route_open_to_first_pressure_request_s"] == 0.001
    assert high["enabled"] is True
    assert high["baseline_pressure_hpa"] == 1009.0
    assert high["first_pressure_hpa"] == 1111.0
    assert high["route_open_to_first_pressure_request_s"] == 0.001
    assert high["ready_to_seal_command_s"] == 0.001
    assert high["abort_pressure_hpa"] == 1150.0
    assert manifest["high_pressure_first_point_evidence_artifact"].endswith("high_pressure_first_point_evidence.json")
    assert manifest["critical_pressure_freshness_evidence_artifact"].endswith("critical_pressure_freshness_evidence.json")
    assert str(artifact_dir / "high_pressure_first_point_evidence.json") in manifest["artifacts"]["output_files"]
    assert str(artifact_dir / "critical_pressure_freshness_evidence.json") in manifest["artifacts"]["output_files"]
    assert critical["decision"] == "sealed"
    assert critical["pressure_source_used_for_ready"] == "digital_pressure_gauge_continuous"
    assert "1100 高压首点正压封路诊断" in report
    assert "关键压力取数新鲜度诊断" in report


def test_a2_preseal_diagnostic_warnings_are_merged_into_workflow_summary(tmp_path) -> None:
    truth = {
        "read_only": True,
        "passive_listen_only": True,
        "commands_sent": [],
        "analyzers": [
            _truth_row("COM35", "001"),
            _truth_row("COM37", "029"),
            _truth_row("COM41", "003"),
            _truth_row("COM42", "004"),
        ],
    }
    (tmp_path / "truth.json").write_text(json.dumps(truth), encoding="utf-8")
    raw_cfg = _a2_raw_config("truth.json")
    raw_cfg["workflow"]["pressure"] = {
        "positive_preseal_pressurization_enabled": True,
        "preseal_ready_pressure_hpa": 1110.0,
        "preseal_abort_pressure_hpa": 1150.0,
        "preseal_ready_timeout_s": 30.0,
        "preseal_pressure_poll_interval_s": 0.2,
        "pressure_rise_detection_threshold_hpa": 2.0,
        "expected_ready_to_seal_command_max_s": 0.5,
        "expected_ready_to_seal_confirm_max_s": 2.0,
        "expected_vent_hold_pressure_rise_rate_max_hpa_per_s": 5.0,
        "expected_abort_margin_min_hpa": 10.0,
        "expected_vent_hold_tick_interval_s": 2.0,
        "timing_warning_only": True,
        "fail_if_sealed_pressure_below_target": True,
    }
    artifact_dir = tmp_path / "artifacts"
    artifact_dir.mkdir()
    for filename, content in {
        "summary.json": "{}",
        "run_manifest.json": "{}",
        "points.csv": "timestamp,point_index,status\n",
        "io_log.csv": "timestamp,device,direction,data\n",
        "run.log": "aborted\n",
        "samples.csv": "timestamp,point_index\n",
    }.items():
        (artifact_dir / filename).write_text(content, encoding="utf-8")
    trace_rows = [
        {
            "ts": "2026-04-26T09:17:39+00:00",
            "action": "set_co2_valves",
            "route": "co2",
            "point_index": 1,
            "target": {"pressure_hpa": 1100.0},
            "actual": {"pressure_hpa": 1020.0},
            "result": "ok",
        },
        {
            "ts": "2026-04-26T09:17:41+00:00",
            "action": "set_vent",
            "route": "co2",
            "point_index": 1,
            "target": {"vent_on": True},
            "actual": {"pressure_hpa": 1020.0, "vent_status_raw": 1, "output_state": 0, "isolation_state": 1},
            "result": "ok",
        },
        {
            "ts": "2026-04-26T09:17:51+00:00",
            "action": "preseal_atmosphere_flush_ready_handoff",
            "route": "co2",
            "point_index": 1,
            "actual": {"pressure_hpa": 1114.249, "ready_pressure_hpa": 1110.0, "abort_pressure_hpa": 1150.0},
            "result": "ok",
        },
        {
            "ts": "2026-04-26T09:17:51.010000+00:00",
            "action": "positive_preseal_pressurization_start",
            "route": "co2",
            "point_index": 1,
            "actual": {
                "target_pressure_hpa": 1100.0,
                "ambient_reference_pressure_hpa": 1020.0,
                "measured_atmospheric_pressure_hpa": 1020.0,
                "preseal_ready_pressure_hpa": 1110.0,
                "preseal_abort_pressure_hpa": 1150.0,
                "vent_close_arm_trigger": "ready_pressure",
                "vent_close_arm_pressure_hpa": 1114.249,
            },
            "result": "ok",
        },
        {
            "ts": "2026-04-26T09:18:08.330000+00:00",
            "action": "set_vent",
            "route": "co2",
            "point_index": 1,
            "target": {"vent_on": False},
            "actual": {
                "vent_status_raw": 0,
                "output_state": 0,
                "isolation_state": 1,
                "vent_close_arm_trigger": "ready_pressure",
                "vent_close_arm_pressure_hpa": 1114.249,
            },
            "result": "ok",
            "message": "positive CO2 preseal pressurization before route seal",
        },
        {
            "ts": "2026-04-26T09:18:11+00:00",
            "action": "positive_preseal_pressure_check",
            "route": "co2",
            "point_index": 1,
            "actual": {
                "target_pressure_hpa": 1100.0,
                "pressure_hpa": 1170.772,
                "preseal_ready_pressure_hpa": 1110.0,
                "preseal_abort_pressure_hpa": 1150.0,
            },
            "result": "ok",
        },
        {
            "ts": "2026-04-26T09:18:11.010000+00:00",
            "action": "positive_preseal_abort",
            "route": "co2",
            "point_index": 1,
            "actual": {
                "target_pressure_hpa": 1100.0,
                "pressure_hpa": 1170.772,
                "preseal_ready_pressure_hpa": 1110.0,
                "preseal_abort_pressure_hpa": 1150.0,
                "abort_reason": "preseal_abort_pressure_exceeded",
                "seal_command_blocked_reason": "preseal_abort_pressure_exceeded",
                "vent_close_arm_trigger": "ready_pressure",
                "vent_close_arm_pressure_hpa": 1114.249,
                "decision": "FAIL",
            },
            "result": "fail",
        },
    ]
    (artifact_dir / "route_trace.jsonl").write_text(
        "\n".join(json.dumps(row) for row in trace_rows) + "\n",
        encoding="utf-8",
    )
    payload = build_run001_a2_evidence_payload(
        raw_cfg,
        config_path=tmp_path / "config.json",
        run_dir=artifact_dir,
        point_rows=_a2_points(),
        guard=build_no_write_guard_from_raw_config(raw_cfg),
        artifact_paths={
            "summary": str(artifact_dir / "summary.json"),
            "manifest": str(artifact_dir / "run_manifest.json"),
            "trace": str(artifact_dir / "route_trace.jsonl"),
        },
        require_runtime_artifacts=True,
        service_status={"phase": "failed", "completed_points": 0},
    )

    write_run001_a2_artifacts(artifact_dir, payload)
    timing_diagnostics = json.loads(
        (artifact_dir / "positive_preseal_timing_diagnostics.json").read_text(encoding="utf-8")
    )
    timing_summary = json.loads((artifact_dir / "workflow_timing_summary.json").read_text(encoding="utf-8"))
    report = (artifact_dir / "human_readable_report.md").read_text(encoding="utf-8")

    diagnostic_codes = set(timing_diagnostics["warning_codes"])
    summary_codes = {item["warning_code"] for item in timing_summary["preseal_timing_warnings_all"]}
    assert diagnostic_codes
    assert diagnostic_codes.issubset(summary_codes)
    assert timing_diagnostics["warning_count"] == timing_summary["preseal_timing_warning_count_total"]
    assert timing_summary["preseal_timing_warning_count_total"] == len(timing_summary["preseal_timing_warnings_all"])
    assert timing_summary["preseal_timing_warning_count"] == timing_summary["preseal_timing_warning_count_total"]
    assert timing_summary["severe_preseal_timing_warning_count"] == timing_summary["preseal_timing_warning_count_severe"]
    assert "preseal_timing_warning_count_total:" in report
    assert "positive_preseal_ready_without_seal_start" in report
    assert "vent_hold_pressure_rise_rate_high" in summary_codes
    assert "pressure_max_before_seal_near_abort_threshold" in summary_codes


def test_co2_preseal_soak_reasserts_pressure_atmosphere_hold(monkeypatch) -> None:
    orchestrator = WorkflowOrchestrator.__new__(WorkflowOrchestrator)
    orchestrator.run_state = SimpleNamespace(
        humidity=SimpleNamespace(first_co2_route_soak_pending=True)
    )
    orchestrator._last_co2_route_dewpoint_gate_summary = {}
    orchestrator._first_co2_route_soak_pending = True
    orchestrator._post_h2o_co2_zero_flush_pending = False
    orchestrator._initial_co2_zero_flush_pending = False
    orchestrator._active_post_h2o_co2_zero_flush = False
    orchestrator.pressure_control_service = SimpleNamespace(_current_pressure=lambda: 1008.0)
    orchestrator.status_service = SimpleNamespace(record_route_trace=lambda **kwargs: None)
    calls: list[dict] = []

    def cfg_get(path: str, default=None):
        values = {
            "workflow.stability.co2_route.first_point_preseal_soak_s": 5.0,
            "workflow.pressure.continuous_atmosphere_hold": True,
            "workflow.pressure.vent_hold_interval_s": 2.0,
        }
        return values.get(path, default)

    clock = {"now": 100.0}

    def fake_time() -> float:
        return clock["now"]

    def fake_sleep(seconds: float) -> None:
        clock["now"] += float(seconds)

    def set_vent(vent_on: bool, reason: str = "", *, wait_after_command: bool = True, **kwargs) -> None:
        calls.append(
            {
                "vent_on": vent_on,
                "reason": reason,
                "wait_after_command": wait_after_command,
                **kwargs,
                "at": clock["now"],
            }
        )

    monkeypatch.setattr(orchestrator_module.time, "time", fake_time)
    monkeypatch.setattr(orchestrator_module.time, "sleep", fake_sleep)
    orchestrator._collect_only_fast_path_enabled = lambda: False
    orchestrator._has_special_co2_zero_flush_pending = lambda: False
    orchestrator._is_zero_co2_point = lambda point: False
    orchestrator._cfg_get = cfg_get
    orchestrator._log = lambda message: None
    orchestrator._check_stop = lambda: None
    orchestrator._refresh_live_analyzer_snapshots = lambda **kwargs: True
    orchestrator._set_pressure_controller_vent = set_vent
    orchestrator._wait_co2_route_dewpoint_gate_before_seal = lambda *args, **kwargs: True
    point = CalibrationPoint(index=1, temperature_c=20.0, co2_ppm=100.0, pressure_hpa=1100.0, route="co2")

    assert orchestrator._wait_co2_route_soak_before_seal(point) is True

    assert [call["at"] for call in calls] == [100.0, 102.0, 104.0]
    assert all(call["vent_on"] is True for call in calls)
    assert all(call["reason"] == "CO2 route pre-seal atmosphere hold" for call in calls)
    assert all(call["wait_after_command"] is False for call in calls)


def test_co2_preseal_soak_aborts_when_atmosphere_hold_pressure_rises(monkeypatch) -> None:
    orchestrator = WorkflowOrchestrator.__new__(WorkflowOrchestrator)
    route_traces: list[dict] = []
    orchestrator.run_state = SimpleNamespace(
        humidity=SimpleNamespace(first_co2_route_soak_pending=True)
    )
    orchestrator._last_co2_route_dewpoint_gate_summary = {}
    orchestrator._first_co2_route_soak_pending = True
    orchestrator._post_h2o_co2_zero_flush_pending = False
    orchestrator._initial_co2_zero_flush_pending = False
    orchestrator._active_post_h2o_co2_zero_flush = False
    orchestrator.pressure_control_service = SimpleNamespace(_current_pressure=lambda: 1985.0)
    orchestrator.status_service = SimpleNamespace(record_route_trace=lambda **kwargs: route_traces.append(kwargs))
    orchestrator.route_planner = SimpleNamespace(co2_point_tag=lambda point: "co2_groupa_100ppm_1100hpa")

    def cfg_get(path: str, default=None):
        values = {
            "workflow.stability.co2_route.first_point_preseal_soak_s": 5.0,
            "workflow.pressure.continuous_atmosphere_hold": True,
            "workflow.pressure.vent_hold_interval_s": 2.0,
            "workflow.pressure.preseal_atmosphere_hold_max_hpa": 1110.0,
        }
        return values.get(path, default)

    clock = {"now": 100.0}
    monkeypatch.setattr(orchestrator_module.time, "time", lambda: clock["now"])
    monkeypatch.setattr(orchestrator_module.time, "sleep", lambda seconds: clock.__setitem__("now", clock["now"] + float(seconds)))
    orchestrator._collect_only_fast_path_enabled = lambda: False
    orchestrator._has_special_co2_zero_flush_pending = lambda: False
    orchestrator._is_zero_co2_point = lambda point: False
    orchestrator._cfg_get = cfg_get
    orchestrator._log = lambda message: None
    orchestrator._check_stop = lambda: None
    orchestrator._refresh_live_analyzer_snapshots = lambda **kwargs: True
    orchestrator._set_pressure_controller_vent = lambda *args, **kwargs: None
    orchestrator._wait_co2_route_dewpoint_gate_before_seal = lambda *args, **kwargs: True
    point = CalibrationPoint(index=1, temperature_c=20.0, co2_ppm=100.0, pressure_hpa=1100.0, route="co2")

    with pytest.raises(WorkflowValidationError) as excinfo:
        orchestrator._wait_co2_route_soak_before_seal(point)

    assert excinfo.value.context["reason"] == "co2_preseal_atmosphere_hold_pressure_exceeded"
    assert excinfo.value.context["pressure_hpa"] == 1985.0
    assert route_traces[-1]["action"] == "co2_preseal_atmosphere_hold_pressure_guard"
    assert route_traces[-1]["result"] == "fail"


def test_co2_preseal_soak_handoffs_ready_pressure_without_hard_fail(monkeypatch) -> None:
    orchestrator = WorkflowOrchestrator.__new__(WorkflowOrchestrator)
    route_traces: list[dict] = []
    orchestrator.run_state = SimpleNamespace(
        humidity=SimpleNamespace(first_co2_route_soak_pending=True)
    )
    orchestrator._last_co2_route_dewpoint_gate_summary = {}
    orchestrator._first_co2_route_soak_pending = True
    orchestrator._post_h2o_co2_zero_flush_pending = False
    orchestrator._initial_co2_zero_flush_pending = False
    orchestrator._active_post_h2o_co2_zero_flush = False
    orchestrator.pressure_control_service = SimpleNamespace(_current_pressure=lambda: 1110.0)
    orchestrator.status_service = SimpleNamespace(record_route_trace=lambda **kwargs: route_traces.append(kwargs))
    orchestrator.route_planner = SimpleNamespace(co2_point_tag=lambda point: "co2_groupa_100ppm_1100hpa")
    timing_events: list[dict] = []
    vent_calls: list[dict] = []

    def cfg_get(path: str, default=None):
        values = {
            "workflow.stability.co2_route.first_point_preseal_soak_s": 5.0,
            "workflow.pressure.continuous_atmosphere_hold": True,
            "workflow.pressure.vent_hold_interval_s": 2.0,
            "workflow.pressure.positive_preseal_pressurization_enabled": True,
            "workflow.pressure.preseal_ready_pressure_hpa": 1110.0,
            "workflow.pressure.preseal_abort_pressure_hpa": 1150.0,
        }
        return values.get(path, default)

    clock = {"now": 100.0}
    monkeypatch.setattr(orchestrator_module.time, "time", lambda: clock["now"])
    monkeypatch.setattr(orchestrator_module.time, "sleep", lambda seconds: clock.__setitem__("now", clock["now"] + float(seconds)))
    orchestrator._collect_only_fast_path_enabled = lambda: False
    orchestrator._has_special_co2_zero_flush_pending = lambda: False
    orchestrator._is_zero_co2_point = lambda point: False
    orchestrator._cfg_get = cfg_get
    orchestrator._log = lambda message: None
    orchestrator._check_stop = lambda: None
    orchestrator._refresh_live_analyzer_snapshots = lambda **kwargs: True
    orchestrator._record_workflow_timing = lambda event_name, event_type="info", **kwargs: timing_events.append(
        {"event_name": event_name, "event_type": event_type, **kwargs}
    )
    orchestrator._set_pressure_controller_vent = lambda vent_on, **kwargs: vent_calls.append(
        {"vent_on": vent_on, **kwargs}
    )
    orchestrator._wait_co2_route_dewpoint_gate_before_seal = lambda *args, **kwargs: True
    point = CalibrationPoint(index=1, temperature_c=20.0, co2_ppm=100.0, pressure_hpa=1100.0, route="co2")

    assert orchestrator._wait_co2_route_soak_before_seal(point) is True

    assert route_traces[-1]["action"] == "preseal_vent_close_arm_triggered"
    assert route_traces[-1]["result"] == "ok"
    assert route_traces[-1]["actual"]["vent_close_arm_pressure_hpa"] == 1110.0
    assert route_traces[-1]["actual"]["vent_close_arm_trigger"] == "arm_pressure"
    assert route_traces[-1]["actual"]["late_arm_at_ready"] is True
    assert not any(row.get("result") == "fail" for row in route_traces)
    assert vent_calls == []
    assert any(event["event_name"] == "preseal_vent_close_arm_triggered" for event in timing_events)


def test_orchestrator_vent_wrapper_returns_diagnostics() -> None:
    orchestrator = WorkflowOrchestrator.__new__(WorkflowOrchestrator)
    expected = {
        "vent_command_ack": True,
        "vent_status_raw": 0,
        "command_method": "exit_atmosphere_mode",
    }
    calls: list[dict] = []

    def set_pressure_controller_vent(vent_on: bool, **kwargs):
        calls.append({"vent_on": vent_on, **kwargs})
        return expected

    orchestrator.pressure_control_service = SimpleNamespace(
        set_pressure_controller_vent=set_pressure_controller_vent
    )

    result = orchestrator._set_pressure_controller_vent(False, reason="before CO2 pressure seal")

    assert result == expected
    assert calls == [
        {
            "vent_on": False,
            "reason": "before CO2 pressure seal",
            "wait_after_command": True,
            "capture_pressure": True,
            "transition_timeout_s": None,
        }
    ]


def _preseal_arm_orchestrator(monkeypatch, pressures: list[float], *, cfg_overrides: dict | None = None):
    orchestrator = WorkflowOrchestrator.__new__(WorkflowOrchestrator)
    route_traces: list[dict] = []
    timing_events: list[dict] = []
    vent_calls: list[dict] = []
    pressure_values = list(pressures)
    last_pressure = pressure_values[-1]
    orchestrator.run_state = SimpleNamespace(humidity=SimpleNamespace(first_co2_route_soak_pending=True))
    orchestrator._last_co2_route_dewpoint_gate_summary = {}
    orchestrator._first_co2_route_soak_pending = True
    orchestrator._post_h2o_co2_zero_flush_pending = False
    orchestrator._initial_co2_zero_flush_pending = False
    orchestrator._active_post_h2o_co2_zero_flush = False

    def current_pressure() -> float:
        nonlocal last_pressure
        if pressure_values:
            last_pressure = float(pressure_values.pop(0))
        return last_pressure

    orchestrator.pressure_control_service = SimpleNamespace(_current_pressure=current_pressure)
    orchestrator.status_service = SimpleNamespace(record_route_trace=lambda **kwargs: route_traces.append(kwargs))
    orchestrator.route_planner = SimpleNamespace(co2_point_tag=lambda point: "co2_groupa_100ppm_1100hpa")

    values = {
        "workflow.stability.co2_route.first_point_preseal_soak_s": 5.0,
        "workflow.pressure.continuous_atmosphere_hold": True,
        "workflow.pressure.vent_hold_interval_s": 2.0,
        "workflow.pressure.positive_preseal_pressurization_enabled": True,
        "workflow.pressure.preseal_ready_pressure_hpa": 1110.0,
        "workflow.pressure.preseal_abort_pressure_hpa": 1150.0,
        "workflow.pressure.preseal_pressure_poll_interval_s": 0.2,
        "workflow.pressure.preseal_vent_close_arm_pressure_hpa": 1080.0,
        "workflow.pressure.preseal_vent_close_arm_margin_hpa": 30.0,
        "workflow.pressure.preseal_vent_close_arm_time_to_ready_s": 3.0,
    }
    values.update(cfg_overrides or {})
    clock = {"now": 100.0}
    monkeypatch.setattr(orchestrator_module.time, "time", lambda: clock["now"])
    monkeypatch.setattr(orchestrator_module.time, "monotonic", lambda: clock["now"])
    monkeypatch.setattr(orchestrator_module.time, "sleep", lambda seconds: clock.__setitem__("now", clock["now"] + float(seconds)))
    orchestrator._collect_only_fast_path_enabled = lambda: False
    orchestrator._has_special_co2_zero_flush_pending = lambda: False
    orchestrator._is_zero_co2_point = lambda point: False
    orchestrator._cfg_get = lambda path, default=None: values.get(path, default)
    orchestrator._log = lambda message: None
    orchestrator._check_stop = lambda: None
    orchestrator._refresh_live_analyzer_snapshots = lambda **kwargs: True
    orchestrator._record_workflow_timing = lambda event_name, event_type="info", **kwargs: timing_events.append(
        {"event_name": event_name, "event_type": event_type, **kwargs}
    )
    orchestrator._set_pressure_controller_vent = lambda vent_on, **kwargs: vent_calls.append(
        {"vent_on": vent_on, **kwargs}
    )
    orchestrator._wait_co2_route_dewpoint_gate_before_seal = lambda *args, **kwargs: True
    point = CalibrationPoint(index=1, temperature_c=20.0, co2_ppm=100.0, pressure_hpa=1100.0, route="co2")
    return orchestrator, point, route_traces, timing_events, vent_calls


def test_high_pressure_first_point_skips_long_atmosphere_flush_after_route_open(monkeypatch) -> None:
    orchestrator, point, _route_traces, timing_events, vent_calls = _preseal_arm_orchestrator(
        monkeypatch,
        [1111.0],
    )
    orchestrator._a2_high_pressure_first_point_mode_enabled = True
    orchestrator._a2_high_pressure_first_point_initial_decision = "positive_preseal_ready_handoff"
    orchestrator._a2_high_pressure_first_point_context = {
        "enabled": True,
        "first_target_pressure_hpa": 1100.0,
        "ambient_reference_pressure_hpa": 1009.0,
    }
    orchestrator._wait_co2_route_dewpoint_gate_before_seal = lambda *args, **kwargs: (_ for _ in ()).throw(
        AssertionError("high-pressure first point must not wait for dewpoint/flush gate")
    )
    orchestrator._verify_co2_preseal_atmosphere_hold_pressure = lambda point: (_ for _ in ()).throw(
        AssertionError("initial route-open decision should hand off immediately")
    )

    assert orchestrator._wait_co2_route_soak_before_seal(point) is True

    assert vent_calls == []
    assert any(event["event_name"] == "preseal_atmosphere_flush_hold_end" for event in timing_events)
    assert not any(event["event_name"] == "preseal_vent_hold_tick" for event in timing_events)


def test_co2_preseal_soak_arms_vent_close_before_ready_pressure(monkeypatch) -> None:
    orchestrator, point, route_traces, timing_events, _vent_calls = _preseal_arm_orchestrator(
        monkeypatch,
        [1000.0, 1082.0],
    )

    assert orchestrator._wait_co2_route_soak_before_seal(point) is True

    assert route_traces[-1]["action"] == "preseal_vent_close_arm_triggered"
    assert route_traces[-1]["actual"]["vent_close_arm_trigger"] == "arm_pressure"
    assert route_traces[-1]["actual"]["vent_close_arm_pressure_hpa"] == 1082.0
    assert not any(row["action"] == "preseal_atmosphere_flush_ready_handoff" for row in route_traces)
    assert any(event["event_name"] == "preseal_vent_close_arm_triggered" for event in timing_events)
    assert _vent_calls == []


def test_co2_preseal_soak_arms_when_pressure_is_within_ready_margin(monkeypatch) -> None:
    orchestrator, point, route_traces, _timing_events, _vent_calls = _preseal_arm_orchestrator(
        monkeypatch,
        [1000.0, 1081.0],
        cfg_overrides={
            "workflow.pressure.preseal_vent_close_arm_pressure_hpa": 1095.0,
            "workflow.pressure.preseal_vent_close_arm_margin_hpa": 30.0,
        },
    )

    assert orchestrator._wait_co2_route_soak_before_seal(point) is True

    assert route_traces[-1]["action"] == "preseal_vent_close_arm_triggered"
    assert route_traces[-1]["actual"]["vent_close_arm_trigger"] == "arm_margin"
    assert route_traces[-1]["actual"]["vent_close_arm_pressure_hpa"] == 1081.0


def test_co2_preseal_soak_prefers_arm_trigger_when_sample_is_already_ready(monkeypatch) -> None:
    orchestrator, point, route_traces, _timing_events, _vent_calls = _preseal_arm_orchestrator(
        monkeypatch,
        [1112.0],
    )

    assert orchestrator._wait_co2_route_soak_before_seal(point) is True

    actual = route_traces[-1]["actual"]
    assert actual["vent_close_arm_trigger"] == "arm_pressure"
    assert actual["late_arm_at_ready"] is True
    assert actual["ready_reached_monotonic_s"] is not None


def test_co2_preseal_soak_arms_when_predicted_time_to_ready_is_short(monkeypatch) -> None:
    orchestrator, point, route_traces, _timing_events, _vent_calls = _preseal_arm_orchestrator(
        monkeypatch,
        [1000.0, 1055.0],
        cfg_overrides={
            "workflow.pressure.preseal_vent_close_arm_pressure_hpa": 1090.0,
            "workflow.pressure.preseal_vent_close_arm_margin_hpa": 10.0,
        },
    )

    assert orchestrator._wait_co2_route_soak_before_seal(point) is True

    assert route_traces[-1]["action"] == "preseal_vent_close_arm_triggered"
    assert route_traces[-1]["actual"]["vent_close_arm_trigger"] == "time_to_ready"
    assert route_traces[-1]["actual"]["estimated_time_to_ready_s"] <= 3.0


def test_positive_preseal_ready_closes_vent_and_seals_before_pressure_control() -> None:
    service, host, controller, status = _positive_preseal_service([1009.0, 1109.0, 1110.5, 1110.2])
    point = CalibrationPoint(index=1, temperature_c=20.0, co2_ppm=100.0, pressure_hpa=1100.0, route="co2")

    result = service.pressurize_and_hold(point, route="co2")

    assert result.ok is True
    assert controller.vent_on is False
    actions = [row["action"] for row in status.rows]
    assert "positive_preseal_ready" in actions
    assert "seal_route" in actions
    assert actions.index("positive_preseal_ready") < actions.index("seal_route")
    assert not [row for row in status.rows if row["action"] == "set_vent" and row.get("target", {}).get("vent_on") is True]
    assert any(event["event_name"] == "positive_preseal_pressurization_start" for event in host._recorded_timing)
    assert any(event["event_name"] == "positive_preseal_vent_close_start" for event in host._recorded_timing)
    assert any(event["event_name"] == "positive_preseal_vent_close_end" for event in host._recorded_timing)
    assert any(event["event_name"] == "positive_preseal_ready" for event in host._recorded_timing)
    assert any(event["event_name"] == "positive_preseal_seal_end" for event in host._recorded_timing)
    ready_event_index = next(
        index for index, event in enumerate(host._recorded_timing) if event["event_name"] == "positive_preseal_ready"
    )
    seal_start_index = next(
        index for index, event in enumerate(host._recorded_timing) if event["event_name"] == "positive_preseal_seal_start"
    )
    seal_start_event = host._recorded_timing[seal_start_index]
    assert ready_event_index < seal_start_index
    assert seal_start_event["duration_s"] is not None
    assert seal_start_event["duration_s"] <= 0.5


def test_positive_preseal_vent_close_command_arms_guard_immediately() -> None:
    service, _host, _controller, status = _positive_preseal_service([1009.0, 1105.0, 1105.0])
    point = CalibrationPoint(index=1, temperature_c=20.0, co2_ppm=100.0, pressure_hpa=1100.0, route="co2")

    result = service.pressurize_and_hold(point, route="co2")

    assert result.ok is True
    guard = next(row for row in status.rows if row["action"] == "positive_preseal_guard_armed")
    assert guard["actual"]["preseal_guard_armed"] is True
    assert guard["actual"]["preseal_guard_arm_source"] == "atmosphere_vent_close_command"
    assert guard["actual"]["preseal_guard_armed_from_vent_close_command"] is True
    assert guard["actual"]["preseal_guard_expected_arm_source"] == "atmosphere_vent_close_command"
    assert guard["actual"]["preseal_guard_actual_arm_source"] == "atmosphere_vent_close_command"
    assert guard["actual"]["preseal_guard_arm_source_alignment_ok"] is True
    assert guard["actual"]["vent_close_command_sent_at"]
    assert guard["actual"]["vent_close_to_preseal_guard_arm_latency_s"] == 0.0
    assert guard["actual"]["vent_close_to_positive_preseal_start_latency_s"] == 0.0


def test_positive_preseal_vent_off_settle_over_urgent_threshold_triggers_seal() -> None:
    service, _host, _controller, status = _positive_preseal_service([1009.0, 1153.0, 1100.0])
    point = CalibrationPoint(index=1, temperature_c=20.0, co2_ppm=100.0, pressure_hpa=1100.0, route="co2")

    result = service.pressurize_and_hold(point, route="co2")

    assert result.ok is True
    assert result.diagnostics["vent_off_settle_wait_pressure_monitored"] is True
    assert result.diagnostics["vent_off_settle_monitor_started"] is True
    assert result.diagnostics["vent_off_settle_monitor_sample_count"] >= 1
    assert result.diagnostics["vent_off_settle_wait_overlimit_seen"] is False
    assert result.diagnostics["preseal_capture_urgent_seal_threshold_hpa"] == 1150.0
    assert result.diagnostics["preseal_capture_hard_abort_pressure_hpa"] == 1250.0
    assert result.diagnostics["preseal_capture_urgent_seal_triggered"] is True
    assert result.diagnostics["preseal_capture_urgent_seal_pressure_hpa"] == 1153.0
    assert result.diagnostics["preseal_capture_over_urgent_threshold_action"] == "urgent_seal"
    assert result.diagnostics["preseal_capture_hard_abort_triggered"] is False
    assert result.diagnostics["seal_command_allowed_after_atmosphere_vent_closed"] is True
    actions = [row["action"] for row in status.rows]
    assert "positive_preseal_ready" in actions
    assert "seal_route" in actions
    assert actions.index("positive_preseal_ready") < actions.index("seal_route")
    if "set_pressure" in actions:
        assert actions.index("seal_route") < actions.index("set_pressure")


def test_positive_preseal_vent_off_settle_ready_to_seal_near_first_target() -> None:
    service, _host, _controller, status = _positive_preseal_service([1009.0, 1105.0, 1105.0])
    point = CalibrationPoint(index=1, temperature_c=20.0, co2_ppm=100.0, pressure_hpa=1100.0, route="co2")

    result = service.pressurize_and_hold(point, route="co2")

    assert result.ok is True
    ready = next(row for row in status.rows if row["action"] == "positive_preseal_ready")
    assert ready["actual"]["vent_off_settle_wait_pressure_monitored"] is True
    assert ready["actual"]["vent_off_settle_monitor_started"] is True
    assert ready["actual"]["vent_off_settle_monitor_sample_count"] >= 1
    assert ready["actual"]["vent_off_settle_wait_ready_to_seal_seen"] is True
    assert ready["actual"]["vent_off_settle_first_ready_to_seal_sample_hpa"] == 1105.0
    assert ready["actual"]["ready_to_seal_window_entered"] is True
    assert ready["actual"]["ready_to_seal_window_missed_reason"] == ""
    assert ready["actual"]["first_target_ready_to_seal_min_hpa"] == 1100.0
    assert ready["actual"]["first_target_ready_to_seal_max_hpa"] < 1150.0
    assert ready["actual"]["first_target_ready_to_seal_pressure_hpa"] == 1105.0
    assert ready["actual"]["first_target_ready_to_seal_before_abort"] is True
    assert ready["actual"]["first_target_ready_to_seal_missed"] is False


def test_seal_preparation_vent_off_settle_ready_to_seal_arms_context(monkeypatch) -> None:
    orchestrator, point, _clock, timing_events, _route_traces, vent_calls = _conditioning_guard_orchestrator(
        monkeypatch,
        [{"pressure_hpa": 1105.0, "age_s": 0.0, "sequence_id": 9}],
        cfg_overrides={"workflow.pressure.seal_preparation_vent_off_settle_s": 0.1},
    )
    orchestrator._a2_high_pressure_first_point_mode_enabled = True
    orchestrator._a2_co2_route_conditioning_completed = True
    orchestrator._a2_co2_route_conditioning_completed_at = "2026-05-01T00:00:00+00:00"
    orchestrator._a2_preseal_analyzer_gate_passed = True
    orchestrator._a2_high_pressure_first_point_context = {
        "enabled": True,
        "first_target_pressure_hpa": 1100.0,
        "baseline_pressure_hpa": 1009.0,
    }

    diagnostics = orchestrator._preclose_a2_high_pressure_first_point_vent(point)

    context = orchestrator._a2_preseal_vent_close_arm_context
    assert diagnostics["output_state"] == 0
    assert vent_calls[-1]["vent_on"] is False
    assert context["preseal_guard_arm_source"] == "atmosphere_vent_close_command"
    assert context["preseal_guard_armed_from_vent_close_command"] is True
    assert context["vent_off_settle_wait_pressure_monitored"] is True
    assert context["vent_off_settle_wait_ready_to_seal_seen"] is True
    assert context["vent_off_settle_first_ready_to_seal_sample_hpa"] == 1105.0
    assert context["ready_to_seal_window_entered"] is True
    assert context["vent_close_arm_trigger"] == "ready_pressure"
    assert any(
        event["event_name"] == "seal_preparation_vent_off_settle_pressure_check"
        and event["decision"] == "ready_to_seal"
        for event in timing_events
    )


def test_preseal_capture_monitor_arms_before_vent_close_command(monkeypatch) -> None:
    orchestrator, point, _clock, _timing_events, _route_traces, vent_calls = _conditioning_guard_orchestrator(
        monkeypatch,
        [{"pressure_hpa": 1104.5, "age_s": 0.0, "sequence_id": 11}],
        cfg_overrides={"workflow.pressure.seal_preparation_vent_off_settle_s": 0.1},
    )
    orchestrator._a2_high_pressure_first_point_mode_enabled = True
    orchestrator._a2_co2_route_conditioning_completed = True
    orchestrator._a2_high_pressure_first_point_context = {
        "enabled": True,
        "first_target_pressure_hpa": 1100.0,
        "baseline_pressure_hpa": 1009.0,
    }

    def vent_close(vent_on: bool, **kwargs):
        armed = dict(orchestrator._a2_preseal_vent_close_arm_context)
        vent_calls.append({"vent_on": vent_on, "armed": armed, **kwargs})
        return {"output_state": 0, "isolation_state": 1, "vent_status_raw": 1}

    orchestrator.pressure_control_service.set_pressure_controller_vent = vent_close

    orchestrator._preclose_a2_high_pressure_first_point_vent(point)

    armed = vent_calls[-1]["armed"]
    assert vent_calls[-1]["vent_on"] is False
    assert armed["preseal_capture_started"] is True
    assert armed["preseal_capture_not_pressure_control"] is True
    assert armed["preseal_capture_monitor_armed_before_vent_close_command"] is True
    assert armed["preseal_guard_armed"] is True
    assert armed["preseal_guard_arm_source"] == "atmosphere_vent_close_command"
    assert armed["preseal_guard_armed_from_vent_close_command"] is True
    assert armed["vent_close_command_sent_at"]
    assert armed["vent_off_settle_monitor_started"] is True
    assert armed["vent_off_settle_wait_pressure_monitored"] is True


def test_preseal_capture_ready_window_after_vent_close_enters_ready_to_seal(monkeypatch) -> None:
    orchestrator, point, _clock, timing_events, route_traces, _vent_calls = _conditioning_guard_orchestrator(
        monkeypatch,
        [{"pressure_hpa": 1104.5, "age_s": 0.0, "sequence_id": 12}],
        cfg_overrides={
            "workflow.pressure.preseal_ready_pressure_hpa": 1100.0,
            "workflow.pressure.preseal_abort_pressure_hpa": 1150.0,
            "workflow.pressure.preseal_atmosphere_flush_abort_pressure_hpa": 1150.0,
        },
    )
    orchestrator._a2_high_pressure_first_point_mode_enabled = True
    orchestrator._a2_co2_route_conditioning_at_atmosphere_active = False
    orchestrator._a2_high_pressure_first_point_context = {
        "enabled": True,
        "first_target_pressure_hpa": 1100.0,
    }
    orchestrator._a2_preseal_vent_close_arm_context = {
        "preseal_capture_started": True,
        "preseal_guard_armed": True,
        "preseal_guard_arm_source": "atmosphere_vent_close_command",
        "preseal_guard_armed_from_vent_close_command": True,
        "vent_close_command_sent_at": "2026-05-01T00:00:00+00:00",
        "vent_close_command_monotonic_s": 99.0,
        "first_target_ready_to_seal_min_hpa": 1100.0,
        "first_target_ready_to_seal_max_hpa": 1112.0,
    }

    decision = orchestrator._verify_co2_preseal_atmosphere_hold_pressure(point)

    context = orchestrator._a2_preseal_vent_close_arm_context
    assert decision == "positive_preseal_ready_handoff"
    assert context["ready_to_seal_window_entered"] is True
    assert context["first_target_ready_to_seal_pressure_hpa"] == 1104.5
    assert context["seal_command_allowed_after_atmosphere_vent_closed"] is True
    assert route_traces[-1]["action"] == "preseal_atmosphere_flush_ready_handoff"
    assert any(event["event_name"] == "high_pressure_ready_detected" for event in timing_events)


def test_preseal_capture_predictive_ready_triggers_below_target_when_rise_is_fast(monkeypatch) -> None:
    orchestrator, point, clock, timing_events, route_traces, _vent_calls = _conditioning_guard_orchestrator(
        monkeypatch,
        [
            {"pressure_hpa": 1088.0, "age_s": 0.0, "sequence_id": 15},
            {"pressure_hpa": 1094.0, "age_s": 0.0, "sequence_id": 16},
        ],
        cfg_overrides={
            "workflow.pressure.preseal_ready_pressure_hpa": 1100.0,
            "workflow.pressure.preseal_abort_pressure_hpa": 1150.0,
            "workflow.pressure.preseal_atmosphere_flush_abort_pressure_hpa": 1150.0,
            "workflow.pressure.preseal_capture_predictive_seal_latency_s": 0.1,
        },
    )
    orchestrator._a2_high_pressure_first_point_mode_enabled = True
    orchestrator._a2_co2_route_conditioning_at_atmosphere_active = False
    orchestrator._a2_high_pressure_first_point_context = {
        "enabled": True,
        "first_target_pressure_hpa": 1100.0,
    }
    orchestrator._a2_preseal_vent_close_arm_context = {
        "preseal_capture_started": True,
        "preseal_capture_not_pressure_control": True,
        "preseal_guard_armed": True,
        "preseal_guard_arm_source": "atmosphere_vent_close_command",
        "preseal_guard_armed_from_vent_close_command": True,
        "vent_close_command_sent_at": "2026-05-01T00:00:00+00:00",
        "vent_close_command_monotonic_s": 99.0,
    }

    assert orchestrator._verify_co2_preseal_atmosphere_hold_pressure(point) == "within_limit"
    clock["now"] += 0.1
    decision = orchestrator._verify_co2_preseal_atmosphere_hold_pressure(point)

    context = orchestrator._a2_preseal_vent_close_arm_context
    assert decision == "positive_preseal_ready_handoff"
    assert context["preseal_capture_predictive_ready_to_seal"] is True
    assert context["vent_close_arm_trigger"] == "predictive_ready_to_seal"
    assert context["first_target_ready_to_seal_pressure_hpa"] == 1094.0
    assert context["preseal_capture_predicted_seal_completion_pressure_hpa"] == pytest.approx(1100.0)
    assert context["ready_to_seal_window_entered"] is False
    assert context["seal_command_allowed_after_atmosphere_vent_closed"] is True
    assert route_traces[-1]["action"] == "preseal_atmosphere_flush_ready_handoff"
    assert any(event["event_name"] == "high_pressure_ready_detected" for event in timing_events)


def test_preseal_capture_before_vent_close_over_1100_is_not_actionable(monkeypatch) -> None:
    orchestrator, point, _clock, timing_events, route_traces, _vent_calls = _conditioning_guard_orchestrator(
        monkeypatch,
        [{"pressure_hpa": 1104.5, "age_s": 0.0, "sequence_id": 13}],
        cfg_overrides={
            "workflow.pressure.preseal_ready_pressure_hpa": 1100.0,
            "workflow.pressure.preseal_abort_pressure_hpa": 1150.0,
            "workflow.pressure.preseal_atmosphere_flush_abort_pressure_hpa": 1150.0,
        },
    )
    orchestrator._a2_high_pressure_first_point_mode_enabled = True
    orchestrator._a2_co2_route_conditioning_at_atmosphere_active = False
    orchestrator._a2_high_pressure_first_point_context = {
        "enabled": True,
        "first_target_pressure_hpa": 1100.0,
    }

    decision = orchestrator._verify_co2_preseal_atmosphere_hold_pressure(point)

    context = orchestrator._a2_high_pressure_first_point_context
    assert decision == "within_limit"
    assert context["first_over_1100_before_vent_close"] is True
    assert context["first_over_1100_not_actionable_reason"] == "before_vent_close_or_wrong_phase"
    assert context["ready_to_seal_window_entered"] is False
    assert route_traces == []
    assert any(
        event["event_name"] == "high_pressure_ready_before_vent_close_not_actionable"
        for event in timing_events
    )


def test_seal_preparation_vent_off_settle_over_urgent_threshold_arms_seal(monkeypatch) -> None:
    orchestrator, point, _clock, timing_events, _route_traces, vent_calls = _conditioning_guard_orchestrator(
        monkeypatch,
        [{"pressure_hpa": 1154.0, "age_s": 0.0, "sequence_id": 10}],
        cfg_overrides={"workflow.pressure.seal_preparation_vent_off_settle_s": 0.1},
    )
    orchestrator._a2_high_pressure_first_point_mode_enabled = True
    orchestrator._a2_co2_route_conditioning_completed = True
    orchestrator._a2_co2_route_conditioning_completed_at = "2026-05-01T00:00:00+00:00"
    orchestrator._a2_preseal_analyzer_gate_passed = True
    orchestrator._a2_high_pressure_first_point_context = {
        "enabled": True,
        "first_target_pressure_hpa": 1100.0,
        "baseline_pressure_hpa": 1009.0,
    }

    orchestrator._preclose_a2_high_pressure_first_point_vent(point)

    context = orchestrator._a2_preseal_vent_close_arm_context
    assert vent_calls[-1]["vent_on"] is False
    assert context["vent_off_settle_wait_pressure_monitored"] is True
    assert context["vent_off_settle_wait_overlimit_seen"] is False
    assert context["preseal_capture_urgent_seal_triggered"] is True
    assert context["preseal_capture_urgent_seal_pressure_hpa"] == 1154.0
    assert context["preseal_capture_over_urgent_threshold_action"] == "urgent_seal"
    assert context["preseal_capture_hard_abort_triggered"] is False
    assert context["seal_command_allowed_after_atmosphere_vent_closed"] is True
    assert any(
        event["event_name"] == "seal_preparation_vent_off_settle_pressure_check"
        and event["decision"] == "ready_to_seal"
        for event in timing_events
    )


def test_preseal_capture_over_urgent_threshold_propagates_monitor_context(monkeypatch) -> None:
    orchestrator, point, _clock, timing_events, route_traces, _vent_calls = _conditioning_guard_orchestrator(
        monkeypatch,
        [{"pressure_hpa": 1153.674, "age_s": 0.0, "sequence_id": 14}],
        cfg_overrides={
            "workflow.pressure.preseal_ready_pressure_hpa": 1100.0,
            "workflow.pressure.preseal_abort_pressure_hpa": 1150.0,
            "workflow.pressure.preseal_atmosphere_flush_abort_pressure_hpa": 1150.0,
        },
    )
    orchestrator._a2_high_pressure_first_point_mode_enabled = True
    orchestrator._a2_co2_route_conditioning_at_atmosphere_active = False
    orchestrator._a2_high_pressure_first_point_context = {
        "enabled": True,
        "first_target_pressure_hpa": 1100.0,
    }
    orchestrator._a2_preseal_vent_close_arm_context = {
        "preseal_capture_started": True,
        "preseal_capture_not_pressure_control": True,
        "preseal_capture_monitor_armed_before_vent_close_command": True,
        "preseal_guard_armed": True,
        "preseal_guard_arm_source": "atmosphere_vent_close_command",
        "preseal_guard_armed_from_vent_close_command": True,
        "vent_close_command_sent_at": "2026-05-01T00:00:00+00:00",
        "vent_close_command_monotonic_s": 99.0,
    }

    decision = orchestrator._verify_co2_preseal_atmosphere_hold_pressure(point)

    context = orchestrator._a2_preseal_vent_close_arm_context
    assert decision == "positive_preseal_ready_handoff"
    assert context["preseal_capture_started"] is True
    assert context["preseal_capture_not_pressure_control"] is True
    assert context["preseal_capture_monitor_covers_abort_path"] is True
    assert context["vent_off_settle_monitor_started"] is True
    assert context["vent_off_settle_wait_pressure_monitored"] is True
    assert context["vent_off_settle_wait_overlimit_seen"] is False
    assert context["preseal_capture_urgent_seal_triggered"] is True
    assert context["preseal_capture_urgent_seal_pressure_hpa"] == 1153.674
    assert context["preseal_capture_over_urgent_threshold_action"] == "urgent_seal"
    assert context["preseal_capture_hard_abort_triggered"] is False
    assert context["preseal_capture_abort_pressure_hpa"] is None
    assert context["positive_preseal_pressure_hpa"] == 1153.674
    assert context["seal_command_allowed_after_atmosphere_vent_closed"] is True
    assert route_traces[-1]["action"] == "preseal_atmosphere_flush_ready_handoff"
    assert any(event["event_name"] == "high_pressure_ready_detected" for event in timing_events)


def test_positive_preseal_target_1100_over_urgent_threshold_does_not_abort() -> None:
    service, _host, _controller, _status = _positive_preseal_service([1009.0, 1150.0, 1100.0])
    point = CalibrationPoint(index=1, temperature_c=20.0, co2_ppm=100.0, pressure_hpa=1100.0, route="co2")

    result = service.pressurize_and_hold(point, route="co2")

    assert result.ok is True
    assert result.diagnostics["first_target_ready_to_seal_min_hpa"] == 1100.0
    assert result.diagnostics["first_target_ready_to_seal_max_hpa"] < 1150.0
    assert result.diagnostics["first_target_ready_to_seal_missed"] is False
    assert result.diagnostics["positive_preseal_abort_pressure_hpa"] == 1150.0
    assert result.diagnostics["preseal_capture_urgent_seal_triggered"] is True
    assert result.diagnostics["preseal_capture_urgent_seal_pressure_hpa"] == 1150.0
    assert result.diagnostics["preseal_capture_hard_abort_triggered"] is False


def test_positive_preseal_predictive_ready_triggers_below_target_without_pressure_control() -> None:
    service, _host, _controller, status = _positive_preseal_service(
        [1009.0, 1088.0, 1094.0, 1099.0],
        cfg_overrides={
            "workflow.pressure.preseal_ready_pressure_hpa": 1100.0,
            "workflow.pressure.preseal_capture_predictive_seal_latency_s": 0.05,
            "workflow.pressure.fail_if_sealed_pressure_below_target": False,
        },
    )
    point = CalibrationPoint(index=1, temperature_c=20.0, co2_ppm=100.0, pressure_hpa=1100.0, route="co2")

    result = service.pressurize_and_hold(point, route="co2")

    assert result.ok is True
    ready = next(row for row in status.rows if row["action"] == "positive_preseal_ready")
    assert ready["actual"]["preseal_capture_predictive_ready_to_seal"] is True
    assert ready["actual"]["seal_trigger_pressure_hpa"] == 1099.0
    assert ready["actual"]["preseal_capture_predicted_seal_completion_pressure_hpa"] == pytest.approx(1104.0, abs=0.2)
    assert ready["actual"]["ready_to_seal_window_entered"] is False
    assert ready["actual"]["seal_command_allowed_after_atmosphere_vent_closed"] is True
    actions = [row["action"] for row in status.rows]
    assert "seal_route" in actions
    if "set_pressure" in actions:
        assert actions.index("seal_route") < actions.index("set_pressure")
    if "set_output" in actions:
        assert actions.index("seal_route") < actions.index("set_output")


def test_preseal_gate_refreshes_controller_after_verified_vent_off() -> None:
    class LaggingVentStatusController(_FakePressureController):
        simulated_device = True

        def __init__(self) -> None:
            super().__init__()
            self._status_reads = [0, 1, 1]

        def get_vent_status(self) -> int:
            if self._status_reads:
                return self._status_reads.pop(0)
            return self.vent_status

    service, host, stale_controller, status = _positive_preseal_service(
        [1013.25],
        cfg_overrides={
            "workflow.pressure.positive_preseal_pressurization_enabled": False,
            "workflow.pressure.pressurize_wait_after_vent_off_s": 0.0,
            "workflow.pressure.fail_if_sealed_pressure_below_target": False,
        },
    )
    active_controller = LaggingVentStatusController()
    stale_controller.vent_on = True
    stale_controller.vent_status = 1
    active_controller.vent_on = True
    active_controller.vent_status = 1
    original_device = host._device
    pressure_controller_calls = {"count": 0}

    def refreshed_device(*names):
        if "pressure_controller" in names or "pace" in names:
            pressure_controller_calls["count"] += 1
            return stale_controller if pressure_controller_calls["count"] == 1 else active_controller
        return original_device(*names)

    host._device = refreshed_device
    point = CalibrationPoint(index=1, temperature_c=20.0, co2_ppm=100.0, pressure_hpa=1100.0, route="co2")

    result = service.pressurize_and_hold(point, route="co2")

    assert result.ok is True
    preseal_exit = next(row for row in status.rows if row["action"] == "preseal_final_atmosphere_exit")
    assert preseal_exit["result"] == "ok"
    assert preseal_exit["actual"]["pressure_controller_vent_status"] == 1
    assert preseal_exit["actual"]["preseal_controller_refreshed_after_vent_off"] is True
    assert preseal_exit["actual"]["preseal_final_vent_off_snapshot_status"] == 0
    assert preseal_exit["actual"]["preseal_final_vent_off_snapshot_idle"] is True
    assert preseal_exit["actual"]["preseal_final_vent_off_snapshot_accepted"] is True
    assert preseal_exit["actual"]["preseal_final_vent_off_snapshot_acceptance_scope"] == "simulation_only"
    assert any(row["action"] == "seal_route" for row in status.rows)


def test_high_pressure_first_point_ready_sends_seal_command_immediately() -> None:
    service, host, controller, status = _positive_preseal_service([1110.5, 1110.2])
    controller.vent_on = False
    controller.vent_status = 0
    host._a2_high_pressure_first_point_mode_enabled = True
    host._a2_high_pressure_first_point_vent_preclosed = True
    host._a2_preseal_vent_close_arm_context = {
        "vent_close_arm_trigger": "ready_pressure",
        "vent_close_arm_pressure_hpa": 1110.5,
        "ready_pressure_hpa": 1110.0,
        "abort_pressure_hpa": 1150.0,
        "ready_reached_monotonic_s": time.monotonic(),
        "pressure_sample_source": "digital_pressure_gauge",
        "pressure_sample_age_s": 0.0,
        "pressure_sample_is_stale": False,
        "pressure_sample_sequence_id": 8,
    }
    point = CalibrationPoint(index=1, temperature_c=20.0, co2_ppm=100.0, pressure_hpa=1100.0, route="co2")

    result = service.pressurize_and_hold(point, route="co2")

    assert result.ok is True
    actions = [row["action"] for row in status.rows]
    assert "seal_route" in actions
    assert not [row for row in status.rows if row["action"] == "set_vent"]
    ready_index = next(
        index for index, event in enumerate(host._recorded_timing) if event["event_name"] == "high_pressure_ready_detected"
    )
    seal_index = next(
        index for index, event in enumerate(host._recorded_timing) if event["event_name"] == "high_pressure_seal_command_sent"
    )
    assert ready_index < seal_index
    assert host._recorded_timing[seal_index]["duration_s"] <= 0.5
    assert any(event["event_name"] == "high_pressure_seal_confirmed" for event in host._recorded_timing)


def test_high_pressure_first_point_abort_threshold_remains_hard_fail() -> None:
    service, host, controller, status = _positive_preseal_service([1250.0])
    controller.vent_on = False
    controller.vent_status = 0
    host._a2_high_pressure_first_point_mode_enabled = True
    host._a2_high_pressure_first_point_vent_preclosed = True
    host._a2_preseal_vent_close_arm_context = {
        "vent_close_arm_trigger": "ready_pressure",
        "vent_close_arm_pressure_hpa": 1250.0,
        "ready_pressure_hpa": 1110.0,
        "abort_pressure_hpa": 1150.0,
        "ready_reached_monotonic_s": time.monotonic(),
        "pressure_sample_source": "digital_pressure_gauge",
        "pressure_sample_age_s": 0.0,
        "pressure_sample_is_stale": False,
    }
    point = CalibrationPoint(index=1, temperature_c=20.0, co2_ppm=100.0, pressure_hpa=1100.0, route="co2")

    result = service.pressurize_and_hold(point, route="co2")

    assert result.ok is False
    assert result.error == "Positive preseal pressurization exceeded hard abort pressure"
    assert result.diagnostics["preseal_capture_hard_abort_pressure_hpa"] == 1250.0
    assert result.diagnostics["preseal_capture_hard_abort_triggered"] is True
    assert not any(row["action"] == "seal_route" for row in status.rows)
    assert any(event["event_name"] == "high_pressure_abort" for event in host._recorded_timing)


def test_positive_preseal_ready_arm_context_seals_without_extra_pressure_poll() -> None:
    service, host, _controller, status = _positive_preseal_service([1110.5])
    host._a2_preseal_vent_close_arm_context = {
        "preseal_vent_close_arm_pressure_hpa": 1080.0,
        "preseal_vent_close_arm_margin_hpa": 30.0,
        "preseal_vent_close_arm_time_to_ready_s": 3.0,
        "vent_close_arm_trigger": "arm_pressure",
        "vent_close_arm_pressure_hpa": 1110.5,
        "vent_close_arm_elapsed_s": 0.2,
        "ready_pressure_hpa": 1110.0,
        "abort_pressure_hpa": 1150.0,
        "ready_reached_monotonic_s": 100.0,
        "late_arm_at_ready": True,
        "pressure_sample_source": "pressure_gauge",
        "pressure_sample_timestamp": "2026-04-26T10:00:00+00:00",
        "pressure_sample_age_s": 0.0,
        "pressure_sample_is_stale": False,
        "pressure_sample_sequence_id": 7,
    }
    point = CalibrationPoint(index=1, temperature_c=20.0, co2_ppm=100.0, pressure_hpa=1100.0, route="co2")

    result = service.pressurize_and_hold(point, route="co2")

    assert result.ok is True
    actions = [row["action"] for row in status.rows]
    assert "positive_preseal_ready" in actions
    assert "seal_route" in actions
    assert "positive_preseal_pressure_check" not in actions
    ready = next(row for row in status.rows if row["action"] == "positive_preseal_ready")
    assert ready["actual"]["pressure_sample_source"] == "pressure_gauge"
    assert ready["actual"]["pressure_sample_sequence_id"] == 7


def test_positive_preseal_vent_close_prefers_fast_direct_command_over_blocking_exit() -> None:
    service, _host, controller, status = _positive_preseal_service([1009.0, 1110.5, 1110.5])

    def exit_atmosphere_mode(**kwargs) -> int:
        raise AssertionError("slow exit_atmosphere_mode should not be used for positive preseal")

    controller.exit_atmosphere_mode = exit_atmosphere_mode
    point = CalibrationPoint(index=1, temperature_c=20.0, co2_ppm=100.0, pressure_hpa=1100.0, route="co2")

    result = service.pressurize_and_hold(point, route="co2")

    assert result.ok is True
    set_vent_off = [
        row for row in status.rows if row["action"] == "set_vent" and row.get("target", {}).get("vent_on") is False
    ][-1]
    assert set_vent_off["actual"]["command_method"] == "set_output_false_vent_false_set_isolation_open_fast"
    assert set_vent_off["actual"]["snapshot_after_command"] is False
    assert set_vent_off["actual"]["vent_command_ack"] is True


def test_positive_preseal_vent_close_failure_hard_fails_before_pressure_polling() -> None:
    service, host, controller, status = _positive_preseal_service(
        [1009.0, 1110.5],
        cfg_overrides={
            "workflow.pressure.preseal_vent_close_verify_timeout_s": 0.05,
            "workflow.pressure.preseal_vent_close_verify_poll_s": 0.05,
        },
    )

    def stuck_vent(enabled: bool) -> bool:
        controller.vent_on = True
        controller.vent_status = 1
        return True

    controller.vent = stuck_vent
    point = CalibrationPoint(index=1, temperature_c=20.0, co2_ppm=100.0, pressure_hpa=1100.0, route="co2")

    result = service.pressurize_and_hold(point, route="co2")

    assert result.ok is False
    assert result.diagnostics["abort_reason"] == "preseal_vent_close_failed"
    assert result.diagnostics["vent_command_result"] == "not_closed"
    assert not any(row["action"] == "positive_preseal_pressure_check" for row in status.rows)
    assert not any(row["action"] == "seal_route" for row in status.rows)
    assert any(event["event_name"] == "positive_preseal_vent_close_fail" for event in host._recorded_timing)


def test_positive_preseal_vent_close_accepts_blocking_command_return_status_with_lagged_status() -> None:
    service, host, controller, status = _positive_preseal_service([1009.0, 1109.0, 1110.5, 1110.2])

    def exit_atmosphere_mode(**kwargs) -> int:
        controller.vent_on = False
        controller.vent_status = 1
        controller.output_state = 0
        controller.isolation_state = 1
        return 0

    controller.exit_atmosphere_mode = exit_atmosphere_mode
    point = CalibrationPoint(index=1, temperature_c=20.0, co2_ppm=100.0, pressure_hpa=1100.0, route="co2")

    result = service.pressurize_and_hold(point, route="co2")

    assert result.ok is True
    set_vent_off = [
        row for row in status.rows if row["action"] == "set_vent" and row.get("target", {}).get("vent_on") is False
    ][-1]
    assert set_vent_off["actual"]["vent_command_return_status"] == 0
    assert any(row["action"] == "positive_preseal_pressure_check" for row in status.rows)
    assert any(event["event_name"] == "positive_preseal_vent_close_end" for event in host._recorded_timing)


def test_positive_preseal_abort_pressure_hard_fails_before_seal() -> None:
    service, host, _controller, status = _positive_preseal_service([1009.0, 1250.0])
    point = CalibrationPoint(index=1, temperature_c=20.0, co2_ppm=100.0, pressure_hpa=1100.0, route="co2")

    result = service.pressurize_and_hold(point, route="co2")

    assert result.ok is False
    assert result.error == "Positive preseal pressurization exceeded hard abort pressure"
    assert result.diagnostics["preseal_capture_hard_abort_pressure_hpa"] == 1250.0
    assert result.diagnostics["preseal_capture_hard_abort_triggered"] is True
    assert any(row["action"] == "positive_preseal_abort" for row in status.rows)
    assert not any(row["action"] == "seal_route" for row in status.rows)
    assert any(event["event_name"] == "positive_preseal_abort" for event in host._recorded_timing)


def test_positive_preseal_urgent_seal_transition_failure_fails_closed_before_pressure_control() -> None:
    service, host, _controller, status = _positive_preseal_service([1009.0, 1153.0, 1153.0])
    host._apply_valve_states = lambda open_valves: {"co2": {"1": True}}
    point = CalibrationPoint(index=1, temperature_c=20.0, co2_ppm=100.0, pressure_hpa=1100.0, route="co2")

    result = service.pressurize_and_hold(point, route="co2")

    assert result.ok is False
    assert result.error == "Seal transition incomplete before pressure control"
    assert result.diagnostics["preseal_capture_urgent_seal_triggered"] is True
    assert result.diagnostics["preseal_capture_urgent_seal_pressure_hpa"] == 1153.0
    assert result.diagnostics["preseal_capture_hard_abort_triggered"] is False
    assert result.diagnostics["pressure_control_started_after_seal_confirmed"] is False
    assert any(row["action"] == "positive_preseal_ready" for row in status.rows)
    assert any(row["action"] == "seal_transition" and row["result"] == "fail" for row in status.rows)
    assert not any(row["action"] == "set_pressure" for row in status.rows)
    assert any(event["event_name"] == "positive_preseal_abort" for event in host._recorded_timing)


def test_positive_preseal_pressure_guard_blocks_before_vent_close() -> None:
    service, _host, _controller, status = _positive_preseal_service([1250.0])
    point = CalibrationPoint(index=1, temperature_c=20.0, co2_ppm=100.0, pressure_hpa=1100.0, route="co2")

    result = service.pressurize_and_hold(point, route="co2")

    assert result.ok is False
    assert result.error == "Positive preseal pressurization exceeded hard abort pressure"
    assert result.diagnostics["positive_preseal_pressure_guard_checked"] is True
    assert result.diagnostics["positive_preseal_pressure_overlimit"] is True
    assert result.diagnostics["positive_preseal_overlimit_fail_closed"] is True
    assert result.diagnostics["preseal_capture_hard_abort_pressure_hpa"] == 1250.0
    assert result.diagnostics["preseal_capture_hard_abort_triggered"] is True
    assert result.diagnostics["preseal_guard_armed"] is False
    assert result.diagnostics["preseal_guard_arm_source"] == ""
    assert result.diagnostics["preseal_guard_armed_from_vent_close_command"] is False
    assert result.diagnostics["preseal_guard_armed_from_vent_close_command_false_reason"] == (
        "preseal_hard_abort_before_vent_close_guard_arm"
    )
    assert result.diagnostics["positive_preseal_guard_started_before_first_over_abort"] is False
    assert result.diagnostics["positive_preseal_guard_started_after_first_over_abort"] is True
    assert result.diagnostics["positive_preseal_seal_command_sent"] is False
    assert result.diagnostics["positive_preseal_pressure_setpoint_command_sent"] is False
    assert result.diagnostics["positive_preseal_sample_started"] is False
    assert result.diagnostics["emergency_abort_relief_vent_required"] is True
    actions = [row["action"] for row in status.rows]
    assert "positive_preseal_pressure_guard" in actions
    assert "positive_preseal_abort" in actions
    assert not any(row["action"] == "seal_route" for row in status.rows)
    assert not any(
        row["action"] == "set_vent" and row.get("target", {}).get("vent_on") is False
        for row in status.rows
    )


def test_positive_preseal_ignores_stale_pressure_sample_for_abort() -> None:
    service, host, controller, status = _positive_preseal_service([])

    class StaleThenFreshGauge:
        def __init__(self) -> None:
            self.values = [
                {
                    "pressure_hpa": 1200.0,
                    "pressure_sample_source": "pressure_gauge",
                    "pressure_sample_timestamp": "2026-04-26T09:00:00+00:00",
                    "pressure_sample_age_s": 99.0,
                    "pressure_sample_is_stale": True,
                    "pressure_sample_sequence_id": 1,
                },
                {
                    "pressure_hpa": 1110.5,
                    "pressure_sample_source": "pressure_gauge",
                    "pressure_sample_age_s": 0.0,
                    "pressure_sample_is_stale": False,
                    "pressure_sample_sequence_id": 2,
                },
                1110.5,
            ]

        def read_pressure_hpa(self):
            if self.values:
                return self.values.pop(0)
            return 1110.5

    gauge = StaleThenFreshGauge()

    def device(*names):
        if "pressure_controller" in names or "pace" in names:
            return controller
        if "pressure_meter" in names or "pressure_gauge" in names:
            return gauge
        return None

    host._device = device
    host._make_pressure_reader = lambda: gauge.read_pressure_hpa
    point = CalibrationPoint(index=1, temperature_c=20.0, co2_ppm=100.0, pressure_hpa=1100.0, route="co2")

    result = service.pressurize_and_hold(point, route="co2")

    assert result.ok is True
    assert not any(row["action"] == "positive_preseal_abort" for row in status.rows)
    stale_check = next(
        row
        for row in status.rows
        if row["action"] in {"positive_preseal_pressure_guard", "positive_preseal_pressure_check"}
        and row["actual"].get("pressure_sample_is_stale") is True
    )
    assert stale_check["actual"]["pressure_hpa"] == 1200.0


def test_positive_preseal_ready_timeout_hard_fails_before_sample() -> None:
    service, _host, _controller, status = _positive_preseal_service(
        [1009.0, 1009.5, 1009.7, 1009.8],
        cfg_overrides={"workflow.pressure.preseal_ready_timeout_s": 0.05},
    )
    point = CalibrationPoint(index=1, temperature_c=20.0, co2_ppm=100.0, pressure_hpa=1100.0, route="co2")

    result = service.pressurize_and_hold(point, route="co2")

    assert result.ok is False
    assert result.timed_out is True
    assert result.diagnostics["abort_reason"] == "preseal_ready_timeout"
    assert not any(row["action"] == "seal_route" for row in status.rows)
    assert not any(row["action"] == "sample_start" for row in status.rows)


def test_positive_preseal_sealed_pressure_below_target_fails_before_pressure_control() -> None:
    service, _host, _controller, status = _positive_preseal_service([1009.0, 1110.0, 1099.0])
    point = CalibrationPoint(index=1, temperature_c=20.0, co2_ppm=100.0, pressure_hpa=1100.0, route="co2")

    result = service.pressurize_and_hold(point, route="co2")

    assert result.ok is False
    assert result.error == "Sealed CO2 pressure is below target before pressure control"
    assert any(row["action"] == "sealed_pressure_control_start" and row["result"] == "fail" for row in status.rows)
    assert not any(row["action"] == "set_pressure" for row in status.rows)


def test_sealed_route_pressure_control_refuses_repressurize_from_below() -> None:
    service, _host, controller, status = _positive_preseal_service([1098.0])
    point = CalibrationPoint(index=1, temperature_c=20.0, co2_ppm=100.0, pressure_hpa=1100.0, route="co2")
    service._mark_pressure_route_sealed(
        point,
        route="co2",
        final_vent_off_command_sent=True,
        watchlist={
            "preseal_final_atmosphere_exit_required": True,
            "preseal_final_atmosphere_exit_started": True,
            "preseal_final_atmosphere_exit_verified": True,
            "preseal_watchlist_status_seen": False,
            "preseal_watchlist_status_accepted": False,
            "preseal_watchlist_status_reason": "ok",
        },
        sealed_pressure_hpa=1100.0,
        preseal_pressure_peak_hpa=1110.0,
        preseal_pressure_last_hpa=1110.0,
        preseal_trigger="positive_preseal_ready",
        preseal_trigger_pressure_hpa=1110.0,
        preseal_trigger_threshold_hpa=1110.0,
    )
    service._mark_preseal_final_atmosphere_exit(
        {
            "preseal_final_atmosphere_exit_required": True,
            "preseal_final_atmosphere_exit_started": True,
            "preseal_final_atmosphere_exit_verified": True,
            "preseal_final_atmosphere_exit_phase": "preseal_before_full_seal",
            "preseal_final_atmosphere_exit_reason": "ok",
        }
    )
    service._mark_seal_transition(
        {
            "seal_transition_completed": True,
            "seal_transition_status": "verified_closed",
            "seal_transition_reason": "ok",
        }
    )
    controller.vent_status = 0
    controller.output_state = 0
    controller.isolation_state = 1

    result = service.set_pressure_to_target(point)

    assert result.ok is False
    assert "refusing to re-pressurize" in str(result.error)
    assert status.rows[-1]["action"] == "set_pressure"
    assert status.rows[-1]["result"] == "fail"
    assert controller.setpoints == []


def test_setpoint_command_before_seal_is_blocked() -> None:
    service, _host, controller, status = _positive_preseal_service([1100.0])
    point = CalibrationPoint(index=1, temperature_c=20.0, co2_ppm=100.0, pressure_hpa=1100.0, route="co2")

    result = service.set_pressure_to_target(point)

    assert result.ok is False
    assert result.diagnostics["setpoint_command_blocked_before_seal"] is True
    assert result.diagnostics["output_enable_blocked_before_seal"] is True
    assert result.diagnostics["pressure_control_started_after_seal_confirmed"] is False
    assert controller.setpoints == []
    assert status.rows[-1]["action"] == "set_pressure"
    assert status.rows[-1]["result"] == "blocked"


def test_output_enable_before_seal_is_blocked() -> None:
    service, _host, controller, status = _positive_preseal_service([1100.0])

    service.enable_pressure_controller_output(reason="before seal")

    assert controller.output_state == 0
    blocked = status.rows[-1]
    assert blocked["action"] == "set_output"
    assert blocked["result"] == "blocked"
    assert blocked["actual"]["output_enable_blocked_before_seal"] is True


def test_normal_atmosphere_vent_after_pressure_points_started_is_blocked() -> None:
    service, host, controller, status = _positive_preseal_service([1100.0])
    host._a2_pressure_points_started = True
    controller.vent_on = False
    controller.vent_status = 0

    payload = service.set_pressure_controller_vent(True, reason="between pressure points")

    assert payload["normal_atmosphere_vent_attempted_after_pressure_points_started"] is True
    assert payload["normal_atmosphere_vent_blocked_after_pressure_points_started"] is True
    assert controller.vent_on is False
    assert status.rows[-1]["action"] == "set_vent"
    assert status.rows[-1]["result"] == "blocked"


def test_emergency_relief_after_pressure_control_is_abort_only_and_no_resume() -> None:
    service, host, controller, status = _positive_preseal_service([1100.0])
    host._a2_pressure_points_started = True
    controller.vent_on = False
    controller.vent_status = 0

    payload = service.set_pressure_controller_vent(
        True,
        reason="emergency abort relief",
        emergency_abort_relief=True,
        emergency_abort_relief_context={"emergency_abort_relief_vent_required": True},
    )

    assert payload["emergency_relief_after_pressure_control_is_abort_only"] is True
    assert payload["resume_after_emergency_relief_allowed"] is False
    assert controller.vent_on is True
    assert status.rows[-1]["action"] == "set_vent"
    assert status.rows[-1]["result"] == "ok"
