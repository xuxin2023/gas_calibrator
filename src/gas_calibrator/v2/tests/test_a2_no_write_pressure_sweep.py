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
    RUN001_NOT_EXECUTED,
    RUN001_PASS,
    build_run001_a2_evidence_payload,
    evaluate_run001_a2_readiness,
    write_run001_a2_artifacts,
)
from gas_calibrator.v2.core.runners.co2_route_runner import Co2RouteRunner
from gas_calibrator.v2.core.services.pressure_control_service import PressureControlService


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

    def read_pressure_hpa(self) -> float:
        if self.values:
            self.last = float(self.values.pop(0))
        return float(self.last)


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
        "pressure_sample_source": "digital_pressure_gauge",
        "source": "digital_pressure_gauge",
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
        "primary_pressure_source": "digital_pressure_gauge",
        "pressure_source_used_for_decision": "digital_pressure_gauge" if not stale else "",
        "pressure_source_used_for_abort": "digital_pressure_gauge" if not stale else "",
        "pressure_source_used_for_ready": "digital_pressure_gauge" if not stale else "",
        "pressure_source_used_for_seal": "digital_pressure_gauge" if not stale else "",
        "source_selection_reason": "high_pressure_first_point_primary_digital_gauge",
        "digital_gauge_pressure_sample": {
            "pressure_hpa": float(pressure),
            "pressure_sample_source": "digital_pressure_gauge",
            "source": "digital_pressure_gauge",
            "request_sent_at": "2026-04-26T10:00:00+00:00",
            "response_received_at": "2026-04-26T10:00:00.010000+00:00",
            "request_sent_monotonic_s": 100.0,
            "response_received_monotonic_s": 100.01,
            "read_latency_s": 0.01,
            "sample_age_s": 0.0 if not stale else 3.0,
            "is_stale": stale,
            "pressure_sample_is_stale": stale,
            "parse_ok": True,
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

    with pytest.raises(WorkflowValidationError):
        orchestrator._prearm_a2_high_pressure_first_point_mode(point, pressure_points)

    assert any(
        event["event_name"] == "pressure_polling_prearmed" and event["event_type"] == "fail"
        for event in timing_events
    )


def test_co2_runner_prearms_high_pressure_polling_before_route_open() -> None:
    point = CalibrationPoint(index=1, temperature_c=20.0, co2_ppm=100.0, pressure_hpa=1100.0, route="co2")
    pressure_points = [
        CalibrationPoint(index=index + 1, temperature_c=20.0, co2_ppm=100.0, pressure_hpa=pressure, route="co2")
        for index, pressure in enumerate(A2_AUTHORIZED_PRESSURE_POINTS_HPA)
    ]
    order: list[str] = []
    route_traces: list[dict] = []

    class SlowPressure:
        def _current_pressure(self):
            raise AssertionError("route-open high-pressure mode must not block on slow current pressure read")

        def pressurize_and_hold(self, point, route):
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

    def prearm(point, pressure_refs):
        order.append("prearm")
        service._a2_high_pressure_first_point_mode_enabled = True
        return {"enabled": True, "baseline_pressure_hpa": 1009.0}

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

    service._prearm_a2_high_pressure_first_point_mode = prearm
    service._preclose_a2_high_pressure_first_point_vent = preclose
    service._request_a2_high_pressure_route_open_pressure_sample = request
    service._wait_co2_route_soak_before_seal = lambda point: True

    result = Co2RouteRunner(service, point, pressure_points).execute()

    assert result.success is False
    assert order.index("prearm") < order.index("preclose_vent") < order.index("route_open")
    assert order.index("co2_route_open_end") < order.index("first_pressure_request")
    assert order.index("first_pressure_request") < order.index("route_open_pressure_poll_request")


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
    assert written["route_open_pressure_surge_evidence"].endswith("route_open_pressure_surge_evidence.json")
    assert written["pressure_read_latency_diagnostics"].endswith("pressure_read_latency_diagnostics.json")
    assert written["pressure_read_latency_samples"].endswith("pressure_read_latency_samples.csv")
    assert written["high_pressure_first_point_evidence"].endswith("high_pressure_first_point_evidence.json")
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
    assert summary["high_pressure_first_point_evidence_artifact"].endswith(
        "high_pressure_first_point_evidence.json"
    )
    assert manifest["high_pressure_first_point_evidence_artifact"].endswith(
        "high_pressure_first_point_evidence.json"
    )
    assert "workflow_timing_artifacts" in manifest
    assert "Positive preseal pressurization summary" in report
    assert "流程时序摘要" in report
    assert "开阀瞬间升压诊断" in report
    assert "1100 高压首点正压封路诊断" in report
    assert "压力读取延迟与双压力源诊断" in report


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
    assert pressure["primary_pressure_source"] == "digital_pressure_gauge"
    assert pressure["pressure_source_cross_check_enabled"] is True
    assert pressure["pressure_source_disagreement_warn_hpa"] == 10.0
    assert pressure["pressure_sample_stale_threshold_s"] == 2.0
    assert pressure["pressure_read_latency_warn_s"] == 0.5
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
        "baseline_pressure_source": "digital_pressure_gauge",
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
    assert str(artifact_dir / "high_pressure_first_point_evidence.json") in manifest["artifacts"]["output_files"]
    assert "1100 高压首点正压封路诊断" in report


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
    service, host, controller, status = _positive_preseal_service([1155.0])
    controller.vent_on = False
    controller.vent_status = 0
    host._a2_high_pressure_first_point_mode_enabled = True
    host._a2_high_pressure_first_point_vent_preclosed = True
    host._a2_preseal_vent_close_arm_context = {
        "vent_close_arm_trigger": "ready_pressure",
        "vent_close_arm_pressure_hpa": 1155.0,
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
    assert result.error == "Positive preseal pressurization exceeded abort pressure"
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
    service, host, _controller, status = _positive_preseal_service([1009.0, 1155.0])
    point = CalibrationPoint(index=1, temperature_c=20.0, co2_ppm=100.0, pressure_hpa=1100.0, route="co2")

    result = service.pressurize_and_hold(point, route="co2")

    assert result.ok is False
    assert result.error == "Positive preseal pressurization exceeded abort pressure"
    assert any(row["action"] == "positive_preseal_abort" for row in status.rows)
    assert not any(row["action"] == "seal_route" for row in status.rows)
    assert any(event["event_name"] == "positive_preseal_abort" for event in host._recorded_timing)


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
        if row["action"] == "positive_preseal_pressure_check"
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
