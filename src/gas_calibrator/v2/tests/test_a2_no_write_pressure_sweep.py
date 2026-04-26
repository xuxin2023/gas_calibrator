from __future__ import annotations

import json
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


class _QueuedPressureGauge:
    def __init__(self, values: list[float]) -> None:
        self.values = list(values)
        self.last = self.values[-1] if self.values else 1010.0

    def read_pressure_hpa(self) -> float:
        if self.values:
            self.last = float(self.values.pop(0))
        return float(self.last)


class _TraceStatus:
    def __init__(self) -> None:
        self.rows: list[dict] = []

    def record_route_trace(self, **kwargs) -> None:
        self.rows.append(kwargs)


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
    assert "workflow_timing_artifacts" in manifest
    assert "Positive preseal pressurization summary" in report
    assert "流程时序摘要" in report


def test_a2_config_splits_temperature_chamber_and_analyzer_timeouts() -> None:
    config_path = (
        Path(__file__).resolve().parents[1]
        / "configs"
        / "validation"
        / "run001_a2_co2_only_7_pressure_no_write_real_machine.json"
    )
    raw = json.loads(config_path.read_text(encoding="utf-8"))
    temperature = raw["workflow"]["stability"]["temperature"]

    assert temperature["timeout_s"] == 3600
    assert temperature["require_chamber_settle_before_analyzer"] is True
    assert temperature["analyzer_chamber_temp_timeout_s"] == 1800
    assert temperature["analyzer_chamber_temp_span_c"] == 0.08
    assert temperature["analyzer_chamber_temp_window_s"] == 60


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
    timing_summary = json.loads((artifact_dir / "workflow_timing_summary.json").read_text(encoding="utf-8"))
    timing_events = [
        json.loads(line)
        for line in (artifact_dir / "workflow_timing_trace.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    manifest = json.loads((artifact_dir / "run_manifest.json").read_text(encoding="utf-8"))
    report = (artifact_dir / "human_readable_report.md").read_text(encoding="utf-8")
    samples = (artifact_dir / "preseal_atmosphere_hold_samples.csv").read_text(encoding="utf-8")

    assert summary["a2_final_decision"] == "FAIL"
    assert summary["preseal_atmosphere_hold_decision"] == "FAIL"
    assert summary["preseal_atmosphere_hold_pressure_limit_hpa"] == 1110.0
    assert summary["preseal_atmosphere_hold_max_measured_pressure_hpa"] == 1985.0
    assert summary["preseal_atmosphere_hold_pressure_limit_exceeded"] is True
    assert evidence["vent_status_2_is_not_continuous_atmosphere_evidence"] is True
    assert evidence["pressure_control_started"] is False
    assert evidence["sample_started"] is False
    assert guard["attempted_write_count"] == 0
    assert guard["identity_write_command_sent"] is False
    assert "pressure_limit_exceeded" in samples
    assert timing_summary["a2_final_decision"] == "FAIL"
    assert timing_summary["final_decision"] == "FAIL"
    assert timing_summary["preseal_pressure_max_hpa"] == 1985.0
    assert any(event["event_name"] == "preseal_pressure_check" for event in timing_events)
    assert any(event["event_name"] == "run_fail" for event in timing_events)
    assert all(event["no_write_guard_active"] is True for event in timing_events)
    assert manifest["workflow_timing_artifacts"]["trace"].endswith("workflow_timing_trace.jsonl")
    assert "流程时序摘要" in report


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
            "ts": "2026-04-26T04:11:00+00:00",
            "action": "positive_preseal_pressurization_start",
            "route": "co2",
            "point_index": 1,
            "actual": {
                "stage": "positive_preseal_pressurization",
                "target_pressure_hpa": 1100.0,
                "measured_atmospheric_pressure_hpa": 1246.758,
                "preseal_ready_pressure_hpa": 1110.0,
                "preseal_abort_pressure_hpa": 1150.0,
                "preseal_ready_timeout_s": 30.0,
                "preseal_pressure_poll_interval_s": 0.2,
            },
            "result": "ok",
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
            "ts": "2026-04-26T04:11:03+00:00",
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
    assert evidence["current_line_pressure_hpa"] == 1110.5
    assert evidence["pressure_samples_count"] == 2
    assert summary["positive_preseal_ready_reached"] is True
    assert summary["positive_preseal_seal_trigger_pressure_hpa"] == 1110.5
    assert summary["ambient_reference_pressure_hpa"] == 1009.0
    assert timing_summary["positive_preseal_ready_pressure_hpa"] == 1110.5
    assert timing_summary["positive_preseal_abort_pressure_hpa"] == 1150.0
    assert timing_summary["ambient_reference_pressure_hpa"] == 1009.0
    assert manifest["positive_preseal_pressurization_evidence_artifact"].endswith(
        "positive_preseal_pressurization_evidence.json"
    )
    assert "Positive preseal pressurization summary" in report


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

    def set_vent(vent_on: bool, reason: str = "", *, wait_after_command: bool = True) -> None:
        calls.append(
            {
                "vent_on": vent_on,
                "reason": reason,
                "wait_after_command": wait_after_command,
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

    assert route_traces[-1]["action"] == "preseal_atmosphere_flush_ready_handoff"
    assert route_traces[-1]["result"] == "ok"
    assert route_traces[-1]["actual"]["pressure_hpa"] == 1110.0
    assert not any(row.get("result") == "fail" for row in route_traces)
    assert [call["vent_on"] for call in vent_calls] == [True]
    assert any(event["event_name"] == "preseal_pressure_check" for event in timing_events)


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


def test_positive_preseal_vent_close_failure_hard_fails_before_pressure_polling() -> None:
    service, host, controller, status = _positive_preseal_service(
        [1009.0, 1110.5],
        cfg_overrides={
            "workflow.pressure.preseal_vent_close_verify_timeout_s": 0.05,
            "workflow.pressure.preseal_vent_close_verify_poll_s": 0.05,
        },
    )

    def stuck_vent(enabled: bool) -> bool:
        controller.vent_on = bool(enabled)
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
