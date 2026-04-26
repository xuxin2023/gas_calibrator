from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from gas_calibrator.v2.exceptions import WorkflowValidationError
from gas_calibrator.v2.core import orchestrator as orchestrator_module
from gas_calibrator.v2.core.no_write_guard import build_no_write_guard_from_raw_config
from gas_calibrator.v2.core.models import CalibrationPoint
from gas_calibrator.v2.core.orchestrator import WorkflowOrchestrator
from gas_calibrator.v2.core.run001_a2_no_write import (
    A2_AUTHORIZED_PRESSURE_POINTS_HPA,
    RUN001_NOT_EXECUTED,
    RUN001_PASS,
    build_run001_a2_evidence_payload,
    evaluate_run001_a2_readiness,
    write_run001_a2_artifacts,
)


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

    assert written["pressure_gate_evidence"].endswith("pressure_gate_evidence.json")
    assert summary["a2_final_decision"] == RUN001_NOT_EXECUTED
    assert summary["final_decision"] == RUN001_PASS
    assert guard["a2_final_decision"] == RUN001_NOT_EXECUTED
    assert summary["not_real_acceptance_evidence"] is True
    assert summary["v2_replaces_v1_claim"] is False


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
