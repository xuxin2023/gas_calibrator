from __future__ import annotations

import json

from gas_calibrator.v2.core.services import timing_monitor_service as timing_module
from gas_calibrator.v2.core.services.timing_monitor_service import (
    TIMING_EVENT_FIELDS,
    TimingMonitorService,
    ensure_workflow_timing_artifacts,
)


def _jsonl(path):
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def test_timing_trace_records_start_end_tick_fail_abort_and_no_write(monkeypatch, tmp_path) -> None:
    clock = {"now": 100.0}
    monkeypatch.setattr(timing_module.time, "monotonic", lambda: clock["now"])
    monitor = TimingMonitorService(tmp_path, run_id="run-1", no_write_guard_active=True)

    monitor.record_event("run_start", "start", stage="run")
    monitor.record_event("preflight_start", "start", stage="preflight")
    clock["now"] += 2.0
    monitor.record_event("preflight_end", "end", stage="preflight", decision="ok")
    monitor.record_event("preseal_soak_tick", "tick", stage="preseal_soak")
    monitor.record_event("run_fail", "fail", stage="run", error_code="boom")
    monitor.record_event("run_abort", "abort", stage="run", error_code="stop")
    summary = monitor.finalize_summary(final_decision="FAIL", a2_final_decision="FAIL")

    events = _jsonl(tmp_path / "workflow_timing_trace.jsonl")
    assert [event["event_name"] for event in events] == [
        "run_start",
        "preflight_start",
        "preflight_end",
        "preseal_soak_tick",
        "run_fail",
        "run_abort",
    ]
    assert set(TIMING_EVENT_FIELDS).issubset(events[0].keys())
    assert all(event["no_write_guard_active"] is True for event in events)
    assert summary["stage_durations"]["preflight"] == 2.0


def test_timing_summary_warns_on_missing_end_and_long_wait(monkeypatch, tmp_path) -> None:
    clock = {"now": 0.0}
    monkeypatch.setattr(timing_module.time, "monotonic", lambda: clock["now"])
    monitor = TimingMonitorService(tmp_path, run_id="run-2", no_write_guard_active=True)

    monitor.record_event("run_start", "start", stage="run")
    monitor.record_event("preflight_start", "start", stage="preflight")
    monitor.record_event("wait_gate_start", "start", stage="wait_gate", point_index=1, expected_max_s=10.0)
    clock["now"] += 9.0
    monitor.record_event("wait_gate_end", "end", stage="wait_gate", point_index=1, expected_max_s=10.0)
    summary = monitor.finalize_summary(final_decision="PASS", a2_final_decision="PASS")

    assert summary["missing_end_events"][0]["stage"] == "preflight"
    assert any(item["warning_code"] == "wait_gate_over_80pct_timeout" for item in summary["abnormal_waits"])
    assert summary["wait_gate_durations_by_point"]["1"] == 9.0


def test_timing_summary_warns_on_preseal_vent_tick_gap(monkeypatch, tmp_path) -> None:
    clock = {"now": 0.0}
    monkeypatch.setattr(timing_module.time, "monotonic", lambda: clock["now"])
    monitor = TimingMonitorService(tmp_path, run_id="run-3", no_write_guard_active=True)

    monitor.record_event("run_start", "start", stage="run")
    monitor.record_event("preseal_vent_hold_tick", "tick", stage="preseal_soak", point_index=1)
    clock["now"] += 5.0
    monitor.record_event("preseal_vent_hold_tick", "tick", stage="preseal_soak", point_index=1)
    summary = monitor.finalize_summary(
        final_decision="FAIL",
        a2_final_decision="FAIL",
        extra_context={"vent_hold_interval_s": 2.0},
    )

    assert summary["preseal_vent_tick_count"] == 2
    assert summary["repeated_sleep_warnings"][0]["warning_code"] == "preseal_vent_tick_gap_gt_2x_interval"


def test_timing_summary_splits_temperature_chamber_and_analyzer_waits(monkeypatch, tmp_path) -> None:
    clock = {"now": 0.0}
    monkeypatch.setattr(timing_module.time, "monotonic", lambda: clock["now"])
    monitor = TimingMonitorService(tmp_path, run_id="run-temp", no_write_guard_active=True)

    monitor.record_event("run_start", "start", stage="run")
    monitor.record_event(
        "temperature_chamber_settle_start",
        "start",
        stage="temperature_chamber_settle",
        expected_max_s=3600.0,
    )
    clock["now"] += 3500.0
    monitor.record_event(
        "temperature_chamber_settle_end",
        "end",
        stage="temperature_chamber_settle",
        expected_max_s=3600.0,
        decision="ok",
    )
    monitor.record_event(
        "analyzer_chamber_temperature_stability_start",
        "start",
        stage="analyzer_chamber_temperature_stability",
        expected_max_s=1800.0,
    )
    clock["now"] += 61.0
    monitor.record_event(
        "analyzer_chamber_temperature_stability_end",
        "end",
        stage="analyzer_chamber_temperature_stability",
        expected_max_s=1800.0,
        decision="pass",
    )

    summary = monitor.finalize_summary(final_decision="PASS", a2_final_decision="PASS")

    assert summary["temperature_chamber_settle_duration_s"] == 3500.0
    assert summary["analyzer_chamber_temperature_stability_duration_s"] == 61.0
    assert "temperature_stability" not in summary["stage_durations"]
    assert summary["abnormal_waits"] == []


def test_timing_summary_flags_temperature_stages_separately(monkeypatch, tmp_path) -> None:
    clock = {"now": 0.0}
    monkeypatch.setattr(timing_module.time, "monotonic", lambda: clock["now"])
    monitor = TimingMonitorService(tmp_path, run_id="run-temp-warn", no_write_guard_active=True)

    monitor.record_event(
        "temperature_chamber_settle_start",
        "start",
        stage="temperature_chamber_settle",
        expected_max_s=3600.0,
    )
    clock["now"] += 3601.0
    monitor.record_event(
        "temperature_chamber_settle_end",
        "end",
        stage="temperature_chamber_settle",
        expected_max_s=3600.0,
    )
    monitor.record_event(
        "analyzer_chamber_temperature_stability_start",
        "start",
        stage="analyzer_chamber_temperature_stability",
        expected_max_s=1800.0,
    )
    clock["now"] += 1801.0
    monitor.record_event(
        "analyzer_chamber_temperature_stability_timeout",
        "timeout",
        stage="analyzer_chamber_temperature_stability",
        expected_max_s=1800.0,
    )
    summary = monitor.finalize_summary(final_decision="FAIL", a2_final_decision="FAIL")

    stages = {item["stage"] for item in summary["abnormal_waits"]}
    assert "temperature_chamber_settle" in stages
    assert "analyzer_chamber_temperature_stability" in stages
    assert "temperature_stability" not in stages


def test_timing_summary_records_positive_preseal_vent_close(monkeypatch, tmp_path) -> None:
    clock = {"now": 0.0}
    monkeypatch.setattr(timing_module.time, "monotonic", lambda: clock["now"])
    monitor = TimingMonitorService(tmp_path, run_id="run-vent-close", no_write_guard_active=True)

    monitor.record_event(
        "positive_preseal_vent_close_start",
        "start",
        stage="positive_preseal_vent_close",
        expected_max_s=1.5,
    )
    clock["now"] += 0.4
    monitor.record_event(
        "positive_preseal_vent_close_end",
        "end",
        stage="positive_preseal_vent_close",
        expected_max_s=1.5,
    )
    summary = monitor.finalize_summary(
        final_decision="PASS",
        a2_final_decision="PASS",
        extra_context={"ambient_reference_pressure_hpa": 1009.0, "positive_preseal_pressure_max_hpa": 1110.5},
    )

    assert summary["positive_preseal_vent_close_duration_s"] == 0.4
    assert summary["positive_preseal_vent_close_status"] == "PASS"
    assert summary["ambient_reference_pressure_hpa"] == 1009.0
    assert summary["positive_preseal_pressure_max_hpa"] == 1110.5


def test_timing_summary_records_positive_preseal_pressure_rise_warnings(monkeypatch, tmp_path) -> None:
    clock = {"now": 0.0}
    monkeypatch.setattr(timing_module.time, "monotonic", lambda: clock["now"])
    monitor = TimingMonitorService(tmp_path, run_id="run-preseal-timing", no_write_guard_active=True)

    monitor.record_event("run_start", "start", stage="run")
    monitor.record_event("co2_route_open_start", "start", stage="co2_route_open", point_index=1)
    monitor.record_event("co2_route_open_end", "end", stage="co2_route_open", point_index=1, pressure_hpa=1000.0)
    monitor.record_event("preseal_vent_hold_tick", "tick", stage="preseal_soak", point_index=1)
    monitor.record_event(
        "preseal_atmosphere_flush_pressure_check",
        "tick",
        stage="preseal_atmosphere_flush_hold",
        point_index=1,
        pressure_hpa=1000.0,
    )
    clock["now"] += 9.0
    monitor.record_event("preseal_vent_hold_tick", "tick", stage="preseal_soak", point_index=1)
    clock["now"] += 3.0
    monitor.record_event(
        "preseal_atmosphere_flush_pressure_check",
        "tick",
        stage="preseal_atmosphere_flush_hold",
        point_index=1,
        pressure_hpa=1120.0,
    )
    monitor.record_event(
        "pressure_rise_detected",
        "info",
        stage="preseal_atmosphere_flush_hold",
        point_index=1,
        pressure_hpa=1120.0,
    )
    clock["now"] += 8.0
    monitor.record_event(
        "positive_preseal_pressurization_start",
        "start",
        stage="positive_preseal_pressurization",
        point_index=1,
        pressure_hpa=1100.0,
    )
    clock["now"] += 10.0
    monitor.record_event(
        "positive_preseal_ready",
        "info",
        stage="positive_preseal_pressurization",
        point_index=1,
        pressure_hpa=1110.0,
    )
    clock["now"] += 3.0
    monitor.record_event(
        "positive_preseal_seal_start",
        "info",
        stage="positive_preseal_pressurization",
        point_index=1,
        pressure_hpa=1110.0,
    )
    clock["now"] += 3.0
    monitor.record_event(
        "positive_preseal_seal_end",
        "end",
        stage="positive_preseal_pressurization",
        point_index=1,
        pressure_hpa=1130.0,
    )
    summary = monitor.finalize_summary(
        final_decision="PASS",
        a2_final_decision="PASS",
        extra_context={
            "expected_route_open_to_first_pressure_rise_max_s": 5.0,
            "expected_route_open_to_ready_max_s": 20.0,
            "expected_positive_preseal_to_ready_max_s": 5.0,
            "expected_ready_to_seal_command_max_s": 1.0,
            "expected_ready_to_seal_confirm_max_s": 2.0,
            "expected_max_pressure_increase_after_ready_hpa": 5.0,
            "expected_vent_hold_tick_interval_s": 2.0,
            "expected_vent_hold_pressure_rise_rate_max_hpa_per_s": 5.0,
            "timing_warning_only": True,
        },
    )

    warning_codes = {item["warning_code"] for item in summary["preseal_timing_warnings"]}
    assert summary["final_decision"] == "PASS"
    assert summary["a2_final_decision"] == "PASS"
    assert summary["route_open_to_first_pressure_rise_s"] == 12.0
    assert summary["route_open_to_ready_s"] == 30.0
    assert summary["positive_preseal_start_to_ready_s"] == 10.0
    assert summary["vent_hold_pressure_rise_rate_hpa_per_s"] == 10.0
    assert summary["positive_preseal_pressure_rise_rate_hpa_per_s"] == 1.0
    assert summary["ready_to_seal_command_s"] == 3.0
    assert summary["ready_to_seal_confirm_s"] == 6.0
    assert summary["pressure_increase_after_ready_before_seal_hpa"] == 20.0
    assert "route_open_to_first_pressure_rise_s_long" in warning_codes
    assert "route_open_to_ready_s_long" in warning_codes
    assert "positive_preseal_start_to_ready_s_long" in warning_codes
    assert "vent_hold_pressure_rise_rate_high" in warning_codes
    assert "ready_to_seal_command_s_long" in warning_codes
    assert "ready_to_seal_confirm_s_long" in warning_codes
    assert "pressure_increase_after_ready_before_seal_hpa_high" in warning_codes
    assert "vent_hold_tick_count_interval_mismatch" in warning_codes


def test_timing_monitor_does_not_send_device_commands(tmp_path) -> None:
    class Device:
        command_count = 0

        def set_pressure(self, _value):
            self.command_count += 1

        def read_pressure(self):
            self.command_count += 1
            return 1000.0

    device = Device()
    monitor = TimingMonitorService(tmp_path, run_id="run-4", no_write_guard_active=True)

    monitor.record_event("pressure_ready", "end", stage="pressure_setpoint", pressure_hpa=1000.0)
    monitor.finalize_summary(final_decision="PASS", a2_final_decision="PASS")

    assert device.command_count == 0


def test_ensure_timing_artifacts_synthesizes_route_trace_without_changing_decision(tmp_path) -> None:
    route_rows = [
        {
            "ts": "2026-04-26T04:11:52+00:00",
            "action": "set_co2_valves",
            "route": "co2",
            "point_index": 1,
            "target": {"pressure_hpa": 1100.0},
            "actual": {},
            "result": "ok",
        },
        {
            "ts": "2026-04-26T04:12:00+00:00",
            "action": "co2_preseal_atmosphere_hold_pressure_guard",
            "route": "co2",
            "point_index": 1,
            "target": {"pressure_hpa": 1100.0},
            "actual": {"pressure_hpa": 1985.0, "reason": "pressure_limit_exceeded"},
            "result": "fail",
        },
    ]
    (tmp_path / "route_trace.jsonl").write_text(
        "\n".join(json.dumps(row) for row in route_rows) + "\n",
        encoding="utf-8",
    )
    payload = {
        "run_id": "run-5",
        "final_decision": "FAIL",
        "a2_final_decision": "FAIL",
        "no_write": True,
        "vent_hold_interval_s": 2.0,
    }

    result = ensure_workflow_timing_artifacts(tmp_path, payload)
    summary = result["summary_payload"]
    events = _jsonl(tmp_path / "workflow_timing_trace.jsonl")

    assert summary["final_decision"] == "FAIL"
    assert summary["a2_final_decision"] == "FAIL"
    assert summary["preseal_pressure_max_hpa"] == 1985.0
    assert any(event["event_name"] == "run_fail" for event in events)
