from __future__ import annotations

import json
import time
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from gas_calibrator.data.points import CalibrationPoint
from gas_calibrator.devices.serial_base import ReplaySerial, SerialDevice
from gas_calibrator.logging_utils import RunLogger
from gas_calibrator.pace_audit import (
    PressureControllerComLockExists,
    acquire_pressure_controller_com_lock,
)
from gas_calibrator.tools import run_headless
from gas_calibrator.workflow.runner import CalibrationRunner


def _lock_cfg(tmp_path: Path) -> dict:
    disabled = {"enabled": False}
    return {
        "paths": {"output_dir": str(tmp_path)},
        "devices": {
            "pressure_controller": {"enabled": True, "port": "COM23", "baud": 9600},
            "pressure_gauge": disabled,
            "dewpoint_meter": disabled,
            "humidity_generator": disabled,
            "gas_analyzer": disabled,
            "gas_analyzers": [],
            "temperature_chamber": disabled,
            "thermometer": disabled,
            "relay": disabled,
            "relay_8": disabled,
        },
        "workflow": {
            "pressure": {
                "com_lock": {"enabled": True, "device": "pressure_controller"},
            }
        },
    }


def _tap_cfg(tmp_path: Path) -> dict:
    return {
        "paths": {"output_dir": str(tmp_path)},
        "devices": {"pressure_controller": {"port": "COM23"}},
        "workflow": {
            "pressure": {
                "raw_serial_tap": {
                    "enabled": True,
                    "device": "pressure_controller",
                    "fail_on_unexpected_analyzer_gate_write": True,
                }
            }
        },
    }


def _co2_point(index: int = 1, pressure: float = 1100.0) -> CalibrationPoint:
    return CalibrationPoint(
        index=index,
        temp_chamber_c=20.0,
        co2_ppm=1000.0,
        hgen_temp_c=None,
        hgen_rh_pct=None,
        target_pressure_hpa=pressure,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
        co2_group="A",
    )


def test_pace_com_lock_blocks_existing_lock(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _lock_cfg(tmp_path)
    lock_dir = tmp_path / "_runtime_locks"
    lock_dir.mkdir()
    lock_path = lock_dir / "pace_COM23.lock"
    lock_path.write_text('{"pid": 123, "port": "COM23"}\n', encoding="utf-8")
    logger = RunLogger(tmp_path, cfg=cfg)

    def _unexpected_pace(*_args, **_kwargs):
        raise AssertionError("PACE must not be constructed when COM lock exists")

    monkeypatch.setattr(run_headless, "Pace5000", _unexpected_pace)
    with pytest.raises(PressureControllerComLockExists):
        run_headless._build_devices(cfg, io_logger=logger)
    logger.close()


def test_pace_com_lock_blocks_existing_lock_final_decision(tmp_path: Path) -> None:
    cfg = _lock_cfg(tmp_path)
    config_path = tmp_path / "lock_config.json"
    config_path.write_text(json.dumps(cfg), encoding="utf-8")
    lock_dir = tmp_path / "_runtime_locks"
    lock_dir.mkdir()
    lock_path = lock_dir / "pace_COM23.lock"
    lock_path.write_text('{"pid": 123, "port": "COM23"}\n', encoding="utf-8")

    rc = run_headless.main(["--config", str(config_path), "--run-id", "lock-blocked"])

    assert rc == 2
    decision_path = tmp_path / "lock-blocked" / "precheck_final_decision.json"
    decision = json.loads(decision_path.read_text(encoding="utf-8"))
    assert decision["final_decision"] == "BLOCKED_PRESSURE_CONTROLLER_COM_LOCK_EXISTS"
    assert str(lock_path) == decision["lock_path"]


def test_pace_com_lock_records_pid_cmdline_port(tmp_path: Path) -> None:
    cfg = _lock_cfg(tmp_path)
    lock = acquire_pressure_controller_com_lock(cfg, run_id="unit-run", config_path="unit-config.json")
    assert lock is not None
    try:
        payload = json.loads(lock.path.read_text(encoding="utf-8"))
        assert payload["pid"] > 0
        assert "process_command_line" in payload
        assert payload["port"] == "COM23"
        assert payload["run_id"] == "unit-run"
        assert payload["config_path"] == "unit-config.json"
    finally:
        lock.close()


def test_pace_raw_serial_tap_records_write_and_read(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path, cfg=_tap_cfg(tmp_path))
    serial = SerialDevice(
        "COM23",
        9600,
        device_name="pace5000",
        io_logger=logger,
        serial_factory=lambda **kwargs: ReplaySerial(read_lines=["1013.25"], **kwargs),
    )
    serial.open()
    serial.write(":SENS:PRES?\n")
    assert serial.readline() == "1013.25"
    serial.close()
    logger.close()

    rows = [
        json.loads(line)
        for line in logger.raw_serial_tap_jsonl_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert any(row["direction"] == "WRITE" and row["decoded_command"] == ":SENS:PRES?" for row in rows)
    assert any(row["direction"] == "READLINE" and row["raw_text_decoded"].startswith("1013.25") for row in rows)


def test_pace_raw_serial_tap_includes_call_stack_and_thread(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path, cfg=_tap_cfg(tmp_path))
    logger.log_raw_serial_tap(
        port="COM23",
        device_label="pace5000",
        direction="WRITE",
        raw_bytes=b":SOUR:PRES:LEV:IMM:AMPL:VENT 1\n",
    )
    logger.close()
    row = json.loads(logger.raw_serial_tap_jsonl_path.read_text(encoding="utf-8").splitlines()[0])
    assert row["thread_name"]
    assert "test_pace_raw_serial_tap_includes_call_stack_and_thread" in row["python_call_stack_top10"]


def test_analyzer_gate_raw_tap_allows_vent1_and_queries(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path, cfg=_tap_cfg(tmp_path))
    logger.set_workflow_stage("co2_precondition_analyzer_gate")
    logger.begin_pace_raw_tap_analyzer_gate()
    logger.log_raw_serial_tap(
        port="COM23",
        device_label="pace5000",
        direction="WRITE",
        raw_bytes=b":SOUR:PRES:LEV:IMM:AMPL:VENT 1\n",
    )
    logger.log_raw_serial_tap(
        port="COM23",
        device_label="pace5000",
        direction="WRITE",
        raw_bytes=b":SENS:PRES?\n",
    )
    summary = logger.end_pace_raw_tap_analyzer_gate()
    logger.close()

    assert summary["analyzer_gate_raw_tap_write_count"] == 2
    assert summary["analyzer_gate_raw_tap_vent1_count"] == 1
    assert summary["analyzer_gate_raw_tap_query_count"] == 1
    assert summary["analyzer_gate_raw_tap_unexpected_write_count"] == 0


def test_analyzer_gate_raw_tap_summary_blocks_outp_or_vent0(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path, cfg=_tap_cfg(tmp_path))
    logger.set_workflow_stage("co2_precondition_analyzer_gate")
    logger.begin_pace_raw_tap_analyzer_gate()
    logger.log_raw_serial_tap(port="COM23", device_label="pace5000", direction="WRITE", raw_bytes=b":OUTP 1\n")
    logger.log_raw_serial_tap(
        port="COM23",
        device_label="pace5000",
        direction="WRITE",
        raw_bytes=b":SOUR:PRES:LEV:IMM:AMPL:VENT 0\n",
    )
    summary = logger.end_pace_raw_tap_analyzer_gate()
    logger.close()

    assert summary["analyzer_gate_raw_tap_state_changing_count"] == 2
    assert summary["analyzer_gate_raw_tap_unexpected_write_count"] == 2
    assert summary["analyzer_gate_raw_tap_first_unexpected_command"] == ":OUTP 1"
    assert logger.pace_raw_tap_fail_on_unexpected_analyzer_gate_write() is True


def test_analyzer_gate_raw_tap_blocks_outp_or_vent0(tmp_path: Path) -> None:
    cfg = _tap_cfg(tmp_path)
    logger = RunLogger(tmp_path, cfg=cfg)
    runner = CalibrationRunner(cfg, {"pace": _FakePace()}, logger, lambda *_args: None, lambda *_args: None)
    point = _co2_point()

    def _stable_with_hidden_write(*_args, **_kwargs):
        logger.log_raw_serial_tap(
            port="COM23",
            device_label="pace5000",
            direction="WRITE",
            raw_bytes=b":OUTP 1\n",
        )
        return True

    runner._wait_primary_sensor_stable = MagicMock(side_effect=_stable_with_hidden_write)

    assert runner._wait_co2_preseal_primary_sensor_gate(point) is False
    logger.close()

    assert runner._controlled_exit_final_decision == "FAIL_CLOSED_UNEXPECTED_PACE_COMMAND_DURING_ANALYZER_GATE"


class _FakeSerial:
    port = "COM23"


class _FakePace:
    def __init__(self) -> None:
        self.ser = _FakeSerial()
        self.calls: list[tuple] = []
        self._vent_hold_thread = None

    def vent(self, on: bool = True) -> None:
        self.calls.append(("vent", bool(on)))

    def enable_control_output(self) -> None:
        self.calls.append(("enable_control_output",))


class _FakeDewpoint:
    def __init__(self, values: list[float]) -> None:
        self.values = list(values)
        self.calls: list[tuple] = []

    def get_current_fast(self, *args, **kwargs) -> dict:
        self.calls.append(("get_current_fast", args, kwargs))
        if self.values:
            value = self.values.pop(0)
        else:
            value = -32.47
        return {"dewpoint_c": value, "temp_c": 38.0, "rh_pct": 0.8}

    def get_current(self, *args, **kwargs) -> dict:
        self.calls.append(("get_current", args, kwargs))
        return self.get_current_fast(*args, **kwargs)


def _runner_for_audit(tmp_path: Path) -> tuple[CalibrationRunner, RunLogger, _FakePace]:
    logger = RunLogger(tmp_path)
    pace = _FakePace()
    runner = CalibrationRunner(
        {
            "paths": {"output_dir": str(tmp_path)},
            "workflow": {
                "collect_only": True,
                "pressure": {"transition_trace_enabled": True},
                "stability": {
                    "dewpoint_preseal_freshness_max_age_s": 60.0,
                    "dewpoint_preseal_freshness_max_delta_c": 0.20,
                },
            },
        },
        {"pace": pace},
        logger,
        lambda *_args: None,
        lambda *_args: None,
    )
    return runner, logger, pace


def _prepare_analyzer_gate_dewpoint_runner(
    tmp_path: Path,
    *,
    dewpoints: list[float],
    gate_value: float = -32.47,
    raw_tap_enabled: bool = False,
) -> tuple[CalibrationRunner, RunLogger, _FakePace, _FakeDewpoint, CalibrationPoint]:
    monitor_cfg = {
        "analyzer_gate_dewpoint_monitor_interval_s": 0.1,
        "analyzer_gate_dewpoint_monitor_max_gap_s": 5.0,
        "dewpoint_preseal_freshness_max_age_s": 60.0,
        "dewpoint_preseal_freshness_max_delta_c": 0.20,
        "sensor": {
            "enabled": True,
            "co2_ratio_f_preseal_tol": 0.01,
            "co2_ratio_f_preseal_window_s": 60.0,
            "co2_ratio_f_preseal_timeout_s": 300.0,
            "co2_ratio_f_preseal_min_samples": 10,
            "co2_ratio_f_preseal_read_interval_s": 1.0,
        },
    }
    if raw_tap_enabled:
        cfg = _tap_cfg(tmp_path)
        cfg["workflow"]["collect_only"] = True
        cfg["workflow"]["pressure"]["transition_trace_enabled"] = True
        cfg["workflow"]["stability"] = monitor_cfg
        logger = RunLogger(tmp_path, cfg=cfg)
        pace = _FakePace()
        runner = CalibrationRunner(cfg, {"pace": pace}, logger, lambda *_args: None, lambda *_args: None)
    else:
        runner, logger, pace = _runner_for_audit(tmp_path)
        runner.cfg["workflow"]["stability"].update(monitor_cfg)
    dewpoint = _FakeDewpoint(dewpoints)
    runner.devices["dewpoint"] = dewpoint
    point = _co2_point()
    runner._set_point_runtime_fields(
        point,
        phase="co2",
        flush_gate_status="pass",
        dewpoint_gate_pass_ts=time.time(),
        dewpoint_gate_pass_value_c=gate_value,
    )
    return runner, logger, pace, dewpoint, point


def test_analyzer_gate_dewpoint_live_monitor_reads_periodically(tmp_path: Path) -> None:
    runner, logger, _pace, dewpoint, point = _prepare_analyzer_gate_dewpoint_runner(
        tmp_path,
        dewpoints=[-32.47, -32.46],
    )

    def _stable_with_live_dewpoint(*_args, **kwargs):
        callback = kwargs["loop_callback"]
        assert callback() is True
        time.sleep(0.11)
        assert callback() is True
        return True

    runner._wait_primary_sensor_stable = MagicMock(side_effect=_stable_with_live_dewpoint)

    assert runner._wait_co2_preseal_primary_sensor_gate(point) is True
    logger.close()

    state = runner._point_runtime_state(point, phase="co2") or {}
    assert len(dewpoint.calls) == 2
    assert state["analyzer_gate_dewpoint_monitor_enabled"] is True
    assert state["analyzer_gate_dewpoint_live_sample_count"] == 2
    assert state["analyzer_gate_dewpoint_delta_since_gate_c"] == pytest.approx(0.01, abs=0.001)


def test_analyzer_gate_dewpoint_live_monitor_records_timeline(tmp_path: Path) -> None:
    runner, logger, _pace, _dewpoint, point = _prepare_analyzer_gate_dewpoint_runner(
        tmp_path,
        dewpoints=[-32.50, -32.47, -32.44],
        gate_value=-32.50,
    )

    def _stable_with_live_dewpoint(*_args, **kwargs):
        callback = kwargs["loop_callback"]
        assert callback() is True
        time.sleep(0.11)
        assert callback() is True
        time.sleep(0.11)
        assert callback() is True
        return True

    runner._wait_primary_sensor_stable = MagicMock(side_effect=_stable_with_live_dewpoint)

    assert runner._wait_co2_preseal_primary_sensor_gate(point) is True
    logger.close()

    state = runner._point_runtime_state(point, phase="co2") or {}
    assert state["analyzer_gate_dewpoint_live_sample_count"] == 3
    assert state["analyzer_gate_dewpoint_live_max_gap_s"] is not None
    assert state["analyzer_gate_dewpoint_live_first_ts"]
    assert state["analyzer_gate_dewpoint_live_last_ts"]
    assert state["analyzer_gate_dewpoint_live_first_value_c"] == pytest.approx(-32.50)
    assert state["analyzer_gate_dewpoint_live_last_value_c"] == pytest.approx(-32.44)
    assert state["analyzer_gate_dewpoint_live_min_c"] == pytest.approx(-32.50)
    assert state["analyzer_gate_dewpoint_live_max_c"] == pytest.approx(-32.44)
    assert state["analyzer_gate_dewpoint_trend"] == "rising"


def test_analyzer_gate_dewpoint_rise_fails_closed(tmp_path: Path) -> None:
    runner, logger, _pace, _dewpoint, point = _prepare_analyzer_gate_dewpoint_runner(
        tmp_path,
        dewpoints=[-32.47, -26.09],
    )

    def _fail_on_live_dewpoint_rise(*_args, **kwargs):
        callback = kwargs["loop_callback"]
        assert callback() is True
        time.sleep(0.11)
        assert callback() is False
        return False

    runner._wait_primary_sensor_stable = MagicMock(side_effect=_fail_on_live_dewpoint_rise)

    assert runner._wait_co2_preseal_primary_sensor_gate(point) is False
    logger.close()

    state = runner._point_runtime_state(point, phase="co2") or {}
    assert runner._controlled_exit_final_decision == (
        "FAIL_CLOSED_DEWPOINT_FRESHNESS_EXPIRED_DURING_ANALYZER_GATE"
    )
    assert state["analyzer_gate_dewpoint_live_sample_count"] == 2
    assert state["analyzer_gate_dewpoint_delta_since_gate_c"] == pytest.approx(6.38, abs=0.01)
    assert state["analyzer_gate_dewpoint_first_rise_value_c"] == pytest.approx(-26.09)


def test_analyzer_gate_dewpoint_failure_does_not_send_vent0(tmp_path: Path) -> None:
    runner, logger, pace, _dewpoint, point = _prepare_analyzer_gate_dewpoint_runner(
        tmp_path,
        dewpoints=[-32.47, -26.09],
    )

    def _fail_on_live_dewpoint_rise(*_args, **kwargs):
        callback = kwargs["loop_callback"]
        assert callback() is True
        time.sleep(0.11)
        assert callback() is False
        return False

    runner._wait_primary_sensor_stable = MagicMock(side_effect=_fail_on_live_dewpoint_rise)

    assert runner._wait_co2_preseal_primary_sensor_gate(point) is False
    logger.close()

    assert ("vent", False) not in pace.calls


def test_analyzer_gate_dewpoint_failure_does_not_close_route_or_enter_sealed(tmp_path: Path) -> None:
    runner, logger, _pace = _runner_for_audit(tmp_path)
    _prepare_route_runner(runner)

    def _fail_gate(point, *_args, **_kwargs):
        runner._mark_co2_route_terminal_failure(
            final_decision="FAIL_CLOSED_DEWPOINT_FRESHNESS_EXPIRED_DURING_ANALYZER_GATE",
            reason="unit analyzer gate dewpoint failure",
            point=point,
            phase="co2",
        )
        return False

    runner._wait_co2_preseal_primary_sensor_gate = MagicMock(side_effect=_fail_gate)
    runner._pressurize_route_for_sealed_points = MagicMock()

    runner._run_co2_point(
        _co2_point(index=3),
        pressure_points=[_co2_point(3, 1100.0), _co2_point(4, 1000.0)],
    )
    logger.close()

    runner._pressurize_route_for_sealed_points.assert_not_called()
    runner._set_pressure_to_target.assert_not_called()
    runner._set_pressure_to_target_in_active_co2_sealed_sweep.assert_not_called()
    runner._cleanup_co2_route.assert_called_once_with(
        reason="FAIL_CLOSED_DEWPOINT_FRESHNESS_EXPIRED_DURING_ANALYZER_GATE"
    )


def test_analyzer_gate_dewpoint_failure_stops_remaining_pressure_points(tmp_path: Path) -> None:
    runner, logger, _pace = _runner_for_audit(tmp_path)
    _prepare_route_runner(runner)

    def _fail_gate(point, *_args, **_kwargs):
        runner._mark_co2_route_terminal_failure(
            final_decision="FAIL_CLOSED_DEWPOINT_FRESHNESS_EXPIRED_DURING_ANALYZER_GATE",
            reason="unit analyzer gate dewpoint failure",
            point=point,
            phase="co2",
        )
        return False

    runner._wait_co2_preseal_primary_sensor_gate = MagicMock(side_effect=_fail_gate)

    runner._run_co2_point(
        _co2_point(index=3),
        pressure_points=[_co2_point(3, 1100.0), _co2_point(4, 1000.0)],
    )
    logger.close()

    trace_files = list(tmp_path.rglob("pressure_transition_trace.csv"))
    assert trace_files
    trace_text = trace_files[0].read_text(encoding="utf-8")
    assert "skipped_due_to_upstream_dewpoint_freshness_failure" in trace_text
    assert "1000.0" in trace_text
    runner._set_pressure_to_target.assert_not_called()
    runner._sample_and_log.assert_not_called()


def test_analyzer_gate_dewpoint_failure_final_decision_not_overwritten(tmp_path: Path) -> None:
    runner, logger, _pace, _dewpoint, point = _prepare_analyzer_gate_dewpoint_runner(
        tmp_path,
        dewpoints=[-32.47, -26.09],
        raw_tap_enabled=True,
    )

    def _fail_on_live_dewpoint_rise(*_args, **kwargs):
        callback = kwargs["loop_callback"]
        assert callback() is True
        time.sleep(0.11)
        assert callback() is False
        logger.log_raw_serial_tap(
            port="COM23",
            device_label="pace5000",
            direction="WRITE",
            raw_bytes=b":OUTP 1\n",
        )
        return False

    runner._wait_primary_sensor_stable = MagicMock(side_effect=_fail_on_live_dewpoint_rise)

    assert runner._wait_co2_preseal_primary_sensor_gate(point) is False
    logger.close()

    assert runner._controlled_exit_final_decision == (
        "FAIL_CLOSED_DEWPOINT_FRESHNESS_EXPIRED_DURING_ANALYZER_GATE"
    )


def test_analyzer_gate_dewpoint_monitor_does_not_change_pace_commands(tmp_path: Path) -> None:
    runner, logger, pace, _dewpoint, point = _prepare_analyzer_gate_dewpoint_runner(
        tmp_path,
        dewpoints=[-32.47, -32.46],
    )

    def _stable_with_live_dewpoint(*_args, **kwargs):
        callback = kwargs["loop_callback"]
        assert callback() is True
        time.sleep(0.11)
        assert callback() is True
        return True

    runner._wait_primary_sensor_stable = MagicMock(side_effect=_stable_with_live_dewpoint)

    assert runner._wait_co2_preseal_primary_sensor_gate(point) is True
    logger.close()

    assert pace.calls == []


def test_raw_tap_still_records_analyzer_gate_vent1_only(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path, cfg=_tap_cfg(tmp_path))
    logger.set_workflow_stage("co2_precondition_analyzer_gate")
    logger.begin_pace_raw_tap_analyzer_gate()
    logger.log_raw_serial_tap(
        port="COM23",
        device_label="pace5000",
        direction="WRITE",
        raw_bytes=b":SOUR:PRES:LEV:IMM:AMPL:VENT 1\n",
    )
    summary = logger.end_pace_raw_tap_analyzer_gate()
    logger.close()

    assert summary["analyzer_gate_raw_tap_write_count"] == 1
    assert summary["analyzer_gate_raw_tap_vent1_count"] == 1
    assert summary["analyzer_gate_raw_tap_unexpected_write_count"] == 0


def test_second_pressure_controller_instance_detected(tmp_path: Path) -> None:
    runner, logger, _pace = _runner_for_audit(tmp_path)
    logger.log_io(port="COM23", device="pace5000", direction="OPEN", command="open")
    logger.log_io(port="COM23", device="pace5000", direction="OPEN", command="open")

    fields = runner._emit_pressure_controller_runtime_audit(stage="unit", point=_co2_point())
    logger.close()

    assert fields["final_decision"] == "FAIL_CLOSED_SECOND_PRESSURE_CONTROLLER_INSTANCE_DETECTED"
    assert runner._controlled_exit_final_decision == "FAIL_CLOSED_SECOND_PRESSURE_CONTROLLER_INSTANCE_DETECTED"


def _prepare_route_runner(runner: CalibrationRunner) -> None:
    runner._route_entry_context_for_co2_source = MagicMock(return_value={"point_tag": "co2-1000"})
    runner._apply_idle_route_isolation = MagicMock()
    runner._set_temperature_for_point = MagicMock(return_value=True)
    runner._capture_temperature_calibration_snapshot = MagicMock()
    runner._open_co2_route_for_conditioning = MagicMock()
    runner._wait_co2_route_soak_before_seal = MagicMock(return_value=True)
    runner._gas_route_dewpoint_gate_enabled = MagicMock(return_value=False)
    runner._wait_co2_preseal_primary_sensor_gate = MagicMock(return_value=True)
    runner._wait_cold_co2_quality_gate = MagicMock(return_value=True)
    runner._sample_open_route_point = MagicMock()
    runner._cleanup_co2_route = MagicMock()
    runner._set_pressure_to_target = MagicMock()
    runner._set_pressure_to_target_in_active_co2_sealed_sweep = MagicMock()
    runner._sample_and_log = MagicMock()
    runner._wait_after_pressure_stable_before_sampling = MagicMock(return_value=True)
    runner._build_co2_pressure_point = MagicMock(side_effect=lambda _lead, ref: ref)
    runner._request_sample_export_deferral = MagicMock(return_value=False)
    runner._clear_requested_sample_export_deferral = MagicMock()
    runner._flush_deferred_sample_exports = MagicMock()
    runner._flush_deferred_point_exports = MagicMock()


def test_dewpoint_freshness_fail_stops_entire_co2_route(tmp_path: Path) -> None:
    runner, logger, _pace = _runner_for_audit(tmp_path)
    _prepare_route_runner(runner)

    def _fail_first(point, *_args, **_kwargs):
        runner._mark_co2_route_terminal_failure(
            final_decision="FAIL_CLOSED_DEWPOINT_FRESHNESS_EXPIRED",
            reason="unit freshness failure",
            point=point,
        )
        return False

    runner._pressurize_route_for_sealed_points = MagicMock(side_effect=_fail_first)
    runner._run_co2_point(_co2_point(index=3), pressure_points=[_co2_point(3, 1100.0), _co2_point(4, 1000.0)])
    logger.close()

    assert runner._pressurize_route_for_sealed_points.call_count == 1
    runner._cleanup_co2_route.assert_called_once_with(reason="FAIL_CLOSED_DEWPOINT_FRESHNESS_EXPIRED")
    runner._set_pressure_to_target.assert_not_called()
    runner._sample_and_log.assert_not_called()


def test_dewpoint_freshness_fail_does_not_send_vent0(tmp_path: Path) -> None:
    runner, logger, pace = _runner_for_audit(tmp_path)
    point = _co2_point()
    runner._clear_preseal_pressure_control_ready_state = MagicMock()
    runner._start_pressure_transition_fast_signal_context = MagicMock()
    runner._emit_stage_event = MagicMock()
    runner._append_pressure_trace_row = MagicMock()
    runner._capture_preseal_dewpoint_snapshot = MagicMock()
    runner._set_point_runtime_fields(
        point,
        phase="co2",
        flush_gate_status="pass",
        dewpoint_gate_pass_ts=1000.0,
        dewpoint_gate_pass_value_c=-31.51,
    )
    runner._preseal_dewpoint_snapshot = {"sample_wall_ts": 1065.0, "dewpoint_c": -23.38}

    assert runner._pressurize_and_hold(point, route="co2") is False
    logger.close()

    assert ("vent", False) not in pace.calls
    assert runner._controlled_exit_final_decision == "FAIL_CLOSED_DEWPOINT_FRESHNESS_EXPIRED"


def test_dewpoint_freshness_fail_does_not_enter_sealed_control(tmp_path: Path) -> None:
    runner, logger, pace = _runner_for_audit(tmp_path)
    point = _co2_point()
    runner._clear_preseal_pressure_control_ready_state = MagicMock()
    runner._start_pressure_transition_fast_signal_context = MagicMock()
    runner._emit_stage_event = MagicMock()
    runner._append_pressure_trace_row = MagicMock()
    runner._capture_preseal_dewpoint_snapshot = MagicMock()
    runner._apply_valve_states = MagicMock()
    runner._set_point_runtime_fields(
        point,
        phase="co2",
        flush_gate_status="pass",
        dewpoint_gate_pass_ts=1000.0,
        dewpoint_gate_pass_value_c=-31.51,
    )
    runner._preseal_dewpoint_snapshot = {"sample_wall_ts": 1065.0, "dewpoint_c": -23.38}

    assert runner._pressurize_and_hold(point, route="co2") is False
    logger.close()

    runner._apply_valve_states.assert_not_called()
    assert ("enable_control_output",) not in pace.calls


def test_dewpoint_freshness_failure_final_decision_not_overwritten(tmp_path: Path) -> None:
    runner, logger, _pace = _runner_for_audit(tmp_path)
    point = _co2_point()
    runner._mark_co2_route_terminal_failure(
        final_decision="FAIL_CLOSED_DEWPOINT_FRESHNESS_EXPIRED",
        reason="unit freshness failure",
        point=point,
    )
    runner._cleanup_co2_route = MagicMock()
    runner._cleanup_co2_route(reason=runner._controlled_exit_final_decision)
    logger.close()

    assert runner._controlled_exit_final_decision == "FAIL_CLOSED_DEWPOINT_FRESHNESS_EXPIRED"
