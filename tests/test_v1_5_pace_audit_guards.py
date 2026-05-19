from __future__ import annotations

import json
import csv
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


def test_open_flow_until_preseal_raw_tap_allows_vent1_and_queries(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path, cfg=_tap_cfg(tmp_path))
    logger.set_workflow_stage("co2_open_flow")
    begin = logger.begin_pace_raw_tap_open_flow_until_preseal()
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
    summary = logger.end_pace_raw_tap_open_flow_until_preseal()
    logger.close()

    assert begin["open_flow_until_preseal_window_begin_ts"]
    assert summary["open_flow_until_preseal_window_end_ts"]
    assert summary["pace_write_count_total"] == 2
    assert summary["vent1_write_count"] == 1
    assert summary["readonly_query_count"] == 1
    assert summary["unexpected_state_changing_write_count"] == 0
    assert summary["first_unexpected_state_changing_write"] == ""


def test_open_flow_until_preseal_raw_tap_blocks_state_changes(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path, cfg=_tap_cfg(tmp_path))
    logger.set_workflow_stage("co2_open_flow")
    logger.begin_pace_raw_tap_open_flow_until_preseal()
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
        raw_bytes=b":OUTP 1\n",
    )
    summary = logger.end_pace_raw_tap_open_flow_until_preseal()
    logger.close()

    assert summary["pace_write_count_total"] == 2
    assert summary["vent1_write_count"] == 1
    assert summary["unexpected_state_changing_write_count"] == 1
    assert summary["first_unexpected_state_changing_write"] == ":OUTP 1"
    assert "test_open_flow_until_preseal_raw_tap_blocks_state_changes" in summary["first_unexpected_call_stack"]
    assert summary["first_unexpected_thread"]


def test_no_change_to_pace_or_valve_sequence(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path, cfg=_tap_cfg(tmp_path))
    logger.set_workflow_stage("co2_open_flow")
    logger.begin_pace_raw_tap_open_flow_until_preseal()
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
    summary = logger.end_pace_raw_tap_open_flow_until_preseal()
    logger.close()

    assert summary["vent1_write_count"] == 1
    assert summary["readonly_query_count"] == 1
    assert summary["unexpected_state_changing_write_count"] == 0
    assert ":OUTP" not in str(summary)
    assert ":SOUR:PRES:LEV:IMM:AMPL:VENT 0" not in str(summary)


def test_base_soak_boundary_audit_records_pace_commands(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path, cfg=_tap_cfg(tmp_path))
    begin_ts = "2000-01-01T00:00:00.000"
    end_ts = "2999-01-01T00:00:00.000"
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
    summary = logger.summarize_pace_raw_tap_window(begin_ts, end_ts)
    logger.close()

    assert summary["pace_write_count"] == 2
    assert summary["vent1_count"] == 1
    assert summary["readonly_query_count"] == 1
    assert summary["unexpected_state_changing_write_count"] == 0


def test_base_soak_to_dewpoint_gate_has_no_forbidden_pace_writes(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path, cfg=_tap_cfg(tmp_path))
    begin_ts = "2000-01-01T00:00:00.000"
    end_ts = "2999-01-01T00:00:00.000"
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
    summary = logger.summarize_pace_raw_tap_window(begin_ts, end_ts)
    logger.close()

    assert summary["vent1_count"] == 1
    assert summary["readonly_query_count"] == 1
    assert summary["vent0_count"] == 0
    assert summary["outp1_count"] == 0
    assert summary["isol_command_count"] == 0
    assert summary["setpoint_sour_pres_count"] == 0
    assert summary["mode_range_command_count"] == 0
    assert summary["unexpected_state_changing_write_count"] == 0


def test_base_soak_boundary_detects_unexpected_vent0(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path, cfg=_tap_cfg(tmp_path))
    begin_ts = "2000-01-01T00:00:00.000"
    end_ts = "2999-01-01T00:00:00.000"
    logger.log_raw_serial_tap(
        port="COM23",
        device_label="pace5000",
        direction="WRITE",
        raw_bytes=b":SOUR:PRES:LEV:IMM:AMPL:VENT 0\n",
    )
    summary = logger.summarize_pace_raw_tap_window(begin_ts, end_ts)
    logger.close()

    assert summary["vent0_count"] == 1
    assert summary["unexpected_state_changing_write_count"] == 1
    assert summary["first_unexpected_state_changing_write"] == ":SOUR:PRES:LEV:IMM:AMPL:VENT 0"


def test_open_flow_until_preseal_unexpected_write_blocks_preseal_vent0(tmp_path: Path) -> None:
    cfg = _tap_cfg(tmp_path)
    logger = RunLogger(tmp_path, cfg=cfg)
    pace = _FakePace()
    runner = CalibrationRunner(cfg, {"pace": pace}, logger, lambda *_args: None, lambda *_args: None)
    point = _co2_point()
    runner._clear_preseal_pressure_control_ready_state = MagicMock()
    runner._start_pressure_transition_fast_signal_context = MagicMock()
    runner._emit_stage_event = MagicMock()
    runner._capture_preseal_dewpoint_snapshot = MagicMock()
    runner._check_preseal_dewpoint_freshness = MagicMock(return_value=True)
    runner._begin_co2_open_flow_until_preseal_raw_tap_window(point, reason="unit open-flow")
    logger.log_raw_serial_tap(
        port="COM23",
        device_label="pace5000",
        direction="WRITE",
        raw_bytes=b":SOUR:PRES:LEV:IMM:AMPL:VENT 0\n",
    )

    assert runner._pressurize_and_hold(point, route="co2") is False
    logger.close()

    assert runner._controlled_exit_final_decision == (
        "FAIL_CLOSED_UNEXPECTED_PACE_COMMAND_DURING_OPEN_FLOW_TO_PRESEAL"
    )
    assert ("vent", False) not in pace.calls


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
        self.output_state = 0
        self.isolation_state = 1
        self.vent_status = 0
        self.query_responses = {
            "*IDN?": "Druck,K0472,FAKE,1.0",
            ":SYST:ERR?": "0,No error",
            ":UNIT:PRES?": "HPA",
            ":OUTP:MODE?": "ACT",
            ":OUTP:STAT?": "0",
            ":OUTP:ISOL:STAT?": "1",
            ":SOUR:PRES:LEV:IMM:AMPL:VENT?": "0",
            ":SOUR:PRES:INL?": "0.02",
            ":SOUR:PRES:INL:TIME?": "2",
            ":SOUR:PRES:SLEW?": "10",
            ":SOUR:PRES:SLEW:MODE?": "LIN",
            ":SOUR:PRES:SLEW:OVER:STAT?": "0",
            ":SOUR:PRES:LEV:IMM:AMPL:VENT:RATE?": "10",
            ":SOUR:PRES:LEV:IMM:AMPL:VENT:UNIT?": "HPA/S",
            ":SENS:PRES:FILT:LPAS:STAT?": "0",
            ":SENS:PRES:CORR:HEAD:STAT?": "0",
            ":SENS:PRES:CORR:OFFS:STAT?": "0",
            ":STAT:OPER:PRES:COND?": "0",
            ":STAT:OPER:PRES:EVEN?": "0",
        }

    def vent(self, on: bool = True) -> None:
        self.calls.append(("vent", bool(on)))

    def enable_control_output(self) -> None:
        self.calls.append(("enable_control_output",))

    def query(self, cmd: str) -> str:
        self.calls.append(("query", cmd))
        if cmd in self.query_responses:
            response = self.query_responses[cmd]
            if isinstance(response, Exception):
                raise response
            return str(response)
        return "0"

    def get_output_state(self) -> int:
        return int(self.output_state)

    def get_isolation_state(self) -> int:
        return int(self.isolation_state)

    def get_vent_status(self) -> int:
        return int(self.vent_status)

    def read_pressure(self) -> float:
        return 1013.25

    def set_units_hpa(self) -> None:
        self.calls.append(("set_units_hpa",))

    def set_output_mode_active(self) -> None:
        self.calls.append(("set_output_mode_active",))

    def set_in_limits(self, pct: float, seconds: float) -> None:
        self.calls.append(("set_in_limits", pct, seconds))


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
            "devices": {
                "pressure_controller": {
                    "in_limits_pct": 0.02,
                    "in_limits_time_s": 2.0,
                }
            },
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


def _pace_trace_rows(tmp_path: Path) -> list[dict[str, str]]:
    trace_path = tmp_path / "pressure_transition_trace.csv"
    with trace_path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def test_pace_baseline_snapshot_before_and_after_config(tmp_path: Path) -> None:
    runner, logger, pace = _runner_for_audit(tmp_path)

    before = runner._record_pace_manual_baseline_snapshot("pace_baseline_before_config")
    runner._configure_devices()
    after = runner._record_pace_manual_baseline_snapshot("pace_baseline_after_config")
    logger.close()

    assert before["pace_output_state"] == 0
    assert before["pace_isolation_state"] == 1
    assert before["pace_vent_status"] == 0
    assert after["manual_profile_ready"] is True
    assert ("query", ":OUTP:STAT?") in pace.calls
    rows = _pace_trace_rows(logger.run_dir)
    before_row = next(row for row in rows if row["trace_stage"] == "pace_baseline_before_config")
    after_row = next(row for row in rows if row["trace_stage"] == "pace_baseline_after_config")
    assert json.loads(before_row["pace_baseline_before_config"])["profile_version"]
    assert json.loads(after_row["pace_baseline_after_config"])["vent_status_semantics"]
    assert before_row["pace_vent_status_semantics"] == "manual_0_ok_1_in_progress_2_completed_3_unrecognized_watchlist"


def test_pace_baseline_skips_unsupported_optional_vent_rate_queries(tmp_path: Path) -> None:
    runner, logger, pace = _runner_for_audit(tmp_path)

    snapshot = runner._record_pace_manual_baseline_snapshot("pace_baseline_before_config")
    logger.close()

    assert ("query", ":SOUR:PRES:LEV:IMM:AMPL:VENT:RATE?") not in pace.calls
    assert ("query", ":SOUR:PRES:LEV:IMM:AMPL:VENT:UNIT?") not in pace.calls
    queries = snapshot["queries"]
    assert queries["vent_rate"]["unsupported"] is True
    assert queries["vent_rate"]["skipped"] is True
    assert queries["vent_rate_unit"]["unsupported"] is True
    assert queries["vent_rate_unit"]["skipped"] is True
    assert "vent_rate" not in snapshot["errors"]
    assert "vent_rate_unit" not in snapshot["errors"]


def test_pace_phase_profile_open_flow_requires_vent1_outp0_isol1(tmp_path: Path) -> None:
    runner, logger, _pace = _runner_for_audit(tmp_path)
    result = runner._evaluate_pace_phase_profile(
        "open_flow",
        pace_snapshot={"pace_output_state": 0, "pace_isolation_state": 1, "pace_vent_status": 1},
        route_valves=[6, 7, 8, 11],
        vent1_fresh=True,
    )
    logger.close()

    assert result["passed"] is True
    assert result["actual"]["decision"] == "pass"


def test_pace_phase_profile_open_flow_blocks_outp1_or_vent0(tmp_path: Path) -> None:
    runner, logger, _pace = _runner_for_audit(tmp_path)
    result = runner._evaluate_pace_phase_profile(
        "open_flow",
        pace_snapshot={"pace_output_state": 1, "pace_isolation_state": 1, "pace_vent_status": 0},
        route_valves=[6, 7, 8, 11],
        vent1_fresh=True,
        forbidden_command_count=1,
    )
    logger.close()

    assert result["passed"] is False
    assert any("OUTP_expected_0_actual_1" in failure for failure in result["failures"])
    assert "forbidden_command_count=1" in result["failures"]


def test_pace_phase_profile_base_soak_dewpoint_gate_allows_only_vent1_and_query(tmp_path: Path) -> None:
    runner, logger, _pace = _runner_for_audit(tmp_path)
    allowed = runner._evaluate_pace_phase_profile(
        "base_soak_dewpoint_analyzer",
        pace_snapshot={"pace_output_state": 0, "pace_isolation_state": 1, "pace_vent_status": 1},
        forbidden_command_count=0,
    )
    blocked = runner._evaluate_pace_phase_profile(
        "base_soak_dewpoint_analyzer",
        pace_snapshot={"pace_output_state": 0, "pace_isolation_state": 1, "pace_vent_status": 1},
        forbidden_command_count=1,
    )
    logger.close()

    assert allowed["passed"] is True
    assert blocked["passed"] is False
    assert "forbidden_command_count=1" in blocked["failures"]


def test_pace_phase_profile_preseal_allows_vent3_watchlist_before_route_close(tmp_path: Path) -> None:
    runner, logger, _pace = _runner_for_audit(tmp_path)
    result = runner._evaluate_pace_phase_profile(
        "preseal_pressure_build",
        pace_snapshot={"pace_output_state": 0, "pace_isolation_state": 1, "pace_vent_status": 3},
        route_valves=[6, 7, 8, 11],
        new_vent1_count=0,
    )
    logger.close()

    assert result["passed"] is True
    assert "vent3_diagnostic_only_before_route_close" in result["warnings"]


def test_pace_phase_profile_control_ready_blocks_vent3(tmp_path: Path) -> None:
    runner, logger, _pace = _runner_for_audit(tmp_path)
    result = runner._evaluate_pace_phase_profile(
        "sealed_control_ready",
        pace_snapshot={"pace_output_state": 0, "pace_isolation_state": 1, "pace_vent_status": 3},
        route_valves=[],
    )
    logger.close()

    assert result["passed"] is False
    assert "vent_window_latched" in result["failures"]


def test_pace_phase_profile_control_ready_blocks_vent2_conservatively(tmp_path: Path) -> None:
    runner, logger, _pace = _runner_for_audit(tmp_path)
    result = runner._evaluate_pace_phase_profile(
        "sealed_control_ready",
        pace_snapshot={"pace_output_state": 0, "pace_isolation_state": 1, "pace_vent_status": 2},
        route_valves=[],
    )
    logger.close()

    assert result["passed"] is False
    assert "vent_status_2_not_idle_or_completed_not_accepted" in result["failures"]


def test_pace_phase_profile_control_ready_allows_vent0(tmp_path: Path) -> None:
    runner, logger, _pace = _runner_for_audit(tmp_path)
    result = runner._evaluate_pace_phase_profile(
        "sealed_control_ready",
        pace_snapshot={"pace_output_state": 0, "pace_isolation_state": 1, "pace_vent_status": 0},
        route_valves=[],
    )
    logger.close()

    assert result["passed"] is True


def test_pace_error_queue_recorded_without_crash(tmp_path: Path) -> None:
    runner, logger, pace = _runner_for_audit(tmp_path)
    pace.query_responses[":SYST:ERR?"] = RuntimeError("serial timeout")

    snapshot = runner._record_pace_manual_baseline_snapshot("pace_baseline_before_config")
    logger.close()

    assert snapshot["errors"]["system_error"] == "serial timeout"
    row = next(row for row in _pace_trace_rows(logger.run_dir) if row["trace_stage"] == "pace_baseline_before_config")
    evidence = json.loads(row["pace_status_evidence"])
    assert evidence["queries"]["system_error"]["ok"] is False


def test_no_cal_zero_commands_in_startup_config(tmp_path: Path) -> None:
    runner, logger, _pace = _runner_for_audit(tmp_path)

    runner._configure_devices()
    logger.close()

    audit_path = logger.run_dir / "pace_startup_config_audit.csv"
    with audit_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    commands = [row["command"].upper() for row in rows]
    assert ":UNIT:PRES HPA" in commands
    assert ":OUTP:MODE ACT" in commands
    assert not any("CAL" in command or "ZERO" in command for command in commands)
    assert not any(row["is_forbidden"] == "True" for row in rows)


def _prepare_analyzer_gate_dewpoint_runner(
    tmp_path: Path,
    *,
    dewpoints: list[float],
    gate_value: float = -32.47,
    tail_reference: float | None = None,
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
        dewpoint_gate_tail_reference_c=gate_value if tail_reference is None else tail_reference,
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


def test_analyzer_gate_small_rebound_below_dry_enough_warns_not_fail(tmp_path: Path) -> None:
    runner, logger, _pace, _dewpoint, point = _prepare_analyzer_gate_dewpoint_runner(
        tmp_path,
        dewpoints=[-37.07, -36.78],
        gate_value=-37.07,
        tail_reference=-37.025,
    )
    runner.cfg["workflow"]["stability"].update(
        {
            "gas_route_dewpoint_gate_require_dry_enough": True,
            "gas_route_dewpoint_gate_dry_enough_c": -30.0,
            "gas_route_analyzer_gate_rebound_warning_only": True,
            "gas_route_analyzer_gate_hard_rebound_c": 2.0,
            "gas_route_analyzer_gate_fail_if_above_dry_enough": True,
        }
    )

    def _stable_with_small_live_rebound(*_args, **kwargs):
        callback = kwargs["loop_callback"]
        assert callback() is True
        time.sleep(0.11)
        assert callback() is True
        return True

    runner._wait_primary_sensor_stable = MagicMock(side_effect=_stable_with_small_live_rebound)

    assert runner._wait_co2_preseal_primary_sensor_gate(point) is True
    logger.close()

    state = runner._point_runtime_state(point, phase="co2") or {}
    assert getattr(runner, "_controlled_exit_final_decision", "") == ""
    assert state["analyzer_gate_dewpoint_delta_vs_tail_reference_c"] == pytest.approx(0.245, abs=0.001)
    assert state["analyzer_gate_dewpoint_rebound_warning"] is True
    assert state["analyzer_gate_dewpoint_rebound_warning_only"] is True
    assert state["analyzer_gate_dewpoint_hard_rebound_c"] == pytest.approx(2.0)
    assert state["analyzer_gate_dewpoint_dry_enough_c"] == pytest.approx(-30.0)
    assert state["analyzer_gate_dewpoint_dry_enough_passed"] is True


def test_analyzer_gate_fails_when_dewpoint_above_dry_enough(tmp_path: Path) -> None:
    runner, logger, _pace, _dewpoint, point = _prepare_analyzer_gate_dewpoint_runner(
        tmp_path,
        dewpoints=[-29.5],
        gate_value=-31.0,
        tail_reference=-31.0,
    )
    runner.cfg["workflow"]["stability"].update(
        {
            "gas_route_dewpoint_gate_require_dry_enough": True,
            "gas_route_dewpoint_gate_dry_enough_c": -30.0,
            "gas_route_analyzer_gate_hard_rebound_c": 2.0,
            "gas_route_analyzer_gate_fail_if_above_dry_enough": True,
        }
    )

    def _fail_on_wet_live_dewpoint(*_args, **kwargs):
        assert kwargs["loop_callback"]() is False
        return False

    runner._wait_primary_sensor_stable = MagicMock(side_effect=_fail_on_wet_live_dewpoint)

    assert runner._wait_co2_preseal_primary_sensor_gate(point) is False
    logger.close()

    state = runner._point_runtime_state(point, phase="co2") or {}
    assert runner._controlled_exit_final_decision == "FAIL_CLOSED_DEWPOINT_NOT_DRY_ENOUGH_DURING_ANALYZER_GATE"
    assert state["analyzer_gate_dewpoint_dry_enough_passed"] is False
    assert state["analyzer_gate_dewpoint_fail_reason"].startswith("dewpoint_not_dry_enough_c=")


def test_analyzer_gate_dry_enough_small_overshoot_warns_not_fail(tmp_path: Path) -> None:
    runner, logger, _pace, _dewpoint, point = _prepare_analyzer_gate_dewpoint_runner(
        tmp_path,
        dewpoints=[-29.95],
        gate_value=-30.50,
        tail_reference=-30.50,
    )
    runner.cfg["workflow"]["stability"].update(
        {
            "gas_route_dewpoint_gate_require_dry_enough": True,
            "gas_route_dewpoint_gate_dry_enough_c": -30.0,
            "gas_route_analyzer_gate_fail_if_above_dry_enough": True,
            "analyzer_gate_dry_enough_tolerance_c": 0.3,
            "analyzer_gate_dry_enough_grace_s": 30.0,
        }
    )

    def _stable_with_small_overshoot(*_args, **kwargs):
        assert kwargs["loop_callback"]() is True
        return True

    runner._wait_primary_sensor_stable = MagicMock(side_effect=_stable_with_small_overshoot)

    assert runner._wait_co2_preseal_primary_sensor_gate(point) is True
    logger.close()

    state = runner._point_runtime_state(point, phase="co2") or {}
    assert getattr(runner, "_controlled_exit_final_decision", "") == ""
    assert state["analyzer_gate_dewpoint_dry_enough_passed"] is False
    assert state["analyzer_gate_dewpoint_dry_enough_warning_zone"] is True
    assert state["analyzer_gate_dewpoint_dry_enough_gate_effect"] == "warning_only"
    assert state["analyzer_gate_dewpoint_dry_enough_terminal"] is False


def test_analyzer_gate_dry_enough_sustained_warning_zone_can_fail(tmp_path: Path) -> None:
    runner, logger, _pace, _dewpoint, point = _prepare_analyzer_gate_dewpoint_runner(
        tmp_path,
        dewpoints=[-29.95, -29.95],
        gate_value=-30.50,
        tail_reference=-30.50,
    )
    runner.cfg["workflow"]["stability"].update(
        {
            "gas_route_dewpoint_gate_require_dry_enough": True,
            "gas_route_dewpoint_gate_dry_enough_c": -30.0,
            "gas_route_analyzer_gate_fail_if_above_dry_enough": True,
            "analyzer_gate_dry_enough_tolerance_c": 0.3,
            "analyzer_gate_dry_enough_grace_s": 0.05,
        }
    )

    def _fail_after_sustained_overshoot(*_args, **kwargs):
        callback = kwargs["loop_callback"]
        assert callback() is True
        time.sleep(0.11)
        assert callback() is False
        return False

    runner._wait_primary_sensor_stable = MagicMock(side_effect=_fail_after_sustained_overshoot)

    assert runner._wait_co2_preseal_primary_sensor_gate(point) is False
    logger.close()

    state = runner._point_runtime_state(point, phase="co2") or {}
    assert runner._controlled_exit_final_decision == "FAIL_CLOSED_DEWPOINT_NOT_DRY_ENOUGH_DURING_ANALYZER_GATE"
    assert state["analyzer_gate_dewpoint_dry_enough_warning_zone"] is True
    assert state["analyzer_gate_dewpoint_dry_enough_terminal"] is True
    assert state["analyzer_gate_dewpoint_dry_enough_violation_s"] > 0.05


def test_analyzer_gate_dry_enough_hard_margin_fails(tmp_path: Path) -> None:
    runner, logger, _pace, _dewpoint, point = _prepare_analyzer_gate_dewpoint_runner(
        tmp_path,
        dewpoints=[-29.60],
        gate_value=-30.50,
        tail_reference=-30.50,
    )
    runner.cfg["workflow"]["stability"].update(
        {
            "gas_route_dewpoint_gate_require_dry_enough": True,
            "gas_route_dewpoint_gate_dry_enough_c": -30.0,
            "gas_route_analyzer_gate_fail_if_above_dry_enough": True,
            "analyzer_gate_dry_enough_tolerance_c": 0.3,
            "analyzer_gate_dry_enough_grace_s": 30.0,
        }
    )

    def _fail_on_hard_margin(*_args, **kwargs):
        assert kwargs["loop_callback"]() is False
        return False

    runner._wait_primary_sensor_stable = MagicMock(side_effect=_fail_on_hard_margin)

    assert runner._wait_co2_preseal_primary_sensor_gate(point) is False
    logger.close()

    state = runner._point_runtime_state(point, phase="co2") or {}
    assert runner._controlled_exit_final_decision == "FAIL_CLOSED_DEWPOINT_NOT_DRY_ENOUGH_DURING_ANALYZER_GATE"
    assert state["analyzer_gate_dewpoint_hard_fail_threshold_c"] == pytest.approx(-29.7)
    assert state["analyzer_gate_dewpoint_dry_enough_terminal"] is True


def test_analyzer_gate_hard_rebound_below_dry_enough_warns_not_fail(tmp_path: Path) -> None:
    runner, logger, _pace, _dewpoint, point = _prepare_analyzer_gate_dewpoint_runner(
        tmp_path,
        dewpoints=[-32.94],
        gate_value=-34.84,
        tail_reference=-34.95,
    )
    runner.cfg["workflow"]["stability"].update(
        {
            "gas_route_dewpoint_gate_require_dry_enough": True,
            "gas_route_dewpoint_gate_dry_enough_c": -30.0,
            "gas_route_analyzer_gate_rebound_warning_only": True,
            "gas_route_analyzer_gate_hard_rebound_c": 2.0,
        }
    )

    def _stable_with_hard_live_rebound(*_args, **kwargs):
        assert kwargs["loop_callback"]() is True
        return True

    runner._wait_primary_sensor_stable = MagicMock(side_effect=_stable_with_hard_live_rebound)

    assert runner._wait_co2_preseal_primary_sensor_gate(point) is True
    logger.close()

    state = runner._point_runtime_state(point, phase="co2") or {}
    assert getattr(runner, "_controlled_exit_final_decision", "") == ""
    assert state["analyzer_gate_dewpoint_delta_vs_tail_reference_c"] == pytest.approx(2.01, abs=0.001)
    assert state["analyzer_gate_dewpoint_rebound_warning"] is True
    assert state["analyzer_gate_dewpoint_hard_rebound_warning"] is True
    assert state["analyzer_gate_dewpoint_hard_rebound_terminal"] is False
    assert state["analyzer_gate_dewpoint_dry_enough_passed"] is True
    assert state["analyzer_gate_dewpoint_gate_effect"] == "warning_only"
    assert state["analyzer_gate_dewpoint_fail_reason"] == ""


def test_analyzer_gate_dewpoint_rise_fails_closed(tmp_path: Path) -> None:
    runner, logger, _pace, _dewpoint, point = _prepare_analyzer_gate_dewpoint_runner(
        tmp_path,
        dewpoints=[-32.47, -26.09],
    )
    runner.cfg["workflow"]["stability"].update(
        {
            "gas_route_dewpoint_gate_require_dry_enough": True,
            "gas_route_dewpoint_gate_dry_enough_c": -30.0,
        }
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
    assert runner._controlled_exit_final_decision == "FAIL_CLOSED_DEWPOINT_NOT_DRY_ENOUGH_DURING_ANALYZER_GATE"
    assert state["analyzer_gate_dewpoint_live_sample_count"] == 2
    assert state["analyzer_gate_dewpoint_delta_since_gate_c"] == pytest.approx(6.38, abs=0.01)
    assert state["analyzer_gate_dewpoint_first_rise_value_c"] == pytest.approx(-26.09)


def test_analyzer_gate_dewpoint_failure_does_not_send_vent0(tmp_path: Path) -> None:
    runner, logger, pace, _dewpoint, point = _prepare_analyzer_gate_dewpoint_runner(
        tmp_path,
        dewpoints=[-32.47, -26.09],
    )
    runner.cfg["workflow"]["stability"].update(
        {
            "gas_route_dewpoint_gate_require_dry_enough": True,
            "gas_route_dewpoint_gate_dry_enough_c": -30.0,
        }
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
    runner.cfg["workflow"]["stability"].update(
        {
            "gas_route_dewpoint_gate_require_dry_enough": True,
            "gas_route_dewpoint_gate_dry_enough_c": -30.0,
        }
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

    assert runner._controlled_exit_final_decision == "FAIL_CLOSED_DEWPOINT_NOT_DRY_ENOUGH_DURING_ANALYZER_GATE"


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


def test_analyzer_rebound_uses_tail_reference_not_pass_snapshot(tmp_path: Path) -> None:
    runner, logger, _pace, _dewpoint, point = _prepare_analyzer_gate_dewpoint_runner(
        tmp_path,
        dewpoints=[-25.00],
        gate_value=-24.89,
        tail_reference=-30.00,
    )
    runner.cfg["workflow"]["stability"].update(
        {
            "gas_route_dewpoint_gate_require_dry_enough": True,
            "gas_route_dewpoint_gate_dry_enough_c": -30.0,
        }
    )

    def _fail_against_tail_reference(*_args, **kwargs):
        assert kwargs["loop_callback"]() is False
        return False

    runner._wait_primary_sensor_stable = MagicMock(side_effect=_fail_against_tail_reference)

    assert runner._wait_co2_preseal_primary_sensor_gate(point) is False
    logger.close()

    state = runner._point_runtime_state(point, phase="co2") or {}
    assert state["analyzer_gate_dewpoint_delta_since_gate_c"] == pytest.approx(5.0, abs=0.001)
    assert runner._controlled_exit_final_decision == "FAIL_CLOSED_DEWPOINT_NOT_DRY_ENOUGH_DURING_ANALYZER_GATE"


def test_analyzer_gate_small_rebound_does_not_change_pace_commands(tmp_path: Path) -> None:
    runner, logger, pace, _dewpoint, point = _prepare_analyzer_gate_dewpoint_runner(
        tmp_path,
        dewpoints=[-37.07, -36.78],
        gate_value=-37.07,
        tail_reference=-37.025,
    )
    runner.cfg["workflow"]["stability"].update(
        {
            "gas_route_dewpoint_gate_require_dry_enough": True,
            "gas_route_dewpoint_gate_dry_enough_c": -30.0,
            "gas_route_analyzer_gate_rebound_warning_only": True,
            "gas_route_analyzer_gate_hard_rebound_c": 2.0,
        }
    )

    def _stable_with_small_live_rebound(*_args, **kwargs):
        callback = kwargs["loop_callback"]
        assert callback() is True
        time.sleep(0.11)
        assert callback() is True
        return True

    runner._wait_primary_sensor_stable = MagicMock(side_effect=_stable_with_small_live_rebound)

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
    runner._capture_preseal_dewpoint_snapshot = MagicMock(
        side_effect=lambda *_args, **_kwargs: setattr(
            runner,
            "_preseal_dewpoint_snapshot",
            {"sample_wall_ts": 1065.0, "dewpoint_c": -23.38},
        )
    )
    runner._set_point_runtime_fields(
        point,
        phase="co2",
        flush_gate_status="pass",
        dewpoint_gate_pass_ts=1000.0,
        dewpoint_gate_pass_value_c=-31.51,
        dewpoint_gate_tail_reference_c=-31.51,
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
    runner._capture_preseal_dewpoint_snapshot = MagicMock(
        side_effect=lambda *_args, **_kwargs: setattr(
            runner,
            "_preseal_dewpoint_snapshot",
            {"sample_wall_ts": 1065.0, "dewpoint_c": -23.38},
        )
    )
    runner._apply_valve_states = MagicMock()
    runner._set_point_runtime_fields(
        point,
        phase="co2",
        flush_gate_status="pass",
        dewpoint_gate_pass_ts=1000.0,
        dewpoint_gate_pass_value_c=-31.51,
        dewpoint_gate_tail_reference_c=-31.51,
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
