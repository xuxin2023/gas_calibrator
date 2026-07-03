from __future__ import annotations

import csv
import json
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List

from gas_calibrator.data.points import CalibrationPoint
from gas_calibrator.logging_utils import RunLogger
from gas_calibrator.tools.run_v1_5_dewpoint_gate_extended_hold_after_gate import (
    DEWPOINT_GATE_EXTENDED_HOLD_NO_REBOUND,
    DEWPOINT_GATE_EXTENDED_HOLD_REBOUND_OBSERVED,
    DewpointGateExtendedHoldDiagnosticRunner,
    write_analyzer_gate_dewpoint_vs_analyzer_events,
)
from gas_calibrator.workflow.runner import CalibrationRunner


class _FakeDewpoint:
    def __init__(self, values: Iterable[float]):
        self.values = list(values)
        self.index = 0

    def get_current_fast(self, **_kwargs: Any) -> Dict[str, float]:
        if not self.values:
            raise RuntimeError("no dewpoint values")
        if self.index < len(self.values):
            value = self.values[self.index]
            self.index += 1
        else:
            value = self.values[-1]
        return {"dewpoint_c": value, "temp_c": 20.0, "rh_pct": 1.0}


class _ForbiddenAnalyzer:
    def read_latest_data(self, *_args: Any, **_kwargs: Any) -> str:
        raise AssertionError("dewpoint gate extended hold must not read analyzer data")


class _ForbiddenPace:
    def get_vent_status(self) -> int:
        return 1

    def get_output_state(self) -> int:
        return 0

    def get_isolation_state(self) -> int:
        return 1

    def set_output(self, *_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("dewpoint gate extended hold must not write OUTP")

    def vent(self, *_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("dewpoint gate extended hold must not write VENT0/VENT1 directly")

    def set_isolation_open(self, *_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("dewpoint gate extended hold must not write ISOL")


def _cfg(tmp_path: Path) -> dict:
    return {
        "paths": {"output_dir": str(tmp_path)},
        "workflow": {
            "collect_only": True,
            "diagnostics": {
                "dewpoint_gate_extended_hold": {
                    "enabled": True,
                    "duration_s": 0.01,
                    "max_gap_s": 1.0,
                }
            },
            "stability": {
                "gas_route_dewpoint_gate_enabled": True,
                "gas_route_dewpoint_gate_poll_s": 0.2,
            },
            "pressure": {
                "raw_serial_tap": {
                    "enabled": True,
                    "device": "pressure_controller",
                    "fail_on_unexpected_analyzer_gate_write": True,
                }
            },
        },
        "devices": {"pressure_controller": {"port": "COM23"}},
    }


def _point() -> CalibrationPoint:
    return CalibrationPoint(
        index=3,
        temp_chamber_c=20.0,
        co2_ppm=1000.0,
        hgen_temp_c=None,
        hgen_rh_pct=None,
        target_pressure_hpa=1100.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
        co2_group="A",
    )


def _runner(tmp_path: Path, dewpoints: Iterable[float]) -> tuple[DewpointGateExtendedHoldDiagnosticRunner, RunLogger, CalibrationPoint]:
    cfg = _cfg(tmp_path)
    logger = RunLogger(tmp_path, run_id="unit", cfg=cfg)
    point = _point()
    runner = DewpointGateExtendedHoldDiagnosticRunner(
        cfg,
        {
            "dewpoint": _FakeDewpoint(dewpoints),
            "pace": _ForbiddenPace(),
            "gas_analyzer": _ForbiddenAnalyzer(),
        },
        logger,
        lambda *_args: None,
        lambda *_args: None,
    )
    runner._set_point_runtime_fields(
        point,
        phase="co2",
        dewpoint_gate_tail_reference_c=-33.5,
        dewpoint_gate_pass_value_c=-33.57,
        dewpoint_gate_pass_ts=time.time(),
        flush_gate_status="pass",
    )
    runner._read_pace_pressure_now = lambda _pace=None: 1008.0  # type: ignore[method-assign]
    runner._read_com22_pressure_now = lambda: 1008.1  # type: ignore[method-assign]
    runner._cached_actual_open_valves = lambda: [6, 7, 8, 11]  # type: ignore[method-assign]
    runner._route_open_vent1_freshness_evidence = lambda *_args, **_kwargs: {  # type: ignore[method-assign]
        "vent1_last_refresh_age_s": 0.2,
        "vent1_last_refresh_source": "raw_tap",
    }
    return runner, logger, point


def _summary(logger: RunLogger) -> dict:
    path = logger.run_dir / "dewpoint_gate_extended_hold_summary.json"
    return json.loads(path.read_text(encoding="utf-8"))


def test_dewpoint_gate_extended_hold_does_not_enter_analyzer_gate(tmp_path: Path) -> None:
    runner, logger, point = _runner(tmp_path, [-33.6, -33.6])
    runner._wait_primary_sensor_stable = lambda *_args, **_kwargs: (_ for _ in ()).throw(  # type: ignore[method-assign]
        AssertionError("must not enter analyzer stability wait")
    )

    assert runner._run_dewpoint_gate_extended_hold_after_gate(point) is False
    logger.close()

    summary = _summary(logger)
    assert summary["final_decision"] == DEWPOINT_GATE_EXTENDED_HOLD_NO_REBOUND
    assert summary["analyzer_gate_entered"] is False
    assert summary["analyzer_rx_stability_loop_started"] is False
    assert summary["analyzer_tx_count"] == 0


def test_dewpoint_gate_extended_hold_keeps_same_reader(tmp_path: Path) -> None:
    runner, logger, point = _runner(tmp_path, [-33.6])

    assert runner._run_dewpoint_gate_extended_hold_after_gate(point) is False
    summary = _summary(logger)
    logger.close()

    assert summary["dewpoint_gate_extended_hold_same_reader"] is True
    assert summary["dewpoint_gate_extended_hold_reader_function"] == "_read_precondition_dewpoint_gate_snapshot"
    assert summary["dewpoint_gate_extended_hold_same_sampling_interval"] is True
    assert summary["dewpoint_gate_extended_hold_previous_stage"] == "co2_precondition_dewpoint_gate"
    assert summary["dewpoint_gate_extended_hold_stage"] == "co2_precondition_dewpoint_gate_extended_hold"


def test_dewpoint_gate_extended_hold_does_not_send_vent0_or_outp(tmp_path: Path) -> None:
    runner, logger, point = _runner(tmp_path, [-33.6])

    assert runner._run_dewpoint_gate_extended_hold_after_gate(point) is False
    summary = _summary(logger)
    logger.close()

    assert summary["vent0_count"] == 0
    assert summary["outp0_count"] == 0
    assert summary["outp1_count"] == 0
    assert summary["isol_command_count"] == 0
    assert summary["setpoint_sour_pres_count"] == 0


def test_dewpoint_gate_extended_hold_has_fixed_duration(tmp_path: Path) -> None:
    runner, logger, point = _runner(tmp_path, [-33.6])

    assert runner._run_dewpoint_gate_extended_hold_after_gate(point) is False
    summary = _summary(logger)
    logger.close()

    assert 0.0 <= float(summary["dewpoint_gate_extended_hold_duration_s"]) < 1.0
    assert summary["dewpoint_gate_extended_hold_sample_interval_s"] == 0.2


def test_dewpoint_gate_extended_hold_keeps_vent1_and_route_open(tmp_path: Path) -> None:
    runner, logger, point = _runner(tmp_path, [-33.6])
    logger.log_raw_serial_tap(
        port="COM23",
        device_label="pace5000",
        direction="WRITE",
        raw_bytes=b":SOUR:PRES:LEV:IMM:AMPL:VENT 1\n",
    )

    assert runner._run_dewpoint_gate_extended_hold_after_gate(point) is False
    summary = _summary(logger)
    logger.close()

    assert summary["actual_open_valves"] == "6,7,8,11"
    assert summary["relay_write_count"] == 0
    assert summary["valve_change_count"] == 0


def test_dewpoint_gate_extended_hold_records_rebound_vs_tail_reference(tmp_path: Path) -> None:
    runner, logger, point = _runner(tmp_path, [-33.0])

    assert runner._run_dewpoint_gate_extended_hold_after_gate(point) is False
    summary = _summary(logger)
    timeline_rows = list(csv.DictReader((logger.run_dir / "dewpoint_gate_extended_hold_timeline.csv").open(newline="", encoding="utf-8")))
    logger.close()

    assert summary["dewpoint_gate_tail_reference_c"] == -33.5
    assert summary["dewpoint_gate_extended_hold_delta_vs_tail_reference_c"] > 0.20
    assert summary["dewpoint_gate_extended_hold_rebound_exceeded"] is True
    assert timeline_rows
    assert {
        "nearest_prev_vent1_ts",
        "age_since_prev_vent1_s",
        "nearest_next_vent1_ts",
        "time_to_next_vent1_s",
        "vent1_gap_s",
        "nearest_pace_pressure_hpa",
        "nearest_com22_pressure_hpa",
        "nearest_vent_status",
        "nearest_outp_state",
        "nearest_isol_state",
        "raw_tap_unexpected_write_nearby",
        "notes",
    }.issubset(timeline_rows[0].keys())
    assert timeline_rows[0]["nearest_vent_status"] == "1"
    assert timeline_rows[0]["nearest_outp_state"] == "0"
    assert timeline_rows[0]["nearest_isol_state"] == "1"


def test_dewpoint_gate_extended_hold_rebound_observed_decision(tmp_path: Path) -> None:
    runner, logger, point = _runner(tmp_path, [-33.0])

    assert runner._run_dewpoint_gate_extended_hold_after_gate(point) is False
    logger.close()

    assert _summary(logger)["final_decision"] == DEWPOINT_GATE_EXTENDED_HOLD_REBOUND_OBSERVED


def test_dewpoint_gate_extended_hold_no_rebound_decision(tmp_path: Path) -> None:
    runner, logger, point = _runner(tmp_path, [-33.7, -33.6])

    assert runner._run_dewpoint_gate_extended_hold_after_gate(point) is False
    logger.close()

    assert _summary(logger)["final_decision"] == DEWPOINT_GATE_EXTENDED_HOLD_NO_REBOUND


def test_dewpoint_gate_extended_hold_is_no_write(tmp_path: Path) -> None:
    runner, logger, point = _runner(tmp_path, [-33.6])

    assert runner._run_dewpoint_gate_extended_hold_after_gate(point) is False
    logger.close()
    io_text = logger.io_path.read_text(encoding="utf-8")

    for forbidden in ("SENCO", "ZERO", "SPAN", "CALIB", "COEFF", "COEFFICIENT"):
        assert forbidden not in io_text.upper()


def test_dewpoint_gate_extended_hold_does_not_call_wait_primary_sensor_stable(tmp_path: Path) -> None:
    runner, logger, point = _runner(tmp_path, [-33.6])
    called = False

    def _forbidden(*_args: Any, **_kwargs: Any) -> bool:
        nonlocal called
        called = True
        raise AssertionError("must not enter analyzer stability wait")

    runner._wait_primary_sensor_stable = _forbidden  # type: ignore[method-assign]

    assert runner._run_dewpoint_gate_extended_hold_after_gate(point) is False
    summary = _summary(logger)
    logger.close()

    assert called is False
    assert summary["wait_primary_sensor_stable_called"] is False


def test_no_change_to_normal_runner_path() -> None:
    assert (
        DewpointGateExtendedHoldDiagnosticRunner._wait_co2_preseal_primary_sensor_gate
        is CalibrationRunner._wait_co2_preseal_primary_sensor_gate
    )


def test_analyzer_gate_dewpoint_vs_analyzer_events_csv_written(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "pressure_transition_trace.csv").write_text(
        "ts,trace_stage,analyzer_gate_begin_ts,analyzer_gate_end_ts,dewpoint_gate_tail_reference_c\n"
        "2026-05-18T10:45:13.430,co2_precondition_dewpoint_gate_end,,,-33.5\n"
        "2026-05-18T10:46:00.000,co2_precondition_analyzer_gate_end,"
        "2026-05-18T10:45:16.000,2026-05-18T10:46:00.000,\n",
        encoding="utf-8",
    )
    (run_dir / "io_20260518_000000.csv").write_text(
        "timestamp,port,device,direction,command,response,error\n"
        "2026-05-18T10:45:20.000,COM17,dewpoint_meter,TX,001_GetCurData_END\\r\\n,,\n"
        "2026-05-18T10:45:20.120,COM17,dewpoint_meter,RX,,"
        "001_GetCurData_-33.20_34.0_1.0_END,\n"
        "2026-05-18T10:45:20.500,COM35,gas_analyzer,RX,,YGAS,001,1000.0,\n",
        encoding="utf-8",
    )
    (run_dir / "pace_raw_serial_tap.csv").write_text(
        "wall_ts,monotonic_ts,run_id,device_label,port,direction,raw_bytes_hex,raw_text_decoded,"
        "decoded_command,command_category,is_state_changing_command,thread_name,workflow_stage,"
        "python_call_stack_top10,linked_io_log_sequence_id\n"
        "2026-05-18T10:45:19.900,1,run,pace5000,COM23,WRITE,,,"
        ":SOUR:PRES:LEV:IMM:AMPL:VENT 1,VENT,true,pace5000-vent-hold-COM23,,,\n",
        encoding="utf-8",
    )

    path = write_analyzer_gate_dewpoint_vs_analyzer_events(run_dir)

    rows: List[dict] = list(csv.DictReader(path.open(newline="", encoding="utf-8")))
    assert rows
    assert rows[0]["event_type"] == "dewpoint_sample"
    assert float(rows[0]["delta_vs_tail_reference_c"]) > 0.20
