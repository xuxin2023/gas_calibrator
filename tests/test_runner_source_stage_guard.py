import csv
from pathlib import Path

from gas_calibrator.data.points import CalibrationPoint
from gas_calibrator.logging_utils import RunLogger
from gas_calibrator.workflow.runner import CalibrationRunner


def _load_pressure_trace_rows(logger: RunLogger):
    path = logger.run_dir / "pressure_transition_trace.csv"
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _co2_point(*, ppm: float = 600.0, group: str = "A", pressure_hpa: float = 1000.0) -> CalibrationPoint:
    return CalibrationPoint(
        index=1,
        temp_chamber_c=20.0,
        co2_ppm=ppm,
        hgen_temp_c=None,
        hgen_rh_pct=None,
        target_pressure_hpa=pressure_hpa,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
        co2_group=group,
    )


def test_source_stage_safe_false_blocks_dewpoint_gate(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {"workflow": {"stability": {"gas_route_dewpoint_gate_enabled": True, "gas_route_dewpoint_gate_policy": "reject"}}},
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _co2_point()
    runner._sync_co2_source_stage_runtime_fields(point, phase="co2")

    assert runner._wait_co2_route_dewpoint_gate_before_seal(point, base_soak_s=0.0, log_context="unit test") is False
    logger.close()

    state = runner._point_runtime_state(point, phase="co2") or {}
    assert state["abort_reason"] == "SourceStageNotVerified"
    assert state["source_stage_safe"] is False
    trace_rows = _load_pressure_trace_rows(logger)
    assert any(row["trace_stage"] == "co2_precondition_dewpoint_gate_blocked" for row in trace_rows)
    assert not any(row["trace_stage"] == "co2_precondition_dewpoint_gate_begin" for row in trace_rows)


def test_source_stage_safe_false_blocks_sampling(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {"workflow": {"sampling": {"stable_count": 1, "interval_s": 0.0, "quality": {"enabled": False}}}},
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _co2_point()
    runner._sync_co2_source_stage_runtime_fields(point, phase="co2")
    calls = {"collect": 0}
    runner._collect_samples = lambda *_args, **_kwargs: calls.__setitem__("collect", calls["collect"] + 1) or []  # type: ignore[method-assign]

    runner._sample_open_route_point(point, phase="co2", point_tag="demo")
    logger.close()

    assert calls["collect"] == 0
    trace_rows = _load_pressure_trace_rows(logger)
    assert any(row["trace_stage"] == "sampling_blocked" for row in trace_rows)
    assert not any(row["trace_stage"] == "sampling_begin" for row in trace_rows)


def test_source_stage_pass_sets_source_stage_safe(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "valves": {
                "h2o_path": 8,
                "gas_main": 11,
                "co2_path": 7,
                "co2_map": {"600": 4},
            }
        },
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _co2_point()
    runner._apply_valve_states = lambda open_valves: setattr(runner, "_current_open_valves", tuple(open_valves))  # type: ignore[method-assign]
    runner._run_route_open_pressure_guard = lambda *args, **kwargs: (  # type: ignore[method-assign]
        True,
        {
            "route_pressure_guard_status": "pass",
            "route_pressure_guard_reason": "",
            "pressure_delta_from_ambient_hpa": 10.0,
            "analyzer_pressure_kpa": 110.0,
            "pace_syst_err_query": '0,"No error"',
            "abort_reason": "",
            "vent_recovery_result": "",
        },
    )

    assert runner._open_route_with_pressure_guard(
        point,
        phase="co2",
        point_tag="demo",
        open_valves=[8, 11, 7, 4],
        log_context="unit source stage pass",
    ) is True
    logger.close()

    state = runner._point_runtime_state(point, phase="co2") or {}
    assert runner._source_stage_safety["co2_a"] is True
    assert state["source_stage_safe"] is True


def test_source_stage_fail_records_route_vent_path_not_effective(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "valves": {
                "h2o_path": 8,
                "gas_main": 11,
                "co2_path": 7,
                "co2_map": {"600": 4},
            }
        },
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _co2_point()
    runner._apply_valve_states = lambda open_valves: setattr(runner, "_current_open_valves", tuple(open_valves))  # type: ignore[method-assign]
    runner._run_route_open_pressure_guard = lambda *args, **kwargs: (  # type: ignore[method-assign]
        False,
        {
            "route_pressure_guard_status": "fail",
            "route_pressure_guard_reason": "RouteVentPathNotEffective",
            "pressure_delta_from_ambient_hpa": 445.916,
            "analyzer_pressure_kpa": 119.0,
            "pace_syst_err_query": '0,"No error"',
            "abort_reason": "RouteVentPathNotEffective",
            "vent_recovery_result": "vent_refresh_failed",
        },
    )

    assert runner._open_route_with_pressure_guard(
        point,
        phase="co2",
        point_tag="demo",
        open_valves=[8, 11, 7, 4],
        log_context="unit source stage fail",
    ) is False
    logger.close()

    state = runner._point_runtime_state(point, phase="co2") or {}
    assert runner._source_stage_safety["co2_a"] is False
    assert state["source_stage_safe"] is False
    assert state["source_stage_guard_reason"] == "RouteVentPathNotEffective"


class _FakePaceCapability:
    def __init__(self) -> None:
        self.queries: list[str] = []

    def get_device_identity(self) -> str:
        return "*IDN GE Druck,Pace5000 User Interface,3213201,02.00.07"

    def get_instrument_version(self) -> str:
        return ':INST:VERS "02.00.07"'

    def supports_sens_pres_cont(self) -> bool:
        return False

    def get_pressure_unit(self) -> str:
        return "HPA"

    def get_output_mode(self) -> str:
        return "ACT"

    def get_output_state(self) -> int:
        return 1

    def get_control_range(self) -> str:
        return "1600HPAG"

    def get_in_limits_setting(self) -> float:
        return 0.02

    def get_in_limits_time_setting_s(self) -> float:
        return 10.0

    def get_system_error(self) -> str:
        return '0,"No error"'

    def drain_system_errors(self, *, max_reads: int = 8):
        return []

    def query(self, cmd: str) -> str:
        self.queries.append(str(cmd))
        if str(cmd).strip() == ":SENS:PRES:RANG?":
            return '"1600HPAG"'
        if str(cmd).strip() == ":SENS:PRES:CONT?":
            raise RuntimeError("UNSUPPORTED")
        return ""


def test_k0472_capability_snapshot_records_unit_mode_ranges(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({}, {"pace": _FakePaceCapability()}, logger, lambda *_: None, lambda *_: None)

    snapshot = runner._capture_pace_capability_snapshot(reason="unit test", include_optional_probe=True)
    logger.close()

    assert snapshot["profile"] == "OLD_PACE5000"
    assert snapshot["pressure_unit"] == "HPA"
    assert snapshot["pressure_unit_status"] == "known_hpa"
    assert snapshot["output_mode"] == "ACT"
    assert snapshot["output_state"] == 1
    assert snapshot["source_pressure_range"] == "1600HPAG"
    assert snapshot["sensor_pressure_range"] == "1600HPAG"
    assert snapshot["source_pressure_in_limits_pct"] == 0.02
    assert snapshot["source_pressure_in_limits_time_s"] == 10.0


class _FakePaceUnknownUnit:
    def get_pressure_unit(self) -> str:
        raise RuntimeError("NO_RESPONSE")

    def get_system_error(self) -> str:
        return '0,"No error"'

    def drain_system_errors(self, *, max_reads: int = 8):
        return []


def test_pressure_unit_unknown_blocks_formal_setpoint(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    point = _co2_point()
    runner = CalibrationRunner({}, {"pace": _FakePaceUnknownUnit()}, logger, lambda *_: None, lambda *_: None)

    assert runner._set_pressure_to_target(point) is False
    logger.close()

    state = runner._point_runtime_state(point, phase="co2") or {}
    assert state["abort_reason"] == "PressureUnitUnknown"


def test_analyzer_pressure_unavailable_is_reported_not_silently_ignored(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "workflow": {
                "pressure": {
                    "route_open_guard_enabled": True,
                    "route_open_guard_monitor_s": 0.0,
                    "route_open_guard_poll_s": 0.0,
                }
            }
        },
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _co2_point()
    runner._last_atmosphere_gate_summary = {"ambient_hpa": 1012.0, "atmosphere_ready": True}
    runner._route_stage_fresh_vent_refresh = lambda *args, **kwargs: {  # type: ignore[method-assign]
        "ambient_hpa": 1012.0,
        "pressure_hpa": 1012.0,
        "pressure_delta_from_ambient_hpa": 0.0,
        "pressure_slope_hpa_s": 0.0,
        "fresh_vent_command_sent": True,
        "vent_status_sequence_text": "1|2",
        "abort_reason": "",
        "pace_pressure_hpa": 1012.0,
        "pressure_gauge_hpa": 1012.0,
    }
    runner._read_current_pressure_hpa_for_atmosphere = lambda: {  # type: ignore[method-assign]
        "pressure_hpa": 1012.0,
        "pace_pressure_hpa": 1012.0,
        "pressure_gauge_hpa": 1012.0,
    }
    runner._read_route_guard_analyzer_pressure_kpa = lambda: (None, "")  # type: ignore[method-assign]
    runner._read_route_guard_dewpoint_line_pressure_hpa = lambda: None  # type: ignore[method-assign]
    runner._read_pace_system_error_text = lambda: '0,"No error"'  # type: ignore[method-assign]

    ok, summary = runner._run_route_open_pressure_guard(
        point,
        phase="co2",
        log_context="unit analyzer unavailable",
        point_tag="demo",
        stage_label="8|11|7",
    )
    logger.close()

    assert ok is True
    assert summary["analyzer_pressure_available"] is False
    assert summary["analyzer_pressure_protection_active"] is False
    assert summary["analyzer_pressure_status"] == "unavailable"
