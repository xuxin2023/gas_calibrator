import csv
from pathlib import Path

import pytest

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


def _h2o_point(*, pressure_hpa: float = 1000.0) -> CalibrationPoint:
    return CalibrationPoint(
        index=2,
        temp_chamber_c=20.0,
        co2_ppm=None,
        hgen_temp_c=20.0,
        hgen_rh_pct=30.0,
        target_pressure_hpa=pressure_hpa,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )


def test_h2o_stage_10_is_source_like_final_stage(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {"valves": {"h2o_path": 8, "hold": 9, "flow_switch": 10}},
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )

    metadata = runner.valve_role_map_for_ids([10])[0]
    logger.close()

    assert metadata["source_like_stage"] is True
    assert metadata["final_stage"] is True
    assert metadata["requires_explicit_allow"] is True


def test_h2o_source_stage_safe_default_false(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({}, {}, logger, lambda *_: None, lambda *_: None)
    point = _h2o_point()

    runner._sync_h2o_source_stage_runtime_fields(point, phase="h2o")
    logger.close()

    state = runner._point_runtime_state(point, phase="h2o") or {}
    assert runner._source_stage_safety["h2o"] is False
    assert runner._route_final_stage_atmosphere_safety["h2o"] is False
    assert runner._route_final_stage_seal_safety["h2o"] is False
    assert state["source_stage_key"] == "h2o"
    assert state["source_stage_safe"] is False
    assert state["route_final_stage_atmosphere_safe"] is False
    assert state["route_final_stage_seal_safe"] is False


def test_h2o_final_stage_false_blocks_dewpoint_gate(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {"workflow": {"stability": {"water_route_dewpoint_gate_enabled": True, "water_route_dewpoint_gate_policy": "reject"}}},
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _h2o_point()
    runner._sync_h2o_source_stage_runtime_fields(point, phase="h2o")

    assert runner._wait_h2o_route_dewpoint_gate_before_sampling(point, log_context="unit test") is False
    logger.close()

    state = runner._point_runtime_state(point, phase="h2o") or {}
    assert state["abort_reason"] == "SourceStageNotVerified"
    trace_rows = _load_pressure_trace_rows(logger)
    assert any(row["trace_stage"] == "h2o_precondition_dewpoint_gate_blocked" for row in trace_rows)
    assert not any(row["trace_stage"] == "h2o_precondition_dewpoint_gate_begin" for row in trace_rows)


def test_h2o_final_stage_false_blocks_sampling(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {"workflow": {"sampling": {"stable_count": 1, "interval_s": 0.0, "quality": {"enabled": False}}}},
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _h2o_point()
    runner._sync_h2o_source_stage_runtime_fields(point, phase="h2o")
    calls = {"collect": 0}
    runner._collect_samples = lambda *_args, **_kwargs: calls.__setitem__("collect", calls["collect"] + 1) or []  # type: ignore[method-assign]

    runner._sample_open_route_point(point, phase="h2o", point_tag="demo")
    logger.close()

    assert calls["collect"] == 0
    trace_rows = _load_pressure_trace_rows(logger)
    assert any(row["trace_stage"] == "sampling_blocked" for row in trace_rows)
    assert not any(row["trace_stage"] == "sampling_begin" for row in trace_rows)


def test_h2o_final_stage_false_blocks_fast_handoff_full_open(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "workflow": {"pressure": {"route_open_guard_enabled": True}},
            "valves": {"h2o_path": 8, "hold": 9, "flow_switch": 10},
        },
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _h2o_point()
    point_tag = runner._h2o_point_tag(point)
    runner._sync_h2o_source_stage_runtime_fields(point, phase="h2o")
    runner._pending_route_handoff = {
        "next_phase": "h2o",
        "next_point_tag": point_tag,
        "next_point": point,
        "sample_done_ts": 1.0,
        "vent_command_ts": 1.1,
    }
    runner._wait_until_safe_to_open_next_route = lambda *_args, **_kwargs: {  # type: ignore[method-assign]
        "safe_open_ts": 1.2,
        "pressure_gauge_hpa": 1013.0,
        "atmosphere_reference_hpa": 1013.0,
        "safe_open_delta_hpa": 0.0,
        "safe_open_baseline_source": "pressure_gauge",
        "safe_open_baseline_hpa": 1013.0,
    }
    runner._stop_pressure_transition_fast_signal_context = lambda *args, **kwargs: None  # type: ignore[method-assign]

    assert runner._complete_pending_route_handoff(
        point,
        phase="h2o",
        point_tag=point_tag,
        open_valves=[8, 9, 10],
    ) is False
    logger.close()

    state = runner._point_runtime_state(point, phase="h2o") or {}
    assert state["handoff_source_stage_block_reason"] == "HandoffSourceStageBlockedUntilVerified"


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
    assert runner._route_final_stage_atmosphere_safety["co2_a"] is True
    assert runner._route_final_stage_seal_safety["co2_a"] is False
    assert state["route_final_stage_atmosphere_safe"] is True
    assert state["route_final_stage_seal_safe"] is False


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


def test_h2o_final_stage_failure_records_route_vent_path_not_effective(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {"valves": {"h2o_path": 8, "hold": 9, "flow_switch": 10}},
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _h2o_point()
    runner._apply_valve_states = lambda open_valves: setattr(runner, "_current_open_valves", tuple(open_valves))  # type: ignore[method-assign]

    def _guard(*args, **kwargs):
        stage_label = str(kwargs.get("stage_label") or "")
        if stage_label == "8|9|10":
            return (
                False,
                {
                    "route_pressure_guard_status": "fail",
                    "route_pressure_guard_reason": "RouteVentPathNotEffective",
                    "pressure_delta_from_ambient_hpa": 331.726,
                    "analyzer_pressure_kpa": None,
                    "pace_syst_err_query": '0,"No error"',
                    "abort_reason": "RouteVentPathNotEffective",
                    "vent_recovery_result": "vent_refresh_failed",
                    "hidden_syst_err_count": 0,
                    "unclassified_syst_err_count": 0,
                },
            )
        return (
            True,
            {
                "route_pressure_guard_status": "pass",
                "route_pressure_guard_reason": "",
                "pressure_delta_from_ambient_hpa": 0.5,
                "analyzer_pressure_kpa": None,
                "pace_syst_err_query": '0,"No error"',
                "abort_reason": "",
                "vent_recovery_result": "",
                "hidden_syst_err_count": 0,
                "unclassified_syst_err_count": 0,
            },
        )

    runner._run_route_open_pressure_guard = _guard  # type: ignore[method-assign]

    assert runner._open_route_with_pressure_guard(
        point,
        phase="h2o",
        point_tag="demo",
        open_valves=[8, 9, 10],
        log_context="unit h2o stage10 fail",
    ) is False
    logger.close()

    state = runner._point_runtime_state(point, phase="h2o") or {}
    assert runner._source_stage_safety["h2o"] is False
    assert state["source_stage_key"] == "h2o"
    assert state["source_stage_guard_reason"] == "RouteVentPathNotEffective"


def test_unclassified_syst_err_during_route_stage_aborts(tmp_path: Path) -> None:
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
    state = {"armed": False}

    def _apply(open_valves):
        setattr(runner, "_current_open_valves", tuple(open_valves))
        state["armed"] = True

    def _read_syst_err():
        if state["armed"]:
            state["armed"] = False
            return ':SYST:ERR -102,"Syntax error"'
        return '0,"No error"'

    runner._apply_valve_states = _apply  # type: ignore[method-assign]
    runner._read_pace_system_error_text = _read_syst_err  # type: ignore[method-assign]

    assert runner._open_route_with_pressure_guard(
        point,
        phase="co2",
        point_tag="demo",
        open_valves=[8, 11, 7, 4],
        log_context="unit unclassified syst err",
    ) is False
    logger.close()

    state_row = runner._point_runtime_state(point, phase="co2") or {}
    assert state_row["abort_reason"] == "UnclassifiedPaceSystErrDuringRouteStage"
    assert state_row["unclassified_syst_err_count"] >= 1


class _FakePaceOptionalAttribution:
    def __init__(self) -> None:
        self.errors: list[str] = []

    def query(self, cmd: str) -> str:
        if str(cmd).strip() == ":SOUR:PRES:COMP1?":
            self.errors.append(':SYST:ERR -102,"Syntax error"')
        return "OK"

    def get_system_error(self) -> str:
        return self.errors[0] if self.errors else '0,"No error"'

    def drain_system_errors(self):
        drained = list(self.errors)
        self.errors.clear()
        return drained


def test_optional_diagnostic_syst_err_is_attributed_and_drained_before_route(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({}, {"pace": _FakePaceOptionalAttribution()}, logger, lambda *_: None, lambda *_: None)

    rows = runner._pace_optional_query_error_attribution((":SOUR:PRES:COMP1?",), reason="unit optional attribution")
    counts = runner._pace_error_attribution_counts()
    logger.close()

    assert rows[0]["command"] == ":SOUR:PRES:COMP1?"
    assert rows[0]["syst_err"] == ':SYST:ERR -102,"Syntax error"'
    assert rows[0]["drained_errors"] == [':SYST:ERR -102,"Syntax error"']
    assert counts["optional_probe_error_count"] == 1
    assert counts["unclassified_syst_err_count"] == 0


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
        if str(cmd).strip() == ":UNIT:PRES?":
            return ":UNIT:PRES HPA"
        if str(cmd).strip() == ":SENS:PRES:RANG?":
            return '"1600HPAG"'
        if str(cmd).strip() == ":SENS:PRES:CONT?":
            raise RuntimeError("UNSUPPORTED")
        return ""


class _FakeOldPaceVentAuxProbe:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.profile = "OLD_PACE5000"

    def get_device_identity(self) -> str:
        return "*IDN GE Druck,Pace5000 User Interface,3213201,02.00.07"

    def get_instrument_version(self) -> str:
        return ':INST:VERS "02.00.07"'

    def get_vent_elapsed_time_s(self) -> float:
        self.calls.append("get_vent_elapsed_time_s")
        raise RuntimeError("should not query old vent elapsed time")

    def get_vent_over_range_protect_state(self) -> str:
        self.calls.append("get_vent_over_range_protect_state")
        raise RuntimeError("should not query old vent ORPV")

    def get_vent_power_up_protect_state(self) -> str:
        self.calls.append("get_vent_power_up_protect_state")
        raise RuntimeError("should not query old vent PUPV")


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
    assert snapshot["sens_pres_cont_supported"] is False


class _FakePaceSensPresContCapabilityOnly:
    def __init__(self) -> None:
        self.errors: list[str] = []
        self.get_control_pressure_calls = 0

    def get_output_state(self) -> int:
        return 0

    def get_isolation_state(self) -> int:
        return 1

    def get_vent_status(self) -> int:
        return 0

    def query(self, cmd: str) -> str:
        if str(cmd).strip() == ":SENS:PRES:CONT?":
            self.errors.append(':SYST:ERR -113,"Undefined header"')
            return ""
        return ""

    def get_system_error(self) -> str:
        return self.errors[0] if self.errors else '0,"No error"'

    def drain_system_errors(self):
        drained = list(self.errors)
        self.errors.clear()
        return drained

    def get_control_pressure(self):
        self.get_control_pressure_calls += 1
        return 123.4


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


def test_sens_pres_cont_probe_is_capability_only_and_not_used_in_route_guard(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    pace = _FakePaceSensPresContCapabilityOnly()
    runner = CalibrationRunner({}, {"pace": pace}, logger, lambda *_: None, lambda *_: None)

    snapshot = runner._capture_pace_capability_snapshot(reason="unit sens pres cont", include_optional_probe=True)
    diag_snapshot = runner._pace_diagnostic_state_snapshot(refresh=True, refresh_aux=True)
    logger.close()

    assert snapshot["sens_pres_cont_supported"] is False
    assert snapshot["sens_pres_cont_error"] == ':SYST:ERR -113,"Undefined header"'
    assert pace.get_control_pressure_calls == 0
    assert diag_snapshot["pace_sens_pres_cont_query"] == ""


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


def test_atmosphere_flow_safe_does_not_imply_seal_pressure_safe(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({}, {}, logger, lambda *_: None, lambda *_: None)
    point = _co2_point()

    runner._route_final_stage_atmosphere_safety["co2_a"] = True
    runner._sync_co2_source_stage_runtime_fields(point, phase="co2")
    logger.close()

    state = runner._point_runtime_state(point, phase="co2") or {}
    assert state["route_final_stage_atmosphere_safe"] is True
    assert state["route_final_stage_seal_safe"] is False
    assert state["atmosphere_flow_safe"] is True
    assert state["seal_pressure_safe"] is False


def test_dewpoint_gate_requires_continuous_atmosphere_active_when_flowing(tmp_path: Path) -> None:
    logger = RunLogger(
        tmp_path,
    )
    runner = CalibrationRunner(
        {"workflow": {"stability": {"gas_route_dewpoint_gate_enabled": True, "gas_route_dewpoint_gate_policy": "reject"}}},
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _co2_point()
    runner._source_stage_safety["co2_a"] = True
    runner._route_final_stage_atmosphere_safety["co2_a"] = True
    runner._sync_co2_source_stage_runtime_fields(point, phase="co2")

    assert runner._wait_co2_route_dewpoint_gate_before_seal(point, base_soak_s=0.0, log_context="unit continuous active") is False
    logger.close()

    state = runner._point_runtime_state(point, phase="co2") or {}
    assert state["abort_reason"] == "ContinuousAtmosphereFlowthroughNotActive"


def test_seal_transition_requires_seal_pressure_safe_not_only_atmosphere_safe(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({}, {}, logger, lambda *_: None, lambda *_: None)
    point = _co2_point()
    runner._source_stage_safety["co2_a"] = True
    runner._route_final_stage_atmosphere_safety["co2_a"] = True
    runner._sync_co2_source_stage_runtime_fields(point, phase="co2")
    runner._pressurize_and_hold = lambda *_args, **_kwargs: True  # type: ignore[method-assign]

    assert runner._pressurize_route_for_sealed_points(point, route="co2", sealed_control_refs=[point]) is False
    logger.close()

    state = runner._point_runtime_state(point, phase="co2") or {}
    assert state["abort_reason"] == "SealPressureStageNotVerified"


def test_sampling_under_pressure_blocks_when_only_atmosphere_flow_safe(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({}, {}, logger, lambda *_: None, lambda *_: None)
    point = _h2o_point()
    runner._source_stage_safety["h2o"] = True
    runner._route_final_stage_atmosphere_safety["h2o"] = True
    runner._sync_h2o_source_stage_runtime_fields(point, phase="h2o")
    runner._pressurize_and_hold = lambda *_args, **_kwargs: True  # type: ignore[method-assign]

    assert runner._pressurize_route_for_sealed_points(point, route="h2o", sealed_control_refs=[point]) is False
    logger.close()

    state = runner._point_runtime_state(point, phase="h2o") or {}
    assert state["abort_reason"] == "SealPressureStageNotVerified"


def test_source_stage_open_requires_continuous_atmosphere_window(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {"valves": {"h2o_path": 8, "gas_main": 11, "co2_path": 7, "co2_map": {"600": 4}}},
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _co2_point()
    runner._apply_valve_states = lambda open_valves: setattr(runner, "_current_open_valves", tuple(open_valves))  # type: ignore[method-assign]
    seen: list[bool] = []

    def _guard(*args, **kwargs):
        if str(kwargs.get("stage_label") or "") == "8|11|7|4":
            state = runner._point_runtime_state(point, phase="co2") or {}
            seen.append(bool(state.get("continuous_atmosphere_active")))
        return True, {
            "route_pressure_guard_status": "pass",
            "route_pressure_guard_reason": "",
            "pressure_delta_from_ambient_hpa": 0.5,
            "analyzer_pressure_kpa": None,
            "pace_syst_err_query": '0,"No error"',
            "abort_reason": "",
            "vent_recovery_result": "",
            "hidden_syst_err_count": 0,
            "unclassified_syst_err_count": 0,
        }

    runner._run_route_open_pressure_guard = _guard  # type: ignore[method-assign]

    assert runner._open_route_with_pressure_guard(
        point,
        phase="co2",
        point_tag="demo",
        open_valves=[8, 11, 7, 4],
        log_context="unit continuous source stage window",
    ) is True
    logger.close()

    assert seen == [True]


def test_h2o_final_stage_open_requires_continuous_atmosphere_window(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {"valves": {"h2o_path": 8, "hold": 9, "flow_switch": 10}},
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _h2o_point()
    runner._apply_valve_states = lambda open_valves: setattr(runner, "_current_open_valves", tuple(open_valves))  # type: ignore[method-assign]
    seen: list[bool] = []

    def _guard(*args, **kwargs):
        if str(kwargs.get("stage_label") or "") == "8|9|10":
            state = runner._point_runtime_state(point, phase="h2o") or {}
            seen.append(bool(state.get("continuous_atmosphere_active")))
        return True, {
            "route_pressure_guard_status": "pass",
            "route_pressure_guard_reason": "",
            "pressure_delta_from_ambient_hpa": 0.5,
            "analyzer_pressure_kpa": None,
            "pace_syst_err_query": '0,"No error"',
            "abort_reason": "",
            "vent_recovery_result": "",
            "hidden_syst_err_count": 0,
            "unclassified_syst_err_count": 0,
        }

    runner._run_route_open_pressure_guard = _guard  # type: ignore[method-assign]

    assert runner._open_route_with_pressure_guard(
        point,
        phase="h2o",
        point_tag="demo",
        open_valves=[8, 9, 10],
        log_context="unit h2o final stage continuous window",
    ) is True
    logger.close()

    assert seen == [True]


def test_pressure_rise_during_flowthrough_triggers_vent_keepalive(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({}, {}, logger, lambda *_: None, lambda *_: None)
    point = _co2_point()
    runner._last_atmosphere_gate_summary = {"ambient_hpa": 1012.0, "atmosphere_ready": True}
    runner._continuous_atmosphere_state = {
        "active": True,
        "route_flow_active": True,
        "route_key": "co2_a",
        "phase_name": "ContinuousAtmosphereFlowThrough",
        "pressure_mode": "AtmosphereFlush",
        "keepalive_count": 0,
        "last_keepalive_ts": 0.0,
        "last_keepalive_reason": "",
        "last_keepalive_summary": {},
    }
    runner._read_current_pressure_hpa_for_atmosphere = lambda: {  # type: ignore[method-assign]
        "pressure_hpa": 1045.0,
        "pace_pressure_hpa": 1045.0,
        "pressure_gauge_hpa": 1045.0,
    }
    runner._route_stage_fresh_vent_refresh = lambda *args, **kwargs: {  # type: ignore[method-assign]
        "fresh_vent_command_sent": True,
        "vent_status_sequence": [1, 2],
        "vent_status_sequence_text": "1,2",
        "pressure_hpa": 1012.5,
        "pressure_delta_from_ambient_hpa": 0.5,
        "abort_reason": "",
    }

    ok, state = runner.maintain_continuous_atmosphere_flowthrough(
        "co2_a",
        point=point,
        phase="co2",
        phase_name="ContinuousAtmosphereFlowThrough",
        reason="unit keepalive",
        force=False,
    )
    logger.close()

    assert ok is True
    assert state["keepalive_count"] == 1
    assert state["last_keepalive_summary"]["pace_vent_command_sent"] == ":SOUR:PRES:LEV:IMM:AMPL:VENT 1"


def test_pressure_rise_after_keepalive_fails_atmosphere_path_insufficient(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({}, {}, logger, lambda *_: None, lambda *_: None)
    point = _co2_point()
    runner._last_atmosphere_gate_summary = {"ambient_hpa": 1012.0, "atmosphere_ready": True}
    runner._continuous_atmosphere_state = {
        "active": True,
        "route_flow_active": True,
        "route_key": "co2_a",
        "phase_name": "ContinuousAtmosphereFlowThrough",
        "pressure_mode": "AtmosphereFlush",
        "keepalive_count": 0,
        "last_keepalive_ts": 0.0,
        "last_keepalive_reason": "",
        "last_keepalive_summary": {},
    }
    runner._read_current_pressure_hpa_for_atmosphere = lambda: {  # type: ignore[method-assign]
        "pressure_hpa": 1065.0,
        "pace_pressure_hpa": 1065.0,
        "pressure_gauge_hpa": 1065.0,
    }
    runner._route_stage_fresh_vent_refresh = lambda *args, **kwargs: {  # type: ignore[method-assign]
        "fresh_vent_command_sent": True,
        "vent_status_sequence": [1, 2],
        "vent_status_sequence_text": "1,2",
        "pressure_hpa": 1048.0,
        "pressure_delta_from_ambient_hpa": 36.0,
        "abort_reason": "",
    }

    ok, state = runner.maintain_continuous_atmosphere_flowthrough(
        "co2_a",
        point=point,
        phase="co2",
        phase_name="ContinuousAtmosphereFlowThrough",
        reason="unit keepalive fail",
    )
    logger.close()

    assert ok is False
    assert state["abort_reason"] == "AtmospherePathInsufficientUnderFlow"


def test_no_global_periodic_vent_restored(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({}, {}, logger, lambda *_: None, lambda *_: None)
    calls: list[str] = []
    runner._route_stage_fresh_vent_refresh = lambda *args, **kwargs: calls.append("refresh") or {}  # type: ignore[method-assign]

    runner._refresh_pressure_controller_atmosphere_hold(force=True, reason="unit test")
    logger.close()

    assert calls == []


def test_vent_keepalive_only_active_during_route_flush_or_atmosphere_flowthrough(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({}, {}, logger, lambda *_: None, lambda *_: None)
    point = _co2_point()
    calls: list[str] = []
    runner._route_stage_fresh_vent_refresh = lambda *args, **kwargs: calls.append("refresh") or {}  # type: ignore[method-assign]
    runner._continuous_atmosphere_state = {
        "active": True,
        "route_flow_active": True,
        "route_key": "co2_a",
        "phase_name": "ContinuousAtmosphereFlowThrough",
        "pressure_mode": "AtmosphereFlush",
        "keepalive_count": 0,
        "last_keepalive_ts": 0.0,
        "last_keepalive_reason": "",
        "last_keepalive_summary": {},
    }

    ok, _state = runner.maintain_continuous_atmosphere_flowthrough(
        "co2_a",
        point=point,
        phase="co2",
        phase_name="PressureSetpointHold",
        reason="unit no keepalive in hold",
        force=True,
    )
    logger.close()

    assert ok is True
    assert calls == []


def test_pressure_setpoint_hold_still_has_no_vent_keepalive(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({}, {}, logger, lambda *_: None, lambda *_: None)
    point = _co2_point()
    runner._continuous_atmosphere_state = {
        "active": True,
        "route_flow_active": True,
        "route_key": "co2_a",
        "phase_name": "ContinuousAtmosphereFlowThrough",
        "pressure_mode": "AtmosphereFlush",
        "keepalive_count": 2,
        "last_keepalive_ts": 0.0,
        "last_keepalive_reason": "",
        "last_keepalive_summary": {},
    }
    calls: list[str] = []
    runner._route_stage_fresh_vent_refresh = lambda *args, **kwargs: calls.append("refresh") or {}  # type: ignore[method-assign]

    runner.exit_continuous_atmosphere_flowthrough("co2_a", point=point, phase="co2", reason="before hold")
    ok, state = runner.maintain_continuous_atmosphere_flowthrough(
        "co2_a",
        point=point,
        phase="co2",
        phase_name="PressureSetpointHold",
        reason="unit pressure hold",
        force=True,
    )
    logger.close()

    assert ok is True
    assert calls == []
    assert state["active"] is False


def test_pre_route_drain_error_requires_io_context(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({}, {}, logger, lambda *_: None, lambda *_: None)
    io_path = Path(logger.io_path)
    io_path.write_text(
        "\n".join(
            [
                "timestamp,port,device,direction,duration_ms,command,response,error",
                "2026-04-21T10:00:00.000,COM31,pace5000,TX,0.1,:SOUR:PRES:LEV:IMM:AMPL:VENT:ETIM?\\n,,",
                "2026-04-21T10:00:00.010,COM31,pace5000,QUERY,10,:SOUR:PRES:LEV:IMM:AMPL:VENT:ETIM?\\n,,",
                "2026-04-21T10:00:00.020,COM31,pace5000,TX,0.1,:SYST:ERR?\\n,,",
                "2026-04-21T10:00:00.030,COM31,pace5000,RX,10,,\":SYST:ERR -102,\\\"Syntax error\\\"\",",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    runner._last_clean_syst_err_ts = "2026-04-21T09:59:59.000"
    runner._record_pace_error_attribution(
        classification="pre_route_drain",
        action="pre_route_drain",
        syst_err=':SYST:ERR -102,"Syntax error"',
        command="relay_stage:8",
        response="8",
        reason="unit pre-route drain",
    )
    logger.close()

    snapshot = runner._pace_error_attribution_log_snapshot()
    assert snapshot[0]["commands_since_last_clean_syst_err"]
    assert snapshot[0]["last_20_pace_io_before_error"]
    assert snapshot[0]["next_20_pace_io_after_error"]
    assert "VENT:ETIM?" in snapshot[0]["suspected_command"]


def test_old_pace_vent_aux_probes_are_not_used_in_route_guard(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    pace = _FakePaceCapability()
    runner = CalibrationRunner({}, {"pace": pace}, logger, lambda *_: None, lambda *_: None)
    logger.close()

    assert runner._pace_route_trace_optional_diagnostic_key_enabled(pace, "pace_vent_elapsed_time_query") is False
    assert runner._pace_route_trace_optional_diagnostic_key_enabled(pace, "pace_vent_orpv_state_query") is False
    assert runner._pace_route_trace_optional_diagnostic_key_enabled(pace, "pace_vent_pupv_state_query") is False


def test_refresh_pressure_controller_aux_state_skips_old_pace_vent_aux_probes(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    pace = _FakeOldPaceVentAuxProbe()
    runner = CalibrationRunner({}, {"pace": pace}, logger, lambda *_: None, lambda *_: None)

    runner._refresh_pressure_controller_aux_state(pace)
    logger.close()

    assert pace.calls == []
    assert runner._pace_vent_elapsed_time_query is None
    assert runner._pace_vent_orpv_state_query is None
    assert runner._pace_vent_pupv_state_query is None


def test_sealed_no_vent_guard_blocks_vent_on_during_pressure_hold(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({}, {}, logger, lambda *_: None, lambda *_: None)
    point = _co2_point()
    runner._activate_sealed_no_vent_guard(
        point=point,
        phase="co2",
        guard_phase="PressureSetpointHold",
        reason="unit pressure hold",
    )

    with pytest.raises(RuntimeError, match="sealed_no_vent_guard_violation:vent_on"):
        runner._set_pressure_controller_vent(True, reason="forbidden during pressure hold")
    logger.close()

    trace_rows = _load_pressure_trace_rows(logger)
    assert any(row["trace_stage"] == "sealed_no_vent_guard_violation" for row in trace_rows)


def test_sealed_no_vent_guard_blocks_stale_keepalive_after_seal_transition(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({}, {}, logger, lambda *_: None, lambda *_: None)
    point = _co2_point()
    runner._activate_sealed_no_vent_guard(
        point=point,
        phase="co2",
        guard_phase="SealTransition",
        reason="unit seal transition",
    )

    ok, state = runner.maintain_continuous_atmosphere_flowthrough(
        "co2_a",
        point=point,
        phase="co2",
        phase_name="ContinuousAtmosphereFlowThrough",
        reason="stale keepalive callback",
    )
    logger.close()

    assert ok is False
    assert state["abort_reason"] == "KeepaliveBlockedBySealedNoVentGuard"
    assert state["active"] is False
