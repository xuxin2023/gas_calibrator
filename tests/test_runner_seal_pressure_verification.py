from pathlib import Path

from gas_calibrator.data.points import CalibrationPoint
from gas_calibrator.logging_utils import RunLogger
from gas_calibrator.workflow.runner import CalibrationRunner


class _FakePressureGauge:
    def __init__(self, *, pressure_hpa: float = 1000.0, fresh: bool = True, fail: bool = False) -> None:
        self.pressure_hpa = float(pressure_hpa)
        self.fresh = bool(fresh)
        self.fail = bool(fail)
        self.calls: list[str] = []

    def read_pressure(self) -> float:
        self.calls.append("read_pressure")
        if self.fail:
            raise RuntimeError("pressure gauge unavailable")
        return self.pressure_hpa

    def is_pressure_read_fresh(self) -> bool:
        self.calls.append("is_pressure_read_fresh")
        return self.fresh


class _FakePace:
    def __init__(
        self,
        *,
        supported_targets_hpa: tuple[float, ...] = (1000.0, 800.0, 500.0),
        in_limits_cache_valid: bool = True,
        system_error: str = '0,"No error"',
    ) -> None:
        self.supported_targets_hpa = tuple(float(value) for value in supported_targets_hpa)
        self.in_limits_cache_valid = bool(in_limits_cache_valid)
        self.system_error = str(system_error)
        self.calls: list[str] = []

    def choose_control_range_for_target(self, target_hpa: float) -> str | None:
        self.calls.append(f"choose_control_range_for_target:{float(target_hpa):.1f}")
        rounded = int(round(float(target_hpa)))
        if any(int(round(value)) == rounded for value in self.supported_targets_hpa):
            return "1600HPAG"
        return None

    def status(self) -> dict[str, object]:
        self.calls.append("status")
        return {"in_limits_cache_valid": self.in_limits_cache_valid}

    def get_system_error(self) -> str:
        self.calls.append("get_system_error")
        return self.system_error


def _workflow_cfg() -> dict:
    return {
        "valves": {
            "h2o_path": 8,
            "hold": 9,
            "flow_switch": 10,
            "gas_main": 11,
            "co2_path": 7,
            "co2_path_group2": 16,
            "co2_map": {"600": 4},
            "co2_map_group2": {"600": 24},
        }
    }


def _co2_point(*, group: str = "A", pressure_hpa: float = 1000.0) -> CalibrationPoint:
    return CalibrationPoint(
        index=1,
        temp_chamber_c=20.0,
        co2_ppm=600.0,
        hgen_temp_c=None,
        hgen_rh_pct=None,
        target_pressure_hpa=pressure_hpa,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
        co2_group=group,
    )


def _make_runner(
    tmp_path: Path,
    point: CalibrationPoint,
    *,
    phase: str = "co2",
    gauge: _FakePressureGauge | None = None,
    pace: _FakePace | None = None,
    mechanical_pressure_protection_confirmed: bool = True,
) -> tuple[RunLogger, CalibrationRunner]:
    logger = RunLogger(tmp_path)
    cfg = _workflow_cfg()
    cfg["workflow"] = {
        "pressure": {
            "mechanical_pressure_protection_confirmed": mechanical_pressure_protection_confirmed,
        }
    }
    runner = CalibrationRunner(
        cfg,
        {
            "pressure_gauge": gauge or _FakePressureGauge(pressure_hpa=float(point.target_pressure_hpa or 1000.0)),
            "pace": pace or _FakePace(),
        },
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    key = runner._source_stage_key_for_point(point, phase=phase)
    runner._source_stage_safety[key] = True
    runner._route_final_stage_atmosphere_safety[key] = True
    runner._sync_source_stage_runtime_fields(point, phase=phase)
    return logger, runner


def _verify(
    runner: CalibrationRunner,
    point: CalibrationPoint,
    *,
    phase: str = "co2",
    evidence_source: str = "live_safe_sealed_pressure",
    verification_inputs: dict | None = None,
) -> dict:
    merged_inputs = {
        "pressure_gauge_available": True,
        "pressure_read_fresh": True,
        "in_limits_cache_fresh": True,
        "final_syst_err": '0,"No error"',
        "target_pressure_supported": True,
    }
    if verification_inputs:
        merged_inputs.update(verification_inputs)
    return runner.verify_seal_pressure_stage_preconditions(
        point,
        phase=phase,
        evidence_source=evidence_source,
        verification_inputs=merged_inputs,
    )


def test_seal_pressure_stage_not_verified_blocks_real_transition(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(_workflow_cfg(), {}, logger, lambda *_: None, lambda *_: None)
    point = _co2_point()
    runner._source_stage_safety["co2_a"] = True
    runner._route_final_stage_atmosphere_safety["co2_a"] = True
    runner._sync_co2_source_stage_runtime_fields(point, phase="co2")
    calls: list[str] = []
    runner._pressurize_and_hold = lambda *_args, **_kwargs: calls.append("pressurize") or True  # type: ignore[method-assign]

    assert runner._pressurize_route_for_sealed_points(point, route="co2", sealed_control_refs=[point]) is False
    state = runner._point_runtime_state(point, phase="co2") or {}
    logger.close()

    assert state["abort_reason"] == "SealPressureStageNotVerified"
    assert state["seal_pressure_blocked_operation"] == "seal_route_for_pressure_control"
    assert calls == []


def test_atmosphere_flow_safe_does_not_set_seal_pressure_safe(tmp_path: Path) -> None:
    point = _co2_point()
    logger, runner = _make_runner(tmp_path, point)

    state = runner._point_runtime_state(point, phase="co2") or {}
    verification = _verify(runner, point, phase="co2", evidence_source="live_safe")
    logger.close()

    assert state["atmosphere_flow_safe"] is True
    assert state["seal_pressure_safe"] is False
    assert verification["route_final_stage_atmosphere_safe"] is True
    assert verification["route_final_stage_seal_safe"] is False


def test_seal_pressure_gate_requires_explicit_verification(tmp_path: Path) -> None:
    point = _co2_point()
    logger, runner = _make_runner(tmp_path, point)

    verification = _verify(runner, point, phase="co2")
    calls: list[str] = []
    runner._pressurize_and_hold = lambda *_args, **_kwargs: calls.append("pressurize") or True  # type: ignore[method-assign]

    assert verification["eligible"] is True
    assert runner._route_final_stage_seal_safety["co2_a"] is False
    assert runner._pressurize_route_for_sealed_points(point, route="co2", sealed_control_refs=[point]) is False
    logger.close()

    assert calls == []


def test_seal_pressure_verifier_rejects_active_atmosphere_keepalive(tmp_path: Path) -> None:
    point = _co2_point()
    logger, runner = _make_runner(tmp_path, point)
    runner._continuous_atmosphere_state.update(
        {"active": True, "route_flow_active": True, "route_key": "co2_a", "keepalive_count": 2}
    )

    verification = _verify(runner, point, phase="co2")
    logger.close()

    assert verification["eligible"] is False
    assert "ActiveAtmosphereKeepalive" in verification["reasons"]


def test_seal_pressure_verifier_rejects_post_exit_vent1(tmp_path: Path) -> None:
    point = _co2_point()
    logger, runner = _make_runner(tmp_path, point)

    verification = _verify(runner, point, phase="co2", verification_inputs={"post_exit_vent1_count": 1})
    logger.close()

    assert verification["eligible"] is False
    assert "PostExitVentLeak" in verification["reasons"]


def test_seal_pressure_verifier_allows_single_exit_boundary_vent0_only(tmp_path: Path) -> None:
    point = _co2_point()
    logger, runner = _make_runner(tmp_path, point)

    verification = _verify(runner, point, phase="co2", verification_inputs={"exit_boundary_vent0_count": 1})
    logger.close()

    assert verification["eligible"] is True
    assert verification["exit_boundary_vent0_count"] == 1
    assert verification["route_final_stage_seal_safe"] is False
    assert runner._route_final_stage_seal_safety["co2_a"] is False


def test_seal_pressure_verifier_rejects_vent2_tx(tmp_path: Path) -> None:
    point = _co2_point()
    logger, runner = _make_runner(tmp_path, point)

    verification = _verify(runner, point, phase="co2", verification_inputs={"vent2_tx_count": 1})
    logger.close()

    assert verification["eligible"] is False
    assert "Vent2CommandObserved" in verification["reasons"]


def test_seal_pressure_verifier_rejects_stale_in_limits_cache(tmp_path: Path) -> None:
    point = _co2_point()
    logger, runner = _make_runner(tmp_path, point)

    verification = _verify(runner, point, phase="co2", verification_inputs={"in_limits_cache_fresh": False})
    logger.close()

    assert verification["eligible"] is False
    assert verification["in_limits_cache_fresh"] is False
    assert "StaleInLimitsCache" in verification["reasons"]


def test_seal_pressure_verifier_requires_pressure_gauge(tmp_path: Path) -> None:
    point = _co2_point()
    logger, runner = _make_runner(tmp_path, point)

    verification = _verify(
        runner,
        point,
        phase="co2",
        verification_inputs={"pressure_gauge_available": False, "pressure_read_fresh": False},
    )
    logger.close()

    assert verification["eligible"] is False
    assert verification["pressure_gauge_available"] is False
    assert "PressureGaugeUnavailable" in verification["reasons"]


def test_seal_pressure_verifier_requires_analyzer_or_mechanical_protection(tmp_path: Path) -> None:
    point = _co2_point()
    logger, runner = _make_runner(
        tmp_path,
        point,
        mechanical_pressure_protection_confirmed=False,
    )

    required_verification = _verify(runner, point, phase="co2")
    mechanical_missing_verification = _verify(
        runner,
        point,
        phase="co2",
        verification_inputs={
            "analyzer_pressure_required": False,
            "analyzer_pressure_protection_active": False,
            "mechanical_pressure_protection_confirmed": False,
        },
    )
    logger.close()

    assert required_verification["eligible"] is False
    assert "AnalyzerPressureRequiredButUnavailable" in required_verification["reasons"]
    assert mechanical_missing_verification["eligible"] is False
    assert "MechanicalPressureProtectionNotConfirmed" in mechanical_missing_verification["reasons"]


def test_seal_pressure_verifier_rejects_unsupported_500_hpa_target(tmp_path: Path) -> None:
    point = _co2_point(pressure_hpa=500.0)
    logger, runner = _make_runner(tmp_path, point)

    verification = _verify(runner, point, phase="co2", verification_inputs={"target_pressure_supported": False})
    logger.close()

    assert verification["eligible"] is False
    assert verification["target_pressure_supported"] is False
    assert "PressureTargetUnsupportedByHardware" in verification["reasons"]


def test_seal_pressure_verifier_marks_simulated_evidence_not_acceptance(tmp_path: Path) -> None:
    point = _co2_point()
    logger, runner = _make_runner(tmp_path, point)

    verification = _verify(runner, point, phase="co2", evidence_source="simulated")
    logger.close()

    assert verification["eligible"] is False
    assert verification["not_real_acceptance_evidence"] is True
    assert "SimulatedEvidenceNotAcceptance" in verification["reasons"]


def test_seal_pressure_verifier_keeps_4_24_10_blocked_for_no_source(tmp_path: Path) -> None:
    point = _co2_point()
    logger, runner = _make_runner(tmp_path, point)
    runner._current_open_valves = (8, 11, 7)

    verification = _verify(runner, point, phase="co2", evidence_source="live_safe")
    logger.close()

    assert verification["source_final_valves_open"] == []
    assert verification["blocked_valves"] == [4, 10, 24]
    assert "SourceFinalStageNotAllowed" not in verification["reasons"]


def test_seal_pressure_verifier_is_side_effect_free(tmp_path: Path) -> None:
    point = _co2_point()
    gauge = _FakePressureGauge()
    pace = _FakePace()
    logger, runner = _make_runner(tmp_path, point, gauge=gauge, pace=pace)
    key = runner._source_stage_key_for_point(point, phase="co2")
    runner._continuous_atmosphere_state.update(
        {"active": False, "route_flow_active": False, "route_key": "co2_a", "phase_name": "Idle"}
    )
    before_source_stage = dict(runner._source_stage_safety)
    before_atmosphere_stage = dict(runner._route_final_stage_atmosphere_safety)
    before_seal_stage = dict(runner._route_final_stage_seal_safety)
    before_open_valves = tuple(runner._current_open_valves)
    before_phase = str(runner._continuous_atmosphere_state.get("phase_name") or "")
    before_state = dict(runner._point_runtime_state(point, phase="co2") or {})

    verification = _verify(runner, point, phase="co2")
    after_state = dict(runner._point_runtime_state(point, phase="co2") or {})
    logger.close()

    assert verification["eligible"] is True
    assert gauge.calls == []
    assert pace.calls == []
    assert runner._source_stage_safety == before_source_stage
    assert runner._route_final_stage_atmosphere_safety == before_atmosphere_stage
    assert runner._route_final_stage_seal_safety == before_seal_stage
    assert runner._route_final_stage_seal_safety[key] is False
    assert tuple(runner._current_open_valves) == before_open_valves
    assert str(runner._continuous_atmosphere_state.get("phase_name") or "") == before_phase
    assert before_state == after_state
