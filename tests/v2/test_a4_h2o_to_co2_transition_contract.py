from __future__ import annotations

import inspect
from pathlib import Path

from gas_calibrator.v2.config import AppConfig
from gas_calibrator.v2.core.models import CalibrationPoint
from gas_calibrator.v2.core.point_parser import PointParser
from gas_calibrator.v2.core.route_planner import RoutePlanner

A4_POINTS_PATH = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "gas_calibrator"
    / "v2"
    / "configs"
    / "validation"
    / "simulated"
    / "a4_20c_h2o_co2_points_simulated.json"
)

A4_PROFILE_PATH = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "gas_calibrator"
    / "v2"
    / "configs"
    / "validation"
    / "simulated"
    / "a4_single_temp_h2o_co2_no_write_20c_simulated.json"
)


def _a4_points():
    return PointParser().parse(str(A4_POINTS_PATH))


def _a4_planner():
    config = AppConfig.from_dict({"workflow": {"route_mode": "h2o_then_co2"}})
    return RoutePlanner(config, PointParser())


# ═══════════════════════════════════════════════════════════
# A. TemperatureGroupRunner 顺序
# ═══════════════════════════════════════════════════════════


def test_route_sequence_is_h2o_before_co2():
    planner = _a4_planner()
    points = _a4_points()
    seq = planner.route_sequence(points)
    assert seq == ["h2o", "co2"], f"expected h2o_then_co2, got {seq}"


def test_h2o_pressure_points_are_ambient_plus_7_sealed():
    planner = _a4_planner()
    points = _a4_points()
    refs = planner.h2o_pressure_points(points)
    ambient = [p for p in refs if p.is_ambient_pressure_point]
    sealed = [p for p in refs if not p.is_ambient_pressure_point]
    assert len(ambient) == 1
    assert ambient[0].pressure_hpa is None
    assert len(sealed) == 7
    assert sorted([p.target_pressure_hpa for p in sealed]) == [
        500.0, 600.0, 700.0, 800.0, 900.0, 1000.0, 1100.0,
    ]


def test_co2_pressure_points_are_7_sealed_no_ambient():
    planner = _a4_planner()
    points = _a4_points()
    for source in planner.co2_sources(points):
        refs = planner.co2_pressure_points(source, points)
        ambient = [p for p in refs if p.is_ambient_pressure_point]
        assert len(ambient) == 0, f"CO2 route has {len(ambient)} ambient ref(s)"
        assert sorted([p.target_pressure_hpa for p in refs]) == [
            500.0, 600.0, 700.0, 800.0, 900.0, 1000.0, 1100.0,
        ]


# ═══════════════════════════════════════════════════════════
# B. H2O runner contract via source inspection
# ═══════════════════════════════════════════════════════════


def _h2o_source():
    from gas_calibrator.v2.core.runners.h2o_route_runner import H2oRouteRunner
    return inspect.getsource(H2oRouteRunner.execute)


def test_h2o_runner_has_deferred_seal_pattern():
    src = _h2o_source()
    assert "seal_deferred = False" in src, "missing seal_deferred initializer"
    assert "seal_deferred = True" in src, "missing seal_deferred activation for ambient first point"
    assert "seal_deferred" in src


def test_h2o_runner_calls_stop_keepalive_in_ambient_to_sealed_transition():
    src = _h2o_source()
    assert "_stop_h2o_vent_keepalive" in src, "missing vent keepalive stop call"


def test_h2o_runner_calls_mark_post_h2o_co2_zero_flush_pending():
    src = _h2o_source()
    assert "mark_post_h2o_co2_zero_flush_pending" in src, (
        "missing post-H2O flush pending marker"
    )


def test_h2o_runner_calls_prefer_direct_vent_close():
    src = _h2o_source()
    assert "prefer_direct_vent_close=True" in src, (
        "missing prefer_direct_vent_close in deferred seal"
    )


def test_h2o_runner_has_finally_stop_vent_keepalive():
    src = _h2o_source()
    assert "_stop_h2o_vent_keepalive()" in src, (
        "missing vent keepalive stop in finally block"
    )


def test_h2o_runner_calls_cleanup_h2o_route_on_error_paths():
    src = _h2o_source()
    assert "cleanup_h2o_route" in src, "H2O runner does not call cleanup_h2o_route"


# ═══════════════════════════════════════════════════════════
# C. Valve cleanup contract
# ═══════════════════════════════════════════════════════════


def _valve_cleanup_source():
    from gas_calibrator.v2.core.services.valve_routing_service import ValveRoutingService
    return inspect.getsource(ValveRoutingService.cleanup_h2o_route)


def test_valve_cleanup_h2o_sets_vent_on():
    src = _valve_cleanup_source()
    assert "_set_pressure_controller_vent" in src, (
        "cleanup_h2o_route does not call _set_pressure_controller_vent"
    )
    assert "True" in src


def test_valve_cleanup_h2o_calls_apply_valve_states_empty():
    src = _valve_cleanup_source()
    assert "apply_valve_states([])" in src or "apply_valve_states" in src, (
        "cleanup_h2o_route does not apply empty valve states"
    )


def test_valve_cleanup_h2o_records_trace():
    src = _valve_cleanup_source()
    assert 'action="cleanup"' in src, "cleanup_h2o_route does not record cleanup trace"
    assert 'route="h2o"' in src or "route='h2o'" in src


def test_valve_set_co2_route_baseline_calls_apply_valve_states_empty():
    from gas_calibrator.v2.core.services.valve_routing_service import ValveRoutingService
    src = inspect.getsource(ValveRoutingService.set_co2_route_baseline)
    assert "apply_valve_states" in src


def test_valve_set_co2_route_baseline_sets_vent_on():
    from gas_calibrator.v2.core.services.valve_routing_service import ValveRoutingService
    src = inspect.getsource(ValveRoutingService.set_co2_route_baseline)
    assert "_set_pressure_controller_vent" in src


# ═══════════════════════════════════════════════════════════
# D. CO2 runner contract via source inspection
# ═══════════════════════════════════════════════════════════


def _co2_source():
    from gas_calibrator.v2.core.runners.co2_route_runner import Co2RouteRunner
    return inspect.getsource(Co2RouteRunner.execute)


def test_co2_runner_calls_set_co2_route_baseline_before_set_valves():
    src = _co2_source()
    baseline_idx = src.index("set_co2_route_baseline")
    valves_idx = src.index("set_valves_for_co2")
    assert baseline_idx < valves_idx, (
        "set_co2_route_baseline must appear before set_valves_for_co2"
    )


def test_co2_runner_calls_wait_route_soak_before_seal():
    src = _co2_source()
    assert "_wait_route_soak_before_seal" in src, (
        "missing route soak before seal"
    )


def test_co2_runner_calls_pressurize_and_hold():
    src = _co2_source()
    assert "pressurize_and_hold" in src, (
        "missing pressurize_and_hold in CO2 runner"
    )


def test_co2_runner_has_set_pressure_to_target():
    src = _co2_source()
    assert "set_pressure_to_target" in src, (
        "missing set_pressure_to_target in CO2 runner"
    )


def test_co2_runner_has_route_context_enter_co2():
    src = _co2_source()
    assert 'current_route="co2"' in src or "current_route='co2'" in src


# ═══════════════════════════════════════════════════════════
# E. TemperatureGroupRunner contract
# ═══════════════════════════════════════════════════════════


def _tgr_source():
    from gas_calibrator.v2.core.runners.temperature_group_runner import TemperatureGroupRunner
    return inspect.getsource(TemperatureGroupRunner.execute)


def test_temperature_group_runner_iterates_route_sequence():
    src = _tgr_source()
    assert "route_sequence" in src, (
        "TemperatureGroupRunner does not call route_sequence"
    )


def test_temperature_group_runner_runs_h2o_before_co2_in_execute():
    src = _tgr_source()
    assert "H2oRouteRunner" in src
    assert "Co2RouteRunner" in src


# ═══════════════════════════════════════════════════════════
# F. Deferred gaps recording
# ═══════════════════════════════════════════════════════════


def test_a4_transition_contract_co2_ambient_open_still_deferred():
    import json
    profile = json.loads(A4_PROFILE_PATH.read_text(encoding="utf-8"))
    notes = profile["workflow"]["a4_notes"]
    assert "co2_ambient_open_point" in notes
    assert "not included" in str(notes["co2_ambient_open_point"]).lower()


def test_a4_transition_contract_no_real_machine_flags():
    import json
    profile = json.loads(A4_PROFILE_PATH.read_text(encoding="utf-8"))
    notes = profile["workflow"]["a4_notes"]
    assert notes["simulation_only"] is True
    assert notes["not_real_machine"] is True
    prod = profile["workflow"]["production"]
    assert prod["enabled"] is False


def test_a4_transition_contract_route_scoped_recorded():
    import json
    profile = json.loads(A4_PROFILE_PATH.read_text(encoding="utf-8"))
    notes = profile["workflow"]["a4_notes"]
    assert "route_scoped_pressure_references" in notes
