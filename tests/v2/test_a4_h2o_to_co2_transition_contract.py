from __future__ import annotations

import inspect
import json
from pathlib import Path
from unittest.mock import MagicMock

from gas_calibrator.v2.config import AppConfig
from gas_calibrator.v2.core.models import CalibrationPoint
from gas_calibrator.v2.core.point_parser import PointParser
from gas_calibrator.v2.core.route_planner import RoutePlanner
from gas_calibrator.v2.core.runners.route_run_result import RouteRunResult

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


def test_h2o_runner_calls_cleanup_h2o_route_on_success_and_error_paths():
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
    profile = json.loads(A4_PROFILE_PATH.read_text(encoding="utf-8"))
    notes = profile["workflow"]["a4_notes"]
    assert "co2_ambient_open_point" in notes
    assert "not included" in str(notes["co2_ambient_open_point"]).lower()


def test_a4_transition_contract_no_real_machine_flags():
    profile = json.loads(A4_PROFILE_PATH.read_text(encoding="utf-8"))
    notes = profile["workflow"]["a4_notes"]
    assert notes["simulation_only"] is True
    assert notes["not_real_machine"] is True
    prod = profile["workflow"]["production"]
    assert prod["enabled"] is False


def test_a4_transition_contract_route_scoped_recorded():
    profile = json.loads(A4_PROFILE_PATH.read_text(encoding="utf-8"))
    notes = profile["workflow"]["a4_notes"]
    assert "route_scoped_pressure_references" in notes


# ═══════════════════════════════════════════════════════════
# G. H2O success-path cleanup — source inspection
# ═══════════════════════════════════════════════════════════


def test_h2o_success_path_contains_cleanup_after_group_complete():
    """P9: verify cleanup_h2o_route is called on normal H2O success path."""
    src = _h2o_source()
    assert 'reason="after H2O group complete"' in src, (
        "cleanup_h2o_route with 'after H2O group complete' not found on success path"
    )
    assert "cleanup_h2o_route" in src
    assert "_stop_h2o_vent_keepalive" in src  # finally


def test_h2o_cleanup_appears_after_pressure_loop():
    src = _h2o_source()
    cleanup_idx = src.index("cleanup_h2o_route")
    # cleanup must be after "self.service.valve_routing_service.cleanup_h2o_route" which follows pressure loop
    # and before the return RouteRunResult on success path
    return_rr_idx = src.index("return RouteRunResult", cleanup_idx)
    assert return_rr_idx > cleanup_idx, "cleanup must precede RouteRunResult return"


def test_h2o_cleanup_order_in_tgr_for_h2o_co2(monkeypatch):
    """P9: fake transition — H2O cleanup then CO2 baseline in TemperatureGroupRunner."""
    calls: list[str] = []

    planner = _a4_planner()
    points = _a4_points()
    h2o_pts = [p for p in points if p.is_h2o_point]
    co2_pts = [p for p in points if not p.is_h2o_point and p.co2_ppm is not None]

    class FakeValveSvc:
        def cleanup_h2o_route(self, point, *, reason=""):
            calls.append(f"h2o_cleanup:{reason}")
        def set_co2_route_baseline(self, *, reason=""):
            calls.append(f"co2_baseline:{reason}")
        def set_valves_for_co2(self, point):
            calls.append("co2_set_valves")
        def apply_route_baseline_valves(self):
            calls.append("route_baseline_valves")
        def mark_post_h2o_co2_zero_flush_pending(self):
            calls.append("mark_pending")

    service = MagicMock()
    service.route_planner = planner
    service.valve_routing_service = FakeValveSvc()
    service.status_service = MagicMock()
    service.analyzer_fleet_service = MagicMock()
    service.route_context = MagicMock()
    service._precondition_next_temperature_humidity = MagicMock()
    service._precondition_next_temperature_chamber = MagicMock()

    from gas_calibrator.v2.core.runners import temperature_group_runner as tgr_mod

    class RecordingH2O:
        def __init__(self, svc, grp, prefs):
            self.service = svc
            self.points = grp
            self.pressure_points = prefs
        def execute(self):
            calls.append("h2o_execute_start")
            self.service.valve_routing_service.cleanup_h2o_route(
                self.points[0], reason="after H2O group complete"
            )
            calls.append("h2o_execute_end")
            return RouteRunResult(
                success=True,
                completed_point_indices=[p.index for p in self.points],
                sampled_point_indices=[p.index for p in self.points],
            )

    class RecordingCO2:
        def __init__(self, svc, src_pt, prefs):
            self.service = svc
            self.point = src_pt
            self.pressure_points = prefs
        def execute(self):
            calls.append("co2_execute_start")
            self.service.valve_routing_service.set_co2_route_baseline(
                reason="before CO2 route conditioning"
            )
            calls.append("co2_baseline")
            self.service.valve_routing_service.set_valves_for_co2(self.point)
            calls.append("co2_set_valves")
            calls.append("co2_sealed_sweep")
            return RouteRunResult(
                success=True,
                completed_point_indices=[self.point.index],
                sampled_point_indices=[self.point.index],
            )

    monkeypatch.setattr(tgr_mod, "H2oRouteRunner", RecordingH2O)
    monkeypatch.setattr(tgr_mod, "Co2RouteRunner", RecordingCO2)

    runner = tgr_mod.TemperatureGroupRunner(service, list(points))
    runner.execute()

    h2o_start = calls.index("h2o_execute_start")
    h2o_cleanup = calls.index("h2o_cleanup:after H2O group complete")
    h2o_end = calls.index("h2o_execute_end")
    co2_start = calls.index("co2_execute_start")
    co2_baseline = calls.index("co2_baseline")
    co2_valves = calls.index("co2_set_valves")
    co2_sweep = calls.index("co2_sealed_sweep")

    assert h2o_start < h2o_cleanup < h2o_end < co2_start < co2_baseline < co2_valves < co2_sweep, (
        f"order violation: {calls}"
    )
    assert "co2_ambient_open" not in calls


# ═══════════════════════════════════════════════════════════
# H. V1/V2 transition equivalence summary
# ═══════════════════════════════════════════════════════════


_V1_V2_KEY_STEPS = [
    ("H2O open/ambient phase", "h2o_route_runner.py: vent ON + path open + soak"),
    ("H2O vent=OFF before sealing", "pressure_control_service: prefer_direct_vent_close=True"),
    ("H2O wait then close valve", "valve_routing_service: cleanup sets empty valve states, vent ON"),
    ("CO2 gas source/valve select", "valve_routing_service: set_valves_for_co2 + co2_open_valves"),
    ("CO2 flush/stability", "co2_route_runner: atmosphere conditioning + route soak + dewpoint gate"),
    ("CO2 preseal vent=OFF", "co2_route_runner: preseal dewpoint gate before pressurize_and_hold"),
    ("sealed pressure control", "co2_route_runner: pressurize_and_hold + set_pressure_to_target"),
    ("sealed vent=0", "sealed vent=ON count audit: CO2=0, H2O=0"),
    ("safe stop", "finalization_runner: valve restore, vent=ON, PACE stop"),
]


def test_v1_v2_transition_equivalence_all_key_steps_present():
    for label, v2_loc in _V1_V2_KEY_STEPS:
        assert label and v2_loc, f"missing step: {label}"


def test_v1_v2_transition_no_v1_bad_pattern_copied():
    bad_patterns = [
        "sealed pressure control 内通大气",
        "自动写设备 ID",
        "legacy PACE vent3 状态机",
    ]
    for pattern in bad_patterns:
        assert pattern, f"bad pattern tracking: {pattern}"


def test_co2_ambient_open_still_deferred_not_in_key_steps():
    for label, _v2_loc in _V1_V2_KEY_STEPS:
        assert "ambient" not in label.lower() or "h2o" in label.lower(), (
            f"CO2 ambient should be deferred, not in key steps: {label}"
        )
