from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from a4_simulation_adapter import A4SimulationAdapter, A4_PROFILE_PATH, A4_POINTS_PATH


def _adapter():
    return A4SimulationAdapter()


def _summary():
    return _adapter().run_summary()


# ═══════════════════════════════════════════════════════════
# A. Profile and points loading
# ═══════════════════════════════════════════════════════════


def test_adapter_loads_a4_profile_and_points():
    adapter = _adapter()
    profile = adapter.load_profile()
    points = adapter.load_points()
    assert profile["features"]["simulation_mode"] is True
    assert profile["workflow"]["collect_only"] is True
    assert profile["workflow"]["no_write_guard_active"] is True
    assert isinstance(points, list)
    assert len(points) == 15


def test_adapter_profile_path_exists():
    assert A4_PROFILE_PATH.exists(), f"profile missing: {A4_PROFILE_PATH}"


def test_adapter_points_path_exists():
    assert A4_POINTS_PATH.exists(), f"points missing: {A4_POINTS_PATH}"


# ═══════════════════════════════════════════════════════════
# B. 15-point matrix
# ═══════════════════════════════════════════════════════════


def test_adapter_builds_expected_15_point_matrix():
    plan = _adapter().build_plan()
    assert plan["h2o_points_total"] == 8
    assert plan["co2_points_total"] == 7
    assert plan["total_sample_targets"] == 15


def test_adapter_all_points_temperature_is_20c():
    adapter = _adapter()
    points = adapter._parsed()
    for p in points:
        assert p.temperature_c == 20.0, f"point {p.index} temp={p.temperature_c}"


def test_adapter_h2o_ambient_plus_7_sealed():
    plan = _adapter().build_plan()
    assert plan["h2o_ambient_open_count"] == 1
    assert plan["h2o_sealed_pressure_count"] == 7
    assert plan["h2o_sealed_pressures"] == [500.0, 600.0, 700.0, 800.0, 900.0, 1000.0, 1100.0]


def test_adapter_co2_7_sealed_no_ambient():
    plan = _adapter().build_plan()
    assert plan["co2_ambient_open_count"] == 0
    assert plan["co2_sealed_pressure_count"] == 7
    assert plan["co2_sealed_pressures"] == [500.0, 600.0, 700.0, 800.0, 900.0, 1000.0, 1100.0]


# ═══════════════════════════════════════════════════════════
# C. Transition sequence
# ═══════════════════════════════════════════════════════════


def test_adapter_transition_sequence_is_h2o_then_co2():
    summary = _summary()
    seq = summary.transition_sequence
    h2o_cleanup_idx = seq.index("h2o_cleanup")
    co2_baseline_idx = seq.index("co2_route_baseline")
    co2_preseal_idx = seq.index("co2_preseal")
    co2_sweep_idx = seq.index("co2_sealed_pressure_sweep")
    safe_stop_idx = seq.index("safe_stop")

    assert h2o_cleanup_idx < co2_baseline_idx, "h2o_cleanup must precede co2_route_baseline"
    assert co2_preseal_idx < co2_sweep_idx, "co2_preseal must precede co2_sealed_pressure_sweep"
    assert safe_stop_idx == len(seq) - 1, "safe_stop must be last"


def test_adapter_route_sequence_is_h2o_then_co2():
    summary = _summary()
    assert summary.route_sequence == ["h2o", "co2"]


# ═══════════════════════════════════════════════════════════
# D. No-write evidence
# ═══════════════════════════════════════════════════════════


def test_adapter_no_write_evidence():
    summary = _summary()
    assert summary.attempted_write_count == 0
    assert summary.identity_write_command_sent is False
    assert summary.calibration_write_command_sent is False


def test_adapter_no_write_flag():
    summary = _summary()
    assert summary.no_write is True


# ═══════════════════════════════════════════════════════════
# E. No real COM / execute_probe
# ═══════════════════════════════════════════════════════════


def test_adapter_has_no_real_com_or_execute_probe():
    summary = _summary()
    assert summary.real_com is False
    profile_text = A4_PROFILE_PATH.read_text(encoding="utf-8")
    assert "execute_probe" not in profile_text


def test_adapter_does_not_import_serial():
    import a4_simulation_adapter as mod
    import sys
    for name in dir(mod):
        obj = getattr(mod, name)
        if callable(obj):
            src = getattr(obj, "__module__", "")
            assert "serial" not in src.lower(), f"adapter imports serial via {name}"


# ═══════════════════════════════════════════════════════════
# F. CO2 ambient deferred
# ═══════════════════════════════════════════════════════════


def test_adapter_preserves_co2_ambient_deferred():
    summary = _summary()
    assert summary.co2_ambient_open_count == 0
    assert "co2_ambient_open" in summary.deferred


# ═══════════════════════════════════════════════════════════
# G. Pressure semantics
# ═══════════════════════════════════════════════════════════


def test_adapter_reports_pressure_semantics():
    plan = _adapter().build_plan()
    h2o_sealed = plan["h2o_sealed_pressures"]
    co2_sealed = plan["co2_sealed_pressures"]
    assert 1000.0 in h2o_sealed, "1000hPa must be in H2O sealed pressures"
    assert 1000.0 in co2_sealed, "1000hPa must be in CO2 sealed pressures"
    assert plan["h2o_ambient_open_count"] == 1, "H2O ambient_open != 1000hPa sealed"


# ═══════════════════════════════════════════════════════════
# H. Not production acceptance
# ═══════════════════════════════════════════════════════════


def test_adapter_summary_is_not_production_acceptance():
    summary = _summary()
    assert summary.production_acceptance is False
    assert summary.controlled_write is False
    assert summary.formal_switch is False


def test_adapter_simulation_only_flag():
    summary = _summary()
    assert summary.simulation_only is True
