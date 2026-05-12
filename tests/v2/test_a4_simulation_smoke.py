from __future__ import annotations

import json
from pathlib import Path

from gas_calibrator.v2.config import AppConfig
from gas_calibrator.v2.core.point_parser import PointParser
from gas_calibrator.v2.core.route_planner import RoutePlanner
from gas_calibrator.v2.core.models import CalibrationPoint

PROFILE_PATH = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "gas_calibrator"
    / "v2"
    / "configs"
    / "validation"
    / "simulated"
    / "a4_single_temp_h2o_co2_no_write_20c_simulated.json"
)

POINTS_PATH = PROFILE_PATH.parent / "a4_20c_h2o_co2_points_simulated.json"


def _profile():
    return json.loads(PROFILE_PATH.read_text(encoding="utf-8"))


def _points_json():
    return json.loads(POINTS_PATH.read_text(encoding="utf-8"))


def _parsed_points():
    return PointParser().parse(str(POINTS_PATH))


def _planner():
    config = AppConfig.from_dict({"workflow": {"route_mode": "h2o_then_co2"}})
    return RoutePlanner(config, PointParser())


# ═══════════════════════════════════════════════════════════
# A. Profile safety markers
# ═══════════════════════════════════════════════════════════


def test_a4_simulation_profile_loads_cleanly():
    profile = _profile()
    assert profile["features"]["simulation_mode"] is True
    assert profile["workflow"]["collect_only"] is True
    assert profile["workflow"]["no_write_guard_active"] is True
    assert profile["workflow"]["production"]["enabled"] is False


def test_a4_profile_has_no_execute_probe():
    profile = _profile()
    payload_str = json.dumps(profile)
    assert "execute_probe" not in payload_str, "execute_probe found in profile"


def test_a4_profile_has_no_real_machine_dry_run():
    profile = _profile()
    payload_str = json.dumps(profile)
    assert "real_machine_dry_run" not in payload_str, "real_machine_dry_run found in profile"


def test_a4_profile_has_no_allow_write_true():
    profile = _profile()
    disallowed = [
        "allow_write_coefficients", "allow_write_zero", "allow_write_span",
        "write_coefficients", "write_zero", "write_span",
    ]
    payload_str = json.dumps(profile).lower()
    for field in disallowed:
        assert f'"{field}": true' not in payload_str, f"{field} is True"


# ═══════════════════════════════════════════════════════════
# B. Points matrix
# ═══════════════════════════════════════════════════════════


def test_a4_simulation_points_expand_expected_matrix():
    points = _parsed_points()
    h2o_points = [p for p in points if p.is_h2o_point]
    co2_points = [p for p in points if not p.is_h2o_point and p.co2_ppm is not None]

    h2o_ambient = [p for p in h2o_points if p.is_ambient_pressure_point]
    h2o_sealed = [p for p in h2o_points if not p.is_ambient_pressure_point]

    assert len(h2o_ambient) == 1, f"H2O ambient: expected 1, got {len(h2o_ambient)}"
    assert len(h2o_sealed) == 7, f"H2O sealed: expected 7, got {len(h2o_sealed)}"
    assert len(co2_points) >= 1, f"CO2 points: expected >= 1, got {len(co2_points)}"

    total = len(points)
    assert total == 15, f"Total points: expected 15, got {total}"


def test_a4_all_points_temperature_is_20c():
    points = _parsed_points()
    for p in points:
        assert p.temperature_c == 20.0, f"point {p.index} temp={p.temperature_c}"


def test_a4_1000hpa_is_sealed_not_ambient():
    points = _parsed_points()
    sealed_1000 = [p for p in points if p.target_pressure_hpa == 1000.0]
    assert len(sealed_1000) >= 1, "no 1000hPa sealed points"
    for p in sealed_1000:
        assert not p.is_ambient_pressure_point, f"point {p.index} 1000hPa incorrectly marked ambient"


# ═══════════════════════════════════════════════════════════
# C. Route planner expansion
# ═══════════════════════════════════════════════════════════


def test_a4_route_planner_expands_h2o_then_co2():
    planner = _planner()
    points = _parsed_points()
    seq = planner.route_sequence(points)
    assert seq == ["h2o", "co2"], f"route_sequence={seq}"


def test_a4_h2o_pressure_refs_are_ambient_plus_7_sealed():
    planner = _planner()
    points = _parsed_points()
    refs = planner.h2o_pressure_points(points)
    ambient = [p for p in refs if p.is_ambient_pressure_point]
    sealed = [p for p in refs if not p.is_ambient_pressure_point]
    assert len(ambient) == 1
    assert len(sealed) == 7


def test_a4_co2_pressure_refs_are_7_sealed_no_ambient():
    planner = _planner()
    points = _parsed_points()
    for source in planner.co2_sources(points):
        refs = planner.co2_pressure_points(source, points)
        ambient = [p for p in refs if p.is_ambient_pressure_point]
        assert len(ambient) == 0, f"CO2 has {len(ambient)} ambient refs"
        assert len(refs) == 7


# ═══════════════════════════════════════════════════════════
# D. Transition contract (covered in test_a4_h2o_to_co2_transition_contract.py)
# ═══════════════════════════════════════════════════════════


def test_a4_transition_contract_file_exists():
    contract_path = Path(__file__).resolve().parent / "test_a4_h2o_to_co2_transition_contract.py"
    assert contract_path.exists(), f"transition contract test missing: {contract_path}"


# ═══════════════════════════════════════════════════════════
# E. Expected artifact counts
# ═══════════════════════════════════════════════════════════


def test_a4_smoke_artifact_expected_counts():
    points = _parsed_points()
    h2o_points = [p for p in points if p.is_h2o_point]
    co2_points = [p for p in points if not p.is_h2o_point and p.co2_ppm is not None]

    assert len(h2o_points) == 8, f"H2O points: expected 8, got {len(h2o_points)}"
    assert len(co2_points) == 7, f"CO2 points: expected 7, got {len(co2_points)}"
    assert len(points) == 15, f"Total points: expected 15, got {len(points)}"


def test_a4_smoke_no_write_real_com_flags():
    profile = _profile()
    notes = profile["workflow"]["a4_notes"]
    assert notes["no_write"] is True
    assert notes["simulation_only"] is True
    assert notes["not_real_machine"] is True


# ═══════════════════════════════════════════════════════════
# F. No real COM / serial (static check)
# ═══════════════════════════════════════════════════════════


def test_a4_smoke_profile_declares_no_real_com():
    profile = _profile()
    notes = profile["workflow"]["a4_notes"]
    assert notes["simulation_only"] is True
    assert notes["not_real_machine"] is True


def test_a4_smoke_profile_has_no_execute_probe():
    profile_str = PROFILE_PATH.read_text(encoding="utf-8")
    assert "execute_probe" not in profile_str, "execute_probe found in profile"


# ═══════════════════════════════════════════════════════════
# G. CO2 ambient deferred
# ═══════════════════════════════════════════════════════════


def test_a4_co2_ambient_open_still_deferred():
    profile = _profile()
    notes = profile["workflow"]["a4_notes"]
    assert "co2_ambient_open_point" in notes
    assert "not included" in str(notes["co2_ambient_open_point"]).lower()


def test_a4_co2_pressure_refs_have_no_ambient():
    planner = _planner()
    points = _parsed_points()
    for source in planner.co2_sources(points):
        refs = planner.co2_pressure_points(source, points)
        for r in refs:
            assert not r.is_ambient_pressure_point, f"CO2 ref {r.index} is ambient"
