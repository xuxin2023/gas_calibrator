from __future__ import annotations

from gas_calibrator.v2.core.run001_a2_pressure_profile_gate import (
    completed_profile_from_trace,
    normalize_pressure_profile,
    normalize_pressure_profile_numeric_only,
    operator_required_true_acks,
    planned_pressure_profile,
    planned_pressure_point_count,
    resolve_acceptance_mode,
    scope_for_mode,
    validate_completed_profile,
    validate_configured_profile,
    validate_operator_confirmation_profile,
)


def test_normalize_ambient_open():
    result = normalize_pressure_profile(["ambient_open", 800])
    assert result == ["ambient_open", 800.0]


def test_normalize_numeric_only():
    result = normalize_pressure_profile([1100, 1000, 900, 800, 700, 600, 500])
    assert result == [1100.0, 1000.0, 900.0, 800.0, 700.0, 600.0, 500.0]


def test_normalize_mixed_string_numbers():
    result = normalize_pressure_profile(["ambient_open", "800", 700])
    assert result == ["ambient_open", 800.0, 700.0]


def test_normalize_numeric_only_skips_ambient():
    result = normalize_pressure_profile_numeric_only(["ambient_open", 800.0])
    assert result == [800.0]


def test_resolve_engineering_smoke_via_flag():
    cfg = {"run001_a2": {"engineering_smoke_only": True}}
    assert resolve_acceptance_mode(cfg) == "engineering_smoke"


def test_resolve_seven_pressure_with_not_real_flag():
    cfg = {"run001_a2": {"not_real_acceptance_evidence": True, "engineering_smoke_only": False}}
    assert resolve_acceptance_mode(cfg) == "seven_pressure_formal"


def test_resolve_seven_pressure_formal():
    cfg = {"run001_a2": {"engineering_smoke_only": False, "not_real_acceptance_evidence": False}}
    assert resolve_acceptance_mode(cfg) == "seven_pressure_formal"


def test_scope_for_engineering_smoke():
    assert scope_for_mode("engineering_smoke") == "run001_a2_co2_no_write_pressure_profile"


def test_scope_for_seven_pressure():
    assert scope_for_mode("seven_pressure_formal") == "run001_a2_co2_no_write_pressure_sweep"


def test_planned_profile_from_run001_a2():
    cfg = {"run001_a2": {"authorized_pressure_points_hpa": ["ambient_open", 800]}}
    result = planned_pressure_profile(cfg)
    assert result == ["ambient_open", 800.0]


def test_planned_profile_from_probe_section():
    cfg = {
        "run001_a2": {},
        "a2_co2_7_pressure_no_write_probe": {"pressure_points_hpa": [1100, 1000, 800]},
    }
    result = planned_pressure_profile(cfg)
    assert result == [1100.0, 1000.0, 800.0]


def test_validate_configured_engineering_smoke_ambient_800():
    cfg = {
        "run001_a2": {
            "scope": "run001_a2_co2_no_write_pressure_profile",
            "engineering_smoke_only": True,
            "authorized_pressure_points_hpa": ["ambient_open", 800],
        }
    }
    ok, reasons = validate_configured_profile(cfg)
    assert ok
    assert reasons == []


def test_validate_configured_engineering_smoke_no_numeric():
    cfg = {
        "run001_a2": {
            "scope": "run001_a2_co2_no_write_pressure_profile",
            "engineering_smoke_only": True,
            "authorized_pressure_points_hpa": ["ambient_open"],
        }
    }
    ok, reasons = validate_configured_profile(cfg)
    assert not ok
    assert "engineering_smoke_no_numeric_pressure_points" in reasons


def test_validate_configured_seven_pressure_ok():
    cfg = {
        "run001_a2": {
            "scope": "run001_a2_co2_no_write_pressure_sweep",
            "authorized_pressure_points_hpa": [1100, 1000, 900, 800, 700, 600, 500],
        }
    }
    ok, reasons = validate_configured_profile(cfg)
    assert ok
    assert reasons == []


def test_validate_configured_seven_pressure_wrong():
    cfg = {
        "run001_a2": {
            "scope": "run001_a2_co2_no_write_pressure_sweep",
            "authorized_pressure_points_hpa": [800],
        }
    }
    ok, reasons = validate_configured_profile(cfg)
    assert not ok
    assert "a2_authorized_pressure_points_mismatch" in reasons


def test_validate_completed_engineering_smoke_ok():
    ok, reasons = validate_completed_profile(
        ["ambient_open", 800.0], ["ambient_open", 800.0], "engineering_smoke"
    )
    assert ok
    assert reasons == []


def test_validate_completed_engineering_smoke_missing():
    ok, reasons = validate_completed_profile(
        ["ambient_open", 800.0], ["ambient_open"], "engineering_smoke"
    )
    assert not ok
    assert "planned_pressure_points_not_completed" in reasons


def test_validate_completed_seven_pressure_ok():
    ok, reasons = validate_completed_profile(
        [1100, 1000, 900, 800, 700, 600, 500],
        [1100, 1000, 900, 800, 700, 600, 500],
        "seven_pressure_formal",
    )
    assert ok
    assert reasons == []


def test_validate_completed_seven_pressure_missing():
    ok, reasons = validate_completed_profile(
        [1100, 1000, 900, 800, 700, 600, 500],
        [1100],
        "seven_pressure_formal",
    )
    assert not ok
    assert "points_completed_not_7" in reasons


def test_validate_completed_ambient_800_not_pass_seven():
    ok, reasons = validate_completed_profile(
        ["ambient_open", 800.0], ["ambient_open", 800.0], "seven_pressure_formal"
    )
    assert not ok
    assert "planned_pressure_points_not_7_point_formal" in reasons


def test_validate_operator_profile_ambient_800_ok():
    ok, reasons = validate_operator_confirmation_profile(
        ["ambient_open", 800.0], ["ambient_open", 800.0], "engineering_smoke"
    )
    assert ok
    assert reasons == []


def test_validate_operator_profile_ambient_800_mismatch():
    ok, reasons = validate_operator_confirmation_profile(
        ["ambient_open", 800.0], ["ambient_open"], "engineering_smoke"
    )
    assert not ok
    assert "operator_confirmation_pressure_points_mismatch" in reasons


def test_operator_required_acks_engineering_smoke():
    acks = operator_required_true_acks("engineering_smoke")
    assert "pressure_profile_acknowledged" in acks
    assert "only_a2_co2_pressure_profile_no_write" in acks
    assert "seven_pressure_points" not in acks
    assert "only_a2_co2_7_pressure_no_write" not in acks
    assert "no_write" in acks
    assert "co2_only" in acks


def test_operator_required_acks_seven_pressure():
    acks = operator_required_true_acks("seven_pressure_formal")
    assert "seven_pressure_points" in acks
    assert "only_a2_co2_7_pressure_no_write" in acks


def test_planned_pressure_point_count():
    assert planned_pressure_point_count(["ambient_open", 800.0]) == 2
    assert planned_pressure_point_count([1100, 1000, 900, 800, 700, 600, 500]) == 7


def test_completed_profile_from_trace_ambient_plus_800(tmp_path):
    import json
    trace_path = tmp_path / "route_trace.jsonl"
    trace_path.write_text("\n".join([
        json.dumps({"action": "sample_end", "result": "ok", "point_tag": "co2_groupa_1000ppm_ambient", "target": {"pressure_hpa": None}}),
        json.dumps({"action": "sample_end", "result": "ok", "point_tag": "co2_groupa_1000ppm_800hpa", "target": {"pressure_hpa": 800.0}}),
    ]), encoding="utf-8")
    result = completed_profile_from_trace(tmp_path)
    assert result == ["ambient_open", 800.0]


def test_completed_profile_from_trace_seven_pressure(tmp_path):
    import json
    trace_path = tmp_path / "route_trace.jsonl"
    lines = []
    for p in [1100.0, 1000.0, 900.0, 800.0, 700.0, 600.0, 500.0]:
        lines.append(json.dumps({
            "action": "sample_end", "result": "ok",
            "point_tag": f"co2_groupa_1000ppm_{(str(int(p)))}hpa",
            "target": {"pressure_hpa": p},
        }))
    trace_path.write_text("\n".join(lines), encoding="utf-8")
    result = completed_profile_from_trace(tmp_path)
    assert result == [1100.0, 1000.0, 900.0, 800.0, 700.0, 600.0, 500.0]


def test_completed_profile_from_trace_empty(tmp_path):
    trace_path = tmp_path / "route_trace.jsonl"
    trace_path.write_text("", encoding="utf-8")
    result = completed_profile_from_trace(tmp_path)
    assert result == []


def test_completed_profile_from_trace_none():
    assert completed_profile_from_trace(None) == []


def test_completed_profile_from_trace_skips_non_ok(tmp_path):
    import json
    trace_path = tmp_path / "route_trace.jsonl"
    trace_path.write_text(json.dumps({"action": "sample_end", "result": "skip", "point_tag": "co2_groupa_1000ppm_800hpa"}), encoding="utf-8")
    result = completed_profile_from_trace(tmp_path)
    assert result == []
