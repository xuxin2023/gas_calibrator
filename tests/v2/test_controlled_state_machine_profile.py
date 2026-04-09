from datetime import datetime, timezone

import pytest

from gas_calibrator.v2.core.controlled_state_machine_profile import (
    ALLOWED_TRANSITIONS,
    CANONICAL_STATES,
    STATE_TRANSITION_EVIDENCE_FILENAME,
    STATE_TRANSITION_EVIDENCE_MARKDOWN_FILENAME,
    build_state_transition_evidence,
    compile_controlled_state_machine_profile,
    validate_transition,
)
from gas_calibrator.v2.core.models import CalibrationPoint, SamplingResult
from gas_calibrator.v2.core.plan_compiler import PlanCompiler
from gas_calibrator.v2.domain.plan_models import (
    CalibrationPlanProfile,
    GasPointSpec,
    HumiditySpec,
    PlanOrderingOptions,
    PressureSpec,
    TemperatureSpec,
)
from gas_calibrator.v2.domain.pressure_selection import AMBIENT_PRESSURE_TOKEN


def _sample(point: CalibrationPoint, *, point_phase: str, sample_index: int = 1) -> SamplingResult:
    return SamplingResult(
        point=point,
        analyzer_id="ga01",
        timestamp=datetime(2026, 4, 8, 11, sample_index, tzinfo=timezone.utc),
        co2_ppm=400.0,
        h2o_mmol=0.72 if point.route == "h2o" else None,
        co2_signal=4500.0 if point.route != "h2o" else None,
        h2o_signal=2500.0 if point.route == "h2o" else None,
        co2_ratio_f=1.000 if point.route != "h2o" else None,
        co2_ratio_raw=1.001 if point.route != "h2o" else None,
        h2o_ratio_f=0.700 if point.route == "h2o" else None,
        h2o_ratio_raw=0.699 if point.route == "h2o" else None,
        ref_signal=3500.0,
        temperature_c=25.0,
        pressure_hpa=1000.0 if not point.is_ambient_pressure_point else None,
        frame_has_data=True,
        frame_usable=True,
        point_phase=point_phase,
        point_tag=point.pressure_display_label or point.route,
        sample_index=sample_index,
        stability_time_s=12.0,
        total_time_s=30.0,
    )


def test_controlled_state_machine_profile_compiles_from_current_plan() -> None:
    compiled = PlanCompiler().compile(
        CalibrationPlanProfile(
            name="controlled_state_machine_profile",
            temperatures=[TemperatureSpec(temperature_c=25.0)],
            humidities=[HumiditySpec(hgen_temp_c=25.0, hgen_rh_pct=50.0)],
            gas_points=[GasPointSpec(co2_ppm=400.0)],
            pressures=[PressureSpec(pressure_hpa=1000.0)],
            ordering=PlanOrderingOptions(selected_pressure_points=[AMBIENT_PRESSURE_TOKEN, 1000.0]),
        )
    )

    profile = compile_controlled_state_machine_profile(compiled)

    assert profile["profile_version"] == "controlled_flex_v1"
    assert profile["enabled_states"][0:3] == ["INIT", "DEVICE_READY", "PLAN_COMPILED"]
    assert "PRESEAL_STABILITY" in profile["enabled_states"]
    assert "PRESSURE_STABLE" in profile["enabled_states"]
    assert "RUN_COMPLETE" in profile["enabled_states"]
    assert "ABORT" in profile["enabled_states"]
    assert set(profile["route_families"]) >= {"water", "gas", "ambient"}
    assert set(profile["allowed_transitions"]) >= set(ALLOWED_TRANSITIONS)
    assert all(state in CANONICAL_STATES for state in profile["enabled_states"])


def test_controlled_state_machine_profile_rejects_illegal_transition() -> None:
    validate_transition("INIT", "DEVICE_READY")
    with pytest.raises(ValueError, match="illegal transition"):
        validate_transition("INIT", "RUN_COMPLETE")


def test_state_transition_evidence_captures_recovery_trace_and_boundaries() -> None:
    gas_point = CalibrationPoint(index=1, temperature_c=25.0, co2_ppm=400.0, pressure_hpa=1000.0, route="co2")
    samples = [
        _sample(gas_point, point_phase="pressure_stable", sample_index=1),
        _sample(gas_point, point_phase="sample_ready", sample_index=2),
    ]
    point_summaries = [
        {
            "point": {"index": 1, "route": "co2", "pressure_mode": "sealed"},
            "stats": {"point_phase": "sample_ready", "failed_checks": ["pressure_leak"], "valid": False},
        }
    ]

    evidence = build_state_transition_evidence(
        run_id="run_trace",
        samples=samples,
        point_summaries=point_summaries,
        route_trace_events=[
            {
                "point_index": 1,
                "route": "co2",
                "phase": "pressure_stable",
                "event": "retry",
                "point_tag": "sealed_gas",
            },
            {
                "point_index": 1,
                "route": "co2",
                "phase": "sample_ready",
                "event": "recovered",
                "point_tag": "sealed_gas",
            },
        ],
        artifact_paths={
            "state_transition_evidence": STATE_TRANSITION_EVIDENCE_FILENAME,
            "state_transition_evidence_markdown": STATE_TRANSITION_EVIDENCE_MARKDOWN_FILENAME,
        },
    )
    raw = evidence["raw"]
    states = [row["to_state"] for row in raw["state_transition_logs"]]

    assert raw["artifact_type"] == "state_transition_evidence"
    assert raw["artifact_paths"]["state_transition_evidence"].endswith(STATE_TRANSITION_EVIDENCE_FILENAME)
    assert raw["artifact_paths"]["state_transition_evidence_markdown"].endswith(
        STATE_TRANSITION_EVIDENCE_MARKDOWN_FILENAME
    )
    assert "FAULT_CAPTURE" in states
    assert "SAFE_RECOVERY" in states
    assert any(
        str(row.get("decision_result") or "") == "fault_capture_recovery"
        for row in raw["phase_decision_logs"]
    )
    assert raw["illegal_transitions"] == []
    assert raw["review_surface"]["anchor_id"] == "state-transition-evidence"
    assert "actual_simulated_run" in raw["review_surface"]["evidence_source_filters"]
    assert "gas" in raw["review_surface"]["route_filters"]
    assert "shadow evaluation only" in raw["boundary_statements"]
    assert "does not modify live sampling gate by default" in raw["boundary_statements"]
    assert "not real acceptance" in evidence["markdown"]
