from datetime import datetime, timedelta, timezone

from gas_calibrator.v2.core.controlled_state_machine_profile import build_state_transition_evidence
from gas_calibrator.v2.core.models import CalibrationPoint, SamplingResult
from gas_calibrator.v2.core.multi_source_stability import (
    MULTI_SOURCE_STABILITY_EVIDENCE_FILENAME,
    MULTI_SOURCE_STABILITY_EVIDENCE_MARKDOWN_FILENAME,
    SIMULATION_EVIDENCE_SIDECAR_BUNDLE_FILENAME,
    build_multi_source_stability_evidence,
    build_simulation_evidence_sidecar_bundle,
)


def _point(
    index: int,
    *,
    route: str,
    pressure_hpa: float | None = 1000.0,
    pressure_mode: str = "",
) -> CalibrationPoint:
    return CalibrationPoint(
        index=index,
        temperature_c=25.0,
        co2_ppm=400.0 if route != "h2o" else None,
        humidity_pct=55.0 if route == "h2o" else None,
        pressure_hpa=pressure_hpa,
        route=route,
        pressure_mode=pressure_mode,
        pressure_target_label="ambient" if pressure_mode == "ambient_open" else None,
    )


def _sample(
    point: CalibrationPoint,
    *,
    seconds: int,
    analyzer_id: str = "ga01",
    point_phase: str = "sample_ready",
    **kwargs: float | str | bool | None,
) -> SamplingResult:
    base_payload = {
        "co2_ppm": 400.0,
        "h2o_mmol": 0.72,
        "co2_signal": 4500.0,
        "h2o_signal": 2500.0,
        "co2_ratio_f": 1.000,
        "co2_ratio_raw": 1.001,
        "h2o_ratio_f": 0.700,
        "h2o_ratio_raw": 0.699,
        "ref_signal": 3500.0,
        "temperature_c": 25.0,
        "pressure_hpa": 1000.0,
        "pressure_gauge_hpa": 999.8,
        "thermometer_temp_c": 24.9,
        "dew_point_c": 5.1,
        "analyzer_pressure_kpa": 100.0,
        "analyzer_chamber_temp_c": 25.2,
        "case_temp_c": 26.0,
        "frame_has_data": True,
        "frame_usable": True,
        "frame_status": "",
        "point_phase": point_phase,
        "point_tag": point.pressure_target_label or point.route,
        "sample_index": max(1, seconds // 5 + 1),
        "stability_time_s": float(max(seconds, 1)),
        "total_time_s": 30.0,
    }
    base_payload.update(kwargs)
    return SamplingResult(
        point=point,
        analyzer_id=analyzer_id,
        timestamp=datetime(2026, 4, 8, 10, 0, tzinfo=timezone.utc) + timedelta(seconds=seconds),
        **base_payload,
    )


def test_multi_source_stability_reports_signal_groups_and_partial_coverage() -> None:
    point = _point(1, route="h2o")
    samples = [
        _sample(
            point,
            seconds=0,
            point_phase="preseal",
            co2_ppm=None,
            co2_ratio_f=None,
            co2_ratio_raw=None,
            co2_signal=None,
            h2o_mmol=0.71,
            h2o_ratio_f=0.701,
            h2o_ratio_raw=0.700,
            h2o_signal=None,
            ref_signal=3480.0,
            dew_point_c=6.0,
        ),
        _sample(
            point,
            seconds=6,
            point_phase="preseal",
            co2_ppm=None,
            co2_ratio_f=None,
            co2_ratio_raw=None,
            co2_signal=None,
            h2o_mmol=0.715,
            h2o_ratio_f=0.702,
            h2o_ratio_raw=0.701,
            h2o_signal=None,
            ref_signal=3481.0,
            dew_point_c=6.0,
        ),
    ]

    evidence = build_multi_source_stability_evidence(
        run_id="run_partial",
        samples=samples,
        point_summaries=[
            {
                "point": {"index": 1, "route": "h2o", "pressure_mode": "sealed"},
                "stats": {"point_phase": "preseal", "stability_time_s": 6.0},
            }
        ],
        artifact_paths={
            "multi_source_stability_evidence": MULTI_SOURCE_STABILITY_EVIDENCE_FILENAME,
            "multi_source_stability_evidence_markdown": MULTI_SOURCE_STABILITY_EVIDENCE_MARKDOWN_FILENAME,
        },
    )

    raw = evidence["raw"]

    assert set(raw["signal_group_coverage"]) == {"reference", "analyzer_raw", "output", "data_quality"}
    assert raw["signal_group_coverage"]["reference"]["coverage_status"] == "complete"
    assert raw["signal_group_coverage"]["analyzer_raw"]["coverage_status"] == "partial"
    assert "h2o_ratio_raw" in raw["available_channels_by_group"]["analyzer_raw"]
    assert "h2o_signal" in raw["missing_channels_by_group"]["analyzer_raw"]
    assert "h2o_signal" not in raw["available_channels_by_group"]["analyzer_raw"]
    assert raw["stability_decisions"][0]["partial_coverage"] is True
    assert raw["stability_decisions"][0]["decision_result"] == "partial_coverage_gap"
    assert raw["review_surface"]["anchor_id"] == "multi-source-stability-evidence"
    assert "shadow evaluation only" in raw["boundary_statements"]
    assert "does not modify live sampling gate by default" in raw["boundary_statements"]
    assert "shadow evaluation only" in evidence["markdown"]


def test_multi_source_stability_applies_route_specific_shadow_policies() -> None:
    gas_point = _point(1, route="co2")
    water_point = _point(2, route="h2o")
    ambient_point = _point(3, route="co2", pressure_hpa=None, pressure_mode="ambient_open")
    samples = [
        _sample(gas_point, seconds=0, point_phase="pressure_stable"),
        _sample(gas_point, seconds=9, point_phase="pressure_stable", co2_ratio_raw=1.0005, co2_ppm=401.0),
        _sample(water_point, seconds=0, point_phase="preseal", co2_ppm=None, co2_ratio_f=None, co2_ratio_raw=None, co2_signal=None),
        _sample(water_point, seconds=14, point_phase="preseal", co2_ppm=None, co2_ratio_f=None, co2_ratio_raw=None, co2_signal=None, h2o_ratio_raw=0.7005),
        _sample(ambient_point, seconds=0, point_phase="diagnostic", co2_signal=None, co2_ratio_raw=None, co2_ratio_f=None, ref_signal=3400.0),
        _sample(ambient_point, seconds=8, point_phase="diagnostic", co2_signal=None, co2_ratio_raw=None, co2_ratio_f=None, ref_signal=3402.0, co2_ppm=398.0),
    ]

    evidence = build_multi_source_stability_evidence(
        run_id="run_routes",
        samples=samples,
        point_summaries=[
            {"point": {"index": 1, "route": "co2", "pressure_mode": "sealed"}, "stats": {"point_phase": "pressure_stable", "stability_time_s": 12.0}},
            {"point": {"index": 2, "route": "h2o", "pressure_mode": "sealed"}, "stats": {"point_phase": "preseal", "stability_time_s": 14.0}},
            {"point": {"index": 3, "route": "co2", "pressure_mode": "ambient_open"}, "stats": {"point_phase": "diagnostic", "stability_time_s": 8.0}},
        ],
    )

    decisions_by_route = {
        row["route_family"]: dict(row)
        for row in list(evidence["raw"]["stability_decisions"] or [])
    }

    assert decisions_by_route["gas"]["policy_version"] == "shadow_gas_v1"
    assert decisions_by_route["water"]["policy_version"] == "shadow_water_v1"
    assert decisions_by_route["ambient"]["policy_version"] == "shadow_ambient_v1"
    assert decisions_by_route["gas"]["hard_gate_passed"] is True
    assert decisions_by_route["water"]["hold_time_met"] is True
    assert evidence["raw"]["review_surface"]["route_filters"] == ["gas", "water", "ambient"]
    assert "Step 2 tail / Stage 3 bridge" in evidence["raw"]["digest"]["summary"]
    assert any(
        "shadow evaluation only" in str(line)
        for line in list(evidence["raw"]["review_surface"]["detail_lines"] or [])
    )


def test_simulation_evidence_sidecar_bundle_stays_contract_only() -> None:
    point = _point(1, route="co2")
    samples = [
        _sample(point, seconds=0, point_phase="pressure_stable"),
        _sample(point, seconds=14, point_phase="pressure_stable", co2_ratio_raw=1.0002, co2_ppm=401.0),
    ]
    point_summaries = [
        {"point": {"index": 1, "route": "co2", "pressure_mode": "sealed"}, "stats": {"point_phase": "pressure_stable", "stability_time_s": 14.0}}
    ]
    stability = build_multi_source_stability_evidence(run_id="run_sidecar", samples=samples, point_summaries=point_summaries)
    transition = build_state_transition_evidence(run_id="run_sidecar", samples=samples, point_summaries=point_summaries)

    bundle = build_simulation_evidence_sidecar_bundle(
        run_id="run_sidecar",
        multi_source_stability_evidence=stability,
        state_transition_evidence=transition,
        artifact_paths={"simulation_evidence_sidecar_bundle": SIMULATION_EVIDENCE_SIDECAR_BUNDLE_FILENAME},
    )

    assert bundle["artifact_type"] == "simulation_evidence_sidecar_bundle"
    assert bundle["artifact_paths"]["simulation_evidence_sidecar_bundle"].endswith(
        SIMULATION_EVIDENCE_SIDECAR_BUNDLE_FILENAME
    )
    assert bundle["stores"]["stability_windows"]
    assert bundle["stores"]["state_transition_logs"]
    assert "future database intake / sidecar-ready" in bundle["boundary_statements"]
    assert "not the primary evidence chain" in bundle["boundary_statements"]
    assert "simulation / offline / headless only" in bundle["digest"]["summary"]
